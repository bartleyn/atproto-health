/**
 * Edge capture for the Jetstream activity collector.
 *
 * The activity collector already subscribes to follow/block/repost/like/post creates but only
 * records a per-(did,date) activity bitmask — the edge payload (who followed/blocked/liked whom)
 * is discarded. This module pulls the directed edges out of those same events and lands them as a
 * COMPACT, append-only Parquet dataset on the external drive, to feed weekly incremental ER on
 * Modal. See the weekly-delta plan: firehose = cheap hot path, snapshot scan = correctness backstop.
 *
 * Why a Parquet *dataset* (dir of part files), not one appended file: a Parquet file is immutable
 * (footer written only on close; a crash mid-write leaves an unreadable file), so you can't append
 * rows to it. Instead each flush writes a new zstd part into edges/dt=<date>/, which readers glob as
 * one table; on date-rollover the day's parts compact into a single dt=<date>.parquet.
 *
 * Edge relations captured:
 *   follow  did -> subject DID          (app.bsky.graph.follow)
 *   block   did -> subject DID          (app.bsky.graph.block)
 *   repost  did -> subject post URI     (app.bsky.feed.repost)
 *   like    did -> subject post URI     (app.bsky.feed.like)      — gated by captureLikes
 *   reply   did -> parent post URI      (app.bsky.feed.post, record.reply.parent.uri)
 *   quote   did -> quoted post URI      (app.bsky.feed.post, record.embed.record.uri)
 *
 * The log is APPEND-ONLY. Deletes (unfollow/unblock/unlike/…) arrive as a commit with no record
 * body, so `dst` is unknown; we append an op:'d' row carrying (src, rel, rkey) NEXT TO the original
 * op:'c' create — we never remove the create. Downstream ER can pair them on (src, rel, rkey) to
 * derive net edges, or use the create/delete churn itself as a signal. This lets the firehose path
 * capture removals the snapshot scan can never see.
 */
import { execFile } from "node:child_process";
import { mkdir, rename, rm, writeFile, readdir } from "node:fs/promises";
import { promisify } from "node:util";
import path from "node:path";

const execFileAsync = promisify(execFile);

export type EdgeOp = "c" | "d";
export interface EdgeRow {
  dt: string;        // UTC date partition, YYYY-MM-DD
  src: string;       // actor DID (record author)
  rel: string;       // follow|block|repost|like|reply|quote|listitem|list|listblock
  dst: string | null; // subject DID or post/list URI; null for deletes
  rkey: string;      // record key (TID)
  t: number;         // firehose event time, microseconds since epoch
  op: EdgeOp;        // c(reate) | d(elete)
  ctx: string | null; // extra context; currently the list URI for listitem edges, else null
}

// collection -> relation for records whose delete we can attribute by (collection, rkey). Posts are
// handled separately because one post can yield reply AND/OR quote edges under the same rkey.
const COLLECTION_REL: Record<string, string> = {
  "app.bsky.graph.follow":    "follow",
  "app.bsky.graph.block":     "block",
  "app.bsky.feed.repost":     "repost",
  "app.bsky.feed.like":       "like",
  "app.bsky.graph.listitem":  "listitem",  // actor adds subject DID to a list (list URI in ctx)
  "app.bsky.graph.listblock": "listblock", // actor subscribes to a moderation/block list (dst=list URI)
  "app.bsky.graph.list":      "list",      // actor creates a list (dst = the list's own URI)
};

const dtOf = (timeUs: number) => new Date(timeUs / 1000).toISOString().slice(0, 10);

/**
 * Minimal shape of a Jetstream commit event we read. Kept loose (unknown record) so this stays a
 * pure transform over already-parsed JSON — the collector passes evt straight through.
 */
export interface CommitEvent {
  did: string;
  time_us: number;
  commit: {
    operation: "create" | "update" | "delete";
    collection: string;
    rkey: string;
    record?: any;
  };
}

/**
 * Extract directed edges from a single commit event. Pure — no I/O — so it unit-tests without the
 * firehose. Returns [] for events that carry no edge (e.g. profile updates, text-less non-edges).
 */
export function edgesFromEvent(evt: CommitEvent, opts: { captureLikes: boolean }): EdgeRow[] {
  const { operation, collection, rkey, record } = evt.commit;
  const dt = dtOf(evt.time_us);
  const base = { dt, src: evt.did, rkey, t: evt.time_us, ctx: null as string | null };

  if (operation === "delete") {
    // Post deletes can drop a reply and/or a quote sharing this rkey; emit one row per possible rel
    // so the compaction can null out whichever creates existed. Structural collections map 1:1.
    if (collection === "app.bsky.feed.post") {
      return [
        { ...base, rel: "reply", dst: null, op: "d" },
        { ...base, rel: "quote", dst: null, op: "d" },
      ];
    }
    const rel = COLLECTION_REL[collection];
    if (rel && (rel !== "like" || opts.captureLikes)) {
      return [{ ...base, rel, dst: null, op: "d" }];
    }
    return [];
  }

  if (operation !== "create" || !record) return [];

  switch (collection) {
    case "app.bsky.graph.follow":
    case "app.bsky.graph.block": {
      const dst = typeof record.subject === "string" ? record.subject : null;
      return dst ? [{ ...base, rel: COLLECTION_REL[collection], dst, op: "c" }] : [];
    }
    case "app.bsky.feed.repost": {
      const dst = typeof record.subject?.uri === "string" ? record.subject.uri : null;
      return dst ? [{ ...base, rel: "repost", dst, op: "c" }] : [];
    }
    case "app.bsky.feed.like": {
      if (!opts.captureLikes) return [];
      const dst = typeof record.subject?.uri === "string" ? record.subject.uri : null;
      return dst ? [{ ...base, rel: "like", dst, op: "c" }] : [];
    }
    case "app.bsky.feed.post": {
      const out: EdgeRow[] = [];
      const parent = record.reply?.parent?.uri;
      if (typeof parent === "string") out.push({ ...base, rel: "reply", dst: parent, op: "c" });
      const embedType = record.embed?.$type;
      if (embedType === "app.bsky.embed.record" || embedType === "app.bsky.embed.recordWithMedia") {
        const quoted = record.embed?.record?.uri;
        if (typeof quoted === "string") out.push({ ...base, rel: "quote", dst: quoted, op: "c" });
      }
      return out;
    }
    case "app.bsky.graph.listitem": {
      // actor adds subject DID to their list; keep the list URI in ctx so downstream can group
      // memberships by list (list-stuffing / curated-list coordination signal).
      const dst = typeof record.subject === "string" ? record.subject : null;
      const listUri = typeof record.list === "string" ? record.list : null;
      return dst ? [{ ...base, rel: "listitem", dst, op: "c", ctx: listUri }] : [];
    }
    case "app.bsky.graph.listblock": {
      // actor subscribes to someone's moderation/block list; dst = the subscribed list URI.
      const dst = typeof record.subject === "string" ? record.subject : null;
      return dst ? [{ ...base, rel: "listblock", dst, op: "c" }] : [];
    }
    case "app.bsky.graph.list": {
      // actor creates a list; no second party, so dst = the list's own URI (node-as-edge for parity
      // with the old social-graph capture; low volume).
      return [{ ...base, rel: "list", dst: `at://${evt.did}/app.bsky.graph.list/${rkey}`, op: "c" }];
    }
    default:
      return [];
  }
}

const NDJSON_COLS =
  "{src:'VARCHAR', rel:'VARCHAR', dst:'VARCHAR', rkey:'VARCHAR', t:'BIGINT', op:'VARCHAR', ctx:'VARCHAR'}";

/**
 * Write a batch of edges as compact zstd Parquet part files, one per date partition, under
 * `${dir}/dt=<date>/`. Staged via a temp NDJSON the duckdb CLI converts, then the part is renamed
 * into place atomically (readers never see a partial part). Returns the number of parts written.
 *
 * `partId` should be monotonic within a process (the cursor works well) so part filenames are unique
 * and sortable. Throws on failure; the caller treats edge writes as best-effort so a miniext outage
 * never blocks the activity cursor.
 */
export async function writeEdgeParts(rows: EdgeRow[], dir: string, partId: number | string): Promise<number> {
  if (rows.length === 0) return 0;
  const byDate = new Map<string, EdgeRow[]>();
  for (const r of rows) (byDate.get(r.dt) ?? byDate.set(r.dt, []).get(r.dt)!).push(r);

  let written = 0;
  for (const [dt, drs] of byDate) {
    const partDir = path.join(dir, `dt=${dt}`);
    await mkdir(partDir, { recursive: true });
    const stem = path.join(partDir, `part-${partId}`);
    const ndTmp = `${stem}.ndjson.tmp`;
    const pqTmp = `${stem}.parquet.tmp`;
    const pqOut = `${stem}.parquet`;
    // src/rel/rkey/op are non-null; dst is null for deletes. JSON.stringify emits null cleanly.
    const body = drs.map(r => JSON.stringify({ src: r.src, rel: r.rel, dst: r.dst, rkey: r.rkey, t: r.t, op: r.op, ctx: r.ctx ?? null })).join("\n");
    await writeFile(ndTmp, body);
    try {
      await execFileAsync("duckdb", ["-c",
        `COPY (SELECT * FROM read_json('${ndTmp}', columns=${NDJSON_COLS}, format='newline_delimited')) ` +
        `TO '${pqTmp}' (FORMAT parquet, COMPRESSION zstd);`]);
      await rename(pqTmp, pqOut);
      written++;
    } finally {
      await rm(ndTmp, { force: true });
      await rm(pqTmp, { force: true });
    }
  }
  return written;
}

/**
 * Compact the part-*.parquet in a day's partition into a single c-<ts>.parquet WITHIN the same
 * dt=<date>/ dir, then remove just those parts. Staying inside the partition dir keeps the dataset
 * Hive-globbable (`edges/dt=*​/*.parquet` with hive_partitioning). Reads only part-* (never the
 * c-* it writes), so re-running on a day that gained new parts merges them into a fresh c-* file
 * without self-reading. No-op when the partition has no parts. Call on date-rollover for the
 * previous day (and on shutdown for the current one).
 */
export async function compactDay(dir: string, dt: string): Promise<boolean> {
  const partDir = path.join(dir, `dt=${dt}`);
  let parts: string[];
  try {
    parts = (await readdir(partDir)).filter(f => f.startsWith("part-") && f.endsWith(".parquet"));
  } catch {
    return false; // partition doesn't exist
  }
  if (parts.length === 0) return false;
  const ts = Date.now();
  const mergedTmp = path.join(partDir, `.c-${ts}.parquet.tmp`);
  const mergedOut = path.join(partDir, `c-${ts}.parquet`);
  await execFileAsync("duckdb", ["-c",
    `COPY (SELECT * FROM read_parquet('${path.join(partDir, "part-*.parquet")}')) ` +
    `TO '${mergedTmp}' (FORMAT parquet, COMPRESSION zstd);`]);
  await rename(mergedTmp, mergedOut);
  for (const p of parts) await rm(path.join(partDir, p), { force: true });  // drop only merged parts
  return true;
}
