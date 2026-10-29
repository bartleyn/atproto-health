/**
 * Fetches account labels from a labeler and stores them in the DB.
 *
 * Filters to did: URIs only (skips post/record labels); bsky-mod also keeps feed-generator labels
 * in plc.bsky_mod_feed_labels.
 * Skips negated labels (val starting with "!") — stores current positive labels only,
 * unless retractOnNegation is set, in which case a negation deletes the row.
 * Resumable: stores cursor so re-runs continue from where they left off.
 * Idempotent: upserts on (did, label) primary key.
 *
 * Usage:
 *   npm run collect:skywatch              # Skywatch labeler (default; subscribeLabels stream)
 *   npm run collect:skywatch -- --reset
 *   npm run collect:bsky-mod             # Bluesky moderation labeler (subscribeLabels stream)
 *   npm run collect:bsky-mod -- --reset
 *   npm run collect:bsky-mod -- --stream-from 0   # replay the stream from a given seq
 *   npm run collect:bottags              # gracekind bottags labeler (undisclosed-bot/-human)
 *   npm run collect:bsky-mod -- --subjects --feeds   # backfill labels on every live feed generator
 *
 *   # Backfill mode — recover labels the normal scan skips.
 *   # The labeler's queryLabels?uriPatterns=* pagination occasionally returns a
 *   # `cursor` that JUMPS forward by 100k–3M, leaping over labels that ARE
 *   # retrievable if you request an intermediate cursor. (Observed: ~Jan–Sep 2025
 *   # labels were skipped by a normal scan.) Backfill follows the cursor when it
 *   # advances normally but steps a small fixed amount when it jumps, walking
 *   # through the skipped region. Upserts dedupe overlaps; does NOT touch the
 *   # incremental cursor.
 *   npm run collect:skywatch -- --backfill                    # full sweep from start
 *   npm run collect:skywatch -- --backfill --from 1300000 --to 4260000   # target a range
 *   npm run collect:skywatch -- --backfill --step 100         # smaller step (safer/slower)
 */

import WebSocket from "ws";
import sql from "../db/pg";

const LABELERS: Record<string, {
  endpoint: string; did: string;
  table: string; cursorTable: string;
  labelFilter?: string[];
  // "stream" = incremental runs read subscribeLabels instead of queryLabels?uriPatterns=*
  mode?: "query" | "stream";
  // current-state table for MOD_ACTIONS; negations delete rows
  actionTable?: string;
  // queryLabels uriPatterns (default "*"); "did:*" skips post labels server-side
  uriPattern?: string;
  // negations delete the (did, label) row instead of being skipped
  retractOnNegation?: boolean;
  // current-state table for this labeler's labels on feed generators; negations delete rows
  feedLabelTable?: string;
}> = {
  skywatch: {
    endpoint:    "https://ozone.skywatch.blue",
    did:         "did:plc:e4elbtctnfqocyfcml6h2lf7",
    table:       "plc.skywatch_labels",
    cursorTable: "plc.skywatch_labels_cursor",
    // wildcard queryLabels returns 400 "temporarily disabled" since ~2026-10-16
    mode:        "stream",
  },
  "bsky-mod": {
    endpoint:    "https://mod.bsky.app",
    did:         "did:plc:ar7c4by46qjdydhdevvrndac",
    table:       "plc.bsky_mod_labels",
    cursorTable: "plc.bsky_mod_labels_cursor",
    labelFilter: ["spam", "impersonation", "inauthentic", "security", "engagement-farming"],
    // wildcard queryLabels returns 400 "temporarily disabled" since 2026-10-08
    mode:        "stream",
    actionTable: "plc.bsky_mod_actions",
    feedLabelTable: "plc.bsky_mod_feed_labels",
  },
  // Behavioral/activity labeler: 25 "inform"-severity activity labels (post/reply volume, 24/7
  // posting gaps, follow/unfollow churn, profile-metadata churn, same-URL spam). No labelFilter =
  // collect all of them as activity-proxy FEATURES. Negations are skipped + rows never deleted, so a
  // row = "this account was EVER flagged with this label" (labeled_at = most recent occurrence).
  stechlab: {
    endpoint:    "https://labels.stech-gcp-labels.com",
    did:         "did:plc:oubsyca6hhgqhmbbk27lvs7c",
    table:       "plc.stechlab_labels",
    cursorTable: "plc.stechlab_labels_cursor",
  },
  // gracekind's bottags labeler: curated, report-driven account labels. Post labels (bot-tag
  // replies) dominate its stream, so query did:* only. Labels get retracted, so negations delete.
  bottags: {
    endpoint:          "https://bot-tag-labeler.gracekind.net",
    did:               "did:plc:cdkysjo2taj2jpy3qoostthu",
    table:             "plc.bottags_labels",
    cursorTable:       "plc.bottags_labels_cursor",
    labelFilter:       ["undisclosed-bot", "undisclosed-human"],
    uriPattern:        "did:*",
    retractOnNegation: true,
  },
};

const PAGE_SIZE = 50;
const REQUEST_TIMEOUT_MS = 30_000;
const COURTESY_DELAY_MS = 100;
const STREAM_FLUSH_FRAMES = 1000;
const STREAM_IDLE_MS = 15_000;
const STREAM_CAUGHT_UP_MS = 60_000;
const MOD_ACTIONS = ["!takedown", "!suspend", "!hide"];

const args = process.argv.slice(2);
const labelerName = args.includes("--labeler")
  ? args[args.indexOf("--labeler") + 1]
  : args.find(a => LABELERS[a]) ?? "skywatch";

const config = LABELERS[labelerName];
if (!config) {
  console.error(`Unknown labeler: ${labelerName}. Available: ${Object.keys(LABELERS).join(", ")}`);
  process.exit(1);
}

interface RawLabel {
  src: string;
  uri: string;
  val: string;
  cts: string;
  exp?: string;
  neg?: boolean;
}

interface QueryLabelsResponse {
  cursor?: string;
  labels: RawLabel[];
}

async function fetchPage(cursor?: string, limit = PAGE_SIZE): Promise<QueryLabelsResponse> {
  const params = new URLSearchParams({ uriPatterns: config.uriPattern ?? "*", limit: String(limit) });
  if (cursor) params.set("cursor", cursor);
  const url = `${config.endpoint}/xrpc/com.atproto.label.queryLabels?${params}`;
  const res = await fetch(url, { signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS) });
  if (!res.ok) throw new Error(`HTTP ${res.status} from ${url}: ${(await res.text()).slice(0, 300)}`);
  return res.json() as Promise<QueryLabelsResponse>;
}

/**
 * Backfill-only resilient fetch. This labeler returns HTTP 500 when a page of the default size spans
 * a record it can't serialize; the SAME cursor succeeds at a smaller limit (verified: 250/200 -> 500,
 * 50/100 -> 200). Shrink the limit on any 5xx (250->100->50->25->10->1); if even limit=1 5xx's, the
 * poison record sits exactly at `cursor` -> return null so the caller can skip it. Non-5xx errors
 * (network/4xx) propagate so we still fail loudly on real problems.
 */
async function fetchPageResilient(cursor: string | undefined, maxLimit: number): Promise<QueryLabelsResponse | null> {
  const ladder = [maxLimit, 100, 50, 25, 10, 1].filter((l, i, a) => l <= maxLimit && a.indexOf(l) === i);
  for (const limit of ladder) {
    try {
      const page = await fetchPage(cursor, limit);
      if (limit < maxLimit) console.log(`  (recovered at limit=${limit}, cursor=${cursor})`);
      return page;
    } catch (e) {
      if (!/HTTP 5\d\d/.test(String(e))) throw e;   // real error -> propagate
      // 5xx -> shrink and retry
    }
  }
  return null;   // even limit=1 5xx'd -> unservable record at this cursor
}

/** Filter a raw page to the labels we store and upsert them. Returns # inserted. */
async function ingestLabels(labels: RawLabel[]): Promise<number> {
  await ingestActions(labels);
  await ingestFeedLabels(labels);
  // latest occurrence per (did, label); ON CONFLICT can't touch a row twice in one INSERT
  const byKey = new Map<string, RawLabel>();
  for (const label of labels) {
    if (!label.uri.startsWith("did:")) continue;
    if (label.val.startsWith("!")) continue;
    if (label.neg && !config.retractOnNegation) continue;
    if (label.src !== config.did) continue;
    if (config.labelFilter && !config.labelFilter.includes(label.val)) continue;
    byKey.set(`${label.uri}|${label.val}`, label);
  }
  const latest = [...byKey.values()];
  const retracted = latest.filter(l => l.neg);
  if (retracted.length > 0) {
    const values = retracted.map((_, i) => `($${i * 2 + 1}, $${i * 2 + 2})`).join(", ");
    await sql.unsafe(
      `DELETE FROM ${config.table} t USING (VALUES ${values}) AS v(did, label)
       WHERE t.did = v.did AND t.label = v.label`,
      retracted.flatMap(l => [l.uri, l.val])
    );
  }
  const batch = latest.filter(l => !l.neg).map(l => ({ did: l.uri, label: l.val, labeled_at: l.cts }));
  if (batch.length > 0) {
    const values = batch.map((_, i) => `($${i * 3 + 1}, $${i * 3 + 2}, $${i * 3 + 3})`).join(", ");
    const params = batch.flatMap(r => [r.did, r.label, r.labeled_at]);
    await sql.unsafe(
      `INSERT INTO ${config.table} (did, label, labeled_at) VALUES ${values}
       ON CONFLICT (did, label) DO UPDATE SET labeled_at = EXCLUDED.labeled_at`,
      params
    );
  }
  return batch.length;
}

/** Keep `table` as current state, in order: the latest label per (subject, value) upserts its row, a negation deletes it. */
async function applyCurrentState(table: string, [subject, value]: [string, string], labels: RawLabel[]) {
  const latest = new Map<string, RawLabel>();
  for (const label of labels) latest.set(`${label.uri}|${label.val}`, label);
  const all = [...latest.values()];
  const applied = all.filter(l => !l.neg), negated = all.filter(l => l.neg);
  if (applied.length > 0) {
    const values = applied.map((_, i) => `($${i * 4 + 1}, $${i * 4 + 2}, $${i * 4 + 3}, $${i * 4 + 4}::timestamptz)`).join(", ");
    await sql.unsafe(
      `INSERT INTO ${table} (${subject}, ${value}, labeled_at, expires_at) VALUES ${values}
       ON CONFLICT (${subject}, ${value}) DO UPDATE SET labeled_at = EXCLUDED.labeled_at, expires_at = EXCLUDED.expires_at`,
      applied.flatMap(l => [l.uri, l.val, l.cts, l.exp ?? null])
    );
  }
  if (negated.length > 0) {
    const values = negated.map((_, i) => `($${i * 2 + 1}, $${i * 2 + 2})`).join(", ");
    await sql.unsafe(
      `DELETE FROM ${table} t USING (VALUES ${values}) AS v(${subject}, ${value})
       WHERE t.${subject} = v.${subject} AND t.${value} = v.${value}`,
      negated.flatMap(l => [l.uri, l.val])
    );
  }
}

/** Account MOD_ACTIONS -> actionTable. */
async function ingestActions(labels: RawLabel[]) {
  if (!config.actionTable) return;
  await applyCurrentState(config.actionTable, ["did", "action"], labels.filter(l =>
    l.uri.startsWith("did:") && l.src === config.did && MOD_ACTIONS.includes(l.val)));
}

const FEED_URI = /^at:\/\/[^/]+\/app\.bsky\.feed\.generator\/[^/]+$/;

/** This labeler's labels on feed generators (porn, sexual, !takedown, ...) -> feedLabelTable. */
async function ingestFeedLabels(labels: RawLabel[]) {
  if (!config.feedLabelTable) return;
  await applyCurrentState(config.feedLabelTable, ["uri", "label"], labels.filter(l =>
    l.src === config.did && FEED_URI.test(l.uri)));
}

/**
 * Backfill walk: pages with a large limit, trusting the returned cursor when it
 * advances normally but stepping a small fixed amount when it jumps (so skipped
 * labels in the jumped-over range get fetched). Never updates the incremental cursor.
 */
async function backfill(opts: { from?: string; to?: number; step: number }) {
  const BACKFILL_LIMIT = 250;
  const JUMP_THRESHOLD = 5000;  // contiguous advances are <500; real jumps are >100k

  console.log(`\nBackfill mode: step=${opts.step}, limit=${BACKFILL_LIMIT}` +
    `${opts.from ? `, from=${opts.from}` : ""}${opts.to ? `, to=${opts.to}` : ""}`);

  const MAX_POISON_SKIPS = 200;   // guard: give up if we hit a long unservable stretch
  let cursor = opts.from;
  let totalFetched = 0, totalInserted = 0, pages = 0, jumpSteps = 0, poisonSkips = 0;

  while (true) {
    const page = await fetchPageResilient(cursor, BACKFILL_LIMIT);
    if (page === null) {
      // Unservable record exactly at `cursor` (500s even at limit=1): step over it and continue.
      if (++poisonSkips > MAX_POISON_SKIPS) {
        console.error(`  Stopping: ${poisonSkips} consecutive unservable records at cursor=${cursor}`);
        break;
      }
      cursor = String((cursor ? Number(cursor) : 0) + 1);
      continue;
    }
    poisonSkips = 0;
    pages++;
    totalFetched += page.labels.length;
    totalInserted += await ingestLabels(page.labels);

    if (page.labels.length === 0 || !page.cursor) break;

    const next = Number(page.cursor);
    let advance: number;
    if (!cursor) {
      advance = next;                       // first page (no input cursor): accept
    } else if (next - Number(cursor) > JUMP_THRESHOLD) {
      advance = Number(cursor) + opts.step; // jump detected: step through instead of leaping
      jumpSteps++;
    } else {
      advance = next;                       // normal advance: trust it
    }
    if (cursor && advance <= Number(cursor)) advance = Number(cursor) + opts.step; // ensure progress
    if (opts.to && advance > opts.to) break;
    cursor = String(advance);

    if (pages % 50 === 0) {
      console.log(`  pages=${pages} fetched=${totalFetched.toLocaleString()} ` +
        `upserted=${totalInserted.toLocaleString()} cursor=${cursor} jumpSteps=${jumpSteps}`);
    }
    await new Promise(r => setTimeout(r, COURTESY_DELAY_MS));
  }

  console.log(`\n=== Backfill done ===`);
  console.log(`  Pages:        ${pages.toLocaleString()}`);
  console.log(`  Fetched:      ${totalFetched.toLocaleString()}`);
  console.log(`  Upserted:     ${totalInserted.toLocaleString()}`);
  console.log(`  Jump-steps:   ${jumpSteps.toLocaleString()}`);
}

/**
 * Query the labeler for a small batch of specific subject DIDs (max ~20 uriPatterns;
 * 25+ silently returns nothing) and upsert their labels. This recovers ACCOUNT labels
 * that the `uriPatterns=*` firehose under-returns: that stream is dominated by post
 * labels and surfaces only a tiny fraction of account labels (e.g. bluesky-elder).
 */
const SUBJECT_BATCH = 20;

async function fetchSubjectLabels(subjects: string[]): Promise<RawLabel[]> {
  const params = new URLSearchParams({ limit: "250" });
  for (const s of subjects) params.append("uriPatterns", s);
  const url = `${config.endpoint}/xrpc/com.atproto.label.queryLabels?${params}`;
  let lastErr: unknown;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const body = await res.json() as QueryLabelsResponse;
      return body.labels ?? [];
    } catch (err) {
      lastErr = err;
      await new Promise(r => setTimeout(r, attempt * 1000));
    }
  }
  throw lastErr;
}

/**
 * Subject sweep: pull a candidate cohort of DIDs from plc.plc_account_creations and
 * query the labeler per-subject in batches. Designed for label values the firehose
 * doesn't surface in bulk (e.g. bluesky-elder lives on accounts created before ~2023-09).
 * With feeds, the cohort is every live feed generator in activity.feed_generators instead.
 */
async function subjectSweep(opts: { before: string; after: string; concurrency: number; feeds: boolean }) {
  console.log(`\nSubject-sweep mode: ${opts.feeds ? "live feed generators" : `created_at in [${opts.after}, ${opts.before})`}, ` +
    `batch=${SUBJECT_BATCH}, concurrency=${opts.concurrency}`);

  const candidates = opts.feeds
    ? await sql.unsafe<{ subject: string }[]>(
        `SELECT uri AS subject FROM activity.feed_generators WHERE deleted_at IS NULL ORDER BY uri`)
    : await sql.unsafe<{ subject: string }[]>(
        `SELECT did AS subject FROM plc.plc_account_creations
         WHERE created_at >= $1 AND created_at < $2 ORDER BY created_at`,
        [opts.after, opts.before]);
  console.log(`Candidates: ${candidates.length.toLocaleString()} ${opts.feeds ? "feeds" : "accounts"}`);
  if (candidates.length === 0) return;

  const batches: string[][] = [];
  for (let i = 0; i < candidates.length; i += SUBJECT_BATCH) {
    batches.push(candidates.slice(i, i + SUBJECT_BATCH).map(r => r.subject));
  }

  let done = 0, inserted = 0;
  let next = 0;
  async function worker() {
    while (next < batches.length) {
      const idx = next++;
      try {
        const labels = await fetchSubjectLabels(batches[idx]);
        inserted += await ingestLabels(labels);
      } catch (err) {
        console.error(`  batch ${idx} failed: ${String(err)}`);
      }
      done++;
      if (done % 250 === 0) {
        console.log(`  batches ${done.toLocaleString()}/${batches.length.toLocaleString()} ` +
          `(${((done / batches.length) * 100).toFixed(1)}%) upserted=${inserted.toLocaleString()}`);
      }
    }
  }
  await Promise.all(Array.from({ length: opts.concurrency }, worker));

  console.log(`\n=== Subject sweep done ===`);
  console.log(`  Batches:  ${batches.length.toLocaleString()}`);
  console.log(`  Upserted: ${inserted.toLocaleString()}`);
}

async function saveCursor(cursor: string) {
  await sql.unsafe(
    `INSERT INTO ${config.cursorTable} (id, cursor, updated_at) VALUES (1, $1, NOW())
     ON CONFLICT (id) DO UPDATE SET cursor = EXCLUDED.cursor, updated_at = NOW()`,
    [cursor]
  );
}

/**
 * Replay subscribeLabels from `cursor` until caught up to now (or idle), then close.
 * Stream seq shares the queryLabels cursor space, so the stored cursor resumes exactly.
 */
async function streamLabels(cursor: string | undefined): Promise<{ fetched: number; inserted: number; caughtUp: boolean }> {
  // cborg is ESM-only; tsx runs this file as CJS
  const { decode, decodeFirst } = await import("cborg");
  const url = `${config.endpoint.replace(/^http/, "ws")}/xrpc/com.atproto.label.subscribeLabels` +
    (cursor ? `?cursor=${cursor}` : "");
  const startedAt = Date.now();
  let fetched = 0, inserted = 0, frames = 0, caughtUp = false;
  let pending: RawLabel[] = [];
  let lastSeq: number | undefined;
  let chain: Promise<void> = Promise.resolve();

  const ws = new WebSocket(url);

  // serialised ingest + cursor save; the stream pauses until it lands
  function flush(): Promise<void> {
    const labels = pending, seq = lastSeq;
    pending = [];
    chain = chain.then(async () => {
      inserted += await ingestLabels(labels);
      if (seq !== undefined) await saveCursor(String(seq));
    });
    return chain;
  }

  await new Promise<void>((resolve, reject) => {
    let idle: ReturnType<typeof setTimeout> | undefined;
    let done = false;
    const finish = (err?: Error) => {
      if (done) return;
      done = true;
      clearTimeout(idle);
      ws.terminate();
      flush().then(() => (err ? reject(err) : resolve()), reject);
    };
    const armIdle = () => {
      clearTimeout(idle);
      idle = setTimeout(() => finish(), STREAM_IDLE_MS);
    };

    ws.on("open", armIdle);
    ws.on("error", (err) => finish(err));
    ws.on("close", () => finish());
    ws.on("message", (data: Buffer) => {
      if (done) return;
      armIdle();
      const [header, rest] = decodeFirst(data);
      const body = decode(rest);

      if (header.op === -1) return finish(new Error(`subscribeLabels error ${body.error}: ${body.message}`));
      if (header.t === "#info") {
        if (body.name === "OutdatedCursor") {
          return finish(new Error(`cursor ${cursor} is older than the labeler retains; labels were missed. ` +
            `Run with --reset to rebuild. (${body.message ?? ""})`));
        }
        console.log(`  info: ${body.name} ${body.message ?? ""}`);
        return;
      }
      if (header.t !== "#labels") return;

      frames++;
      lastSeq = body.seq;
      for (const label of body.labels as RawLabel[]) {
        fetched++;
        pending.push(label);
        if (Date.parse(label.cts) >= startedAt - STREAM_CAUGHT_UP_MS) caughtUp = true;
      }

      if (caughtUp) return finish();
      if (frames % STREAM_FLUSH_FRAMES === 0) {
        ws.pause();
        flush().then(() => ws.resume(), (err) => finish(err));
      }
      if (frames % 50_000 === 0) {
        console.log(`  Frames: ${frames.toLocaleString()} | Fetched: ${fetched.toLocaleString()} | ` +
          `Inserted: ${inserted.toLocaleString()} | Seq: ${lastSeq}`);
      }
    });
  });

  return { fetched, inserted, caughtUp };
}

function getArg(name: string): string | undefined {
  const i = args.indexOf(name);
  return i >= 0 ? args[i + 1] : undefined;
}

async function main() {
  const reset = args.includes("--reset");
  const isBackfill = args.includes("--backfill");

  console.log(`\n=== Label Collector: ${labelerName} ===`);
  console.log(`Endpoint: ${config.endpoint}`);
  console.log(`DID:      ${config.did}`);
  console.log(`Table:    ${config.table}`);
  if (config.labelFilter) console.log(`Filter:   ${config.labelFilter.join(", ")}`);

  if (isBackfill) {
    await backfill({
      from: getArg("--from"),
      to:   getArg("--to") ? Number(getArg("--to")) : undefined,
      step: getArg("--step") ? Number(getArg("--step")) : 200,
    });
    await sql.end();
    return;
  }

  if (args.includes("--subjects")) {
    await subjectSweep({
      before:      getArg("--created-before") ?? "2023-10-01",
      after:       getArg("--created-after") ?? "1970-01-01",
      concurrency: getArg("--concurrency") ? Number(getArg("--concurrency")) : 6,
      feeds:       args.includes("--feeds"),
    });
    await sql.end();
    return;
  }

  if (reset) {
    await sql.unsafe(`DELETE FROM ${config.table}`);
    await sql.unsafe(`DELETE FROM ${config.cursorTable}`);
    if (config.actionTable) await sql.unsafe(`DELETE FROM ${config.actionTable}`);
    if (config.feedLabelTable) await sql.unsafe(`DELETE FROM ${config.feedLabelTable}`);
    console.log(`Reset: cleared existing labels and cursor.\n`);
  }

  const cursorRows = await sql.unsafe<{ cursor: string }[]>(
    `SELECT cursor FROM ${config.cursorTable} WHERE id = 1`
  );
  let cursor: string | undefined = getArg("--stream-from") ?? cursorRows[0]?.cursor;
  console.log(cursor ? `Resuming from cursor: ${cursor}` : `Starting from beginning`);

  let totalFetched = 0;
  let totalInserted = 0;
  let pages = 0;

  if (config.mode === "stream") {
    console.log(`Mode:     subscribeLabels stream`);
    const r = await streamLabels(cursor);
    totalFetched = r.fetched;
    totalInserted = r.inserted;
    if (!r.caughtUp) console.log(`  Stream went idle before reaching now; next run resumes from the saved cursor.`);
  }

  while (config.mode !== "stream") {
    const page = await fetchPage(cursor);
    pages++;
    totalFetched += page.labels.length;
    totalInserted += await ingestLabels(page.labels);

    if (page.cursor) {
      await saveCursor(page.cursor);
      cursor = page.cursor;
    }

    if (pages % 10 === 0 || !page.cursor) {
      console.log(`  Pages: ${pages} | Fetched: ${totalFetched.toLocaleString()} | Inserted: ${totalInserted.toLocaleString()} | Cursor: ${cursor}`);
    }

    if (!page.cursor || page.labels.length < PAGE_SIZE) break;

    await new Promise(r => setTimeout(r, COURTESY_DELAY_MS));
  }

  console.log(`\n=== Done ===`);
  console.log(`  Total fetched:  ${totalFetched.toLocaleString()}`);
  console.log(`  Total inserted: ${totalInserted.toLocaleString()}`);

  const dist = await sql.unsafe<{ label: string; count: number }[]>(
    `SELECT label, COUNT(*)::int AS count FROM ${config.table} GROUP BY label ORDER BY count DESC`
  );
  console.log(`\nLabel distribution:`);
  for (const row of dist) {
    console.log(`  ${row.label.padEnd(35)} ${row.count.toLocaleString()}`);
  }

  await sql.end();
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
