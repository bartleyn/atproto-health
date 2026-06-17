/**
 * Fetches account labels from a labeler and stores them in the DB.
 *
 * Filters to did: URIs only (skips post/record labels).
 * Skips negated labels (val starting with "!") — stores current positive labels only.
 * Resumable: stores cursor so re-runs continue from where they left off.
 * Idempotent: upserts on (did, label) primary key.
 *
 * Usage:
 *   npm run collect:skywatch              # Skywatch labeler (default)
 *   npm run collect:skywatch -- --reset
 *   npm run collect:bsky-mod             # Bluesky moderation labeler
 *   npm run collect:bsky-mod -- --reset
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

import sql from "../db/pg";

const LABELERS: Record<string, {
  endpoint: string; did: string;
  table: string; cursorTable: string;
  labelFilter?: string[];
}> = {
  skywatch: {
    endpoint:    "https://ozone.skywatch.blue",
    did:         "did:plc:e4elbtctnfqocyfcml6h2lf7",
    table:       "plc.skywatch_labels",
    cursorTable: "plc.skywatch_labels_cursor",
  },
  "bsky-mod": {
    endpoint:    "https://mod.bsky.app",
    did:         "did:plc:ar7c4by46qjdydhdevvrndac",
    table:       "plc.bsky_mod_labels",
    cursorTable: "plc.bsky_mod_labels_cursor",
    labelFilter: ["spam", "impersonation"],
  },
};

const PAGE_SIZE = 50;
const REQUEST_TIMEOUT_MS = 30_000;
const COURTESY_DELAY_MS = 100;

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
  neg?: boolean;
}

interface QueryLabelsResponse {
  cursor?: string;
  labels: RawLabel[];
}

async function fetchPage(cursor?: string, limit = PAGE_SIZE): Promise<QueryLabelsResponse> {
  const params = new URLSearchParams({ uriPatterns: "*", limit: String(limit) });
  if (cursor) params.set("cursor", cursor);
  const url = `${config.endpoint}/xrpc/com.atproto.label.queryLabels?${params}`;
  const res = await fetch(url, { signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS) });
  if (!res.ok) throw new Error(`HTTP ${res.status} from ${url}`);
  return res.json() as Promise<QueryLabelsResponse>;
}

/** Filter a raw page to the labels we store and upsert them. Returns # inserted. */
async function ingestLabels(labels: RawLabel[]): Promise<number> {
  const batch: { did: string; label: string; labeled_at: string }[] = [];
  for (const label of labels) {
    if (!label.uri.startsWith("did:")) continue;
    if (label.val.startsWith("!") || label.neg) continue;
    if (label.src !== config.did) continue;
    if (config.labelFilter && !config.labelFilter.includes(label.val)) continue;
    batch.push({ did: label.uri, label: label.val, labeled_at: label.cts });
  }
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

  let cursor = opts.from;
  let totalFetched = 0, totalInserted = 0, pages = 0, jumpSteps = 0;

  while (true) {
    const page = await fetchPage(cursor, BACKFILL_LIMIT);
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

async function fetchSubjectLabels(dids: string[]): Promise<RawLabel[]> {
  const params = new URLSearchParams({ limit: "250" });
  for (const did of dids) params.append("uriPatterns", did);
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
 */
async function subjectSweep(opts: { before: string; after: string; concurrency: number }) {
  console.log(`\nSubject-sweep mode: created_at in [${opts.after}, ${opts.before}), ` +
    `batch=${SUBJECT_BATCH}, concurrency=${opts.concurrency}`);

  const candidates = await sql.unsafe<{ did: string }[]>(
    `SELECT did FROM plc.plc_account_creations
     WHERE created_at >= $1 AND created_at < $2 ORDER BY created_at`,
    [opts.after, opts.before]
  );
  console.log(`Candidates: ${candidates.length.toLocaleString()} accounts`);
  if (candidates.length === 0) return;

  const batches: string[][] = [];
  for (let i = 0; i < candidates.length; i += SUBJECT_BATCH) {
    batches.push(candidates.slice(i, i + SUBJECT_BATCH).map(r => r.did));
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
    });
    await sql.end();
    return;
  }

  if (reset) {
    await sql.unsafe(`DELETE FROM ${config.table}`);
    await sql.unsafe(`DELETE FROM ${config.cursorTable}`);
    console.log(`Reset: cleared existing labels and cursor.\n`);
  }

  const cursorRows = await sql.unsafe<{ cursor: string }[]>(
    `SELECT cursor FROM ${config.cursorTable} WHERE id = 1`
  );
  let cursor: string | undefined = cursorRows[0]?.cursor;
  console.log(cursor ? `Resuming from cursor: ${cursor}` : `Starting from beginning`);

  let totalFetched = 0;
  let totalInserted = 0;
  let pages = 0;

  while (true) {
    const page = await fetchPage(cursor);
    pages++;

    const batch: { did: string; label: string; labeled_at: string }[] = [];
    for (const label of page.labels) {
      totalFetched++;
      if (!label.uri.startsWith("did:")) continue;
      if (label.val.startsWith("!") || label.neg) continue;
      if (label.src !== config.did) continue;
      if (config.labelFilter && !config.labelFilter.includes(label.val)) continue;
      batch.push({ did: label.uri, label: label.val, labeled_at: label.cts });
    }

    if (batch.length > 0) {
      const values = batch.map((_, i) => `($${i * 3 + 1}, $${i * 3 + 2}, $${i * 3 + 3})`).join(", ");
      const params = batch.flatMap(r => [r.did, r.label, r.labeled_at]);
      await sql.unsafe(
        `INSERT INTO ${config.table} (did, label, labeled_at) VALUES ${values}
         ON CONFLICT (did, label) DO UPDATE SET labeled_at = EXCLUDED.labeled_at`,
        params
      );
      totalInserted += batch.length;
    }

    if (page.cursor) {
      await sql.unsafe(
        `INSERT INTO ${config.cursorTable} (id, cursor, updated_at) VALUES (1, $1, NOW())
         ON CONFLICT (id) DO UPDATE SET cursor = EXCLUDED.cursor, updated_at = NOW()`,
        [page.cursor]
      );
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
