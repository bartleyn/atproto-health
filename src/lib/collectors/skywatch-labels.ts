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

async function fetchPage(cursor?: string): Promise<QueryLabelsResponse> {
  const params = new URLSearchParams({ uriPatterns: "*", limit: String(PAGE_SIZE) });
  if (cursor) params.set("cursor", cursor);
  const url = `${config.endpoint}/xrpc/com.atproto.label.queryLabels?${params}`;
  const res = await fetch(url, { signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS) });
  if (!res.ok) throw new Error(`HTTP ${res.status} from ${url}`);
  return res.json() as Promise<QueryLabelsResponse>;
}

async function main() {
  const reset = args.includes("--reset");

  console.log(`\n=== Label Collector: ${labelerName} ===`);
  console.log(`Endpoint: ${config.endpoint}`);
  console.log(`DID:      ${config.did}`);
  console.log(`Table:    ${config.table}`);
  if (config.labelFilter) console.log(`Filter:   ${config.labelFilter.join(", ")}`);

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
