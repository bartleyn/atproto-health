/**
 * Fetches all account labels from the Skywatch labeler and stores them in the DB.
 *
 * Filters to did: URIs only (skips post labels).
 * Skips negated labels (val starting with "!") — stores current positive labels only.
 * Resumable: stores cursor so re-runs continue from where they left off.
 * Idempotent: upserts on (did, label) primary key.
 *
 * Usage:
 *   npm run collect:skywatch          # Resume from stored cursor
 *   npm run collect:skywatch -- --reset  # Start over from the beginning
 */

import { getPlcDb } from "../db/plc-schema";

const LABELER_ENDPOINT = "https://ozone.skywatch.blue";
const LABELER_DID = "did:plc:e4elbtctnfqocyfcml6h2lf7";
const PAGE_SIZE = 50;
const REQUEST_TIMEOUT_MS = 30_000;
const COURTESY_DELAY_MS = 100;
const LOG_INTERVAL = 10_000;

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
  const url = `${LABELER_ENDPOINT}/xrpc/com.atproto.label.queryLabels?${params}`;
  const res = await fetch(url, { signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS) });
  if (!res.ok) throw new Error(`HTTP ${res.status} from ${url}`);
  return res.json() as Promise<QueryLabelsResponse>;
}

async function main() {
  const db = getPlcDb();
  const reset = process.argv.includes("--reset");

  console.log(`\n=== Skywatch Label Collector ===`);
  console.log(`Labeler: ${LABELER_DID}`);

  if (reset) {
    db.exec(`DELETE FROM skywatch_labels; DELETE FROM skywatch_labels_cursor;`);
    console.log(`Reset: cleared existing labels and cursor.\n`);
  }

  const cursorRow = db.prepare(`SELECT cursor FROM skywatch_labels_cursor WHERE id = 1`).get() as { cursor: string } | undefined;
  let cursor: string | undefined = cursorRow?.cursor;
  console.log(cursor ? `Resuming from cursor: ${cursor}` : `Starting from beginning`);

  const upsert = db.prepare(`
    INSERT INTO skywatch_labels (did, label, labeled_at)
    VALUES (?, ?, ?)
    ON CONFLICT (did, label) DO UPDATE SET labeled_at = excluded.labeled_at
  `);
  const saveCursor = db.prepare(`
    INSERT INTO skywatch_labels_cursor (id, cursor, updated_at)
    VALUES (1, ?, datetime('now'))
    ON CONFLICT (id) DO UPDATE SET cursor = excluded.cursor, updated_at = excluded.updated_at
  `);

  const upsertBatch = db.transaction((rows: { did: string; label: string; labeledAt: string }[]) => {
    for (const r of rows) upsert.run(r.did, r.label, r.labeledAt);
  });

  let totalFetched = 0;
  let totalInserted = 0;
  let pages = 0;

  while (true) {
    const page = await fetchPage(cursor);
    pages++;

    const batch: { did: string; label: string; labeledAt: string }[] = [];
    for (const label of page.labels) {
      totalFetched++;
      // Only account labels, no negations
      if (!label.uri.startsWith("did:")) continue;
      if (label.val.startsWith("!") || label.neg) continue;
      // Only from Skywatch itself
      if (label.src !== LABELER_DID) continue;
      batch.push({ did: label.uri, label: label.val, labeledAt: label.cts });
    }

    if (batch.length > 0) {
      upsertBatch(batch);
      totalInserted += batch.length;
    }

    if (page.cursor) {
      saveCursor.run(page.cursor);
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

  // Label distribution
  const dist = db.prepare(`
    SELECT label, COUNT(*) as count FROM skywatch_labels GROUP BY label ORDER BY count DESC
  `).all() as { label: string; count: number }[];
  console.log(`\nLabel distribution:`);
  for (const row of dist) {
    console.log(`  ${row.label.padEnd(35)} ${row.count.toLocaleString()}`);
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
