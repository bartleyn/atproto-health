/**
 * One-off script: streams the full PLC directory export and counts
 * plc_tombstone operations. No DB writes — prints result and exits.
 *
 * Usage:
 *   npx tsx src/lib/collectors/count-tombstones.ts
 */

const EXPORT_URL = "https://plc.directory/export";
const BATCH_SIZE = 1000;
const COURTESY_DELAY_MS = 150;
const LOG_EVERY = 500_000;

async function fetchBatch(after: string): Promise<any[]> {
  const params = new URLSearchParams({ count: String(BATCH_SIZE) });
  if (after) params.set("after", after);

  const res = await fetch(`${EXPORT_URL}?${params}`, {
    signal: AbortSignal.timeout(30_000),
  });
  if (!res.ok) throw new Error(`PLC export HTTP ${res.status}`);

  const text = await res.text();
  return text.split("\n").filter(Boolean).map((line) => JSON.parse(line));
}

async function main() {
  console.log("Streaming PLC export to count tombstones...\n");

  let cursor = "";
  let totalOps = 0;
  let tombstones = 0;
  let nullified = 0;

  while (true) {
    let batch: any[];
    try {
      batch = await fetchBatch(cursor);
    } catch (err) {
      console.error(`Fetch failed at cursor ${cursor}: ${err} — retrying in 5s`);
      await new Promise((r) => setTimeout(r, 5_000));
      continue;
    }

    if (batch.length === 0) break;

    for (const op of batch) {
      totalOps++;
      if (op.nullified) { nullified++; continue; }
      if (op.operation?.type === "plc_tombstone") tombstones++;
    }

    cursor = batch[batch.length - 1].createdAt;

    if (totalOps % LOG_EVERY < BATCH_SIZE) {
      console.log(`  ${totalOps.toLocaleString()} ops scanned · ${tombstones.toLocaleString()} tombstones so far · cursor ${cursor}`);
    }

    if (batch.length < BATCH_SIZE) break;

    await new Promise((r) => setTimeout(r, COURTESY_DELAY_MS));
  }

  console.log(`\n=== Done ===`);
  console.log(`  Total ops scanned: ${totalOps.toLocaleString()}`);
  console.log(`  Nullified ops:     ${nullified.toLocaleString()}`);
  console.log(`  Tombstones:        ${tombstones.toLocaleString()}`);
  console.log(`  Non-tombstone ops: ${(totalOps - nullified - tombstones).toLocaleString()}`);
  console.log(`\n  Tombstone rate:    ${((tombstones / totalOps) * 100).toFixed(3)}%`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
