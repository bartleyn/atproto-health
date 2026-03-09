/**
 * CLI runner for Jetstream firehose sampling.
 *
 * Usage:
 *   npm run sample                      # 60-second sample (default)
 *   npm run sample -- --duration 300    # 5-minute sample
 */

import { getDb } from "../db/schema";
import { sampleJetstream } from "./jetstream-sample";

const args = process.argv.slice(2);
const durationIdx = args.indexOf("--duration");
const durationSec =
  durationIdx >= 0 ? parseInt(args[durationIdx + 1], 10) : 60;

async function main() {
  console.log(`\n=== Jetstream Firehose Sample (${durationSec}s) ===\n`);

  const result = await sampleJetstream(durationSec);
  const db = getDb();

  db.prepare(
    `INSERT INTO firehose_samples (
      duration_ms, total_events, total_interactions, resolved_interactions,
      cross_pds, same_pds, events_per_second, by_type, federation, top_cross_pds_pairs
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
  ).run(
    result.durationMs,
    result.totalEvents,
    result.totalInteractions,
    result.resolvedInteractions,
    result.crossPds,
    result.samePds,
    result.eventsPerSecond,
    JSON.stringify(result.byType),
    JSON.stringify(result.federation),
    JSON.stringify(result.topCrossPdsPairs)
  );

  const crossRate = result.resolvedInteractions > 0
    ? ((result.crossPds / result.resolvedInteractions) * 100).toFixed(1)
    : "N/A";

  const fed = result.federation;
  const trueFederation = fed["bsky-to-third"] + fed["third-to-bsky"] + fed["third-to-third"];
  const trueFedRate = result.resolvedInteractions > 0
    ? ((trueFederation / result.resolvedInteractions) * 100).toFixed(1)
    : "N/A";

  console.log(`\n=== Sample Complete ===`);
  console.log(`  Duration: ${(result.durationMs / 1000).toFixed(1)}s`);
  console.log(`  Events/sec: ${result.eventsPerSecond}`);
  console.log(`  Total events: ${result.totalEvents.toLocaleString()}`);
  console.log(`  Interactions: ${result.totalInteractions.toLocaleString()}`);
  console.log(`  Resolved: ${result.resolvedInteractions.toLocaleString()}`);

  console.log(`\n  Cross-PDS (raw): ${result.crossPds.toLocaleString()} (${crossRate}%)`);
  console.log(`  Same-PDS: ${result.samePds.toLocaleString()}`);

  console.log(`\n  Federation breakdown:`);
  console.log(`    Bluesky internal (cross-shard): ${fed["bsky-internal"].toLocaleString()}`);
  console.log(`    Bluesky → third-party:          ${fed["bsky-to-third"].toLocaleString()}`);
  console.log(`    Third-party → Bluesky:          ${fed["third-to-bsky"].toLocaleString()}`);
  console.log(`    Third-party → third-party:      ${fed["third-to-third"].toLocaleString()}`);
  console.log(`    Same PDS:                       ${fed["same-pds"].toLocaleString()}`);
  console.log(`    True federation rate:            ${trueFedRate}%`);

  console.log(`\n  By type:`);
  for (const [type, stats] of Object.entries(result.byType)) {
    const rate = stats.crossPds + stats.samePds > 0
      ? ((stats.crossPds / (stats.crossPds + stats.samePds)) * 100).toFixed(1)
      : "N/A";
    console.log(
      `    ${type}: ${stats.total} total, ${rate}% cross-PDS`
    );
  }

  if (result.topCrossPdsPairs.length > 0) {
    console.log(`\n  Top cross-PDS pairs:`);
    for (const pair of result.topCrossPdsPairs.slice(0, 10)) {
      const from = pair.from.replace(/^https?:\/\//, "").replace(/\/$/, "");
      const to = pair.to.replace(/^https?:\/\//, "").replace(/\/$/, "");
      console.log(`    ${from} <-> ${to}: ${pair.count}`);
    }
  }

  console.log();
}

main().catch((err) => {
  console.error("Sampling failed:", err);
  process.exit(1);
});
