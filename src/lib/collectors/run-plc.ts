/**
 * CLI runner for PLC migration collection.
 *
 * Usage:
 *   npm run collect:plc        # Run (resumes from stored cursor)
 */

import { collectPlcMigrations } from "./plc-migrations";
import { backfillPlcHandles } from "./plc-handles";
import { aggregatePlc, aggregateLangs } from "./aggregate-plc";
import sql from "../db/pg";

async function main() {
  console.log("\n=== PLC Migration Collector ===\n");
  console.log("Streaming PLC directory export — this may take hours on first run.\n");

  const start = Date.now();
  const { opsProcessed, migrationsFound, creationsFound } = await collectPlcMigrations();
  const elapsed = ((Date.now() - start) / 1000 / 60).toFixed(1);

  console.log(`\nDone in ${elapsed}m`);
  console.log(`  Ops processed:    ${opsProcessed.toLocaleString()}`);
  console.log(`  Creations found:  ${creationsFound.toLocaleString()}`);
  console.log(`  Migrations found: ${migrationsFound.toLocaleString()}`);

  // Incrementally refresh DID→handle (resumes from plc_handles_cursor; cheap once
  // backfilled). Restricted to repos we have — see plc-handles.ts.
  console.log("\nUpdating handles (incremental)...");
  const hStart = Date.now();
  const { opsScanned, handlesFound } = await backfillPlcHandles();
  console.log(`  Done in ${((Date.now() - hStart) / 1000 / 60).toFixed(1)}m`);
  console.log(`  Ops scanned:      ${opsScanned.toLocaleString()}`);
  console.log(`  Handles upserted: ${handlesFound.toLocaleString()}`);

  console.log("\nAggregating monthly buckets...");
  await aggregatePlc();

  console.log("\nAggregating languages...");
  await aggregateLangs();

  await sql.end();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
