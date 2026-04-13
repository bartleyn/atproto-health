/**
 * CLI runner for PLC migration collection.
 *
 * Usage:
 *   npm run collect:plc        # Run (resumes from stored cursor)
 */

import { collectPlcMigrations } from "./plc-migrations";
import { aggregatePlc } from "./aggregate-plc";

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

  console.log("\nAggregating monthly buckets...");
  aggregatePlc();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
