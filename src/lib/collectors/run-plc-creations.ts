/**
 * CLI runner for PLC account creation backfill.
 *
 * Usage:
 *   npm run collect:plc:creations
 */

import { backfillPlcCreations } from "./plc-creations";

async function main() {
  console.log("\n=== PLC Account Creations Backfill ===\n");
  console.log("Scanning PLC export for creation ops — this may take hours on first run.\n");

  const start = Date.now();
  const { opsScanned, creationsFound } = await backfillPlcCreations();
  const elapsed = ((Date.now() - start) / 1000 / 60).toFixed(1);

  console.log(`\nDone in ${elapsed}m`);
  console.log(`  Ops scanned:      ${opsScanned.toLocaleString()}`);
  console.log(`  Creations found:  ${creationsFound.toLocaleString()}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
