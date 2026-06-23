/**
 * CLI runner for the PLC handle backfill.
 *
 * Usage:
 *   npm run collect:plc:handles
 */

import { backfillPlcHandles } from "./plc-handles";
import sql from "../db/pg";

async function main() {
  console.log("\n=== PLC Handle Backfill ===\n");
  console.log("Scanning PLC export for alsoKnownAs handles — this may take hours on first run.\n");

  const start = Date.now();
  const { opsScanned, handlesFound } = await backfillPlcHandles();
  const elapsed = ((Date.now() - start) / 1000 / 60).toFixed(1);

  console.log(`\nDone in ${elapsed}m`);
  console.log(`  Ops scanned:    ${opsScanned.toLocaleString()}`);
  console.log(`  Handles upserted: ${handlesFound.toLocaleString()}`);

  await sql.end();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
