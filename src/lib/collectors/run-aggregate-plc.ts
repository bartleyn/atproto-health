/**
 * CLI runner for PLC aggregation.
 *
 * Usage:
 *   npm run aggregate:plc
 */

import { aggregatePlc, aggregateLangs } from "./aggregate-plc";

function timed(label: string, fn: () => void) {
  console.log(`\n=== ${label} ===`);
  const t0 = Date.now();
  fn();
  const s = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(`  done in ${s}s`);
}

timed("PLC Aggregation", aggregatePlc);
timed("Language Aggregation", aggregateLangs);

console.log("\nDone.\n");
