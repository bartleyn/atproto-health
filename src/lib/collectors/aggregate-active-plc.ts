/**
 * One-time (re-runnable) aggregation: joins did_in_repo with plc_account_creations
 * to produce active_creation_weekly — weekly counts of accounts that have real repos,
 * keyed by the week they first appeared in the PLC directory.
 *
 * By default counts all accounts with repos regardless of status (takendown, deleted, etc.
 * are still real accounts). Use --exclude-status to filter specific statuses if needed.
 *
 * Run AFTER a full did_in_repo scan (npm run scan:pds-status -- --include-bsky).
 * Safe to re-run: truncates and rebuilds the table each time.
 *
 * Usage:
 *   npx tsx src/lib/collectors/aggregate-active-plc.ts
 *   npx tsx src/lib/collectors/aggregate-active-plc.ts --exclude-status takendown,suspended
 */

import { getPlcDb } from "../db/plc-schema";

const args = process.argv.slice(2);
const excludeStatusIdx = args.indexOf("--exclude-status");
const excludeStatuses: string[] =
  excludeStatusIdx >= 0
    ? args[excludeStatusIdx + 1].split(",").map(s => s.trim()).filter(Boolean)
    : [];

// SQLite expression: Monday of the ISO week containing a datetime column.
const WEEK_EXPR = `date(pac.created_at, '-' || ((strftime('%w', pac.created_at) + 6) % 7) || ' days')`;

async function main() {
  const db = getPlcDb();

  console.log(`\n=== Active Creation Weekly Aggregation ===`);
  console.log(`Excluding statuses: ${excludeStatuses.join(", ")}`);

  // Verify prerequisites
  const didInRepoCount = (db.prepare(`SELECT COUNT(*) AS n FROM did_in_repo`).get() as { n: number }).n;
  if (didInRepoCount === 0) {
    console.error(`\nERROR: did_in_repo is empty. Run the scanner first:`);
    console.error(`  npm run scan:pds-status -- --include-bsky`);
    process.exit(1);
  }
  console.log(`did_in_repo:          ${didInRepoCount.toLocaleString()} rows`);

  const creationsCount = (db.prepare(`SELECT COUNT(*) AS n FROM plc_account_creations`).get() as { n: number }).n;
  if (creationsCount === 0) {
    console.error(`\nERROR: plc_account_creations is empty. Cannot derive creation dates.`);
    process.exit(1);
  }
  console.log(`plc_account_creations: ${creationsCount.toLocaleString()} rows`);

  const nonActiveCount = (db.prepare(`SELECT COUNT(*) AS n FROM did_repo_status`).get() as { n: number }).n;
  console.log(`did_repo_status:       ${nonActiveCount.toLocaleString()} rows (will be excluded)`);

  // Build exclusion clause
  const placeholders = excludeStatuses.map(() => "?").join(", ");
  const excludeClause = excludeStatuses.length > 0
    ? `AND dir.did NOT IN (
         SELECT did FROM did_repo_status WHERE status IN (${placeholders})
       )`
    : "";

  console.log(`\nTruncating active_creation_weekly...`);
  db.exec(`DELETE FROM active_creation_weekly`);

  console.log(`Aggregating (this may take several minutes)...`);
  const start = Date.now();

  db.prepare(`
    INSERT INTO active_creation_weekly (pds_url, week, count)
    SELECT
      dir.pds_url,
      ${WEEK_EXPR} AS week,
      COUNT(*) AS count
    FROM did_in_repo dir
    JOIN plc_account_creations pac ON pac.did = dir.did
    WHERE pac.created_at IS NOT NULL
      ${excludeClause}
    GROUP BY dir.pds_url, week
  `).run(...excludeStatuses);

  const elapsed = ((Date.now() - start) / 1000).toFixed(1);
  const result = db.prepare(`SELECT COUNT(*) AS rows, SUM(count) AS total FROM active_creation_weekly`).get() as { rows: number; total: number };

  console.log(`\n=== Done in ${elapsed}s ===`);
  console.log(`  Rows written: ${result.rows.toLocaleString()}`);
  console.log(`  Total active accounts accounted for: ${result.total.toLocaleString()}`);

  // Quick sanity check: top PDSes
  console.log(`\nTop 10 PDSes by active account count:`);
  const top = db.prepare(`
    SELECT pds_url, SUM(count) AS total
    FROM active_creation_weekly
    GROUP BY pds_url
    ORDER BY total DESC
    LIMIT 10
  `).all() as { pds_url: string; total: number }[];
  for (const row of top) {
    console.log(`  ${row.pds_url.replace("https://", "").padEnd(50)} ${row.total.toLocaleString()}`);
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
