/**
 * Aggregates did_in_repo × plc_account_creations → active_creation_weekly.
 *
 * Incremental by default: only processes DIDs whose scanned_at is newer than the
 * last run's cursor. Use --full to rebuild from scratch (e.g. after changing
 * exclude-status or after a structural schema change).
 *
 * Usage:
 *   npx tsx src/lib/collectors/aggregate-active-plc.ts
 *   npx tsx src/lib/collectors/aggregate-active-plc.ts --full
 *   npx tsx src/lib/collectors/aggregate-active-plc.ts --exclude-status takendown,suspended
 */

import { getPlcDb } from "../db/plc-schema";

const args = process.argv.slice(2);
const fullRebuild = args.includes("--full");
const excludeStatusIdx = args.indexOf("--exclude-status");
const excludeStatuses: string[] =
  excludeStatusIdx >= 0
    ? args[excludeStatusIdx + 1].split(",").map(s => s.trim()).filter(Boolean)
    : [];

const WEEK_EXPR = `date(pac.created_at, '-' || ((strftime('%w', pac.created_at) + 6) % 7) || ' days')`;

async function main() {
  const db = getPlcDb();

  console.log(`\n=== Active Creation Weekly Aggregation ===`);
  console.log(`Mode: ${fullRebuild ? "full rebuild" : "incremental"}`);
  if (excludeStatuses.length) {
    console.log(`Excluding statuses: ${excludeStatuses.join(", ")}`);
    if (!fullRebuild) {
      console.log(`  Note: --exclude-status implies --full for correctness`);
    }
  }

  const didInRepoCount = (db.prepare(`SELECT COUNT(*) AS n FROM did_in_repo`).get() as { n: number }).n;
  if (didInRepoCount === 0) {
    console.error(`\nERROR: did_in_repo is empty. Run the scanner first:`);
    console.error(`  npm run scan:pds-status -- --include-bsky`);
    process.exit(1);
  }

  const creationsCount = (db.prepare(`SELECT COUNT(*) AS n FROM plc_account_creations`).get() as { n: number }).n;
  if (creationsCount === 0) {
    console.error(`\nERROR: plc_account_creations is empty.`);
    process.exit(1);
  }

  const { max_scanned_at } = db.prepare(
    `SELECT MAX(scanned_at) AS max_scanned_at FROM did_in_repo`
  ).get() as { max_scanned_at: string };

  // When exclude-status is in use, always do a full rebuild for correctness:
  // incremental additions can't account for newly takendown DIDs that were already counted.
  const doFull = fullRebuild || excludeStatuses.length > 0;

  let cursor = "1970-01-01T00:00:00Z";
  if (!doFull) {
    const row = db
      .prepare(`SELECT last_scanned_at FROM active_creation_cursor WHERE id = 1`)
      .get() as { last_scanned_at: string } | undefined;
    cursor = row?.last_scanned_at ?? "1970-01-01T00:00:00Z";
  }

  console.log(`did_in_repo: ${didInRepoCount.toLocaleString()} rows (max scanned_at: ${max_scanned_at})`);
  console.log(`plc_account_creations: ${creationsCount.toLocaleString()} rows`);
  if (!doFull) console.log(`Cursor: ${cursor}`);

  const newRows = doFull ? didInRepoCount : (db.prepare(
    `SELECT COUNT(*) AS n FROM did_in_repo WHERE scanned_at > ?`
  ).get(cursor) as { n: number }).n;

  if (!doFull && newRows === 0) {
    console.log(`\nNo new DIDs since last run. Nothing to do.`);
    return;
  }
  console.log(`DIDs to process: ${newRows.toLocaleString()}`);

  const placeholders = excludeStatuses.map(() => "?").join(", ");
  const excludeClause = excludeStatuses.length > 0
    ? `AND dir.did NOT IN (SELECT did FROM did_repo_status WHERE status IN (${placeholders}))`
    : "";

  const start = Date.now();

  if (doFull) {
    console.log(`\nTruncating active_creation_weekly...`);
    db.exec(`DELETE FROM active_creation_weekly`);
  }

  console.log(`Aggregating...`);
  db.prepare(`
    INSERT INTO active_creation_weekly (pds_url, week, count)
    SELECT
      dir.pds_url,
      ${WEEK_EXPR} AS week,
      COUNT(*) AS count
    FROM did_in_repo dir
    JOIN plc_account_creations pac ON pac.did = dir.did
    WHERE pac.created_at IS NOT NULL
      AND dir.scanned_at > ?
      ${excludeClause}
    GROUP BY dir.pds_url, week
    ON CONFLICT (pds_url, week) DO UPDATE SET count = count + excluded.count
  `).run(doFull ? "1970-01-01T00:00:00Z" : cursor, ...excludeStatuses);

  const elapsed = ((Date.now() - start) / 1000).toFixed(1);

  // Update cursor only when not using exclude-status (full rebuild with exclusions
  // can't leave a valid partial cursor).
  if (!excludeStatuses.length) {
    const now = new Date().toISOString();
    db.prepare(`
      INSERT INTO active_creation_cursor (id, last_scanned_at, updated_at)
      VALUES (1, ?, ?)
      ON CONFLICT (id) DO UPDATE SET last_scanned_at = excluded.last_scanned_at, updated_at = excluded.updated_at
    `).run(max_scanned_at, now);
  }

  const result = db.prepare(
    `SELECT COUNT(*) AS rows, SUM(count) AS total FROM active_creation_weekly`
  ).get() as { rows: number; total: number };

  console.log(`\n=== Done in ${elapsed}s ===`);
  console.log(`  Table rows: ${result.rows.toLocaleString()}`);
  console.log(`  Total active accounts: ${result.total.toLocaleString()}`);

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
