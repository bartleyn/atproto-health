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

import sql from "../db/pg";

const args = process.argv.slice(2);
const fullRebuild = args.includes("--full");
const excludeStatusIdx = args.indexOf("--exclude-status");
const excludeStatuses: string[] =
  excludeStatusIdx >= 0
    ? args[excludeStatusIdx + 1].split(",").map(s => s.trim()).filter(Boolean)
    : [];

async function main() {
  console.log(`\n=== Active Creation Weekly Aggregation ===`);
  console.log(`Mode: ${fullRebuild ? "full rebuild" : "incremental"}`);
  if (excludeStatuses.length) {
    console.log(`Excluding statuses: ${excludeStatuses.join(", ")}`);
    if (!fullRebuild) {
      console.log(`  Note: --exclude-status implies --full for correctness`);
    }
  }

  const [[didInRepoRow], [creationsRow]] = await Promise.all([
    sql<{ n: number }[]>`SELECT COUNT(*)::int AS n FROM plc.did_in_repo`,
    sql<{ n: number }[]>`SELECT COUNT(*)::int AS n FROM plc.plc_account_creations`,
  ]);

  if (didInRepoRow.n === 0) {
    console.error(`\nERROR: did_in_repo is empty. Run the scanner first:`);
    console.error(`  npm run scan:pds-status -- --include-bsky`);
    process.exit(1);
  }
  if (creationsRow.n === 0) {
    console.error(`\nERROR: plc_account_creations is empty.`);
    process.exit(1);
  }

  const [{ max_scanned_at }] = await sql<{ max_scanned_at: string | null }[]>`
    SELECT MAX(first_scanned_at)::text AS max_scanned_at FROM plc.did_in_repo
  `;

  const doFull = fullRebuild || excludeStatuses.length > 0;

  let cursor = "1970-01-01T00:00:00Z";
  if (!doFull) {
    const [cursorRow] = await sql<{ last_scanned_at: string }[]>`
      SELECT last_scanned_at::text AS last_scanned_at FROM plc.active_creation_cursor WHERE id = 1
    `;
    cursor = cursorRow?.last_scanned_at ?? "1970-01-01T00:00:00Z";
  }

  console.log(`did_in_repo: ${didInRepoRow.n.toLocaleString()} rows (max first_scanned_at: ${max_scanned_at})`);
  console.log(`plc_account_creations: ${creationsRow.n.toLocaleString()} rows`);
  if (!doFull) console.log(`Cursor: ${cursor}`);

  const [{ n: newRows }] = doFull
    ? [{ n: didInRepoRow.n }]
    : await sql<{ n: number }[]>`SELECT COUNT(*)::int AS n FROM plc.did_in_repo WHERE first_scanned_at > ${cursor}`;

  if (!doFull && newRows === 0) {
    console.log(`\nNo new DIDs since last run. Nothing to do.`);
    await sql.end();
    return;
  }
  console.log(`DIDs to process: ${newRows.toLocaleString()}`);

  const excludeFragment = excludeStatuses.length > 0
    ? sql`AND dir.did NOT IN (SELECT did FROM plc.did_repo_status WHERE status = ANY(${excludeStatuses}))`
    : sql``;

  const start = Date.now();

  if (doFull) {
    console.log(`\nTruncating active_creation_weekly...`);
    await sql`DELETE FROM plc.active_creation_weekly`;
  }

  console.log(`Aggregating...`);
  await sql`
    INSERT INTO plc.active_creation_weekly (pds_url, week, count)
    SELECT
      dir.pds_url,
      DATE_TRUNC('week', pac.created_at)::date::text AS week,
      COUNT(*)::int AS count
    FROM plc.did_in_repo dir
    JOIN plc.plc_account_creations pac ON pac.did = dir.did
    WHERE pac.created_at IS NOT NULL
      AND dir.first_scanned_at > ${cursor}
      ${excludeFragment}
    GROUP BY dir.pds_url, week
    ON CONFLICT (pds_url, week) DO UPDATE SET count = active_creation_weekly.count + EXCLUDED.count
  `;

  const elapsed = ((Date.now() - start) / 1000).toFixed(1);

  if (!excludeStatuses.length) {
    const now = new Date().toISOString();
    await sql`
      INSERT INTO plc.active_creation_cursor (id, last_scanned_at, updated_at)
      VALUES (1, ${max_scanned_at ?? now}, NOW())
      ON CONFLICT (id) DO UPDATE SET last_scanned_at = EXCLUDED.last_scanned_at, updated_at = NOW()
    `;
  }

  const [result] = await sql<{ rows: number; total: number }[]>`
    SELECT COUNT(*)::int AS rows, SUM(count)::int AS total FROM plc.active_creation_weekly
  `;

  console.log(`\n=== Done in ${elapsed}s ===`);
  console.log(`  Table rows: ${result.rows.toLocaleString()}`);
  console.log(`  Total active accounts: ${result.total.toLocaleString()}`);

  console.log(`\nTop 10 PDSes by active account count:`);
  const top = await sql<{ pds_url: string; total: number }[]>`
    SELECT pds_url, SUM(count)::int AS total
    FROM plc.active_creation_weekly
    GROUP BY pds_url
    ORDER BY total DESC
    LIMIT 10
  `;
  for (const row of top) {
    console.log(`  ${row.pds_url.replace("https://", "").padEnd(50)} ${row.total.toLocaleString()}`);
  }

  await sql.end();
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
