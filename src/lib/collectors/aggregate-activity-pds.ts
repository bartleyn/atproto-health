/**
 * Aggregates did_activity_daily × plc_did_pds → pds_activity_summary.
 *
 * Joins activity.did_activity_daily with plc.plc_did_pds (cross-schema, no ATTACH needed).
 * bsky.network shards are collapsed to the label 'bsky.network'.
 *
 * Run this after aggregate:plc has updated plc_did_pds.
 *
 * Usage:
 *   npm run aggregate:activity-pds
 *   npm run aggregate:activity-pds -- --window 7
 */

import sql from "../db/pg";

const args = process.argv.slice(2);
const windowIdx = args.indexOf("--window");
const WINDOW_DAYS = windowIdx >= 0 ? parseInt(args[windowIdx + 1], 10) : 30;

async function main() {
  console.log(`\n=== PDS Activity Summary Aggregation ===`);
  console.log(`Window: last ${WINDOW_DAYS} days`);

  const [[{ n: activityCount }], [{ n: plcCount }]] = await Promise.all([
    sql<{ n: number }[]>`SELECT COUNT(*)::int AS n FROM activity.did_activity_daily`,
    sql<{ n: number }[]>`SELECT COUNT(*)::int AS n FROM plc.plc_did_pds`,
  ]);

  if (activityCount === 0) {
    console.error(`\nERROR: did_activity_daily is empty. Run the activity collector first.`);
    process.exit(1);
  }
  if (plcCount === 0) {
    console.error(`\nERROR: plc_did_pds is empty. Run aggregate:plc first.`);
    process.exit(1);
  }
  console.log(`did_activity_daily: ${activityCount.toLocaleString()} rows`);
  console.log(`plc_did_pds: ${plcCount.toLocaleString()} rows`);

  const start = Date.now();
  console.log(`\nAggregating...`);

  await sql`DELETE FROM activity.pds_activity_summary WHERE window_days = ${WINDOW_DAYS}`;

  // did_activity_daily.date is text (YYYY-MM-DD); cast CURRENT_DATE arithmetic to text for comparison.
  await sql`
    INSERT INTO activity.pds_activity_summary
      (pds_url, window_days, active_dids, poster_dids, liker_dids, reposter_dids, follower_dids, updated_at)
    SELECT
      CASE
        WHEN p.pds_url LIKE '%bsky.network' OR p.pds_url = 'https://bsky.social'
          THEN 'bsky.network'
        ELSE p.pds_url
      END AS pds_url,
      ${WINDOW_DAYS} AS window_days,
      COUNT(DISTINCT a.did)::int                                                  AS active_dids,
      COUNT(DISTINCT CASE WHEN a.activity_types & 1 != 0 THEN a.did END)::int    AS poster_dids,
      COUNT(DISTINCT CASE WHEN a.activity_types & 2 != 0 THEN a.did END)::int    AS liker_dids,
      COUNT(DISTINCT CASE WHEN a.activity_types & 4 != 0 THEN a.did END)::int    AS reposter_dids,
      COUNT(DISTINCT CASE WHEN a.activity_types & 8 != 0 THEN a.did END)::int    AS follower_dids,
      NOW()                                                                       AS updated_at
    FROM activity.did_activity_daily a
    JOIN plc.plc_did_pds p ON a.did = p.did
    WHERE a.date >= (CURRENT_DATE - ${WINDOW_DAYS}::int)::text
    GROUP BY 1
  `;

  const elapsed = ((Date.now() - start) / 1000).toFixed(1);

  const [result] = await sql<{ rows: number; total: number }[]>`
    SELECT COUNT(*)::int AS rows, SUM(active_dids)::int AS total
    FROM activity.pds_activity_summary WHERE window_days = ${WINDOW_DAYS}
  `;

  console.log(`\n=== Done in ${elapsed}s ===`);
  console.log(`  PDSes: ${result.rows.toLocaleString()}`);
  console.log(`  Total active DIDs: ${result.total.toLocaleString()}`);

  console.log(`\nTop 15 PDSes by active DIDs (last ${WINDOW_DAYS} days):`);
  const top = await sql<{ pds_url: string; active_dids: number; poster_dids: number; liker_dids: number }[]>`
    SELECT pds_url, active_dids, poster_dids, liker_dids
    FROM activity.pds_activity_summary
    WHERE window_days = ${WINDOW_DAYS}
    ORDER BY active_dids DESC
    LIMIT 15
  `;
  for (const row of top) {
    const postPct = row.active_dids > 0 ? ((row.poster_dids / row.active_dids) * 100).toFixed(0) : "0";
    const likePct = row.active_dids > 0 ? ((row.liker_dids / row.active_dids) * 100).toFixed(0) : "0";
    console.log(
      `  ${row.pds_url.replace("https://", "").padEnd(50)} ` +
      `active=${row.active_dids.toLocaleString().padStart(8)} ` +
      `post=${postPct}% like=${likePct}%`
    );
  }

  await sql.end();
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
