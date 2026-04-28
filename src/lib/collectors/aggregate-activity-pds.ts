/**
 * Aggregates did_activity_daily × plc_did_pds → pds_activity_summary.
 *
 * Joins the jetstream activity DB with the PLC migrations DB to produce
 * per-PDS unique active DID counts broken down by action type.
 * bsky.network shards are collapsed to the label 'bsky.network'.
 *
 * Run this after aggregate:plc has updated plc_did_pds.
 *
 * Usage:
 *   npx tsx src/lib/collectors/aggregate-activity-pds.ts
 *   npx tsx src/lib/collectors/aggregate-activity-pds.ts --window 7
 */

import { getActivityDb } from "../db/activity-schema";
import path from "path";

const args = process.argv.slice(2);
const windowIdx = args.indexOf("--window");
const WINDOW_DAYS = windowIdx >= 0 ? parseInt(args[windowIdx + 1], 10) : 30;

const PLC_DB_PATH = path.join(process.cwd(), "plc-migrations.db");

async function main() {
  const db = getActivityDb();

  console.log(`\n=== PDS Activity Summary Aggregation ===`);
  console.log(`Window: last ${WINDOW_DAYS} days`);
  console.log(`PLC DB: ${PLC_DB_PATH}`);

  const activityCount = (db.prepare(`SELECT COUNT(*) AS n FROM did_activity_daily`).get() as { n: number }).n;
  if (activityCount === 0) {
    console.error(`\nERROR: did_activity_daily is empty. Run the activity collector first.`);
    process.exit(1);
  }
  console.log(`did_activity_daily: ${activityCount.toLocaleString()} rows`);

  db.exec(`ATTACH DATABASE '${PLC_DB_PATH.replace(/'/g, "''")}' AS plc`);

  const plcCount = (db.prepare(`SELECT COUNT(*) AS n FROM plc.plc_did_pds`).get() as { n: number }).n;
  if (plcCount === 0) {
    console.error(`\nERROR: plc_did_pds is empty. Run aggregate:plc first.`);
    process.exit(1);
  }
  console.log(`plc_did_pds: ${plcCount.toLocaleString()} rows`);

  const start = Date.now();
  console.log(`\nAggregating...`);

  db.prepare(`DELETE FROM pds_activity_summary WHERE window_days = ?`).run(WINDOW_DAYS);

  db.prepare(`
    INSERT INTO pds_activity_summary
      (pds_url, window_days, active_dids, poster_dids, liker_dids, reposter_dids, follower_dids, updated_at)
    SELECT
      CASE
        WHEN p.pds_url LIKE '%bsky.network' OR p.pds_url = 'https://bsky.social'
          THEN 'bsky.network'
        ELSE p.pds_url
      END AS pds_url,
      ? AS window_days,
      COUNT(DISTINCT a.did) AS active_dids,
      COUNT(DISTINCT CASE WHEN a.activity_types & 1  THEN a.did END) AS poster_dids,
      COUNT(DISTINCT CASE WHEN a.activity_types & 2  THEN a.did END) AS liker_dids,
      COUNT(DISTINCT CASE WHEN a.activity_types & 4  THEN a.did END) AS reposter_dids,
      COUNT(DISTINCT CASE WHEN a.activity_types & 8  THEN a.did END) AS follower_dids,
      datetime('now') AS updated_at
    FROM did_activity_daily a
    JOIN plc.plc_did_pds p ON a.did = p.did
    WHERE a.date >= date('now', '-' || ? || ' days')
    GROUP BY 1
  `).run(WINDOW_DAYS, WINDOW_DAYS);

  const elapsed = ((Date.now() - start) / 1000).toFixed(1);

  const result = db.prepare(
    `SELECT COUNT(*) AS rows, SUM(active_dids) AS total FROM pds_activity_summary WHERE window_days = ?`
  ).get(WINDOW_DAYS) as { rows: number; total: number };

  console.log(`\n=== Done in ${elapsed}s ===`);
  console.log(`  PDSes: ${result.rows.toLocaleString()}`);
  console.log(`  Total active DIDs: ${result.total.toLocaleString()}`);

  console.log(`\nTop 15 PDSes by active DIDs (last ${WINDOW_DAYS} days):`);
  const top = db.prepare(`
    SELECT pds_url, active_dids, poster_dids, liker_dids
    FROM pds_activity_summary
    WHERE window_days = ?
    ORDER BY active_dids DESC
    LIMIT 15
  `).all(WINDOW_DAYS) as { pds_url: string; active_dids: number; poster_dids: number; liker_dids: number }[];

  for (const row of top) {
    const postPct = row.active_dids > 0 ? ((row.poster_dids / row.active_dids) * 100).toFixed(0) : "0";
    const likePct = row.active_dids > 0 ? ((row.liker_dids / row.active_dids) * 100).toFixed(0) : "0";
    console.log(
      `  ${row.pds_url.replace("https://", "").padEnd(50)} ` +
      `active=${row.active_dids.toLocaleString().padStart(8)} ` +
      `post=${postPct}% like=${likePct}%`
    );
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
