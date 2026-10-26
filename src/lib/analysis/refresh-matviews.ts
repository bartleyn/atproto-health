/**
 * Refresh the analysis materialized views (migrations/006_analysis_matviews.sql).
 *
 * Run after a collector backfill, or any time the underlying activity tables
 * have moved, before `npm run analysis`:
 *
 *   npm run analysis:matviews
 *
 * Views are refreshed CONCURRENTLY so readers are never blocked — this requires
 * the unique index each view declares. A view that has never been populated
 * cannot be refreshed concurrently, so the first refresh after creation falls
 * back to a plain (locking) refresh.
 *
 * The /analytics page marts (migrations/010_analysis_marts.sql) are NOT here —
 * they're plain tables refreshed incrementally by refresh-analytics-marts.ts
 * (npm run analysis:marts), not full-rebuild materialized views.
 */
import sql from "../db/pg";

const VIEWS = [
  "analysis.mv_delete_events_daily",
  "analysis.mv_did_delete_profile",
  "analysis.mv_post_deletes_daily",
  "analysis.mv_deletion_windows",
];

async function main() {
  for (const view of VIEWS) {
    const started = Date.now();
    try {
      await sql.unsafe(`REFRESH MATERIALIZED VIEW CONCURRENTLY ${view}`);
    } catch (err) {
      // 55000 = object_not_in_prerequisite_state, i.e. never populated.
      if ((err as { code?: string }).code !== "55000") throw err;
      await sql.unsafe(`REFRESH MATERIALIZED VIEW ${view}`);
    }
    const [{ n }] = await sql.unsafe(
      `SELECT COUNT(*)::bigint AS n FROM ${view}`
    ) as { n: number }[];
    const secs = ((Date.now() - started) / 1000).toFixed(1);
    console.log(`  ${view.padEnd(38)} ${n.toLocaleString().padStart(12)} rows  ${secs}s`);
  }

  const windows = await sql`SELECT * FROM analysis.mv_deletion_windows ORDER BY source`;
  console.log("\nDeletion source windows:");
  console.table(windows);

  await sql.end();
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
