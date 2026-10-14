/**
 * Incrementally refreshes the /analytics page marts (migrations/010_analysis_marts.sql):
 *   analysis.wau_by_cohort, analysis.dau_mau_daily, analysis.retention_cohorts.
 *
 * Run weekly (see the refresh-analytics-marts launchd job), then
 * `npm run analysis:cache` to publish the results:
 *
 *   npm run analysis:marts
 *
 */
import sql from "../db/pg";

const HISTORY_DAYS = 180;          // trailing window retained in dau_mau_daily
const DAU_MAU_REFRESH_DAYS = 10;   // trailing window recomputed each run (new dates + late-data correction)
const WAU_REFRESH_WEEKS = 3;       // trailing weeks recomputed each run
const RETENTION_REFRESH_DAYS = 10; // trailing creation-date window recomputed each run

function addDays(dateStr: string, n: number): string {
  const d = new Date(dateStr + "T00:00:00Z");
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}

async function refreshWauByCohort() {
  const [{ max_week }] = await sql<{ max_week: string | null }[]>`
    SELECT MAX(week_start)::text AS max_week FROM analysis.wau_by_cohort
  `;
  let since: string;
  if (max_week) {
    since = addDays(max_week, -WAU_REFRESH_WEEKS * 7);
  } else {
    const [{ min_date }] = await sql<{ min_date: string }[]>`
      SELECT MIN(date) AS min_date FROM activity.did_activity_daily
    `;
    since = min_date;
  }

  console.log(`wau_by_cohort: recomputing weeks since ${max_week ?? "(full history — first run)"} (from ${since})`);
  const started = Date.now();

  // Widen the delete bound by a week so it always covers every week_start the insert can produce.
  const deleteSince = addDays(since, -7);

  await sql.begin(async tx => {
    const del1 = await tx`
      DELETE FROM analysis.wau_by_cohort
      WHERE week_start >= ${deleteSince}::date
    `;
    console.log(`  delete (pre-insert window clear): ${del1.count} rows`);
    const ins = await tx`
      INSERT INTO analysis.wau_by_cohort (week_start, age_bucket, wau)
      SELECT
        (d.date::date + ((8 - EXTRACT(DOW FROM d.date::date)::int) % 7) - 7) AS week_start,
        CASE
          WHEN p.created_at >= (CURRENT_DATE - 14)                THEN '0. new accounts (last 14d)'
          WHEN p.created_at < '2023-01-01'                        THEN '1. pre-2023'
          WHEN p.created_at < '2024-01-01'                        THEN '2. 2023'
          WHEN p.created_at < '2024-11-01'                        THEN '3. 2024 pre-Nov'
          WHEN p.created_at < '2025-01-01'                        THEN '4. 2024 Nov-Dec (exodus)'
          WHEN p.created_at < '2025-07-01'                        THEN '5. 2025 H1'
          WHEN p.created_at < '2026-01-01'                        THEN '6. 2025 H2'
          WHEN p.created_at < '2026-04-01'                        THEN '7. 2026 Q1 (Jan-Mar)'
          ELSE                                                          '8. 2026 Q2 (Apr-Jun)'
        END AS age_bucket,
        COUNT(DISTINCT d.did)::int AS wau
      FROM activity.did_activity_daily d
      JOIN analysis.cohort_base p ON d.did = p.did
      LEFT JOIN analysis.excluded_dids ex ON d.did = ex.did
      WHERE ex.did IS NULL
        AND d.date >= ${since}
      GROUP BY week_start, age_bucket
    `;
    console.log(`  insert: ${ins.count} rows`);
  });

  const [{ n }] = await sql<{ n: number }[]>`SELECT COUNT(*)::int AS n FROM analysis.wau_by_cohort`;
  console.log(`  done in ${((Date.now() - started) / 1000).toFixed(1)}s — ${n.toLocaleString()} rows total`);
}

async function refreshDauMauDaily() {
  const [{ max_date }] = await sql<{ max_date: string | null }[]>`
    SELECT MAX(date) AS max_date FROM analysis.dau_mau_daily
  `;
  let since: string;
  if (max_date) {
    since = addDays(max_date, -DAU_MAU_REFRESH_DAYS);
  } else {
    const [{ min_date }] = await sql<{ min_date: string }[]>`
      SELECT MIN(date) AS min_date FROM activity.did_activity_daily
    `;
    since = min_date;
  }

  console.log(`dau_mau_daily: recomputing dates since ${max_date ?? "(full history — first run, this takes hours)"} (from ${since})`);
  const started = Date.now();

  await sql.begin(async tx => {
    const del1 = await tx`
      DELETE FROM analysis.dau_mau_daily
      WHERE date >= ${since}
    `;
    console.log(`  delete (pre-insert window clear): ${del1.count} rows`);
    // LATERAL forces Postgres to plan the MAU window as a per-date index range
    // scan (Index Cond) instead of materializing + nested-looping the whole
    // 195M-row table per date (Join Filter)
    const ins = await tx`
      INSERT INTO analysis.dau_mau_daily (date, dau, mau, dau_mau_pct)
      SELECT
        dau.date,
        dau.dau,
        mau.cnt AS mau,
        ROUND(100.0 * dau.dau / mau.cnt, 2)::float8 AS dau_mau_pct
      FROM (
        SELECT d.date, COUNT(DISTINCT d.did)::int AS dau
        FROM activity.did_activity_daily d
        LEFT JOIN analysis.excluded_dids ex ON d.did = ex.did
        WHERE ex.did IS NULL
          AND d.date >= ${since}
        GROUP BY d.date
      ) dau
      CROSS JOIN LATERAL (
        SELECT COUNT(DISTINCT d.did)::int AS cnt
        FROM activity.did_activity_daily d
        LEFT JOIN analysis.excluded_dids ex ON d.did = ex.did
        WHERE ex.did IS NULL
          AND d.date BETWEEN (dau.date::date - 27)::text AND dau.date
      ) mau
    `;
    console.log(`  insert: ${ins.count} rows`);
    const [{ new_max }] = await tx<{ new_max: string }[]>`
      SELECT MAX(date) AS new_max FROM analysis.dau_mau_daily
    `;
    const pruneThreshold = addDays(new_max, -HISTORY_DAYS);
    console.log(`  max_date after insert: ${new_max}, prune threshold: ${pruneThreshold}`);
    const del2 = await tx`
      DELETE FROM analysis.dau_mau_daily
      WHERE date < ${pruneThreshold}
    `;
    console.log(`  delete (retention prune): ${del2.count} rows`);
  });

  const [{ n }] = await sql<{ n: number }[]>`SELECT COUNT(*)::int AS n FROM analysis.dau_mau_daily`;
  console.log(`  done in ${((Date.now() - started) / 1000).toFixed(1)}s — ${n.toLocaleString()} rows retained (trailing ${HISTORY_DAYS}d)`);
}

async function refreshRetentionCohorts() {
  const [{ max_creation }] = await sql<{ max_creation: string | null }[]>`
    SELECT MAX(creation_date)::text AS max_creation FROM analysis.retention_cohorts
  `;
  const [{ min_date, max_date }] = await sql<{ min_date: string; max_date: string }[]>`
    SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM activity.did_activity_daily
  `;
  // D7 requires 7 days of activity data past creation, so the last eligible
  // creation_date is max_date - 7.
  const boundEnd = addDays(max_date, -7);
  const since = max_creation ? addDays(max_creation, -RETENTION_REFRESH_DAYS) : min_date;

  console.log(`retention_cohorts: recomputing creation dates since ${max_creation ?? "(full history — first run)"} (from ${since} through ${boundEnd})`);
  const started = Date.now();

  await sql.begin(async tx => {
    const del1 = await tx`
      DELETE FROM analysis.retention_cohorts
      WHERE creation_date >= ${since}::date
    `;
    console.log(`  delete (pre-insert window clear): ${del1.count} rows`);
    const ins = await tx`
      INSERT INTO analysis.retention_cohorts
        (creation_date, pds_type, cohort_size, d1_n, d1_pct, d3_n, d3_pct, d7_n, d7_pct)
      WITH cohorts AS (
        SELECT c.created_at::date AS creation_date, c.did, c.pds_type
        FROM analysis.cohort_base c
        WHERE c.created_at::date BETWEEN ${since}::date AND ${boundEnd}::date
      ),
      joined AS (
        SELECT
          c.creation_date, c.did, c.pds_type,
          MAX(CASE WHEN a.date = (c.creation_date + 1)::text THEN 1 ELSE 0 END) AS d1,
          MAX(CASE WHEN a.date = (c.creation_date + 3)::text THEN 1 ELSE 0 END) AS d3,
          MAX(CASE WHEN a.date = (c.creation_date + 7)::text THEN 1 ELSE 0 END) AS d7
        FROM cohorts c
        LEFT JOIN activity.did_activity_daily a
          ON a.did = c.did
          AND a.date IN ((c.creation_date + 1)::text, (c.creation_date + 3)::text, (c.creation_date + 7)::text)
        GROUP BY c.creation_date, c.did, c.pds_type
      )
      SELECT
        creation_date, pds_type, COUNT(*)::int,
        SUM(d1)::int, ROUND(100.0 * SUM(d1) / COUNT(*), 2)::float8,
        SUM(d3)::int, ROUND(100.0 * SUM(d3) / COUNT(*), 2)::float8,
        SUM(d7)::int, ROUND(100.0 * SUM(d7) / COUNT(*), 2)::float8
      FROM joined
      GROUP BY creation_date, pds_type
      UNION ALL
      SELECT
        creation_date, '~ total', COUNT(*)::int,
        SUM(d1)::int, ROUND(100.0 * SUM(d1) / COUNT(*), 2)::float8,
        SUM(d3)::int, ROUND(100.0 * SUM(d3) / COUNT(*), 2)::float8,
        SUM(d7)::int, ROUND(100.0 * SUM(d7) / COUNT(*), 2)::float8
      FROM joined
      GROUP BY creation_date
    `;
    console.log(`  insert: ${ins.count} rows`);
  });

  const [{ n }] = await sql<{ n: number }[]>`SELECT COUNT(*)::int AS n FROM analysis.retention_cohorts`;
  console.log(`  done in ${((Date.now() - started) / 1000).toFixed(1)}s — ${n.toLocaleString()} rows total`);
}

async function main() {
  await refreshWauByCohort();
  await refreshDauMauDaily();
  await refreshRetentionCohorts();
  await sql.end();
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
