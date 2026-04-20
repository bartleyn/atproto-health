import Database from "better-sqlite3";
import path from "path";
import fs from "fs";

const args = process.argv.slice(2);
const daysBack = args[0] ? parseInt(args[0], 10) : 3;

const OUT_DIR = path.join(process.cwd(), "analysis-output");
fs.mkdirSync(OUT_DIR, { recursive: true });

// ── Helpers ───────────────────────────────────────────────────────────────────

function printTable(title: string, rows: Record<string, unknown>[]) {
  console.log(`\n=== ${title} ===`);
  if (rows.length === 0) { console.log("No data"); return; }
  console.table(rows);
}

function writeCsv(filename: string, rows: Record<string, unknown>[]) {
  if (rows.length === 0) return;
  const headers = Object.keys(rows[0]);
  const lines = [
    headers.join(","),
    ...rows.map(r =>
      headers.map(h => {
        const v = r[h];
        const s = v === null || v === undefined ? "" : String(v);
        return s.includes(",") || s.includes('"') || s.includes("\n")
          ? `"${s.replace(/"/g, '""')}"` : s;
      }).join(",")
    ),
  ];
  const outPath = path.join(OUT_DIR, filename);
  fs.writeFileSync(outPath, lines.join("\n") + "\n");
  console.log(`  → wrote ${outPath}`);
}

function report(title: string, filename: string, rows: Record<string, unknown>[]) {
  printTable(title, rows);
  writeCsv(filename, rows);
}

// ── DB setup ──────────────────────────────────────────────────────────────────

const activityDb = new Database(path.join(process.cwd(), "jetstream-activity.db"), { readonly: true });
const plcDb = new Database(path.join(process.cwd(), "plc-migrations.db"), { readonly: true });
activityDb.exec(`ATTACH DATABASE '${plcDb.name}' as plc`);

// ── Shared SQL fragments ──────────────────────────────────────────────────────

const AGE_BUCKET = `CASE
  WHEN p.created_at >= date('now', '-7 days') THEN '0. last 7 days'
  WHEN p.created_at < '2023-01-01'            THEN '1. pre-2023'
  WHEN p.created_at < '2024-01-01'            THEN '2. 2023'
  WHEN p.created_at < '2024-11-01'            THEN '3. 2024 pre-Nov'
  WHEN p.created_at < '2025-01-01'            THEN '4. 2024 Nov-Dec (exodus)'
  WHEN p.created_at < '2025-07-01'            THEN '5. 2025 H1'
  WHEN p.created_at < '2026-01-01'            THEN '6. 2025 H2'
  ELSE                                             '7. 2026'
END`;

const ACTIVITY_COLS = `
  COUNT(*)                                              AS total,
  SUM(CASE WHEN activity_types & 1 THEN 1 ELSE 0 END)  AS posted,
  SUM(CASE WHEN activity_types & 2 THEN 1 ELSE 0 END)  AS liked,
  SUM(CASE WHEN activity_types & 4 THEN 1 ELSE 0 END)  AS reposted,
  SUM(CASE WHEN activity_types & 8 THEN 1 ELSE 0 END)  AS followed`;

console.log(`\nActivity crosstabs — window: last ${daysBack} days`);

// ── 1. Activity by age bucket ─────────────────────────────────────────────────

const activityByAge = activityDb.prepare(`
  WITH active AS (
    SELECT DISTINCT did, activity_types
    FROM did_activity_daily
    WHERE date >= date('now', '-' || ? || ' days')
  ),
  buckets AS (
    SELECT
      ${AGE_BUCKET} AS age_bucket,
      a.activity_types
    FROM active a
    JOIN plc.plc_account_creations p ON a.did = p.did
  )
  SELECT age_bucket, ${ACTIVITY_COLS}
  FROM buckets GROUP BY age_bucket
  UNION ALL
  SELECT '~ TOTAL', ${ACTIVITY_COLS}
  FROM buckets
  ORDER BY age_bucket
`).all(daysBack) as Record<string, unknown>[];

report("Activity by Age Bucket", `activity_by_age_${daysBack}d.csv`, activityByAge);

// ── 2. Activity by Skywatch label ─────────────────────────────────────────────

const activityByLabel = activityDb.prepare(`
  WITH active AS (
    SELECT DISTINCT did, activity_types
    FROM did_activity_daily
    WHERE date >= date('now', '-' || ? || ' days')
  ),
  labeled AS (
    SELECT sl.label, a.activity_types
    FROM active a
    JOIN plc.skywatch_labels sl ON a.did = sl.did
  )
  SELECT label, ${ACTIVITY_COLS}
  FROM labeled GROUP BY label
  UNION ALL
  SELECT '~ TOTAL', ${ACTIVITY_COLS}
  FROM labeled
  ORDER BY total DESC
`).all(daysBack) as Record<string, unknown>[];

report("Activity by Skywatch Label", `activity_by_label_${daysBack}d.csv`, activityByLabel);

// ── 3. Overall stickiness ─────────────────────────────────────────────────────

const stickiness = activityDb.prepare(`
  WITH daily_counts AS (
    SELECT date, COUNT(DISTINCT did) AS daily_uniques
    FROM did_activity_daily
    WHERE date >= date('now', '-' || ? || ' days')
    GROUP BY date
  ),
  total_unique AS (
    SELECT COUNT(DISTINCT did) AS total_uniques
    FROM did_activity_daily
    WHERE date >= date('now', '-' || ? || ' days')
  )
  SELECT
    ROUND(AVG(daily_uniques), 0)                                                                   AS avg_daily_uniques,
    ROUND(SQRT(AVG(daily_uniques * daily_uniques) - AVG(daily_uniques) * AVG(daily_uniques)), 1)   AS stddev_daily_uniques,
    total_uniques,
    ROUND(1.0 * AVG(daily_uniques) / total_uniques, 3)                                             AS ratio,
    ROUND(SQRT(AVG(daily_uniques * daily_uniques) - AVG(daily_uniques) * AVG(daily_uniques)) / total_uniques, 4) AS stddev_ratio
  FROM daily_counts, total_unique
`).get(daysBack, daysBack) as Record<string, unknown>;

report("Stickiness", `stickiness_${daysBack}d.csv`, [stickiness]);

// ── 4. Stickiness by age bucket ───────────────────────────────────────────────

const stickinessByBucket = activityDb.prepare(`
  WITH daily_by_bucket AS (
    SELECT
      d.date,
      ${AGE_BUCKET} AS age_bucket,
      COUNT(DISTINCT d.did) AS daily_uniques
    FROM did_activity_daily d
    JOIN plc.plc_account_creations p ON d.did = p.did
    WHERE d.date >= date('now', '-' || ? || ' days')
    GROUP BY d.date, age_bucket
  ),
  totals AS (
    SELECT
      ${AGE_BUCKET} AS age_bucket,
      COUNT(DISTINCT d.did) AS total_uniques
    FROM did_activity_daily d
    JOIN plc.plc_account_creations p ON d.did = p.did
    WHERE d.date >= date('now', '-' || ? || ' days')
    GROUP BY age_bucket
  )
  SELECT
    d.age_bucket,
    ROUND(AVG(d.daily_uniques), 0)                                                                          AS avg_daily_uniques,
    ROUND(SQRT(AVG(d.daily_uniques * d.daily_uniques) - AVG(d.daily_uniques) * AVG(d.daily_uniques)), 1)   AS stddev_daily_uniques,
    t.total_uniques,
    ROUND(1.0 * AVG(d.daily_uniques) / t.total_uniques, 3)                                                 AS ratio,
    ROUND(SQRT(AVG(d.daily_uniques * d.daily_uniques) - AVG(d.daily_uniques) * AVG(d.daily_uniques)) / t.total_uniques, 4) AS stddev_ratio
  FROM daily_by_bucket d
  JOIN totals t ON d.age_bucket = t.age_bucket
  GROUP BY d.age_bucket
  ORDER BY d.age_bucket
`).all(daysBack, daysBack) as Record<string, unknown>[];

report("Stickiness by Age Bucket", `stickiness_by_age_${daysBack}d.csv`, stickinessByBucket);

// ── 5. Active days distribution × age bucket ──────────────────────────────────

const activeDaysDist = activityDb.prepare(`
  WITH user_days AS (
    SELECT
      d.did,
      ${AGE_BUCKET} AS age_bucket,
      COUNT(DISTINCT d.date) AS active_days
    FROM did_activity_daily d
    JOIN plc.plc_account_creations p ON d.did = p.did
    WHERE d.date >= date('now', '-' || ? || ' days')
    GROUP BY d.did, age_bucket
  )
  SELECT
    age_bucket,
    COUNT(*)                                                                                         AS users,
    ROUND(AVG(active_days), 2)                                                                       AS avg_active_days,
    ROUND(SQRT(AVG(active_days * active_days) - AVG(active_days) * AVG(active_days)), 2)            AS stddev_active_days,
    SUM(CASE WHEN active_days = 1                 THEN 1 ELSE 0 END)                                AS days_1,
    SUM(CASE WHEN active_days = 2                 THEN 1 ELSE 0 END)                                AS days_2,
    SUM(CASE WHEN active_days BETWEEN 3 AND 5     THEN 1 ELSE 0 END)                                AS days_3_5,
    SUM(CASE WHEN active_days BETWEEN 6 AND 14    THEN 1 ELSE 0 END)                                AS days_6_14,
    SUM(CASE WHEN active_days >= 15               THEN 1 ELSE 0 END)                                AS days_15plus
  FROM user_days
  GROUP BY age_bucket
  ORDER BY age_bucket
`).all(daysBack) as Record<string, unknown>[];

report(`Active Days Distribution x Age Bucket`, `active_days_by_age_${daysBack}d.csv`, activeDaysDist);

// ── 6. Cohort activation rate ─────────────────────────────────────────────────

const cohortActivationRate = activityDb.prepare(`
  WITH total_by_bucket AS (
    SELECT
      ${AGE_BUCKET} AS age_bucket,
      COUNT(*) AS total_repos
    FROM plc.did_in_repo dir
    JOIN plc.plc_account_creations p ON dir.did = p.did
    LEFT JOIN plc.did_repo_status s ON dir.did = s.did
    WHERE s.did IS NULL
    GROUP BY age_bucket
  ),
  active_by_bucket AS (
    SELECT
      ${AGE_BUCKET} AS age_bucket,
      COUNT(DISTINCT d.did) AS active_users
    FROM did_activity_daily d
    JOIN plc.plc_account_creations p ON d.did = p.did
    WHERE d.date >= date('now', '-' || ? || ' days')
    GROUP BY age_bucket
  )
  SELECT
    t.age_bucket,
    t.total_repos,
    COALESCE(a.active_users, 0)                                      AS active_users,
    ROUND(100.0 * COALESCE(a.active_users, 0) / t.total_repos, 2)   AS pct_active
  FROM total_by_bucket t
  LEFT JOIN active_by_bucket a USING (age_bucket)
  ORDER BY t.age_bucket
`).all(daysBack) as Record<string, unknown>[];

report(`Cohort Activation Rate`, `cohort_activation_${daysBack}d.csv`, cohortActivationRate);

// ── 7. Action rates by cohort ─────────────────────────────────────────────────

const actionRatesByCohort = activityDb.prepare(`
  WITH active_users AS (
    SELECT
      did,
      MAX(CASE WHEN activity_types & 1 THEN 1 ELSE 0 END) AS ever_posted,
      MAX(CASE WHEN activity_types & 2 THEN 1 ELSE 0 END) AS ever_liked,
      MAX(CASE WHEN activity_types & 4 THEN 1 ELSE 0 END) AS ever_reposted,
      MAX(CASE WHEN activity_types & 8 THEN 1 ELSE 0 END) AS ever_followed
    FROM did_activity_daily
    WHERE date >= date('now', '-' || ? || ' days')
    GROUP BY did
  ),
  cohort_sizes AS (
    SELECT
      ${AGE_BUCKET} AS age_bucket,
      COUNT(*) AS cohort_size
    FROM plc.did_in_repo dir
    JOIN plc.plc_account_creations p ON dir.did = p.did
    LEFT JOIN plc.did_repo_status s ON dir.did = s.did
    WHERE s.did IS NULL
    GROUP BY age_bucket
  ),
  action_counts AS (
    SELECT
      ${AGE_BUCKET} AS age_bucket,
      SUM(a.ever_posted)   AS posted,
      SUM(a.ever_liked)    AS liked,
      SUM(a.ever_reposted) AS reposted,
      SUM(a.ever_followed) AS followed
    FROM active_users a
    JOIN plc.plc_account_creations p ON a.did = p.did
    GROUP BY age_bucket
  )
  SELECT
    c.age_bucket,
    c.cohort_size,
    COALESCE(a.posted, 0)                                                   AS posted_n,
    ROUND(100.0 * COALESCE(a.posted, 0)   / c.cohort_size, 2)              AS pct_posted,
    COALESCE(a.liked, 0)                                                    AS liked_n,
    ROUND(100.0 * COALESCE(a.liked, 0)    / c.cohort_size, 2)              AS pct_liked,
    COALESCE(a.reposted, 0)                                                 AS reposted_n,
    ROUND(100.0 * COALESCE(a.reposted, 0) / c.cohort_size, 2)              AS pct_reposted,
    COALESCE(a.followed, 0)                                                 AS followed_n,
    ROUND(100.0 * COALESCE(a.followed, 0) / c.cohort_size, 2)              AS pct_followed
  FROM cohort_sizes c
  LEFT JOIN action_counts a USING (age_bucket)
  ORDER BY c.age_bucket
`).all(daysBack) as Record<string, unknown>[];

report(`Action Rates by Cohort`, `action_rates_by_cohort_${daysBack}d.csv`, actionRatesByCohort);

// ── 8. pds.trump.com weekly DID registrations (cumulative) ───────────────────

const trumpWeekly = plcDb.prepare(`
  SELECT
    week,
    count                                     AS new_dids,
    SUM(count) OVER (ORDER BY week)           AS cumulative_dids
  FROM plc_creation_weekly
  WHERE pds_url LIKE '%trump%'
  ORDER BY week
`).all() as Record<string, unknown>[];

report("pds.trump.com Weekly DID Registrations", "trump_pds_weekly.csv", trumpWeekly);

console.log(`\nDone. CSVs written to ${OUT_DIR}/\n`);
