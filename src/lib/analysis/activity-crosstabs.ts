import Database from "better-sqlite3";
import path from "path";

const args = process.argv.slice(2);
const daysBack = args[0] ? parseInt(args[0], 10) : 3;

function printTable(title: string, rows: Record<string, unknown>[]) {
    console.log(`\n=== ${title} ===`);
    if (rows.length === 0) {
        console.log("No data");
        return;
    }
    console.table(rows);
}

const activityDb = new Database(path.join(process.cwd(), "jetstream-activity.db"), { readonly: true });
const plcDb = new Database(path.join(process.cwd(), "plc-migrations.db"), { readonly: true });

activityDb.exec(`ATTACH DATABASE '${plcDb.name}' as plc`);

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

const result = activityDb.prepare(`
  WITH active AS (
    SELECT DISTINCT did, activity_types
    FROM did_activity_daily
    WHERE date >= date('now', '-' || ? || ' days')
  ),
  aged AS (
    SELECT a.activity_types, p.created_at
    FROM active a
    JOIN plc.plc_account_creations p ON a.did = p.did
  ),
  buckets AS (
    SELECT
      CASE
        WHEN created_at >= date('now', '-7 days') THEN '0. last 7 days'
        WHEN created_at < '2023-01-01'            THEN '1. pre-2023'
        WHEN created_at < '2024-01-01'            THEN '2. 2023'
        WHEN created_at < '2024-11-01'            THEN '3. 2024 pre-Nov'
        WHEN created_at < '2025-01-01'            THEN '4. 2024 Nov-Dec (exodus)'
        WHEN created_at < '2025-07-01'            THEN '5. 2025 H1'
        WHEN created_at < '2026-01-01'            THEN '6. 2025 H2'
        ELSE                                           '7. 2026'
      END AS age_bucket,
      activity_types
    FROM aged
  )
  SELECT age_bucket,
    COUNT(*) AS total,
    SUM(CASE WHEN activity_types & 1 THEN 1 ELSE 0 END) AS posted,
    SUM(CASE WHEN activity_types & 2 THEN 1 ELSE 0 END) AS liked,
    SUM(CASE WHEN activity_types & 4 THEN 1 ELSE 0 END) AS reposted,
    SUM(CASE WHEN activity_types & 8 THEN 1 ELSE 0 END) AS followed
  FROM buckets GROUP BY age_bucket
  UNION ALL
  SELECT '~ TOTAL',
    COUNT(*),
    SUM(CASE WHEN activity_types & 1 THEN 1 ELSE 0 END),
    SUM(CASE WHEN activity_types & 2 THEN 1 ELSE 0 END),
    SUM(CASE WHEN activity_types & 4 THEN 1 ELSE 0 END),
    SUM(CASE WHEN activity_types & 8 THEN 1 ELSE 0 END)
  FROM buckets
  ORDER BY age_bucket
`).all(daysBack);

printTable("Activity by Age Bucket", result);

const labelResult = activityDb.prepare(`
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
  SELECT label,
    COUNT(*) AS total,
    SUM(CASE WHEN activity_types & 1 THEN 1 ELSE 0 END) AS posted,
    SUM(CASE WHEN activity_types & 2 THEN 1 ELSE 0 END) AS liked,
    SUM(CASE WHEN activity_types & 4 THEN 1 ELSE 0 END) AS reposted,
    SUM(CASE WHEN activity_types & 8 THEN 1 ELSE 0 END) AS followed
  FROM labeled GROUP BY label
  UNION ALL
  SELECT '~ TOTAL',
    COUNT(*),
    SUM(CASE WHEN activity_types & 1 THEN 1 ELSE 0 END),
    SUM(CASE WHEN activity_types & 2 THEN 1 ELSE 0 END),
    SUM(CASE WHEN activity_types & 4 THEN 1 ELSE 0 END),
    SUM(CASE WHEN activity_types & 8 THEN 1 ELSE 0 END)
  FROM labeled
  ORDER BY total DESC
`).all(daysBack);

printTable("Activity by Skywatch Label", labelResult);


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
      ROUND(AVG(daily_uniques), 0) AS avg_daily_uniques,
      ROUND(SQRT(AVG(daily_uniques * daily_uniques) - AVG(daily_uniques) * AVG(daily_uniques)), 1) AS stddev_daily_uniques,
      total_uniques,
      ROUND(1.0 * AVG(daily_uniques) / total_uniques, 3) AS ratio,
      ROUND(SQRT(AVG(daily_uniques * daily_uniques) - AVG(daily_uniques) * AVG(daily_uniques)) / total_uniques, 4) AS stddev_ratio
    FROM daily_counts, total_unique
  `).get(daysBack, daysBack);

  printTable("Stickiness (avg daily uniques / unique users in window)", [stickiness as Record<string, unknown>]);

// ── Stickiness by age bucket ──────────────────────────────────────────────────
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
    ROUND(AVG(d.daily_uniques), 0) AS avg_daily_uniques,
    ROUND(SQRT(AVG(d.daily_uniques * d.daily_uniques) - AVG(d.daily_uniques) * AVG(d.daily_uniques)), 1) AS stddev_daily_uniques,
    t.total_uniques,
    ROUND(1.0 * AVG(d.daily_uniques) / t.total_uniques, 3) AS ratio,
    ROUND(SQRT(AVG(d.daily_uniques * d.daily_uniques) - AVG(d.daily_uniques) * AVG(d.daily_uniques)) / t.total_uniques, 4) AS stddev_ratio
  FROM daily_by_bucket d
  JOIN totals t ON d.age_bucket = t.age_bucket
  GROUP BY d.age_bucket
  ORDER BY d.age_bucket
`).all(daysBack, daysBack);

printTable("Stickiness by Age Bucket", stickinessByBucket as Record<string, unknown>[]);

// ── Active days distribution × age bucket ────────────────────────────────────
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
    COUNT(*)                                                            AS users,
    ROUND(AVG(active_days), 2)                                         AS avg_active_days,
    ROUND(SQRT(AVG(active_days * active_days) - AVG(active_days) * AVG(active_days)), 2) AS stddev_active_days,
    SUM(CASE WHEN active_days = 1                    THEN 1 ELSE 0 END) AS days_1,
    SUM(CASE WHEN active_days = 2                    THEN 1 ELSE 0 END) AS days_2,
    SUM(CASE WHEN active_days BETWEEN 3 AND 5        THEN 1 ELSE 0 END) AS days_3_5,
    SUM(CASE WHEN active_days BETWEEN 6 AND 14       THEN 1 ELSE 0 END) AS days_6_14,
    SUM(CASE WHEN active_days >= 15                  THEN 1 ELSE 0 END) AS days_15plus
  FROM user_days
  GROUP BY age_bucket
  ORDER BY age_bucket
`).all(daysBack);

printTable(`Active Days Distribution x Age Bucket (window: ${daysBack}d)`, activeDaysDist as Record<string, unknown>[]);

// ── % active per cohort relative to total recorded repos ─────────────────────
const cohortActivationRate = activityDb.prepare(`
  WITH total_by_bucket AS (
    SELECT
      ${AGE_BUCKET} AS age_bucket,
      COUNT(*) AS total_repos
    FROM plc.did_in_repo dir
    JOIN plc.plc_account_creations p ON dir.did = p.did
    JOIN plc.did_repo_status d ON p.did = d.did
    WHERE status = 'active'
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
    COALESCE(a.active_users, 0) AS active_users,
    ROUND(100.0 * COALESCE(a.active_users, 0) / t.total_repos, 2) AS pct_active
  FROM total_by_bucket t
  LEFT JOIN active_by_bucket a USING (age_bucket)
  ORDER BY t.age_bucket
`).all(daysBack);

printTable(`Cohort Activation Rate (window: ${daysBack}d)`, cohortActivationRate as Record<string, unknown>[]);
                                                                                                                    
