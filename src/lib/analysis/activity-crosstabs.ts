import Database from "better-sqlite3";
import path from "path";
import fs from "fs";

const args = process.argv.slice(2);
const daysBack = args[0] ? parseInt(args[0], 10) : 3;

const RUN_DATE = new Date().toISOString().slice(0, 10);
const OUT_DIR = path.join(process.cwd(), "analysis-output", RUN_DATE);
fs.mkdirSync(OUT_DIR, { recursive: true });

function daysAgoStr(n: number): string {
  const d = new Date(Date.now() - n * 24 * 60 * 60 * 1000);
  return d.toISOString().slice(0, 10);
}

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

const newAcctCutoff = daysAgoStr(7);  // accounts created in the last 7 days

const AGE_BUCKET = `CASE
  WHEN p.created_at >= '${newAcctCutoff}' THEN '0. new accounts (${newAcctCutoff}–${RUN_DATE})'
  WHEN p.created_at < '2023-01-01'        THEN '1. pre-2023'
  WHEN p.created_at < '2024-01-01'        THEN '2. 2023'
  WHEN p.created_at < '2024-11-01'        THEN '3. 2024 pre-Nov'
  WHEN p.created_at < '2025-01-01'        THEN '4. 2024 Nov-Dec (exodus)'
  WHEN p.created_at < '2025-07-01'        THEN '5. 2025 H1'
  WHEN p.created_at < '2026-01-01'        THEN '6. 2025 H2'
  ELSE                                         '7. 2026'
END`;

const ACTIVITY_COLS = `
  COUNT(*)                                              AS total,
  SUM(CASE WHEN activity_types & 1 THEN 1 ELSE 0 END)  AS posted,
  SUM(CASE WHEN activity_types & 2 THEN 1 ELSE 0 END)  AS liked,
  SUM(CASE WHEN activity_types & 4 THEN 1 ELSE 0 END)  AS reposted,
  SUM(CASE WHEN activity_types & 8 THEN 1 ELSE 0 END)  AS followed`;

const windowStart = daysAgoStr(daysBack);
console.log(`\nActivity crosstabs — activity window: ${windowStart} – ${RUN_DATE} (${daysBack} days)`);

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
    -- Use plc_account_creations as denominator: continuously updated by PLC collector,
    -- so it includes accounts created since the last scan:pds-status run.
    -- did_in_repo would undercount new cohorts whose PDSes haven't been scanned yet.
    SELECT
      ${AGE_BUCKET} AS age_bucket,
      COUNT(*) AS total_repos
    FROM plc.plc_account_creations p
    LEFT JOIN plc.did_repo_status s ON p.did = s.did
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
    FROM plc.plc_account_creations p
    LEFT JOIN plc.did_repo_status s ON p.did = s.did
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

// ── 8. Non-active account status by cohort ───────────────────────────────────

// Query 8 runs against plcDb directly (no activityDb join needed), so it uses
// inline CASE expressions — but they must stay in sync with AGE_BUCKET above.
// We define a shared fragment string and interpolate it into the SQL template.
const AGE_BUCKET_PLC = AGE_BUCKET.replace(/\bp\./g, "p.");  // same expression, explicit alias

const nonActiveByCohort = plcDb.prepare(`
  WITH cohort_sizes AS (
    SELECT
      ${AGE_BUCKET_PLC} AS age_bucket,
      COUNT(*) AS cohort_size
    FROM plc_account_creations p
    LEFT JOIN did_repo_status s ON p.did = s.did
    WHERE s.did IS NULL
    GROUP BY age_bucket
  ),
  status_counts AS (
    SELECT
      ${AGE_BUCKET_PLC} AS age_bucket,
      SUM(CASE WHEN s.status = 'deactivated' THEN 1 ELSE 0 END) AS deactivated,
      SUM(CASE WHEN s.status IN ('takendown','takedown') THEN 1 ELSE 0 END) AS takendown,
      COUNT(*) AS total_non_active
    FROM did_repo_status s
    JOIN plc_account_creations p ON s.did = p.did
    GROUP BY age_bucket
  )
  SELECT
    c.age_bucket,
    c.cohort_size,
    COALESCE(sc.deactivated, 0)                                                    AS deactivated_n,
    ROUND(100.0 * COALESCE(sc.deactivated, 0)   / c.cohort_size, 2)               AS pct_deactivated,
    COALESCE(sc.takendown, 0)                                                      AS takendown_n,
    ROUND(100.0 * COALESCE(sc.takendown, 0)     / c.cohort_size, 2)               AS pct_takendown,
    COALESCE(sc.total_non_active, 0)                                               AS total_non_active_n,
    ROUND(100.0 * COALESCE(sc.total_non_active, 0) / c.cohort_size, 2)            AS pct_non_active
  FROM cohort_sizes c
  LEFT JOIN status_counts sc USING (age_bucket)
  ORDER BY c.age_bucket
`).all() as Record<string, unknown>[];

report("Non-Active Account Status by Cohort", `non_active_by_cohort.csv`, nonActiveByCohort);

// ── 9. Account-level event trends ────────────────────────────────────────────

const accountEventTrends = activityDb.prepare(`
  SELECT
    date,
    SUM(CASE WHEN event_type = 'account:deleted'     THEN count ELSE 0 END) AS deleted,
    SUM(CASE WHEN event_type = 'account:deactivated' THEN count ELSE 0 END) AS deactivated,
    SUM(CASE WHEN event_type = 'account:reactivated' THEN count ELSE 0 END) AS reactivated,
    SUM(CASE WHEN event_type = 'account:takendown'   THEN count ELSE 0 END) AS takendown
  FROM delete_events_daily
  WHERE event_type IN ('account:deleted','account:deactivated','account:reactivated','account:takendown')
    AND date >= date('now', '-' || ? || ' days')
  GROUP BY date
  ORDER BY date
`).all(daysBack) as Record<string, unknown>[];

report(`Account Event Trends`, `account_event_trends_${daysBack}d.csv`, accountEventTrends);

// ── 10. pds.trump.com weekly DID registrations (cumulative) ─────────────────

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

// ── 11. Language × activity ───────────────────────────────────────────────────
// Uses did_langs from activityDb (accumulated from post events) joined with
// did_activity_daily. A DID can have multiple lang rows; it counts toward each.

const langActivity = activityDb.prepare(`
  WITH active AS (
    SELECT DISTINCT did, activity_types
    FROM did_activity_daily
    WHERE date >= date('now', '-' || ? || ' days')
  )
  SELECT
    dl.lang,
    COUNT(DISTINCT dl.did)                                                AS active_users,
    SUM(dl.post_count)                                                    AS lifetime_posts,
    ROUND(1.0 * SUM(dl.post_count) / COUNT(DISTINCT dl.did), 1)          AS avg_lifetime_posts_per_user,
    SUM(CASE WHEN a.activity_types & 1 THEN 1 ELSE 0 END)                AS posted_in_window,
    SUM(CASE WHEN a.activity_types & 2 THEN 1 ELSE 0 END)                AS liked_in_window,
    SUM(CASE WHEN a.activity_types & 4 THEN 1 ELSE 0 END)                AS reposted_in_window,
    SUM(CASE WHEN a.activity_types & 8 THEN 1 ELSE 0 END)                AS followed_in_window
  FROM did_langs dl
  JOIN active a ON dl.did = a.did
  GROUP BY dl.lang
  ORDER BY active_users DESC
  LIMIT 40
`).all(daysBack) as Record<string, unknown>[];

report(`Language × Activity`, `lang_activity_${daysBack}d.csv`, langActivity);

// ── 12. Migration × activity ──────────────────────────────────────────────────
// Migrated = has at least one row in plc_migrations. Compares activation rate,
// active days, and action type rates between migrated and non-migrated cohorts.

const migrationActivity = activityDb.prepare(`
  WITH migrated_dids AS (
    SELECT DISTINCT did FROM plc.plc_migrations
  ),
  repo_base AS (
    SELECT dir.did,
           CASE WHEN m.did IS NOT NULL THEN 'migrated' ELSE 'non-migrated' END AS segment
    FROM plc.did_in_repo dir
    LEFT JOIN plc.did_repo_status s ON dir.did = s.did
    LEFT JOIN migrated_dids m ON dir.did = m.did
    WHERE s.did IS NULL   -- exclude deactivated/takendown
  ),
  active_in_window AS (
    SELECT did,
           COUNT(DISTINCT date)                                          AS active_days,
           MAX(CASE WHEN activity_types & 1 THEN 1 ELSE 0 END)          AS ever_posted,
           MAX(CASE WHEN activity_types & 2 THEN 1 ELSE 0 END)          AS ever_liked,
           MAX(CASE WHEN activity_types & 4 THEN 1 ELSE 0 END)          AS ever_reposted,
           MAX(CASE WHEN activity_types & 8 THEN 1 ELSE 0 END)          AS ever_followed
    FROM did_activity_daily
    WHERE date >= date('now', '-' || ? || ' days')
    GROUP BY did
  )
  SELECT
    b.segment,
    COUNT(*)                                                              AS total_repos,
    COUNT(a.did)                                                          AS active_users,
    ROUND(100.0 * COUNT(a.did) / COUNT(*), 2)                            AS pct_active,
    ROUND(AVG(COALESCE(a.active_days, 0)), 2)                            AS avg_active_days,
    ROUND(100.0 * SUM(COALESCE(a.ever_posted,   0)) / COUNT(*), 2)       AS pct_posted,
    ROUND(100.0 * SUM(COALESCE(a.ever_liked,    0)) / COUNT(*), 2)       AS pct_liked,
    ROUND(100.0 * SUM(COALESCE(a.ever_reposted, 0)) / COUNT(*), 2)       AS pct_reposted,
    ROUND(100.0 * SUM(COALESCE(a.ever_followed, 0)) / COUNT(*), 2)       AS pct_followed
  FROM repo_base b
  LEFT JOIN active_in_window a ON b.did = a.did
  GROUP BY b.segment
  ORDER BY b.segment
`).all(daysBack) as Record<string, unknown>[];

report(`Migration × Activity`, `migration_activity_${daysBack}d.csv`, migrationActivity);

// ── 13. Engagement depth by cohort ───────────────────────────────────────────
// For each active user, count how many distinct action types they used in the
// window (0–4). Groups by cohort. Shows depth-of-engagement, not just presence.

const engagementDepth = activityDb.prepare(`
  WITH user_depth AS (
    SELECT
      did,
      SUM(
        (activity_types & 1 > 0) +
        (activity_types & 2 > 0) +
        (activity_types & 4 > 0) +
        (activity_types & 8 > 0)
      ) / COUNT(DISTINCT date)   AS avg_types_per_day,
      (
        MAX(activity_types & 1 > 0) +
        MAX(activity_types & 2 > 0) +
        MAX(activity_types & 4 > 0) +
        MAX(activity_types & 8 > 0)
      )                          AS distinct_types_used
    FROM did_activity_daily
    WHERE date >= date('now', '-' || ? || ' days')
    GROUP BY did
  ),
  with_bucket AS (
    SELECT
      ud.distinct_types_used,
      ${AGE_BUCKET} AS age_bucket
    FROM user_depth ud
    JOIN plc.plc_account_creations p ON ud.did = p.did
  )
  SELECT
    age_bucket,
    COUNT(*)                                                              AS active_users,
    ROUND(AVG(distinct_types_used), 3)                                   AS avg_distinct_types,
    SUM(CASE WHEN distinct_types_used = 1 THEN 1 ELSE 0 END)            AS depth_1,
    SUM(CASE WHEN distinct_types_used = 2 THEN 1 ELSE 0 END)            AS depth_2,
    SUM(CASE WHEN distinct_types_used = 3 THEN 1 ELSE 0 END)            AS depth_3,
    SUM(CASE WHEN distinct_types_used = 4 THEN 1 ELSE 0 END)            AS depth_4,
    ROUND(100.0 * SUM(CASE WHEN distinct_types_used = 4 THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_full_depth
  FROM with_bucket
  GROUP BY age_bucket
  ORDER BY age_bucket
`).all(daysBack) as Record<string, unknown>[];

report(`Engagement Depth by Cohort`, `engagement_depth_${daysBack}d.csv`, engagementDepth);

// ── 14. Starterpack joins ranking ────────────────────────────────────────────
// Simple cumulative join count per starterpack URI, sorted by total joins.
// Data is sparse until the collector has been running longer.

const starterpacks = activityDb.prepare(`
  SELECT
    starterpack_uri,
    SUM(count)   AS total_joins,
    MIN(date)    AS first_recorded,
    MAX(date)    AS last_recorded,
    COUNT(date)  AS days_with_data
  FROM starterpack_joins_daily
  GROUP BY starterpack_uri
  ORDER BY total_joins DESC
  LIMIT 50
`).all() as Record<string, unknown>[];

report("Starterpack Joins Ranking", "starterpack_joins.csv", starterpacks);

// ── 15. AppView / namespace retention ───────────────────────────────────────
// For each non-bsky namespace root (first 2 NSID parts), count unique lifetime
// users from collection_activity, then check what fraction appear in
// did_activity_daily within the window — showing whether appview users are
// retained bsky users or have drifted off.

const rawNsRetention = activityDb.prepare(`
  WITH ns_users AS (
    SELECT collection, did
    FROM collection_activity
    WHERE collection NOT LIKE 'app.bsky.%' AND collection NOT LIKE 'chat.bsky.%'
  ),
  active_in_window AS (
    SELECT DISTINCT did FROM did_activity_daily
    WHERE date >= date('now', '-' || ? || ' days')
  )
  SELECT
    nu.collection,
    COUNT(DISTINCT nu.did)                                                AS lifetime_users,
    COUNT(DISTINCT a.did)                                                 AS active_in_window
  FROM ns_users nu
  LEFT JOIN active_in_window a ON nu.did = a.did
  GROUP BY nu.collection
  HAVING lifetime_users >= 2
`).all(daysBack) as { collection: string; lifetime_users: number; active_in_window: number }[];

// Aggregate to namespace root (first 2 NSID parts) in JS
const nsRootMap = new Map<string, { lifetime_users: number; active_in_window: number }>();
for (const r of rawNsRetention) {
  const ns = r.collection.split(".").slice(0, 2).join(".");
  const cur = nsRootMap.get(ns);
  if (cur) {
    cur.lifetime_users += r.lifetime_users;
    cur.active_in_window += r.active_in_window;
  } else {
    nsRootMap.set(ns, { lifetime_users: r.lifetime_users, active_in_window: r.active_in_window });
  }
}
const nsRetentionRows = [...nsRootMap.entries()]
  .map(([ns, { lifetime_users, active_in_window }]) => ({
    namespace: ns,
    lifetime_users,
    active_in_window,
    pct_retained: Math.round(1000 * active_in_window / lifetime_users) / 10,
  }))
  .sort((a, b) => b.lifetime_users - a.lifetime_users);

report(
  `AppView Namespace Retention (activity window: ${windowStart}–${RUN_DATE})`,
  `ns_retention_${daysBack}d.csv`,
  nsRetentionRows
);

console.log(`\nDone. CSVs written to ${OUT_DIR}/\n`);
