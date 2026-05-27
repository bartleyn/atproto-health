import Database from "better-sqlite3";
import path from "path";
import fs from "fs";

const args = process.argv.slice(2);
const daysBack = args[0] && !args[0].startsWith("--") ? parseInt(args[0], 10) : 3;

// --only <query_name>  (repeatable) → run only named queries; omit to run all
// Query names: activity_by_age, activity_by_label, stickiness, stickiness_by_age,
//   active_days_dist, cohort_activation, action_rates, non_active, account_events,
//   trump_weekly, lang_activity, migration_activity, engagement_depth, starterpacks,
//   ns_retention, daily_actions_by_cohort,
//   inter_visit_gaps, action_cadence, recency,
//   new_user_follow_like, dau_mau
//
// --history N  → days of history for the dau_mau time series (default: 60)
const historyIdx = args.indexOf("--history");
const historyDays = historyIdx >= 0 ? parseInt(args[historyIdx + 1], 10) : 60;

const onlyFlags: string[] = [];
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--only" && args[i + 1]) {
    onlyFlags.push(args[i + 1]);
    i++;
  }
}
const runAll = onlyFlags.length === 0;
const shouldRun = (name: string) => runAll || onlyFlags.includes(name);

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

// 128 MB page cache + in-memory temp tables: keeps sort/hash work out of the
// WAL files so analysis I/O doesn't compete with the live collector writes.
activityDb.pragma("cache_size = -131072");
activityDb.pragma("temp_store = MEMORY");
// Memory-map the DB file so the OS page cache serves reads without syscall overhead.
activityDb.pragma("mmap_size = 2147483648");  // 2 GB

// Materialise once; all queries join against this instead of repeating the CTE.
activityDb.exec(`
  CREATE TEMP TABLE excluded_dids AS
    SELECT did FROM plc.did_repo_status
    UNION
    SELECT did FROM plc.skywatch_labels WHERE label IN ('spam', 'impersonation');
  CREATE INDEX temp.idx_excluded_dids ON excluded_dids(did);
`);

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

// Pre-materialise the active-user window so each query doesn't re-scan 33M rows.
// active_window_clean: unique (did, activity_types) pairs for the window, spam/bots excluded.
// active_window_all: same but no exclusion filter (for label-analysis queries).
console.log(`Pre-materialising active-user window…`);
activityDb.exec(`
  CREATE TEMP TABLE active_window_clean AS
    SELECT DISTINCT d.did, d.activity_types
    FROM did_activity_daily d
    LEFT JOIN excluded_dids ex ON d.did = ex.did
    WHERE d.date >= '${windowStart}'
      AND ex.did IS NULL;
  CREATE INDEX temp.idx_active_window_clean_did ON active_window_clean(did);

  CREATE TEMP TABLE active_window_all AS
    SELECT DISTINCT did, activity_types
    FROM did_activity_daily
    WHERE date >= '${windowStart}';
  CREATE INDEX temp.idx_active_window_all_did ON active_window_all(did);
`);
console.log(`  done.\n`);

// Single read transaction: all queries see the same consistent DB snapshot.
// BEGIN DEFERRED on a readonly WAL connection never blocks the live collector.
activityDb.exec("BEGIN DEFERRED");
try {

// ── 1. Activity by age bucket ─────────────────────────────────────────────────

if (shouldRun("activity_by_age")) {
const activityByAge = activityDb.prepare(`
  WITH
  buckets AS (
    SELECT
      ${AGE_BUCKET} AS age_bucket,
      a.activity_types
    FROM active_window_clean a
    JOIN plc.plc_account_creations p ON a.did = p.did
  )
  SELECT age_bucket, ${ACTIVITY_COLS}
  FROM buckets GROUP BY age_bucket
  UNION ALL
  SELECT '~ TOTAL', ${ACTIVITY_COLS}
  FROM buckets
  ORDER BY age_bucket
`).all() as Record<string, unknown>[];

report("Activity by Age Bucket", `activity_by_age_${daysBack}d.csv`, activityByAge);
}

// ── 2. Activity by Skywatch label ─────────────────────────────────────────────

if (shouldRun("activity_by_label")) {
const activityByLabel = activityDb.prepare(`
  WITH
  labeled AS (
    SELECT sl.label, a.activity_types
    FROM active_window_all a
    JOIN plc.skywatch_labels sl ON a.did = sl.did
  )
  SELECT label, ${ACTIVITY_COLS}
  FROM labeled GROUP BY label
  UNION ALL
  SELECT '~ TOTAL', ${ACTIVITY_COLS}
  FROM labeled
  ORDER BY total DESC
`).all() as Record<string, unknown>[];

report("Activity by Skywatch Label", `activity_by_label_${daysBack}d.csv`, activityByLabel);
}

// ── 3. Overall stickiness ─────────────────────────────────────────────────────

if (shouldRun("stickiness")) {
const stickiness = activityDb.prepare(`
  WITH
  -- Re-join with the raw table for per-day counts; covering index makes this fast.
  daily_counts AS (
    SELECT d.date, COUNT(DISTINCT d.did) AS daily_uniques
    FROM did_activity_daily d
    LEFT JOIN excluded_dids ex ON d.did = ex.did
    WHERE d.date >= '${windowStart}'
      AND ex.did IS NULL
    GROUP BY d.date
  ),
  total_unique AS (
    SELECT COUNT(DISTINCT did) AS total_uniques FROM active_window_clean
  )
  SELECT
    ROUND(AVG(daily_uniques), 0)                                                                   AS avg_daily_uniques,
    ROUND(SQRT(AVG(daily_uniques * daily_uniques) - AVG(daily_uniques) * AVG(daily_uniques)), 1)   AS stddev_daily_uniques,
    total_uniques,
    ROUND(1.0 * AVG(daily_uniques) / total_uniques, 3)                                             AS ratio,
    ROUND(SQRT(AVG(daily_uniques * daily_uniques) - AVG(daily_uniques) * AVG(daily_uniques)) / total_uniques, 4) AS stddev_ratio
  FROM daily_counts, total_unique
`).get() as Record<string, unknown>;

report("Stickiness", `stickiness_${daysBack}d.csv`, [stickiness]);
}

// ── 4. Stickiness by age bucket ───────────────────────────────────────────────

if (shouldRun("stickiness_by_age")) {
const stickinessByBucket = activityDb.prepare(`
  WITH
  daily_by_bucket AS (
    SELECT
      d.date,
      ${AGE_BUCKET} AS age_bucket,
      COUNT(DISTINCT d.did) AS daily_uniques
    FROM did_activity_daily d
    JOIN plc.plc_account_creations p ON d.did = p.did
    LEFT JOIN excluded_dids ex ON d.did = ex.did
    WHERE d.date >= '${windowStart}'
      AND ex.did IS NULL
    GROUP BY d.date, age_bucket
  ),
  totals AS (
    SELECT
      ${AGE_BUCKET} AS age_bucket,
      COUNT(DISTINCT a.did) AS total_uniques
    FROM active_window_clean a
    JOIN plc.plc_account_creations p ON a.did = p.did
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
`).all() as Record<string, unknown>[];

report("Stickiness by Age Bucket", `stickiness_by_age_${daysBack}d.csv`, stickinessByBucket);
}

// ── 5. Active days distribution × age bucket ──────────────────────────────────

if (shouldRun("active_days_dist")) {
const activeDaysDist = activityDb.prepare(`
  WITH
user_days AS (
    SELECT
      d.did,
      ${AGE_BUCKET} AS age_bucket,
      COUNT(DISTINCT d.date) AS active_days
    FROM did_activity_daily d
    JOIN plc.plc_account_creations p ON d.did = p.did
    LEFT JOIN excluded_dids ex ON d.did = ex.did
    WHERE d.date >= date('now', '-' || ? || ' days')
      AND ex.did IS NULL
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
}

// ── 6. Cohort activation rate ─────────────────────────────────────────────────

if (shouldRun("cohort_activation")) {
const cohortActivationRate = activityDb.prepare(`
  WITH
 total_by_bucket AS (
    SELECT
      ${AGE_BUCKET} AS age_bucket,
      COUNT(*) AS total_repos
    FROM plc.plc_account_creations p
    JOIN plc.did_in_repo r ON p.did = r.did
    LEFT JOIN plc.did_repo_status s ON p.did = s.did
    LEFT JOIN plc.skywatch_labels sl ON p.did = sl.did AND sl.label IN ('spam', 'impersonation')
    WHERE s.did IS NULL
      AND sl.did IS NULL
    GROUP BY age_bucket
  ),
  active_by_bucket AS (
    SELECT
      ${AGE_BUCKET} AS age_bucket,
      COUNT(DISTINCT a.did) AS active_users
    FROM active_window_clean a
    JOIN plc.plc_account_creations p ON a.did = p.did
    JOIN plc.did_in_repo r ON a.did = r.did
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
`).all() as Record<string, unknown>[];

report(`Cohort Activation Rate`, `cohort_activation_${daysBack}d.csv`, cohortActivationRate);
}

// ── 7. Action rates by cohort ─────────────────────────────────────────────────

if (shouldRun("action_rates")) {
const actionRatesByCohort = activityDb.prepare(`
  WITH
active_users AS (
    SELECT
      d.did,
      MAX(CASE WHEN d.activity_types & 1 THEN 1 ELSE 0 END) AS ever_posted,
      MAX(CASE WHEN d.activity_types & 2 THEN 1 ELSE 0 END) AS ever_liked,
      MAX(CASE WHEN d.activity_types & 4 THEN 1 ELSE 0 END) AS ever_reposted,
      MAX(CASE WHEN d.activity_types & 8 THEN 1 ELSE 0 END) AS ever_followed
    FROM did_activity_daily d
    LEFT JOIN excluded_dids ex ON d.did = ex.did
    WHERE d.date >= date('now', '-' || ? || ' days')
      AND ex.did IS NULL
    GROUP BY d.did
  ),
  cohort_sizes AS (
    SELECT
      ${AGE_BUCKET} AS age_bucket,
      COUNT(*) AS cohort_size
    FROM plc.plc_account_creations p
    JOIN plc.did_in_repo r ON p.did = r.did
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
}

// ── 8. Non-active account status by cohort ───────────────────────────────────

// Query 8 runs against plcDb directly (no activityDb join needed), so it uses
// inline CASE expressions — but they must stay in sync with AGE_BUCKET above.
// We define a shared fragment string and interpolate it into the SQL template.
const AGE_BUCKET_PLC = AGE_BUCKET.replace(/\bp\./g, "p.");  // same expression, explicit alias

if (shouldRun("non_active")) {
const nonActiveByCohort = plcDb.prepare(`
  WITH
 cohort_sizes AS (
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
}

// ── 9. Account-level event trends ────────────────────────────────────────────

if (shouldRun("account_events")) {
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
}

// ── 10. pds.trump.com weekly DID registrations (cumulative) ─────────────────

if (shouldRun("trump_weekly")) {
const trumpWeekly = plcDb.prepare(`
  SELECT
    week,
    count                                     AS new_dids,
    SUM(count) OVER (ORDER BY week)           AS cumulative_dids
  FROM plc_creation_weekly
  WHERE pds_url = 'https://pds.trump.com'
  ORDER BY week
`).all() as Record<string, unknown>[];

report("pds.trump.com Weekly DID Registrations", "trump_pds_weekly.csv", trumpWeekly);
}

// ── 11. Language × activity ───────────────────────────────────────────────────
// Uses did_langs from activityDb (accumulated from post events) joined with
// did_activity_daily. A DID can have multiple lang rows; it counts toward each.

if (shouldRun("lang_activity")) {
const langActivity = activityDb.prepare(`
  WITH agg AS (
    SELECT did,
      MAX(activity_types & 1) AS posted,
      MAX(activity_types & 2) AS liked,
      MAX(activity_types & 4) AS reposted,
      MAX(activity_types & 8) AS followed
    FROM active_window_clean
    GROUP BY did
  )
  SELECT
    dl.lang,
    COUNT(DISTINCT dl.did)                                                AS active_users,
    SUM(dl.post_count)                                                    AS lifetime_posts,
    ROUND(1.0 * SUM(dl.post_count) / COUNT(DISTINCT dl.did), 1)          AS avg_lifetime_posts_per_user,
    SUM(a.posted)                                                         AS posted_in_window,
    SUM(a.liked)                                                          AS liked_in_window,
    SUM(a.reposted)                                                       AS reposted_in_window,
    SUM(a.followed)                                                       AS followed_in_window
  FROM did_langs dl
  JOIN agg a ON dl.did = a.did
  GROUP BY dl.lang
  ORDER BY active_users DESC
  LIMIT 40
`).all() as Record<string, unknown>[];

report(`Language × Activity`, `lang_activity_${daysBack}d.csv`, langActivity);
}

// ── 12. Migration × activity ──────────────────────────────────────────────────
// Migrated = has at least one row in plc_migrations. Compares activation rate,
// active days, and action type rates between migrated and non-migrated cohorts.

if (shouldRun("migration_activity")) {
const migrationActivity = activityDb.prepare(`
  WITH
 migrated_dids AS (
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
}

// ── 13. Engagement depth by cohort ───────────────────────────────────────────
// For each active user, count how many distinct action types they used in the
// window (0–4). Groups by cohort. Shows depth-of-engagement, not just presence.

if (shouldRun("engagement_depth")) {
const engagementDepth = activityDb.prepare(`
  WITH
user_depth AS (
    SELECT
      d.did AS did,
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
    FROM did_activity_daily d
    LEFT JOIN excluded_dids ex ON d.did = ex.did
    WHERE d.date >= date('now', '-' || ? || ' days')
      AND ex.did IS NULL
    GROUP BY d.did
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
}

// ── 14. Starterpack joins ranking ────────────────────────────────────────────
// Simple cumulative join count per starterpack URI, sorted by total joins.
// Data is sparse until the collector has been running longer.

if (shouldRun("starterpacks")) {
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
}

// ── 15. AppView / namespace retention ───────────────────────────────────────
// For each non-bsky namespace root (first 2 NSID parts), count unique lifetime
// users from collection_activity, then check what fraction appear in
// did_activity_daily within the window — showing whether appview users are
// retained bsky users or have drifted off.

if (shouldRun("ns_retention")) {
const rawNsRetention = activityDb.prepare(`
  WITH
 ns_users AS (
    SELECT collection, did
    FROM collection_activity
    WHERE collection NOT LIKE 'app.bsky.%' AND collection NOT LIKE 'chat.bsky.%'
  ),
  active_in_window AS (
    SELECT DISTINCT did FROM active_window_all
  )
  SELECT
    nu.collection,
    COUNT(DISTINCT nu.did)                                                AS lifetime_users,
    COUNT(DISTINCT a.did)                                                 AS active_in_window
  FROM ns_users nu
  LEFT JOIN active_in_window a ON nu.did = a.did
  GROUP BY nu.collection
  HAVING lifetime_users >= 2
`).all() as { collection: string; lifetime_users: number; active_in_window: number }[];

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
}

// ── 16. Daily unique likers / followers / posters by cohort ──────────────────
// For each day in the window, shows how many unique DIDs in each age cohort
// performed each action type. Useful for seeing intra-window trends per cohort.

if (shouldRun("daily_actions_by_cohort")) {
const dailyActionsByCohort = activityDb.prepare(`
  SELECT
    d.date,
    ${AGE_BUCKET} AS age_bucket,
    COUNT(DISTINCT d.did)                                          AS dau,
    COUNT(DISTINCT CASE WHEN d.activity_types & 2 THEN d.did END) AS unique_likers,
    COUNT(DISTINCT CASE WHEN d.activity_types & 8 THEN d.did END) AS unique_followers,
    COUNT(DISTINCT CASE WHEN d.activity_types & 1 THEN d.did END) AS unique_posters
  FROM did_activity_daily d
  JOIN plc.plc_account_creations p ON d.did = p.did
  LEFT JOIN excluded_dids ex ON d.did = ex.did
  WHERE d.date >= date('now', '-' || ? || ' days')
    AND ex.did IS NULL
  GROUP BY d.date, age_bucket
  ORDER BY age_bucket, d.date
`).all(daysBack) as Record<string, unknown>[];

report(
  `Daily Unique Actions by Cohort (${windowStart}–${RUN_DATE})`,
  `daily_actions_by_cohort_${daysBack}d.csv`,
  dailyActionsByCohort
);
}

// ── 17. Inter-visit gap distribution ─────────────────────────────────────────
// How many days between consecutive active days per user, by cohort.
// Uses a fixed 90-day lookback regardless of daysBack so there are enough
// consecutive-day pairs to compute meaningful distributions.
// "gap = 1" means the user came back the very next day.

const GAP_LOOKBACK = 90;
const needsGapTables = shouldRun("inter_visit_gaps") || shouldRun("action_cadence") || shouldRun("recency");

if (needsGapTables) {
  console.log(`Pre-materialising ${GAP_LOOKBACK}-day activity window for gap/recency analysis…`);
  activityDb.exec(`
    CREATE TEMP TABLE activity_90d AS
      SELECT d.did, d.date, d.activity_types
      FROM did_activity_daily d
      LEFT JOIN excluded_dids ex ON d.did = ex.did
      WHERE d.date >= date('now', '-${GAP_LOOKBACK} days')
        AND ex.did IS NULL;
    CREATE INDEX temp.idx_activity_90d_did_date ON activity_90d(did, date);
  `);
  console.log(`  done.\n`);
}

if (shouldRun("inter_visit_gaps")) {
const interVisitGaps = activityDb.prepare(`
  WITH
  gaps AS (
    SELECT
      did,
      CAST(julianday(date) - julianday(LAG(date) OVER (PARTITION BY did ORDER BY date)) AS INTEGER) AS gap_days
    FROM activity_90d
  )
  SELECT
    ${AGE_BUCKET} AS age_bucket,
    COUNT(DISTINCT g.did)                                                       AS users,
    COUNT(*)                                                                    AS gap_observations,
    ROUND(AVG(g.gap_days), 2)                                                  AS avg_gap_days,
    ROUND(SQRT(AVG(g.gap_days * g.gap_days) - AVG(g.gap_days) * AVG(g.gap_days)), 2) AS stddev_gap_days,
    SUM(CASE WHEN g.gap_days = 1               THEN 1 ELSE 0 END)              AS gap_1d,
    SUM(CASE WHEN g.gap_days BETWEEN 2 AND 3   THEN 1 ELSE 0 END)              AS gap_2_3d,
    SUM(CASE WHEN g.gap_days BETWEEN 4 AND 7   THEN 1 ELSE 0 END)              AS gap_4_7d,
    SUM(CASE WHEN g.gap_days BETWEEN 8 AND 14  THEN 1 ELSE 0 END)              AS gap_8_14d,
    SUM(CASE WHEN g.gap_days > 14              THEN 1 ELSE 0 END)              AS gap_15plus_d,
    ROUND(100.0 * SUM(CASE WHEN g.gap_days = 1 THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_returned_next_day
  FROM gaps g
  JOIN plc.plc_account_creations p ON g.did = p.did
  WHERE g.gap_days IS NOT NULL
  GROUP BY age_bucket
  ORDER BY age_bucket
`).all() as Record<string, unknown>[];

report(
  `Inter-Visit Gap Distribution by Cohort (${GAP_LOOKBACK}d lookback)`,
  `inter_visit_gaps_${GAP_LOOKBACK}d.csv`,
  interVisitGaps
);
}

// ── 18. Action cadence ────────────────────────────────────────────────────────
// Gaps between consecutive days where a *specific* action was performed.
// Each action type is treated independently — e.g. "post cadence" computes the
// gap between consecutive posting days, ignoring days with only likes/follows.
// Rows: one per (cohort × action_type).

if (shouldRun("action_cadence")) {
const actionCadence = activityDb.prepare(`
  WITH
  post_gaps AS (
    SELECT did,
      CAST(julianday(date) - julianday(LAG(date) OVER (PARTITION BY did ORDER BY date)) AS INTEGER) AS gap_days
    FROM activity_90d WHERE activity_types & 1
  ),
  like_gaps AS (
    SELECT did,
      CAST(julianday(date) - julianday(LAG(date) OVER (PARTITION BY did ORDER BY date)) AS INTEGER) AS gap_days
    FROM activity_90d WHERE activity_types & 2
  ),
  repost_gaps AS (
    SELECT did,
      CAST(julianday(date) - julianday(LAG(date) OVER (PARTITION BY did ORDER BY date)) AS INTEGER) AS gap_days
    FROM activity_90d WHERE activity_types & 4
  ),
  follow_gaps AS (
    SELECT did,
      CAST(julianday(date) - julianday(LAG(date) OVER (PARTITION BY did ORDER BY date)) AS INTEGER) AS gap_days
    FROM activity_90d WHERE activity_types & 8
  ),
  all_gaps AS (
    SELECT 'post'    AS action, did, gap_days FROM post_gaps   WHERE gap_days IS NOT NULL
    UNION ALL
    SELECT 'like'    AS action, did, gap_days FROM like_gaps   WHERE gap_days IS NOT NULL
    UNION ALL
    SELECT 'repost'  AS action, did, gap_days FROM repost_gaps WHERE gap_days IS NOT NULL
    UNION ALL
    SELECT 'follow'  AS action, did, gap_days FROM follow_gaps WHERE gap_days IS NOT NULL
  )
  SELECT
    ${AGE_BUCKET} AS age_bucket,
    g.action,
    COUNT(DISTINCT g.did)                                                         AS users,
    COUNT(*)                                                                      AS gap_observations,
    ROUND(AVG(g.gap_days), 2)                                                    AS avg_days_between,
    ROUND(SQRT(AVG(g.gap_days * g.gap_days) - AVG(g.gap_days) * AVG(g.gap_days)), 2) AS stddev,
    SUM(CASE WHEN g.gap_days = 1              THEN 1 ELSE 0 END)                 AS gap_1d,
    SUM(CASE WHEN g.gap_days BETWEEN 2 AND 3  THEN 1 ELSE 0 END)                 AS gap_2_3d,
    SUM(CASE WHEN g.gap_days BETWEEN 4 AND 7  THEN 1 ELSE 0 END)                 AS gap_4_7d,
    SUM(CASE WHEN g.gap_days > 7              THEN 1 ELSE 0 END)                 AS gap_8plus_d
  FROM all_gaps g
  JOIN plc.plc_account_creations p ON g.did = p.did
  GROUP BY age_bucket, g.action
  ORDER BY age_bucket, g.action
`).all() as Record<string, unknown>[];

report(
  `Action Cadence by Cohort (${GAP_LOOKBACK}d lookback)`,
  `action_cadence_${GAP_LOOKBACK}d.csv`,
  actionCadence
);
}

// ── 19. User recency distribution ─────────────────────────────────────────────
// Days since each user's last active day in the 90-day window, by cohort.
// Shows which cohorts are staying engaged vs going dormant.
// Users with no activity in 90d are not in activity_90d and are excluded —
// this measures active-user recency, not churn (see cohort_activation for churn).

if (shouldRun("recency")) {
const recencyDist = activityDb.prepare(`
  WITH
  last_seen AS (
    SELECT did, MAX(date) AS last_date
    FROM activity_90d
    GROUP BY did
  )
  SELECT
    ${AGE_BUCKET} AS age_bucket,
    COUNT(*)                                                                       AS users,
    ROUND(AVG(CAST(julianday('now') - julianday(ls.last_date) AS INTEGER)), 1)    AS avg_days_since_active,
    SUM(CASE WHEN julianday('now') - julianday(ls.last_date) <= 1   THEN 1 ELSE 0 END) AS seen_yesterday,
    SUM(CASE WHEN julianday('now') - julianday(ls.last_date) BETWEEN 2 AND 7  THEN 1 ELSE 0 END) AS seen_2_7d,
    SUM(CASE WHEN julianday('now') - julianday(ls.last_date) BETWEEN 8 AND 30 THEN 1 ELSE 0 END) AS seen_8_30d,
    SUM(CASE WHEN julianday('now') - julianday(ls.last_date) > 30  THEN 1 ELSE 0 END) AS seen_31plus_d
  FROM last_seen ls
  JOIN plc.plc_account_creations p ON ls.did = p.did
  GROUP BY age_bucket
  ORDER BY age_bucket
`).all() as Record<string, unknown>[];

report(
  `User Recency Distribution by Cohort (${GAP_LOOKBACK}d window)`,
  `recency_dist_${GAP_LOOKBACK}d.csv`,
  recencyDist
);
}

// ── 20. New-user follow/like funnel ─────────────────────────────────────────
// For each day in the window, counts new-account DIDs (created within the
// window) whose per-day activity_types show: follow-only vs follow+like.
// A user can appear in different buckets on different days as their behavior
// evolves, which makes this useful as a daily funnel progression view.

if (shouldRun("new_user_follow_like")) {
const newUserFollowLike = activityDb.prepare(`
  SELECT
    d.date,
    COUNT(DISTINCT d.did)                                                                   AS new_user_dau,
    COUNT(DISTINCT CASE WHEN (d.activity_types & 10) = 8  THEN d.did END)                  AS followed_only,
    COUNT(DISTINCT CASE WHEN (d.activity_types & 10) = 10 THEN d.did END)                  AS followed_and_liked,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN (d.activity_types & 10) = 8  THEN d.did END)
                / NULLIF(COUNT(DISTINCT CASE WHEN d.activity_types & 8  THEN d.did END), 0), 1) AS pct_followed_only,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN (d.activity_types & 10) = 10 THEN d.did END)
                / NULLIF(COUNT(DISTINCT CASE WHEN d.activity_types & 8  THEN d.did END), 0), 1) AS pct_followed_and_liked
  FROM did_activity_daily d
  JOIN plc.plc_account_creations p ON d.did = p.did
  LEFT JOIN excluded_dids ex ON d.did = ex.did
  WHERE d.date >= '${windowStart}'
    AND p.created_at >= '${windowStart}'
    AND ex.did IS NULL
  GROUP BY d.date
  ORDER BY d.date
`).all() as Record<string, unknown>[];

report(
  `New-User Follow/Like Funnel (${windowStart}–${RUN_DATE})`,
  `new_user_follow_like_${daysBack}d.csv`,
  newUserFollowLike
);
}

// ── 21. DAU / MAU time series ────────────────────────────────────────────────
// For each calendar day, computes:
//   DAU = distinct active DIDs on that day
//   MAU = distinct active DIDs in the trailing 28-day window ending that day
//   ratio = DAU / MAU  (stickiness trend over time)
//
// Uses historyDays + 28 as the warmup window so the first MAU value is fully populated.

if (shouldRun("dau_mau")) {
const warmupStart = daysAgoStr(historyDays + 28);
const trendStart  = daysAgoStr(historyDays);

const dauMauSeries = activityDb.prepare(`
  WITH
  dau AS (
    SELECT d.date, COUNT(DISTINCT d.did) AS dau
    FROM did_activity_daily d
    LEFT JOIN excluded_dids ex ON d.did = ex.did
    WHERE d.date >= ?
      AND ex.did IS NULL
    GROUP BY d.date
  ),
  trend_dates AS (
    SELECT DISTINCT date FROM dau WHERE date >= ?
  ),
  mau AS (
    SELECT
      td.date,
      COUNT(DISTINCT d.did) AS mau
    FROM trend_dates td
    JOIN did_activity_daily d
      ON d.date BETWEEN date(td.date, '-27 days') AND td.date
    LEFT JOIN excluded_dids ex ON d.did = ex.did
    WHERE ex.did IS NULL
    GROUP BY td.date
  )
  SELECT
    dau.date,
    dau.dau,
    mau.mau,
    ROUND(100.0 * dau.dau / mau.mau, 2) AS dau_mau_pct
  FROM dau
  JOIN mau ON dau.date = mau.date
  ORDER BY dau.date
`).all(warmupStart, trendStart) as Record<string, unknown>[];

report(`DAU / MAU Time Series (last ${historyDays} days)`, `dau_mau_${historyDays}d.csv`, dauMauSeries);
}

console.log(`\nDone. CSVs written to ${OUT_DIR}/\n`);
} finally {
  activityDb.exec("COMMIT");
}
