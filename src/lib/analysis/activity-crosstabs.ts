import path from "path";
import fs from "fs";
import sql from "../db/pg";

const args = process.argv.slice(2);

if (args.includes("--help") || args.includes("-h")) {
  console.log(`
usage: npm run analysis -- [daysBack] [flags]

  daysBack              activity window in days (default: 3)

flags:
  --only <query>        run one query; repeatable to run several
  --history N           days of history for time-series queries (default: 60)
  --bsky-only           restrict cohort to bsky.network origin/current accounts
  --cohort-end DATE     only include accounts created before DATE (YYYY-MM-DD)
  --window-end DATE     pin the active_days_histogram window to end at DATE
                        (default: today); window = [end − daysBack, end)
  --window-start DATE   pin the active_days_histogram window start explicitly
                        (overrides daysBack-derived start); lets you fix a
                        time-range fully after a cohort's creation
  --help                show this message

queries:
  activity_by_age       activity by cohort age bucket
  activity_by_label     activity by Skywatch label
  stickiness            overall DAU/WAU stickiness ratio
  stickiness_by_age     stickiness broken down by cohort
  active_days_dist      active days distribution × cohort
  active_days_histogram exact per-day-count user histogram ("L plot"), split by cohort (+ '~ all cohorts')
  cohort_activation     % of each cohort active in window
  action_rates          post/like/repost/follow rates by cohort
  non_active            deactivated/takendown breakdown by cohort
  account_events        daily delete/deactivate/reactivate/takendown counts
  trump_weekly          pds.trump.com weekly DID registrations
  lang_activity         language × activity breakdown
  migration_activity    migrated vs non-migrated account activity
  engagement_depth      distinct action types used per user by cohort
  starterpacks          starterpack join rankings
  ns_retention          AppView namespace retention rates
  daily_actions_by_cohort  daily unique posters/likers/followers by cohort
  inter_visit_gaps      days between consecutive active days by cohort
  action_cadence        gaps between consecutive posting/liking/etc days
  recency               days since last active, by cohort
  new_user_follow_like  new-user follow→like funnel by day
  dau_mau               DAU/MAU time series (uses --history)
  dau_mau_snapshot       current DAU and trailing-28d MAU as a single point-in-time pair
  wau_by_cohort         weekly active users by cohort (uses --history)
  mau_by_cohort         monthly active users by cohort (uses --history)
  new_user_activation_28d  % of new accounts active within 28d of creation
  creation_weekly       new account creation counts by week

examples:
  npm run analysis -- 7
  npm run analysis -- 7 --only cohort_activation --only action_rates
  npm run analysis -- --only dau_mau --history 90
  npm run analysis -- 30 --only active_days_histogram --window-end 2026-04-01
  npm run analysis -- --only new_user_activation_28d --history 90 --cohort-end 2026-06-01
`);
  process.exit(0);
}

const daysBack = args[0] && !args[0].startsWith("--") ? parseInt(args[0], 10) : 3;
const historyIdx = args.indexOf("--history");
const historyDays = historyIdx >= 0 ? parseInt(args[historyIdx + 1], 10) : 60;
const bskyOnly = args.includes("--bsky-only");
const cohortEndIdx = args.indexOf("--cohort-end");
const cohortEnd = cohortEndIdx >= 0 ? args[cohortEndIdx + 1] : null;

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

// Date math on 'YYYY-MM-DD' strings (UTC, no TZ drift) for the pinned L-plot window.
function addDays(dateStr: string, n: number): string {
  const d = new Date(dateStr + "T00:00:00Z");
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}
function dayDiff(start: string, end: string): number {
  return Math.round((Date.parse(end) - Date.parse(start)) / 86_400_000);
}

// L-plot ("active_days_histogram") window. By default it's the rolling
const winEndIdx = args.indexOf("--window-end");
const histWindowEnd = winEndIdx >= 0 ? args[winEndIdx + 1] : RUN_DATE;
const winStartIdx = args.indexOf("--window-start");
const histWindowStart = winStartIdx >= 0
  ? args[winStartIdx + 1]
  : addDays(histWindowEnd, -daysBack);
const histWindowDays = dayDiff(histWindowStart, histWindowEnd);

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

// ── Shared SQL fragments ──────────────────────────────────────────────────────

const newAcctCutoff = daysAgoStr(14);  // accounts created in the last 14 days

const AGE_BUCKET = `CASE
  WHEN p.created_at >= '${newAcctCutoff}' THEN '0. new accounts (${newAcctCutoff}–${RUN_DATE})'
  WHEN p.created_at < '2023-01-01'        THEN '1. pre-2023'
  WHEN p.created_at < '2024-01-01'        THEN '2. 2023'
  WHEN p.created_at < '2024-11-01'        THEN '3. 2024 pre-Nov'
  WHEN p.created_at < '2025-01-01'        THEN '4. 2024 Nov-Dec (exodus)'
  WHEN p.created_at < '2025-07-01'        THEN '5. 2025 H1'
  WHEN p.created_at < '2026-01-01'        THEN '6. 2025 H2'
  WHEN p.created_at < '2026-04-01'        THEN '7. 2026 Q1 (Jan-Mar)'
  ELSE                                         '8. 2026 Q2 (Apr-Jun)'
END`;

const ACTIVITY_COLS = `
  COUNT(*)::int                                                       AS total,
  SUM((activity_types & 1 != 0)::int)::int                            AS posted,
  SUM((activity_types & 2 != 0)::int)::int                            AS liked,
  SUM((activity_types & 4 != 0)::int)::int                            AS reposted,
  SUM((activity_types & 8 != 0)::int)::int                            AS followed`;

const windowStart = daysAgoStr(daysBack);
console.log(`\nActivity crosstabs — activity window: ${windowStart} – ${RUN_DATE} (${daysBack} days)`);
if (bskyOnly) console.log(`Cohort filter: bsky.network origin or current`);
if (cohortEnd) console.log(`Cohort end:    created_at < ${cohortEnd}`);

async function main() {
  // Reserve a single connection for the whole run
  const db = await sql.reserve();
  try {

// Pre-materialise the active-user window so each query doesn't re-scan 100M rows.
// active_window_clean: unique (did, activity_types) pairs for the window, spam/bots excluded.
// active_window_all: same but no exclusion filter (for label-analysis queries).
console.log(`Pre-materialising active-user window…`);
await db.unsafe(`
  CREATE TEMP TABLE active_window_clean AS
    SELECT DISTINCT d.did, d.activity_types
    FROM activity.did_activity_daily d
    LEFT JOIN analysis.excluded_dids ex ON d.did = ex.did
    WHERE d.date >= '${windowStart}'
      AND ex.did IS NULL;
  CREATE INDEX idx_active_window_clean_did ON active_window_clean(did);

  CREATE TEMP TABLE active_window_all AS
    SELECT DISTINCT did, activity_types
    FROM activity.did_activity_daily
    WHERE date >= '${windowStart}';
  CREATE INDEX idx_active_window_all_did ON active_window_all(did);
`);
console.log(`  done.\n`);

// When --bsky-only: pre-materialise a filtered cohort (currently on bsky.network OR
// originally created there, covering accounts that have since migrated away).
let COHORT_TABLE = "analysis.cohort_base";
if (bskyOnly) {
  console.log(`Pre-materialising bsky_cohort…`);
  await db.unsafe(`
    CREATE TEMP TABLE bsky_cohort AS
      -- fast path: currently on bsky.network (hits cohort_base pds_url index)
      SELECT * FROM analysis.cohort_base
      WHERE pds_url LIKE '%.bsky.network' OR pds_url LIKE '%.bsky.social'
      UNION ALL
      -- slow path: indie accounts that originated on bsky.network (~2K rows)
      SELECT cb.*
      FROM analysis.cohort_base cb
      JOIN plc.plc_account_creations pac ON pac.did = cb.did
      WHERE (cb.pds_url NOT LIKE '%.bsky.network' AND cb.pds_url NOT LIKE '%.bsky.social')
        AND (pac.pds_url LIKE '%.bsky.network' OR pac.pds_url LIKE '%.bsky.social');
    CREATE INDEX idx_bsky_cohort_did        ON bsky_cohort(did);
    CREATE INDEX idx_bsky_cohort_created_at ON bsky_cohort(created_at);
  `);
  const [{ n }] = await db.unsafe(`SELECT COUNT(*)::int AS n FROM bsky_cohort`) as { n: number }[];
  console.log(`  bsky_cohort: ${n.toLocaleString()} rows\n`);
  COHORT_TABLE = "bsky_cohort";
}

if (cohortEnd) {
  console.log(`Pre-materialising cohort_end_filtered (created_at < ${cohortEnd})…`);
  await db.unsafe(`
    CREATE TEMP TABLE cohort_end_filtered AS
      SELECT * FROM ${COHORT_TABLE}
      WHERE created_at < '${cohortEnd}';
    CREATE INDEX idx_cef_did        ON cohort_end_filtered(did);
    CREATE INDEX idx_cef_created_at ON cohort_end_filtered(created_at);
  `);
  const [{ n }] = await db.unsafe(`SELECT COUNT(*)::int AS n FROM cohort_end_filtered`) as { n: number }[];
  console.log(`  cohort_end_filtered: ${n.toLocaleString()} rows\n`);
  COHORT_TABLE = "cohort_end_filtered";
}

// Single read transaction: all queries see the same consistent DB snapshot.
await db.unsafe("BEGIN");
try {

// ── 1. Activity by age bucket ─────────────────────────────────────────────────

if (shouldRun("activity_by_age")) {
const activityByAge = await db.unsafe(`
  WITH
  buckets AS (
    SELECT
      ${AGE_BUCKET} AS age_bucket,
      a.activity_types
    FROM active_window_clean a
    JOIN ${COHORT_TABLE} p ON a.did = p.did
  )
  SELECT age_bucket, ${ACTIVITY_COLS}
  FROM buckets GROUP BY age_bucket
  UNION ALL
  SELECT '~ TOTAL', ${ACTIVITY_COLS}
  FROM buckets
  ORDER BY age_bucket
`) as Record<string, unknown>[];

report("Activity by Age Bucket", `activity_by_age_${daysBack}d.csv`, activityByAge);
}

// ── 2. Activity by Skywatch label ─────────────────────────────────────────────

if (shouldRun("activity_by_label")) {
const activityByLabel = await db.unsafe(`
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
`) as Record<string, unknown>[];

report("Activity by Skywatch Label", `activity_by_label_${daysBack}d.csv`, activityByLabel);
}

// ── 3. Overall stickiness ─────────────────────────────────────────────────────

if (shouldRun("stickiness")) {
const [stickiness] = await db.unsafe(`
  WITH
  -- Re-join with the raw table for per-day counts; covering index makes this fast.
  daily_counts AS (
    SELECT d.date, COUNT(DISTINCT d.did)::bigint AS daily_uniques
    FROM activity.did_activity_daily d
    LEFT JOIN analysis.excluded_dids ex ON d.did = ex.did
    WHERE d.date >= '${windowStart}'
      AND ex.did IS NULL
    GROUP BY d.date
  ),
  total_unique AS (
    SELECT COUNT(DISTINCT did)::int AS total_uniques FROM active_window_clean
  )
  SELECT
    ROUND(AVG(daily_uniques), 0)::float8                                                              AS avg_daily_uniques,
    ROUND(SQRT(AVG(daily_uniques * daily_uniques) - AVG(daily_uniques) * AVG(daily_uniques)), 1)::float8 AS stddev_daily_uniques,
    MAX(total_uniques)                                                                                 AS total_uniques,
    ROUND(1.0 * AVG(daily_uniques) / MAX(total_uniques), 3)::float8                                     AS ratio,
    ROUND(SQRT(AVG(daily_uniques * daily_uniques) - AVG(daily_uniques) * AVG(daily_uniques)) / MAX(total_uniques), 4)::float8 AS stddev_ratio
  FROM daily_counts, total_unique
`) as Record<string, unknown>[];

report("Stickiness", `stickiness_${daysBack}d.csv`, [stickiness]);
}

// ── 4. Stickiness by age bucket ───────────────────────────────────────────────

if (shouldRun("stickiness_by_age")) {
const stickinessByBucket = await db.unsafe(`
  WITH
  daily_by_bucket AS (
    SELECT
      d.date,
      ${AGE_BUCKET} AS age_bucket,
      COUNT(DISTINCT d.did)::bigint AS daily_uniques
    FROM activity.did_activity_daily d
    JOIN ${COHORT_TABLE} p ON d.did = p.did
    LEFT JOIN analysis.excluded_dids ex ON d.did = ex.did
    WHERE d.date >= '${windowStart}'
      AND ex.did IS NULL
    GROUP BY d.date, age_bucket
  ),
  totals AS (
    SELECT
      ${AGE_BUCKET} AS age_bucket,
      COUNT(DISTINCT a.did)::int AS total_uniques
    FROM active_window_clean a
    JOIN ${COHORT_TABLE} p ON a.did = p.did
    GROUP BY age_bucket
  )
  SELECT
    d.age_bucket,
    ROUND(AVG(d.daily_uniques), 0)::float8                                                                  AS avg_daily_uniques,
    ROUND(SQRT(AVG(d.daily_uniques * d.daily_uniques) - AVG(d.daily_uniques) * AVG(d.daily_uniques)), 1)::float8 AS stddev_daily_uniques,
    MAX(t.total_uniques)                                                                                     AS total_uniques,
    ROUND(1.0 * AVG(d.daily_uniques) / MAX(t.total_uniques), 3)::float8                                      AS ratio,
    ROUND(SQRT(AVG(d.daily_uniques * d.daily_uniques) - AVG(d.daily_uniques) * AVG(d.daily_uniques)) / MAX(t.total_uniques), 4)::float8 AS stddev_ratio
  FROM daily_by_bucket d
  JOIN totals t ON d.age_bucket = t.age_bucket
  GROUP BY d.age_bucket
  ORDER BY d.age_bucket
`) as Record<string, unknown>[];

report("Stickiness by Age Bucket", `stickiness_by_age_${daysBack}d.csv`, stickinessByBucket);
}

// ── 5. Active days distribution × age bucket ──────────────────────────────────

if (shouldRun("active_days_dist")) {
const activeDaysDist = await db.unsafe(`
  WITH
user_days AS (
    SELECT
      d.did,
      ${AGE_BUCKET} AS age_bucket,
      COUNT(DISTINCT d.date)::int AS active_days
    FROM activity.did_activity_daily d
    JOIN ${COHORT_TABLE} p ON d.did = p.did
    LEFT JOIN analysis.excluded_dids ex ON d.did = ex.did
    WHERE d.date >= (CURRENT_DATE - ${daysBack})::text
      AND ex.did IS NULL
    GROUP BY d.did, age_bucket
  )
  SELECT
    age_bucket,
    COUNT(*)::int                                                                                      AS users,
    ROUND(AVG(active_days), 2)::float8                                                                 AS avg_active_days,
    ROUND(SQRT(AVG(active_days * active_days) - AVG(active_days) * AVG(active_days)), 2)::float8       AS stddev_active_days,
    SUM(CASE WHEN active_days = 1                 THEN 1 ELSE 0 END)::int                              AS days_1,
    SUM(CASE WHEN active_days = 2                 THEN 1 ELSE 0 END)::int                              AS days_2,
    SUM(CASE WHEN active_days BETWEEN 3 AND 5     THEN 1 ELSE 0 END)::int                              AS days_3_5,
    SUM(CASE WHEN active_days BETWEEN 6 AND 14    THEN 1 ELSE 0 END)::int                              AS days_6_14,
    SUM(CASE WHEN active_days >= 15               THEN 1 ELSE 0 END)::int                              AS days_15plus
  FROM user_days
  GROUP BY age_bucket
  ORDER BY age_bucket
`) as Record<string, unknown>[];

report(`Active Days Distribution x Age Bucket`, `active_days_by_age_${daysBack}d.csv`, activeDaysDist);
}

// ── 5b. Active-days histogram ("L plot") × cohort ────────────────────────────

if (shouldRun("active_days_histogram")) {
console.log(`L-plot window: ${histWindowStart} – ${histWindowEnd} (${histWindowDays} days, half-open)`);
console.log(`  Cohorts created on/before ${histWindowStart} have had the full window available;`);
console.log(`  any created after are only partially covered.`);
const activeDaysHistogram = await db.unsafe(`
  WITH user_days AS (
    SELECT
      d.did,
      ${AGE_BUCKET} AS age_bucket,
      COUNT(DISTINCT d.date)::int AS active_days
    FROM activity.did_activity_daily d
    JOIN ${COHORT_TABLE} p ON d.did = p.did
    LEFT JOIN analysis.excluded_dids ex ON d.did = ex.did
    WHERE d.date >= '${histWindowStart}'
      AND d.date <  '${histWindowEnd}'
      AND ex.did IS NULL
    GROUP BY d.did, age_bucket
  )
  SELECT age_bucket, active_days, COUNT(*)::int AS users
  FROM user_days
  GROUP BY age_bucket, active_days
  UNION ALL
  SELECT '~ all cohorts', active_days, COUNT(*)::int AS users
  FROM user_days
  GROUP BY active_days
  ORDER BY age_bucket, active_days
`) as Record<string, unknown>[];

report(`Active-Days Histogram ("L plot", ${histWindowStart}–${histWindowEnd}, ${histWindowDays} days) × Cohort`, `active_days_histogram_${histWindowStart}_${histWindowDays}d.csv`, activeDaysHistogram);
}

// ── 6. Cohort activation rate ─────────────────────────────────────────────────

if (shouldRun("cohort_activation")) {
const cohortActivationRate = await db.unsafe(`
  WITH
 total_by_bucket AS (
    SELECT
      ${AGE_BUCKET} AS age_bucket,
      COUNT(*)::int AS total_repos
    FROM ${COHORT_TABLE} p
    GROUP BY age_bucket
  ),
  active_by_bucket AS (
    SELECT
      ${AGE_BUCKET} AS age_bucket,
      COUNT(DISTINCT a.did)::int AS active_users
    FROM active_window_clean a
    JOIN ${COHORT_TABLE} p ON a.did = p.did
    GROUP BY age_bucket
  )
  SELECT
    t.age_bucket,
    t.total_repos,
    COALESCE(a.active_users, 0)                                      AS active_users,
    ROUND(100.0 * COALESCE(a.active_users, 0) / t.total_repos, 2)::float8   AS pct_active
  FROM total_by_bucket t
  LEFT JOIN active_by_bucket a USING (age_bucket)
  ORDER BY t.age_bucket
`) as Record<string, unknown>[];

report(`Cohort Activation Rate`, `cohort_activation_${daysBack}d.csv`, cohortActivationRate);
}

// ── 7. Action rates by cohort ─────────────────────────────────────────────────

if (shouldRun("action_rates")) {
const actionRatesByCohort = await db.unsafe(`
  WITH
active_users AS (
    SELECT
      d.did,
      MAX((d.activity_types & 1 != 0)::int) AS ever_posted,
      MAX((d.activity_types & 2 != 0)::int) AS ever_liked,
      MAX((d.activity_types & 4 != 0)::int) AS ever_reposted,
      MAX((d.activity_types & 8 != 0)::int) AS ever_followed
    FROM activity.did_activity_daily d
    LEFT JOIN analysis.excluded_dids ex ON d.did = ex.did
    WHERE d.date >= (CURRENT_DATE - ${daysBack})::text
      AND ex.did IS NULL
    GROUP BY d.did
  ),
  cohort_sizes AS (
    SELECT
      ${AGE_BUCKET} AS age_bucket,
      COUNT(*)::int AS cohort_size
    FROM ${COHORT_TABLE} p
    GROUP BY age_bucket
  ),
  action_counts AS (
    SELECT
      ${AGE_BUCKET} AS age_bucket,
      SUM(a.ever_posted)::int   AS posted,
      SUM(a.ever_liked)::int    AS liked,
      SUM(a.ever_reposted)::int AS reposted,
      SUM(a.ever_followed)::int AS followed
    FROM active_users a
    JOIN ${COHORT_TABLE} p ON a.did = p.did
    GROUP BY age_bucket
  )
  SELECT
    c.age_bucket,
    c.cohort_size,
    COALESCE(a.posted, 0)                                                   AS posted_n,
    ROUND(100.0 * COALESCE(a.posted, 0)   / c.cohort_size, 2)::float8       AS pct_posted,
    COALESCE(a.liked, 0)                                                    AS liked_n,
    ROUND(100.0 * COALESCE(a.liked, 0)    / c.cohort_size, 2)::float8       AS pct_liked,
    COALESCE(a.reposted, 0)                                                 AS reposted_n,
    ROUND(100.0 * COALESCE(a.reposted, 0) / c.cohort_size, 2)::float8       AS pct_reposted,
    COALESCE(a.followed, 0)                                                 AS followed_n,
    ROUND(100.0 * COALESCE(a.followed, 0) / c.cohort_size, 2)::float8       AS pct_followed
  FROM cohort_sizes c
  LEFT JOIN action_counts a USING (age_bucket)
  ORDER BY c.age_bucket
`) as Record<string, unknown>[];

report(`Action Rates by Cohort`, `action_rates_by_cohort_${daysBack}d.csv`, actionRatesByCohort);
}

// ── 8. Non-active account status by cohort ───────────────────────────────────

const AGE_BUCKET_PLC = AGE_BUCKET;  // same expression, same `p` alias

if (shouldRun("non_active")) {
const nonActiveByCohort = await db.unsafe(`
  WITH
 cohort_sizes AS (
    SELECT
      ${AGE_BUCKET_PLC} AS age_bucket,
      COUNT(*)::int AS cohort_size
    FROM plc.plc_account_creations p
    LEFT JOIN plc.did_repo_status s ON p.did = s.did
    WHERE s.did IS NULL
    GROUP BY age_bucket
  ),
  status_counts AS (
    SELECT
      ${AGE_BUCKET_PLC} AS age_bucket,
      SUM(CASE WHEN s.status = 'deactivated' THEN 1 ELSE 0 END)::int AS deactivated,
      SUM(CASE WHEN s.status IN ('takendown','takedown') THEN 1 ELSE 0 END)::int AS takendown,
      COUNT(*)::int AS total_non_active
    FROM plc.did_repo_status s
    JOIN plc.plc_account_creations p ON s.did = p.did
    GROUP BY age_bucket
  )
  SELECT
    c.age_bucket,
    c.cohort_size,
    COALESCE(sc.deactivated, 0)                                                    AS deactivated_n,
    ROUND(100.0 * COALESCE(sc.deactivated, 0)   / c.cohort_size, 2)::float8        AS pct_deactivated,
    COALESCE(sc.takendown, 0)                                                      AS takendown_n,
    ROUND(100.0 * COALESCE(sc.takendown, 0)     / c.cohort_size, 2)::float8        AS pct_takendown,
    COALESCE(sc.total_non_active, 0)                                               AS total_non_active_n,
    ROUND(100.0 * COALESCE(sc.total_non_active, 0) / c.cohort_size, 2)::float8     AS pct_non_active
  FROM cohort_sizes c
  LEFT JOIN status_counts sc USING (age_bucket)
  ORDER BY c.age_bucket
`) as Record<string, unknown>[];

report("Non-Active Account Status by Cohort", `non_active_by_cohort.csv`, nonActiveByCohort);
}

// ── 9. Account-level event trends ────────────────────────────────────────────

if (shouldRun("account_events")) {
const accountEventTrends = await db.unsafe(`
  SELECT
    date,
    SUM(CASE WHEN event_type = 'account:deleted'     THEN count ELSE 0 END)::int AS deleted,
    SUM(CASE WHEN event_type = 'account:deactivated' THEN count ELSE 0 END)::int AS deactivated,
    SUM(CASE WHEN event_type = 'account:reactivated' THEN count ELSE 0 END)::int AS reactivated,
    SUM(CASE WHEN event_type = 'account:takendown'   THEN count ELSE 0 END)::int AS takendown
  FROM activity.delete_events_daily
  WHERE event_type IN ('account:deleted','account:deactivated','account:reactivated','account:takendown')
    AND date >= (CURRENT_DATE - ${daysBack})::text
  GROUP BY date
  ORDER BY date
`) as Record<string, unknown>[];

report(`Account Event Trends`, `account_event_trends_${daysBack}d.csv`, accountEventTrends);
}

// ── 10. pds.trump.com weekly DID registrations (cumulative) ─────────────────

if (shouldRun("trump_weekly")) {
const trumpWeekly = await db.unsafe(`
  SELECT
    week,
    count                                     AS new_dids,
    SUM(count) OVER (ORDER BY week)::int      AS cumulative_dids
  FROM plc.plc_creation_weekly
  WHERE pds_url = 'https://pds.trump.com'
  ORDER BY week
`) as Record<string, unknown>[];

report("pds.trump.com Weekly DID Registrations", "trump_pds_weekly.csv", trumpWeekly);
}

// ── 11. Language × activity ───────────────────────────────────────────────────
// Uses did_langs from the activity schema (accumulated from post events) joined
// with did_activity_daily. A DID can have multiple lang rows; it counts toward each.

if (shouldRun("lang_activity")) {
const langActivity = await db.unsafe(`
  WITH agg AS (
    SELECT did,
      MAX((activity_types & 1 != 0)::int) AS posted,
      MAX((activity_types & 2 != 0)::int) AS liked,
      MAX((activity_types & 4 != 0)::int) AS reposted,
      MAX((activity_types & 8 != 0)::int) AS followed
    FROM active_window_clean
    GROUP BY did
  )
  SELECT
    dl.lang,
    COUNT(DISTINCT dl.did)::int                                           AS active_users,
    SUM(dl.post_count)::bigint                                            AS lifetime_posts,
    ROUND(1.0 * SUM(dl.post_count) / COUNT(DISTINCT dl.did), 1)::float8   AS avg_lifetime_posts_per_user,
    SUM(a.posted)::int                                                    AS posted_in_window,
    SUM(a.liked)::int                                                     AS liked_in_window,
    SUM(a.reposted)::int                                                  AS reposted_in_window,
    SUM(a.followed)::int                                                  AS followed_in_window
  FROM activity.did_langs dl
  JOIN agg a ON dl.did = a.did
  GROUP BY dl.lang
  ORDER BY active_users DESC
  LIMIT 40
`) as Record<string, unknown>[];

report(`Language × Activity`, `lang_activity_${daysBack}d.csv`, langActivity);
}

// ── 12. Migration × activity ──────────────────────────────────────────────────
// Migrated = has at least one row in plc_migrations. Compares activation rate,
// active days, and action type rates between migrated and non-migrated cohorts.

if (shouldRun("migration_activity")) {
const migrationActivity = await db.unsafe(`
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
           COUNT(DISTINCT date)::int                                       AS active_days,
           MAX((activity_types & 1 != 0)::int)                            AS ever_posted,
           MAX((activity_types & 2 != 0)::int)                            AS ever_liked,
           MAX((activity_types & 4 != 0)::int)                            AS ever_reposted,
           MAX((activity_types & 8 != 0)::int)                            AS ever_followed
    FROM activity.did_activity_daily
    WHERE date >= (CURRENT_DATE - ${daysBack})::text
    GROUP BY did
  )
  SELECT
    b.segment,
    COUNT(*)::int                                                          AS total_repos,
    COUNT(a.did)::int                                                      AS active_users,
    ROUND(100.0 * COUNT(a.did) / COUNT(*), 2)::float8                     AS pct_active,
    ROUND(AVG(COALESCE(a.active_days, 0)), 2)::float8                    AS avg_active_days,
    ROUND(100.0 * SUM(COALESCE(a.ever_posted,   0)) / COUNT(*), 2)::float8 AS pct_posted,
    ROUND(100.0 * SUM(COALESCE(a.ever_liked,    0)) / COUNT(*), 2)::float8 AS pct_liked,
    ROUND(100.0 * SUM(COALESCE(a.ever_reposted, 0)) / COUNT(*), 2)::float8 AS pct_reposted,
    ROUND(100.0 * SUM(COALESCE(a.ever_followed, 0)) / COUNT(*), 2)::float8 AS pct_followed
  FROM repo_base b
  LEFT JOIN active_in_window a ON b.did = a.did
  GROUP BY b.segment
  ORDER BY b.segment
`) as Record<string, unknown>[];

report(`Migration × Activity`, `migration_activity_${daysBack}d.csv`, migrationActivity);
}

// ── 13. Engagement depth by cohort ───────────────────────────────────────────
// For each active user, count how many distinct action types they used in the
// window (0–4). Groups by cohort. Shows depth-of-engagement, not just presence.

if (shouldRun("engagement_depth")) {
const engagementDepth = await db.unsafe(`
  WITH
user_depth AS (
    SELECT
      d.did AS did,
      SUM(
        (activity_types & 1 != 0)::int +
        (activity_types & 2 != 0)::int +
        (activity_types & 4 != 0)::int +
        (activity_types & 8 != 0)::int
      ) / COUNT(DISTINCT date)   AS avg_types_per_day,
      (
        MAX((activity_types & 1 != 0)::int) +
        MAX((activity_types & 2 != 0)::int) +
        MAX((activity_types & 4 != 0)::int) +
        MAX((activity_types & 8 != 0)::int)
      )                          AS distinct_types_used
    FROM activity.did_activity_daily d
    LEFT JOIN analysis.excluded_dids ex ON d.did = ex.did
    WHERE d.date >= (CURRENT_DATE - ${daysBack})::text
      AND ex.did IS NULL
    GROUP BY d.did
  ),
  with_bucket AS (
    SELECT
      ud.distinct_types_used,
      ${AGE_BUCKET} AS age_bucket
    FROM user_depth ud
    JOIN ${COHORT_TABLE} p ON ud.did = p.did
  )
  SELECT
    age_bucket,
    COUNT(*)::int                                                          AS active_users,
    ROUND(AVG(distinct_types_used), 3)::float8                           AS avg_distinct_types,
    SUM(CASE WHEN distinct_types_used = 1 THEN 1 ELSE 0 END)::int        AS depth_1,
    SUM(CASE WHEN distinct_types_used = 2 THEN 1 ELSE 0 END)::int        AS depth_2,
    SUM(CASE WHEN distinct_types_used = 3 THEN 1 ELSE 0 END)::int        AS depth_3,
    SUM(CASE WHEN distinct_types_used = 4 THEN 1 ELSE 0 END)::int        AS depth_4,
    ROUND(100.0 * SUM(CASE WHEN distinct_types_used = 4 THEN 1 ELSE 0 END) / COUNT(*), 2)::float8 AS pct_full_depth
  FROM with_bucket
  GROUP BY age_bucket
  ORDER BY age_bucket
`) as Record<string, unknown>[];

report(`Engagement Depth by Cohort`, `engagement_depth_${daysBack}d.csv`, engagementDepth);
}

// ── 14. Starterpack joins ranking ────────────────────────────────────────────
// Simple cumulative join count per starterpack URI, sorted by total joins.
// Data is sparse until the collector has been running longer.

if (shouldRun("starterpacks")) {
const starterpacks = await db.unsafe(`
  SELECT
    starterpack_uri,
    SUM(count)::int AS total_joins,
    MIN(date)        AS first_recorded,
    MAX(date)        AS last_recorded,
    COUNT(date)::int AS days_with_data
  FROM activity.starterpack_joins_daily
  GROUP BY starterpack_uri
  ORDER BY total_joins DESC
  LIMIT 50
`) as Record<string, unknown>[];

report("Starterpack Joins Ranking", "starterpack_joins.csv", starterpacks);
}

// ── 15. AppView / namespace retention ───────────────────────────────────────
// For each non-bsky namespace root (first 2 NSID parts), count unique lifetime
// users from collection_activity, then check what fraction appear in
// did_activity_daily within the window — showing whether appview users are
// retained bsky users or have drifted off.

if (shouldRun("ns_retention")) {
const rawNsRetention = await db.unsafe(`
  WITH
 ns_users AS (
    SELECT collection, did
    FROM activity.collection_activity
    WHERE collection NOT LIKE 'app.bsky.%' AND collection NOT LIKE 'chat.bsky.%'
  ),
  active_in_window AS (
    SELECT DISTINCT did FROM active_window_all
  )
  SELECT
    nu.collection,
    COUNT(DISTINCT nu.did)::int                                           AS lifetime_users,
    COUNT(DISTINCT a.did)::int                                            AS active_in_window
  FROM ns_users nu
  LEFT JOIN active_in_window a ON nu.did = a.did
  GROUP BY nu.collection
  HAVING COUNT(DISTINCT nu.did) >= 2
`) as { collection: string; lifetime_users: number; active_in_window: number }[];

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
const dailyActionsByCohort = await db.unsafe(`
  SELECT
    d.date,
    ${AGE_BUCKET} AS age_bucket,
    COUNT(DISTINCT d.did)::int                                              AS dau,
    COUNT(DISTINCT CASE WHEN d.activity_types & 2 != 0 THEN d.did END)::int AS unique_likers,
    COUNT(DISTINCT CASE WHEN d.activity_types & 8 != 0 THEN d.did END)::int AS unique_followers,
    COUNT(DISTINCT CASE WHEN d.activity_types & 1 != 0 THEN d.did END)::int AS unique_posters
  FROM activity.did_activity_daily d
  JOIN ${COHORT_TABLE} p ON d.did = p.did
  WHERE d.date >= (CURRENT_DATE - ${daysBack})::text
  GROUP BY d.date, age_bucket
  ORDER BY age_bucket, d.date
`) as Record<string, unknown>[];

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
  await db.unsafe(`
    CREATE TEMP TABLE activity_90d AS
      SELECT d.did, d.date, d.activity_types
      FROM activity.did_activity_daily d
      LEFT JOIN analysis.excluded_dids ex ON d.did = ex.did
      WHERE d.date >= (CURRENT_DATE - ${GAP_LOOKBACK})::text
        AND ex.did IS NULL;
    CREATE INDEX idx_activity_90d_did_date ON activity_90d(did, date);
  `);
  console.log(`  done.\n`);
}

if (shouldRun("inter_visit_gaps")) {
const interVisitGaps = await db.unsafe(`
  WITH
  gaps AS (
    SELECT
      did,
      (date::date - LAG(date::date) OVER (PARTITION BY did ORDER BY date))::int AS gap_days
    FROM activity_90d
  )
  SELECT
    ${AGE_BUCKET} AS age_bucket,
    COUNT(DISTINCT g.did)::int                                                  AS users,
    COUNT(*)::int                                                               AS gap_observations,
    ROUND(AVG(g.gap_days), 2)::float8                                          AS avg_gap_days,
    ROUND(SQRT(AVG(g.gap_days * g.gap_days) - AVG(g.gap_days) * AVG(g.gap_days)), 2)::float8 AS stddev_gap_days,
    SUM(CASE WHEN g.gap_days = 1               THEN 1 ELSE 0 END)::int        AS gap_1d,
    SUM(CASE WHEN g.gap_days BETWEEN 2 AND 3   THEN 1 ELSE 0 END)::int        AS gap_2_3d,
    SUM(CASE WHEN g.gap_days BETWEEN 4 AND 7   THEN 1 ELSE 0 END)::int        AS gap_4_7d,
    SUM(CASE WHEN g.gap_days BETWEEN 8 AND 14  THEN 1 ELSE 0 END)::int        AS gap_8_14d,
    SUM(CASE WHEN g.gap_days > 14              THEN 1 ELSE 0 END)::int        AS gap_15plus_d,
    ROUND(100.0 * SUM(CASE WHEN g.gap_days = 1 THEN 1 ELSE 0 END) / COUNT(*), 1)::float8 AS pct_returned_next_day
  FROM gaps g
  JOIN ${COHORT_TABLE} p ON g.did = p.did
  WHERE g.gap_days IS NOT NULL
  GROUP BY age_bucket
  ORDER BY age_bucket
`) as Record<string, unknown>[];

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
const actionCadence = await db.unsafe(`
  WITH
  post_gaps AS (
    SELECT did,
      (date::date - LAG(date::date) OVER (PARTITION BY did ORDER BY date))::int AS gap_days
    FROM activity_90d WHERE activity_types & 1 != 0
  ),
  like_gaps AS (
    SELECT did,
      (date::date - LAG(date::date) OVER (PARTITION BY did ORDER BY date))::int AS gap_days
    FROM activity_90d WHERE activity_types & 2 != 0
  ),
  repost_gaps AS (
    SELECT did,
      (date::date - LAG(date::date) OVER (PARTITION BY did ORDER BY date))::int AS gap_days
    FROM activity_90d WHERE activity_types & 4 != 0
  ),
  follow_gaps AS (
    SELECT did,
      (date::date - LAG(date::date) OVER (PARTITION BY did ORDER BY date))::int AS gap_days
    FROM activity_90d WHERE activity_types & 8 != 0
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
    COUNT(DISTINCT g.did)::int                                                    AS users,
    COUNT(*)::int                                                                 AS gap_observations,
    ROUND(AVG(g.gap_days), 2)::float8                                            AS avg_days_between,
    ROUND(SQRT(AVG(g.gap_days * g.gap_days) - AVG(g.gap_days) * AVG(g.gap_days)), 2)::float8 AS stddev,
    SUM(CASE WHEN g.gap_days = 1              THEN 1 ELSE 0 END)::int           AS gap_1d,
    SUM(CASE WHEN g.gap_days BETWEEN 2 AND 3  THEN 1 ELSE 0 END)::int           AS gap_2_3d,
    SUM(CASE WHEN g.gap_days BETWEEN 4 AND 7  THEN 1 ELSE 0 END)::int           AS gap_4_7d,
    SUM(CASE WHEN g.gap_days > 7              THEN 1 ELSE 0 END)::int           AS gap_8plus_d
  FROM all_gaps g
  JOIN ${COHORT_TABLE} p ON g.did = p.did
  GROUP BY age_bucket, g.action
  ORDER BY age_bucket, g.action
`) as Record<string, unknown>[];

report(
  `Action Cadence by Cohort (${GAP_LOOKBACK}d lookback)`,
  `action_cadence_${GAP_LOOKBACK}d.csv`,
  actionCadence
);
}

// ── 19. User recency distribution ─────────────────────────────────────────────
// Days since each user's last active day in the 90-day window, by cohort.
// Shows which cohorts are staying engaged vs going dormant.

if (shouldRun("recency")) {
const recencyDist = await db.unsafe(`
  WITH
  last_seen AS (
    SELECT did, MAX(date) AS last_date
    FROM activity_90d
    GROUP BY did
  )
  SELECT
    ${AGE_BUCKET} AS age_bucket,
    COUNT(*)::int                                                                       AS users,
    ROUND(AVG(CURRENT_DATE - ls.last_date::date), 1)::float8                            AS avg_days_since_active,
    SUM(CASE WHEN CURRENT_DATE - ls.last_date::date <= 1   THEN 1 ELSE 0 END)::int       AS seen_yesterday,
    SUM(CASE WHEN CURRENT_DATE - ls.last_date::date BETWEEN 2 AND 7  THEN 1 ELSE 0 END)::int AS seen_2_7d,
    SUM(CASE WHEN CURRENT_DATE - ls.last_date::date BETWEEN 8 AND 30 THEN 1 ELSE 0 END)::int AS seen_8_30d,
    SUM(CASE WHEN CURRENT_DATE - ls.last_date::date > 30  THEN 1 ELSE 0 END)::int        AS seen_31plus_d
  FROM last_seen ls
  JOIN ${COHORT_TABLE} p ON ls.did = p.did
  GROUP BY age_bucket
  ORDER BY age_bucket
`) as Record<string, unknown>[];

report(
  `User Recency Distribution by Cohort (${GAP_LOOKBACK}d window)`,
  `recency_dist_${GAP_LOOKBACK}d.csv`,
  recencyDist
);
}

// ── 20. New-user follow/like funnel ─────────────────────────────────────────
// For each day in the window, counts new-account DIDs (created within the
// window) whose per-day activity_types show: follow-only vs follow+like.

if (shouldRun("new_user_follow_like")) {
const newUserFollowLike = await db.unsafe(`
  SELECT
    d.date,
    COUNT(DISTINCT d.did)::int                                                                       AS new_user_dau,
    COUNT(DISTINCT CASE WHEN (d.activity_types & 10) = 8  THEN d.did END)::int                       AS followed_only,
    COUNT(DISTINCT CASE WHEN (d.activity_types & 10) = 10 THEN d.did END)::int                       AS followed_and_liked,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN (d.activity_types & 10) = 8  THEN d.did END)
                / NULLIF(COUNT(DISTINCT CASE WHEN d.activity_types & 8 != 0 THEN d.did END), 0), 1)::float8 AS pct_followed_only,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN (d.activity_types & 10) = 10 THEN d.did END)
                / NULLIF(COUNT(DISTINCT CASE WHEN d.activity_types & 8 != 0 THEN d.did END), 0), 1)::float8 AS pct_followed_and_liked
  FROM activity.did_activity_daily d
  JOIN ${COHORT_TABLE} p ON d.did = p.did
  WHERE d.date >= '${windowStart}'
    AND p.created_at >= '${windowStart}'
  GROUP BY d.date
  ORDER BY d.date
`) as Record<string, unknown>[];

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

const dauMauSeries = await db.unsafe(`
  WITH
  dau AS (
    SELECT d.date, COUNT(DISTINCT d.did)::int AS dau
    FROM activity.did_activity_daily d
    LEFT JOIN analysis.excluded_dids ex ON d.did = ex.did
    WHERE d.date >= '${warmupStart}'
      AND ex.did IS NULL
    GROUP BY d.date
  ),
  trend_dates AS (
    SELECT DISTINCT date FROM dau WHERE date >= '${trendStart}'
  ),
  mau AS (
    SELECT
      td.date,
      COUNT(DISTINCT d.did)::int AS mau
    FROM trend_dates td
    JOIN activity.did_activity_daily d
      ON d.date BETWEEN (td.date::date - 27)::text AND td.date
    LEFT JOIN analysis.excluded_dids ex ON d.did = ex.did
    WHERE ex.did IS NULL
    GROUP BY td.date
  )
  SELECT
    dau.date,
    dau.dau,
    mau.mau,
    ROUND(100.0 * dau.dau / mau.mau, 2)::float8 AS dau_mau_pct
  FROM dau
  JOIN mau ON dau.date = mau.date
  ORDER BY dau.date
`) as Record<string, unknown>[];

report(`DAU / MAU Time Series (last ${historyDays} days)`, `dau_mau_${historyDays}d.csv`, dauMauSeries);
}

// ── 21b. DAU/MAU snapshot ─────────────────────────────────────────────────────
// Point-in-time health check: DAU on the most recent collected day, and MAU as 28d rolling

if (shouldRun("dau_mau_snapshot")) {
const [dauMauSnapshot] = await db.unsafe(`
  WITH
  latest AS (
    SELECT MAX(date) AS d FROM activity.did_activity_daily
  ),
  dau AS (
    SELECT COUNT(DISTINCT d.did)::int AS dau
    FROM activity.did_activity_daily d
    CROSS JOIN latest l
    LEFT JOIN analysis.excluded_dids ex ON d.did = ex.did
    WHERE d.date = l.d
      AND ex.did IS NULL
  ),
  mau AS (
    SELECT COUNT(DISTINCT d.did)::int AS mau
    FROM activity.did_activity_daily d
    CROSS JOIN latest l
    LEFT JOIN analysis.excluded_dids ex ON d.did = ex.did
    WHERE d.date BETWEEN (l.d::date - 27)::text AND l.d
      AND ex.did IS NULL
  )
  SELECT
    latest.d                                   AS as_of_date,
    dau.dau,
    mau.mau,
    ROUND(100.0 * dau.dau / mau.mau, 2)::float8 AS dau_mau_pct
  FROM dau, mau, latest
`) as Record<string, unknown>[];

report("DAU/MAU Snapshot", "dau_mau_snapshot.csv", [dauMauSnapshot]);
}

// ── 22. WAU by cohort ─────────────────────────────────────────────────────────
// Weekly active users per age cohort — distinct DIDs active in each week.

if (shouldRun("wau_by_cohort")) {
const wauByCohort = await db.unsafe(`
  SELECT
    (d.date::date + ((8 - EXTRACT(DOW FROM d.date::date)::int) % 7) - 7) AS week_start,
    ${AGE_BUCKET}                                                         AS age_bucket,
    COUNT(DISTINCT d.did)::int                                            AS wau
  FROM activity.did_activity_daily d
  JOIN ${COHORT_TABLE} p ON d.did = p.did
  LEFT JOIN analysis.excluded_dids ex ON d.did = ex.did
  WHERE d.date >= (CURRENT_DATE - ${historyDays})::text
    AND ex.did IS NULL
  GROUP BY week_start, age_bucket
  ORDER BY week_start, age_bucket
`) as Record<string, unknown>[];

report(
  `WAU by Cohort (last ${historyDays} days)`,
  `wau_by_cohort_${historyDays}d.csv`,
  wauByCohort
);
}

// ── 23. MAU by cohort ─────────────────────────────────────────────────────────
// Monthly active users per age cohort — distinct DIDs active in each calendar month.

if (shouldRun("mau_by_cohort")) {
const mauByCohort = await db.unsafe(`
  SELECT
    to_char(d.date::date, 'YYYY-MM') AS month,
    ${AGE_BUCKET}                     AS age_bucket,
    COUNT(DISTINCT d.did)::int        AS mau
  FROM activity.did_activity_daily d
  JOIN ${COHORT_TABLE} p ON d.did = p.did
  LEFT JOIN analysis.excluded_dids ex ON d.did = ex.did
  WHERE d.date >= (CURRENT_DATE - ${historyDays})::text
    AND ex.did IS NULL
  GROUP BY month, age_bucket
  ORDER BY month, age_bucket
`) as Record<string, unknown>[];

report(
  `MAU by Cohort (last ${historyDays} days)`,
  `mau_by_cohort_${historyDays}d.csv`,
  mauByCohort
);
}

// ── 24. New-user 28-day activation funnel ────────────────────────────────────
// For each creation week in the lookback window, what % of new accounts showed
// any activity within their first 28 days?
// Only includes creation weeks where 28 days have elapsed (complete windows).

if (shouldRun("new_user_activation_28d")) {
const activationStart = daysAgoStr(historyDays + 28);

const newUserActivation = await db.unsafe(`
  WITH new_accounts AS (
    SELECT
      p.did,
      p.created_at::date                                                                       AS creation_date,
      (p.created_at::date + ((8 - EXTRACT(DOW FROM p.created_at::date)::int) % 7) - 7)         AS creation_week
    FROM ${COHORT_TABLE} p
    LEFT JOIN analysis.excluded_dids ex ON p.did = ex.did
    WHERE p.created_at >= '${activationStart}'
      AND p.created_at::date <= (CURRENT_DATE - 28)
      AND ex.did IS NULL
  ),
  first_activity AS (
    SELECT
      na.did,
      na.creation_date,
      na.creation_week,
      MIN(a.date)::date AS first_active_date
    FROM new_accounts na
    LEFT JOIN activity.did_activity_daily a
      ON a.did = na.did
      AND a.date BETWEEN na.creation_date::text AND (na.creation_date + 27)::text
    GROUP BY na.did, na.creation_date, na.creation_week
  )
  SELECT
    creation_week,
    COUNT(*)::int                                                            AS new_accounts,
    COUNT(first_active_date)::int                                            AS activated,
    ROUND(100.0 * COUNT(first_active_date) / COUNT(*), 2)::float8           AS pct_activated,
    COUNT(CASE WHEN first_active_date = creation_date          THEN 1 END)::int AS activated_d0,
    COUNT(CASE WHEN first_active_date <= creation_date + 6
                AND first_active_date > creation_date          THEN 1 END)::int AS activated_d1_6,
    COUNT(CASE WHEN first_active_date > creation_date + 6 THEN 1 END)::int  AS activated_d7_27
  FROM first_activity
  GROUP BY creation_week
  ORDER BY creation_week
`) as Record<string, unknown>[];

report(
  `New-User 28-Day Activation by Creation Week (${activationStart}–${daysAgoStr(28)})`,
  `new_user_activation_28d_${historyDays}d.csv`,
  newUserActivation
);
}

// ── 25. Weekly creation counts ───────────────────────────────────────────────
// New activated accounts per week, grouped by the Monday-anchored week of their
// PLC creation date. Uses COHORT_TABLE so --bsky-only scopes it to bsky-origin accounts.

if (shouldRun("creation_weekly")) {
const creationWeekly = await db.unsafe(`
  SELECT
    (p.created_at::date - ((EXTRACT(DOW FROM p.created_at::date)::int + 6) % 7)) AS week,
    COUNT(*)::int AS new_accounts
  FROM ${COHORT_TABLE} p
  GROUP BY week
  ORDER BY week
`) as Record<string, unknown>[];

report(`Weekly Creation Counts`, `creation_weekly.csv`, creationWeekly);
}

console.log(`\nDone. CSVs written to ${OUT_DIR}/\n`);
} finally {
  await db.unsafe("COMMIT");
}

  } finally {
    db.release();
    await sql.end();
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
