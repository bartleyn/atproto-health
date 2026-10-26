/**
 * Builds analysis.churn_features — the modelling table for the short-horizon churn model.
 *
 * Problem framing (constrained by the ~69-day activity window 2026-04-11 → 2026-06-19):
 *   - Observation point T = 2026-05-22.
 *   - Population: accounts active at least once BEFORE T (something to churn from),
 *     with spam/farms excluded via analysis.excluded_dids.
 *   - Label: churned = 1 if the account has NO activity in [T, T+28) = [05-22, 06-19).
 *   - All features are computed from data strictly before T (no leakage). The graph
 *     snapshot used (analysis.account_graph_snapshot) is dated 2026-05-11, also < T.
 *
 * This table stores RAW counts/graph columns. The model script (scripts/churn_model.py)
 *   - filters to graph-covered rows (followers IS NOT NULL),
 *   - derives action-mix RATES (xxx_days / active_days) and graph log/ratio features,
 *   - so the table stays reusable for other feature recipes.
 *
 * Run: npx tsx --env-file .env src/lib/analysis/churn-features.ts
 */

import sql from "../db/pg";

// Observation point T (CLI arg, default 2026-05-22). All window boundaries are derived
// from T so we can build earlier snapshots (e.g. 2026-05-08) for TEMPORAL validation.
// Default T writes analysis.churn_features; any other T writes a date-suffixed table.
const DATA_START = "2026-04-11";   // first day of activity history
const DEFAULT_T = "2026-05-22";
const T = process.argv[2] || DEFAULT_T;
if (!/^\d{4}-\d{2}-\d{2}$/.test(T)) { console.error(`bad date: ${T}`); process.exit(1); }

const addDays = (d: string, n: number) => {
  const x = new Date(d + "T00:00:00Z"); x.setUTCDate(x.getUTCDate() + n);
  return x.toISOString().slice(0, 10);
};
const midpoint = (a: string, b: string) =>
  addDays(a, Math.round((Date.parse(b) - Date.parse(a)) / 86_400_000 / 2));

const D7 = addDays(T, -7);
const D14 = addDays(T, -14);
const D28 = addDays(T, -28);
const HALF = midpoint(DATA_START, T);   // split feature window [DATA_START, T) for the trend feature
const TEND = addDays(T, 28);            // T+28, label-window end (exclusive)
const BSKY1 = "%.bsky.network", BSKY2 = "%.bsky.social", EN = "en%";
const TABLE = T === DEFAULT_T ? "analysis.churn_features" : `analysis.churn_features_${T.replace(/-/g, "")}`;

async function main() {
  console.log(`building ${TABLE} (T=${T}, feature<${T}, label [${T}, ${TEND}), trend split @${HALF}) ...`);
  await sql.unsafe(`DROP TABLE IF EXISTS ${TABLE}`);
  // Single pass over did_activity_daily (one GROUP BY did) with before/after-T FILTERs.
  // (did, date) is the PK, so dates are unique per did → COUNT(*) == COUNT(DISTINCT date);
  // we scan only up to the label-window end ($6) and split with FILTER, avoiding the
  // 3 separate table scans + COUNT(DISTINCT) of the earlier version.
  await sql.unsafe(`
CREATE TABLE ${TABLE} AS
WITH agg AS (
  SELECT d.did,
    COUNT(*) FILTER (WHERE d.date < $1)::int                                    AS active_days,
    COUNT(*) FILTER (WHERE d.date >= $1 AND d.date < $6)::int                   AS acts_after,
    ($1::date - MAX(d.date) FILTER (WHERE d.date < $1)::date)::int              AS recency_days,
    (MAX(d.date) FILTER (WHERE d.date < $1)::date
       - MIN(d.date) FILTER (WHERE d.date < $1)::date)::int                     AS span_days,
    COUNT(*) FILTER (WHERE d.date >= $2 AND d.date < $1)::int                   AS active_7d,
    COUNT(*) FILTER (WHERE d.date >= $3 AND d.date < $1)::int                   AS active_14d,
    COUNT(*) FILTER (WHERE d.date >= $4 AND d.date < $1)::int                   AS active_28d,
    COUNT(*) FILTER (WHERE d.date >= $5 AND d.date < $1)::int                   AS active_h2,
    COUNT(*) FILTER (WHERE d.date <  $5)::int                                   AS active_h1,
    COUNT(*) FILTER (WHERE d.date < $1 AND d.activity_types & 1 != 0)::int      AS posted_days,
    COUNT(*) FILTER (WHERE d.date < $1 AND d.activity_types & 2 != 0)::int      AS liked_days,
    COUNT(*) FILTER (WHERE d.date < $1 AND d.activity_types & 4 != 0)::int      AS reposted_days,
    COUNT(*) FILTER (WHERE d.date < $1 AND d.activity_types & 8 != 0)::int      AS followed_days,
    COUNT(*) FILTER (WHERE d.date < $1 AND d.activity_types & 8192 != 0)::int   AS replied_days,
    COUNT(*) FILTER (WHERE d.date < $1 AND d.activity_types & 16384 != 0)::int  AS quoted_days
  FROM activity.did_activity_daily d
  WHERE d.date < $6
  GROUP BY d.did
),
langs AS (
  SELECT did, COUNT(*)::int AS n_langs, MAX((lang LIKE $8)::int) AS is_english
  FROM activity.did_langs GROUP BY did
)
SELECT a.did, a.active_days, a.recency_days, a.span_days, a.active_7d, a.active_14d, a.active_28d,
  a.active_h2, a.active_h1, a.posted_days, a.liked_days, a.reposted_days, a.followed_days,
  a.replied_days, a.quoted_days,
  ($1::date - cb.created_at::date)::int                                  AS account_age_days,
  (cb.pds_url LIKE $7 OR cb.pds_url LIKE $9)::int                        AS is_bsky_pds,
  (mig.did IS NOT NULL)::int                                            AS migrated_ever,
  g.followers, g.follows, g.posts AS graph_posts, g.likes_out, g.likes_in,
  g.reposts_out, g.replies_out, g.quotes_out, g.blocks_out,
  COALESCE(l.n_langs, 0)                                                 AS n_langs,
  COALESCE(l.is_english, 0)                                             AS is_english,
  (a.acts_after = 0)::int                                               AS churned
FROM agg a
LEFT JOIN analysis.excluded_dids ex ON ex.did = a.did
LEFT JOIN analysis.cohort_base cb ON cb.did = a.did
LEFT JOIN (SELECT DISTINCT did FROM plc.plc_migrations) mig ON mig.did = a.did
LEFT JOIN analysis.account_graph_snapshot g ON g.did = a.did
LEFT JOIN langs l ON l.did = a.did
WHERE a.active_days > 0 AND ex.did IS NULL
`, [T, D7, D14, D28, HALF, TEND, BSKY1, EN, BSKY2]);

  const [c] = await sql.unsafe(`
    SELECT COUNT(*)::int AS rows,
      ROUND(100.0*AVG(churned),1)::float8 AS churn_pct,
      COUNT(*) FILTER (WHERE followers IS NOT NULL)::int AS with_graph,
      ROUND(100.0*AVG(churned) FILTER (WHERE followers IS NOT NULL),1)::float8 AS churn_pct_covered
    FROM ${TABLE}`);
  console.log(`${TABLE}:`, c);
  await sql.end();
}

main().catch(err => { console.error(err); process.exit(1); });
