-- Analysis materialized views.
--
-- These pre-aggregate the per-DID deletion tables that the deletion crosstabs
-- in src/lib/analysis/activity-crosstabs.ts read. activity.post_deletes_daily
-- is ~3M rows and every deletion cut re-derives the same per-DID rollup from
-- it, so the rollup lives here once instead of in each query.
--
-- Refresh with `npm run analysis:matviews` after a collector backfill.
--
-- Window note: activity.post_deletes_daily only starts 2026-06-17 (the table
-- postdates the other activity tables), while delete_events_daily reaches back
-- to the start of collection. The per-DID views below are therefore narrower
-- than the network-wide one, and each carries its own window in
-- analysis.mv_deletion_windows so reports can state what they cover.

-- ── Network-wide deletes by record/account type, one row per (date, type) ────
-- Straight passthrough of the collector's counters. Cheap, but materialized so
-- the deletion report reads one consistent snapshot alongside the per-DID views.
DROP MATERIALIZED VIEW IF EXISTS analysis.mv_delete_events_daily CASCADE;
CREATE MATERIALIZED VIEW analysis.mv_delete_events_daily AS
SELECT
  date,
  event_type,
  count,
  CASE WHEN event_type LIKE 'record:%' THEN 'record' ELSE 'account' END AS kind,
  CASE WHEN event_type LIKE 'record:%'
       THEN substring(event_type from 8) ELSE NULL END AS collection
FROM activity.delete_events_daily;

CREATE UNIQUE INDEX ON analysis.mv_delete_events_daily (date, event_type);
CREATE INDEX ON analysis.mv_delete_events_daily (kind, date);

-- ── Per-DID post-delete profile over the whole retained window ───────────────
-- One row per DID that has ever deleted a post. `pattern` splits one-shot
-- history purges from steady auto-delete behaviour: burst_share is the fraction
-- of an account's deletes landing on its single busiest day, which separates an
-- account wiping its archive in one go from one pruning a little every day.
DROP MATERIALIZED VIEW IF EXISTS analysis.mv_did_delete_profile CASCADE;
CREATE MATERIALIZED VIEW analysis.mv_did_delete_profile AS
WITH per_did AS (
  SELECT
    did,
    SUM(count)::bigint  AS deletes,
    COUNT(*)::int       AS active_days,
    MAX(count)::int     AS max_day,
    MIN(date)           AS first_date,
    MAX(date)           AS last_date
  FROM activity.post_deletes_daily
  GROUP BY did
)
SELECT
  p.did,
  p.deletes,
  p.active_days,
  p.max_day,
  p.first_date,
  p.last_date,
  (p.max_day::numeric / p.deletes) AS burst_share,
  CASE
    WHEN p.max_day::numeric / p.deletes >= 0.8  THEN 'A. one-shot purge'
    WHEN p.max_day::numeric / p.deletes >= 0.5  THEN 'B. mostly one day'
    WHEN p.max_day::numeric / p.deletes >= 0.25 THEN 'C. few big days'
    ELSE                                             'D. sustained'
  END AS pattern,
  CASE
    WHEN p.deletes = 1      THEN 'a. 1'
    WHEN p.deletes <= 10    THEN 'b. 2-10'
    WHEN p.deletes <= 100   THEN 'c. 11-100'
    WHEN p.deletes <= 1000  THEN 'd. 101-1k'
    WHEN p.deletes <= 10000 THEN 'e. 1k-10k'
    ELSE                         'f. 10k+'
  END AS volume_bucket,
  -- did_repo_status only holds accounts that have left normal service, so a
  -- missing row means the account is still live rather than unscanned.
  COALESCE(r.status, 'active')            AS repo_status,
  h.handle,
  (h.handle LIKE '%.ap.brid.gy')          AS is_bridged,
  (sw.did IS NOT NULL)                    AS has_skywatch_label,
  (st.did IS NOT NULL)                    AS has_stechlab_label,
  (bm.did IS NOT NULL)                    AS has_bsky_mod_label
FROM per_did p
LEFT JOIN plc.did_repo_status r  ON r.did = p.did
LEFT JOIN plc.plc_did_handle  h  ON h.did = p.did
LEFT JOIN (SELECT DISTINCT did FROM plc.skywatch_labels) sw ON sw.did = p.did
LEFT JOIN (SELECT DISTINCT did FROM plc.stechlab_labels) st ON st.did = p.did
LEFT JOIN (SELECT DISTINCT did FROM plc.bsky_mod_labels) bm ON bm.did = p.did;

CREATE UNIQUE INDEX ON analysis.mv_did_delete_profile (did);
CREATE INDEX ON analysis.mv_did_delete_profile (deletes DESC);
CREATE INDEX ON analysis.mv_did_delete_profile (pattern);
CREATE INDEX ON analysis.mv_did_delete_profile (volume_bucket);

-- ── Daily per-DID delete rollup ─────────────────────────────────────────────
-- deleters = distinct accounts deleting that day, which the network-wide
-- counter cannot give (it discards the DID).
DROP MATERIALIZED VIEW IF EXISTS analysis.mv_post_deletes_daily CASCADE;
CREATE MATERIALIZED VIEW analysis.mv_post_deletes_daily AS
SELECT
  date,
  SUM(count)::bigint                       AS deletes,
  COUNT(*)::int                            AS deleters,
  ROUND(SUM(count)::numeric / COUNT(*), 2) AS deletes_per_deleter,
  MAX(count)::int                          AS largest_single_account
FROM activity.post_deletes_daily
GROUP BY date;

CREATE UNIQUE INDEX ON analysis.mv_post_deletes_daily (date);

-- ── Window registry ─────────────────────────────────────────────────────────
-- Each deletion source covers a different span; reports stamp their window from
-- here rather than implying every figure shares one.
DROP MATERIALIZED VIEW IF EXISTS analysis.mv_deletion_windows CASCADE;
CREATE MATERIALIZED VIEW analysis.mv_deletion_windows AS
SELECT 'delete_events_daily'::text AS source,
       'network-wide deletes by record/account type'::text AS grain,
       MIN(date) AS first_date, MAX(date) AS last_date, COUNT(*)::bigint AS rows
FROM activity.delete_events_daily
UNION ALL
SELECT 'post_deletes_daily',
       'per-DID post deletes',
       MIN(date), MAX(date), COUNT(*)::bigint
FROM activity.post_deletes_daily;

CREATE UNIQUE INDEX ON analysis.mv_deletion_windows (source);
