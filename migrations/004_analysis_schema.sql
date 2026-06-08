-- Schema: analysis (analysis.db)
-- Run against: atproto_health database
--
-- These tables are derived/materialized from plc + activity data.
-- In SQLite, build-analysis-db.ts rebuilds them from scratch by ATTACHing
-- plc-migrations.db. In Postgres, they become regular tables in the same DB —
-- no ATTACH needed, just INSERT ... SELECT from plc.* and activity.*.

CREATE SCHEMA IF NOT EXISTS analysis;

-- DIDs excluded from engagement analysis (spam, suspended, taken down).
-- Rebuilt by analysis:build from plc.did_repo_status + plc.skywatch_labels.
CREATE TABLE IF NOT EXISTS analysis.excluded_dids (
  did TEXT PRIMARY KEY
);

-- Pre-joined base table for cohort queries.
-- did = account, created_at = first PLC entry, pds_type = 'bsky' | 'indie'.
-- Rebuilt by analysis:build from plc.plc_account_creations JOIN plc.did_in_repo.
CREATE TABLE IF NOT EXISTS analysis.cohort_base (
  did        TEXT        PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL,
  pds_url    TEXT        NOT NULL,
  pds_type   TEXT        NOT NULL -- 'bsky' | 'indie'
);

CREATE INDEX IF NOT EXISTS idx_cohort_base_created_at ON analysis.cohort_base(created_at);
CREATE INDEX IF NOT EXISTS idx_cohort_base_pds_type   ON analysis.cohort_base(pds_type);
