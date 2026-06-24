-- Schema: health (atproto-health.db)
-- Run against: atproto_health database

CREATE SCHEMA IF NOT EXISTS health;

CREATE TABLE IF NOT EXISTS health.collection_runs (
  id           BIGSERIAL PRIMARY KEY,
  started_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  source       TEXT        NOT NULL,
  status       TEXT        NOT NULL DEFAULT 'running',
  metadata     JSONB
);

CREATE TABLE IF NOT EXISTS health.pds_instances (
  id           BIGSERIAL PRIMARY KEY,
  url          TEXT        NOT NULL UNIQUE,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS health.pds_snapshots (
  id                   BIGSERIAL PRIMARY KEY,
  pds_id               BIGINT      NOT NULL REFERENCES health.pds_instances(id),
  collected_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  run_id               BIGINT      REFERENCES health.collection_runs(id),

  -- from atproto-scraping state.json
  version              TEXT,
  invite_code_required INTEGER,
  is_online            INTEGER,
  error_at             TIMESTAMPTZ,

  -- from describeServer
  did                  TEXT,
  available_domains    JSONB,
  contact              JSONB,
  links                JSONB,

  -- user counts from listRepos
  user_count_total     INTEGER,
  user_count_active    INTEGER,

  -- geo from ip-api
  ip_address           TEXT,
  country              TEXT,
  country_code         TEXT,
  region               TEXT,
  city                 TEXT,
  latitude             DOUBLE PRECISION,
  longitude            DOUBLE PRECISION,
  isp                  TEXT,
  org                  TEXT,
  as_number            TEXT,
  hosting_provider     TEXT
);

CREATE INDEX IF NOT EXISTS idx_pds_snapshots_pds_id      ON health.pds_snapshots(pds_id);
CREATE INDEX IF NOT EXISTS idx_pds_snapshots_collected_at ON health.pds_snapshots(collected_at);
CREATE INDEX IF NOT EXISTS idx_pds_snapshots_run_id       ON health.pds_snapshots(run_id);

CREATE TABLE IF NOT EXISTS health.github_stats (
  id           BIGSERIAL PRIMARY KEY,
  collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  query        TEXT        NOT NULL,
  repo_count   INTEGER     NOT NULL,
  top_repos    JSONB       NOT NULL  -- [{name, fullName, stars, url, description}]
);

CREATE TABLE IF NOT EXISTS health.pds_manual_geo (
  url       TEXT             PRIMARY KEY,
  city      TEXT,
  country   TEXT,
  latitude  DOUBLE PRECISION NOT NULL,
  longitude DOUBLE PRECISION NOT NULL,
  org       TEXT,
  note      TEXT
);

CREATE TABLE IF NOT EXISTS health.firehose_samples (
  id                     BIGSERIAL PRIMARY KEY,
  sampled_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  duration_ms            INTEGER     NOT NULL,
  total_events           INTEGER     NOT NULL,
  total_interactions     INTEGER     NOT NULL,
  resolved_interactions  INTEGER     NOT NULL,
  cross_pds              INTEGER     NOT NULL,
  same_pds               INTEGER     NOT NULL,
  events_per_second      INTEGER     NOT NULL,
  by_type                JSONB       NOT NULL,  -- {like: {total, crossPds, samePds}, ...}
  federation             JSONB       NOT NULL DEFAULT '{}',
  top_cross_pds_pairs    JSONB       NOT NULL
);
