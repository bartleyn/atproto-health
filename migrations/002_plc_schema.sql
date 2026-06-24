-- Schema: plc (plc-migrations.db)
-- Run against: atproto_health database

CREATE SCHEMA IF NOT EXISTS plc;

CREATE TABLE IF NOT EXISTS plc.plc_did_pds (
  did        TEXT PRIMARY KEY,
  pds_url    TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_plc_did_pds_pds_url ON plc.plc_did_pds(pds_url);

CREATE TABLE IF NOT EXISTS plc.plc_migrations (
  id          BIGSERIAL PRIMARY KEY,
  did         TEXT        NOT NULL,
  from_pds    TEXT        NOT NULL,
  to_pds      TEXT        NOT NULL,
  migrated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_plc_migrations_migrated_at ON plc.plc_migrations(migrated_at);
CREATE INDEX IF NOT EXISTS idx_plc_migrations_from_pds    ON plc.plc_migrations(from_pds);
CREATE INDEX IF NOT EXISTS idx_plc_migrations_to_pds      ON plc.plc_migrations(to_pds);
CREATE INDEX IF NOT EXISTS idx_plc_migrations_did         ON plc.plc_migrations(did);

CREATE TABLE IF NOT EXISTS plc.plc_account_creations (
  did        TEXT PRIMARY KEY,
  pds_url    TEXT        NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_plc_account_creations_created_at ON plc.plc_account_creations(created_at);
CREATE INDEX IF NOT EXISTS idx_plc_account_creations_pds_url    ON plc.plc_account_creations(pds_url);

-- Single-row cursor tables (id=1 enforced by PK + CHECK)
CREATE TABLE IF NOT EXISTS plc.plc_cursor (
  id         INTEGER PRIMARY KEY CHECK (id = 1),
  after      TEXT        NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS plc.plc_creations_cursor (
  id         INTEGER PRIMARY KEY CHECK (id = 1),
  after      TEXT        NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS plc.plc_aggregation_cursor (
  id               INTEGER PRIMARY KEY CHECK (id = 1),
  creations_cursor TEXT        NOT NULL,
  migrations_cursor TEXT       NOT NULL,
  updated_at       TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS plc.plc_aggregation_weekly_cursor (
  id                INTEGER PRIMARY KEY CHECK (id = 1),
  creations_cursor  TEXT        NOT NULL,
  migrations_cursor TEXT        NOT NULL,
  updated_at        TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS plc.active_creation_cursor (
  id              INTEGER PRIMARY KEY CHECK (id = 1),
  last_scanned_at TIMESTAMPTZ NOT NULL,
  updated_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS plc.plc_heavy_recompute_cursor (
  id                 INTEGER PRIMARY KEY CHECK (id = 1),
  migrations_cursor  TEXT        NOT NULL,
  did_scanned_cursor TEXT        NOT NULL,
  updated_at         TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS plc.skywatch_labels_cursor (
  id         INTEGER PRIMARY KEY CHECK (id = 1),
  cursor     TEXT        NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS plc.bsky_mod_labels_cursor (
  id         INTEGER PRIMARY KEY CHECK (id = 1),
  cursor     TEXT        NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

-- Aggregate tables
CREATE TABLE IF NOT EXISTS plc.plc_creation_monthly (
  pds_url TEXT    NOT NULL,
  month   TEXT    NOT NULL, -- YYYY-MM
  count   INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (pds_url, month)
);

CREATE TABLE IF NOT EXISTS plc.plc_migration_monthly (
  from_pds TEXT    NOT NULL,
  to_pds   TEXT    NOT NULL,
  month    TEXT    NOT NULL,
  count    INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (from_pds, to_pds, month)
);

CREATE TABLE IF NOT EXISTS plc.plc_creation_weekly (
  pds_url TEXT    NOT NULL,
  week    TEXT    NOT NULL, -- Monday of ISO week, YYYY-MM-DD
  count   INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (pds_url, week)
);

CREATE INDEX IF NOT EXISTS idx_plc_creation_weekly_week ON plc.plc_creation_weekly(week);

CREATE TABLE IF NOT EXISTS plc.plc_migration_weekly (
  from_pds TEXT    NOT NULL,
  to_pds   TEXT    NOT NULL,
  week     TEXT    NOT NULL,
  count    INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (from_pds, to_pds, week)
);

CREATE INDEX IF NOT EXISTS idx_plc_migration_weekly_week   ON plc.plc_migration_weekly(week);
CREATE INDEX IF NOT EXISTS idx_plc_migration_weekly_to_pds ON plc.plc_migration_weekly(to_pds);

CREATE TABLE IF NOT EXISTS plc.active_creation_weekly (
  pds_url TEXT    NOT NULL,
  week    TEXT    NOT NULL, -- Monday of ISO week, YYYY-MM-DD
  count   INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (pds_url, week)
);

CREATE INDEX IF NOT EXISTS idx_active_creation_weekly_week ON plc.active_creation_weekly(week);

-- DID tracking
CREATE TABLE IF NOT EXISTS plc.did_in_repo (
  did              TEXT PRIMARY KEY,
  pds_url          TEXT        NOT NULL,
  scanned_at       TIMESTAMPTZ NOT NULL,
  first_scanned_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_did_in_repo_pds             ON plc.did_in_repo(pds_url);
CREATE INDEX IF NOT EXISTS idx_did_in_repo_first_scanned_at ON plc.did_in_repo(first_scanned_at);

CREATE TABLE IF NOT EXISTS plc.did_repo_status (
  did        TEXT PRIMARY KEY,
  status     TEXT        NOT NULL,
  pds_url    TEXT        NOT NULL,
  scanned_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_did_repo_status_pds    ON plc.did_repo_status(pds_url);
CREATE INDEX IF NOT EXISTS idx_did_repo_status_status ON plc.did_repo_status(status);

CREATE TABLE IF NOT EXISTS plc.did_web_pds (
  did         TEXT PRIMARY KEY,
  pds_url     TEXT,
  first_seen  TIMESTAMPTZ NOT NULL,
  last_seen   TIMESTAMPTZ NOT NULL,
  resolved_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_did_web_pds_pds_url ON plc.did_web_pds(pds_url);

-- PDS snapshots timeseries
CREATE TABLE IF NOT EXISTS plc.pds_repo_status_snapshots (
  id                   BIGSERIAL PRIMARY KEY,
  pds_url              TEXT             NOT NULL,
  snapshot_date        TEXT             NOT NULL, -- YYYY-MM-DD
  active               INTEGER          NOT NULL DEFAULT 0,
  deactivated          INTEGER          NOT NULL DEFAULT 0,
  deleted              INTEGER          NOT NULL DEFAULT 0,
  takendown            INTEGER          NOT NULL DEFAULT 0,
  suspended            INTEGER          NOT NULL DEFAULT 0,
  other                INTEGER          NOT NULL DEFAULT 0,
  total_scanned        INTEGER          NOT NULL DEFAULT 0,
  is_sampled           INTEGER          NOT NULL DEFAULT 0,
  did_plc_count        INTEGER          NOT NULL DEFAULT 0,
  did_web_count        INTEGER          NOT NULL DEFAULT 0,
  is_partial           INTEGER          NOT NULL DEFAULT 0,
  scanned_at           TIMESTAMPTZ,
  in_directory         INTEGER          NOT NULL DEFAULT 0,
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
  hosting_provider     TEXT,
  version              TEXT,
  invite_code_required INTEGER,
  is_online            INTEGER,
  UNIQUE (pds_url, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_pds_repo_status_snapshots_date    ON plc.pds_repo_status_snapshots(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_pds_repo_status_snapshots_pds     ON plc.pds_repo_status_snapshots(pds_url);
CREATE INDEX IF NOT EXISTS idx_pds_repo_status_snapshots_pds_date ON plc.pds_repo_status_snapshots(pds_url, snapshot_date);
-- Normalized URL index (equivalent to SQLite RTRIM expression index)
CREATE INDEX IF NOT EXISTS idx_pds_status_norm_url_date
  ON plc.pds_repo_status_snapshots(RTRIM(pds_url, '/'), snapshot_date);
CREATE INDEX IF NOT EXISTS idx_pds_status_in_directory ON plc.pds_repo_status_snapshots(in_directory);

-- Labels
CREATE TABLE IF NOT EXISTS plc.skywatch_labels (
  did        TEXT NOT NULL,
  label      TEXT NOT NULL,
  labeled_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (did, label)
);

CREATE INDEX IF NOT EXISTS idx_skywatch_labels_label ON plc.skywatch_labels(label);

CREATE TABLE IF NOT EXISTS plc.bsky_mod_labels (
  did        TEXT NOT NULL,
  label      TEXT NOT NULL,
  labeled_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (did, label)
);

CREATE INDEX IF NOT EXISTS idx_bsky_mod_labels_label ON plc.bsky_mod_labels(label);

-- Cached aggregates / precomputed structures
CREATE TABLE IF NOT EXISTS plc.plc_stats_cache (
  id                     INTEGER PRIMARY KEY CHECK (id = 1),
  total_dids             INTEGER          NOT NULL DEFAULT 0,
  bsky_concentration_pct DOUBLE PRECISION NOT NULL DEFAULT 0,
  unique_migrating_dids  INTEGER          NOT NULL DEFAULT 0,
  updated_at             TIMESTAMPTZ      NOT NULL
);

CREATE TABLE IF NOT EXISTS plc.plc_trajectory_edges (
  source TEXT    NOT NULL,
  target TEXT    NOT NULL,
  value  INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (source, target)
);

CREATE TABLE IF NOT EXISTS plc.plc_migration_hops (
  source TEXT    NOT NULL,
  target TEXT    NOT NULL,
  value  INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (source, target)
);

CREATE TABLE IF NOT EXISTS plc.pds_lang_summary (
  pds_url    TEXT    NOT NULL,
  lang       TEXT    NOT NULL,
  dids       INTEGER NOT NULL DEFAULT 0,
  post_count INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (pds_url, lang)
);

CREATE INDEX IF NOT EXISTS idx_pds_lang_summary_lang ON plc.pds_lang_summary(lang);

-- Canonical geo overrides (merged from health.pds_manual_geo; keep one source of truth)
CREATE TABLE IF NOT EXISTS plc.pds_manual_geo (
  url       TEXT             PRIMARY KEY,
  city      TEXT,
  country   TEXT,
  latitude  DOUBLE PRECISION NOT NULL,
  longitude DOUBLE PRECISION NOT NULL,
  org       TEXT,
  note      TEXT
);
