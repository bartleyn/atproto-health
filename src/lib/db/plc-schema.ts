import Database from "better-sqlite3";
import path from "path";

const DB_PATH = path.join(process.cwd(), "plc-migrations.db");

let _db: Database.Database | null = null;

export function getPlcDb(): Database.Database {
  if (!_db) {
    _db = new Database(DB_PATH, { timeout: 10000 }); // 10s busy timeout
    _db.pragma("journal_mode = WAL");
    migrate(_db);
  }
  return _db;
}

function migrate(db: Database.Database) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS plc_did_pds (
      did TEXT PRIMARY KEY,
      pds_url TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS plc_migrations (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      did TEXT NOT NULL,
      from_pds TEXT NOT NULL,
      to_pds TEXT NOT NULL,
      migrated_at TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_plc_migrations_migrated_at
      ON plc_migrations(migrated_at);
    CREATE INDEX IF NOT EXISTS idx_plc_migrations_from_pds
      ON plc_migrations(from_pds);
    CREATE INDEX IF NOT EXISTS idx_plc_migrations_to_pds
      ON plc_migrations(to_pds);

    CREATE TABLE IF NOT EXISTS plc_account_creations (
      did TEXT PRIMARY KEY,
      pds_url TEXT NOT NULL,
      created_at TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_plc_account_creations_created_at
      ON plc_account_creations(created_at);
    CREATE INDEX IF NOT EXISTS idx_plc_account_creations_pds_url
      ON plc_account_creations(pds_url);

    CREATE TABLE IF NOT EXISTS plc_creations_cursor (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      after TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS plc_cursor (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      after TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS plc_creation_monthly (
      pds_url TEXT NOT NULL,
      month TEXT NOT NULL, -- 'should be YYYY-MM format'
      count INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY( pds_url, month)
    );

    CREATE TABLE IF NOT EXISTS plc_migration_monthly (
      from_pds TEXT NOT NULL,
      to_pds TEXT NOT NULL,
      month TEXT NOT NULL,
      count INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (from_pds, to_pds, month)
    );

    CREATE TABLE IF NOT EXISTS plc_aggregation_cursor (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      creations_cursor TEXT NOT NULL, -- last created_At aggregated
      migrations_cursor TEXT NOT NULL, -- last migrated_at aggregated
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS plc_creation_weekly (
      pds_url TEXT NOT NULL,
      week    TEXT NOT NULL, -- Monday of ISO week, YYYY-MM-DD
      count   INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (pds_url, week)
    );

    CREATE TABLE IF NOT EXISTS plc_migration_weekly (
      from_pds TEXT NOT NULL,
      to_pds   TEXT NOT NULL,
      week     TEXT NOT NULL,
      count    INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (from_pds, to_pds, week)
    );

    CREATE INDEX IF NOT EXISTS idx_plc_creation_weekly_week
      ON plc_creation_weekly(week);
    CREATE INDEX IF NOT EXISTS idx_plc_migration_weekly_week
      ON plc_migration_weekly(week);
    CREATE INDEX IF NOT EXISTS idx_plc_migration_weekly_to_pds
      ON plc_migration_weekly(to_pds);

    CREATE TABLE IF NOT EXISTS plc_aggregation_weekly_cursor (
      id               INTEGER PRIMARY KEY CHECK (id = 1),
      creations_cursor TEXT NOT NULL,
      migrations_cursor TEXT NOT NULL,
      updated_at       TEXT NOT NULL
    );

    -- Weekly active-account creation counts per PDS.
    -- Derived from did_in_repo JOIN plc_account_creations, excluding non-active DIDs.
    -- Run aggregate-active-plc.ts after a full did_in_repo scan to populate.
    CREATE TABLE IF NOT EXISTS active_creation_weekly (
      pds_url TEXT NOT NULL,
      week    TEXT NOT NULL, -- Monday of ISO week, YYYY-MM-DD
      count   INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (pds_url, week)
    );

    CREATE INDEX IF NOT EXISTS idx_active_creation_weekly_week
      ON active_creation_weekly(week);

    -- Every DID observed in a listRepos scan, with its current PDS.
    -- Updated on each scan so it reflects the latest known location.
    CREATE TABLE IF NOT EXISTS did_in_repo (
      did        TEXT PRIMARY KEY,
      pds_url    TEXT NOT NULL,
      scanned_at TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_did_in_repo_pds
      ON did_in_repo(pds_url);

    -- Non-active individual DID statuses from listRepos scans.
    -- Only non-active repos are stored (takendown, deactivated, deleted, suspended).
    CREATE TABLE IF NOT EXISTS did_repo_status (
      did        TEXT PRIMARY KEY,
      status     TEXT NOT NULL,
      pds_url    TEXT NOT NULL,
      scanned_at TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_did_repo_status_pds
      ON did_repo_status(pds_url);
    CREATE INDEX IF NOT EXISTS idx_did_repo_status_status
      ON did_repo_status(status);

    -- Point-in-time snapshots of repo status per PDS.
    -- Run the scanner periodically to build up the timeseries.
    CREATE TABLE IF NOT EXISTS pds_repo_status_snapshots (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      pds_url       TEXT    NOT NULL,
      snapshot_date TEXT    NOT NULL, -- YYYY-MM-DD
      active        INTEGER NOT NULL DEFAULT 0,
      deactivated   INTEGER NOT NULL DEFAULT 0,
      deleted       INTEGER NOT NULL DEFAULT 0,
      takendown     INTEGER NOT NULL DEFAULT 0,
      suspended     INTEGER NOT NULL DEFAULT 0,
      other         INTEGER NOT NULL DEFAULT 0,
      total_scanned INTEGER NOT NULL DEFAULT 0,
      is_sampled    INTEGER NOT NULL DEFAULT 0, -- 1 if sampled, 0 if full scan
      did_plc_count INTEGER NOT NULL DEFAULT 0,
      did_web_count INTEGER NOT NULL DEFAULT 0,
      UNIQUE(pds_url, snapshot_date)
    );

    CREATE INDEX IF NOT EXISTS idx_pds_repo_status_snapshots_date
      ON pds_repo_status_snapshots(snapshot_date);
    CREATE INDEX IF NOT EXISTS idx_pds_repo_status_snapshots_pds
      ON pds_repo_status_snapshots(pds_url);

    CREATE TABLE IF NOT EXISTS skywatch_labels (
      did        TEXT NOT NULL,
      label      TEXT NOT NULL,
      labeled_at TEXT NOT NULL,
      PRIMARY KEY (did, label)
    );

    CREATE INDEX IF NOT EXISTS idx_skywatch_labels_label
      ON skywatch_labels(label);

    CREATE TABLE IF NOT EXISTS skywatch_labels_cursor (
      id         INTEGER PRIMARY KEY CHECK (id = 1),
      cursor     TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS bsky_mod_labels (
      did        TEXT NOT NULL,
      label      TEXT NOT NULL,
      labeled_at TEXT NOT NULL,
      PRIMARY KEY (did, label)
    );

    CREATE INDEX IF NOT EXISTS idx_bsky_mod_labels_label
      ON bsky_mod_labels(label);

    CREATE TABLE IF NOT EXISTS bsky_mod_labels_cursor (
      id         INTEGER PRIMARY KEY CHECK (id = 1),
      cursor     TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    -- Cached aggregate stats derived from did_in_repo (42M rows — too slow to query live).
    -- Recomputed by aggregate-plc.ts. Single row (id=1).
    CREATE TABLE IF NOT EXISTS plc_stats_cache (
      id                    INTEGER PRIMARY KEY CHECK (id = 1),
      total_dids            INTEGER NOT NULL DEFAULT 0,
      bsky_concentration_pct REAL    NOT NULL DEFAULT 0,
      updated_at            TEXT    NOT NULL
    );

    -- Precomputed multi-step migration trajectory edges for the Sankey chart.
    -- Fully recomputed by aggregate-plc.ts each run. Nodes are "pds@step" strings.
    CREATE TABLE IF NOT EXISTS plc_trajectory_edges (
      source TEXT NOT NULL, -- e.g. "bsky.network@0"
      target TEXT NOT NULL, -- e.g. "eurosky.social@1"
      value  INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (source, target)
    );

    -- Actual per-hop migration transitions for the multi-step Sankey chart.
    -- Each row is one hop: "pds@step" → "pds@(step+1)". step < 3 (first 3 hops only).
    -- Fully recomputed by aggregate-plc.ts each run.
    CREATE TABLE IF NOT EXISTS plc_migration_hops (
      source TEXT NOT NULL, -- e.g. "bsky.network@0"
      target TEXT NOT NULL, -- e.g. "eurosky.social@1"
      value  INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (source, target)
    );

    -- Per-PDS language breakdown derived from jetstream-activity.db did_langs JOIN plc_did_pds.
    -- BCP-47 subtags collapsed to base tag (en-US → en, zh-TW → zh).
    -- bsky shards collapsed to 'bsky.network'. Fully recomputed by aggregate-plc.ts.
    CREATE TABLE IF NOT EXISTS pds_lang_summary (
      pds_url    TEXT NOT NULL,
      lang       TEXT NOT NULL,
      dids       INTEGER NOT NULL DEFAULT 0,
      post_count INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (pds_url, lang)
    );

    CREATE INDEX IF NOT EXISTS idx_pds_lang_summary_lang
      ON pds_lang_summary(lang);
  `);

  // Additive migrations for existing databases
  for (const col of ["did_plc_count", "did_web_count"]) {
    try {
      db.exec(`ALTER TABLE pds_repo_status_snapshots ADD COLUMN ${col} INTEGER NOT NULL DEFAULT 0`);
    } catch {
      // Column already exists — ignore
    }
  }
}
