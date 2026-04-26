import Database from "better-sqlite3";
import path from "path";

const DB_PATH = path.join(process.cwd(), "atproto-health.db");

let _db: Database.Database | null = null;

export function getDb(): Database.Database {
  if (!_db) {
    _db = new Database(DB_PATH);
    _db.pragma("journal_mode = WAL");
    _db.pragma("foreign_keys = ON");
    migrate(_db);
  }
  return _db;
}

function migrate(db: Database.Database) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS collection_runs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      started_at TEXT NOT NULL DEFAULT (datetime('now')),
      completed_at TEXT,
      source TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'running',
      metadata TEXT
    );

    CREATE TABLE IF NOT EXISTS pds_instances (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      url TEXT NOT NULL UNIQUE,
      first_seen_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS pds_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      pds_id INTEGER NOT NULL REFERENCES pds_instances(id),
      collected_at TEXT NOT NULL DEFAULT (datetime('now')),
      run_id INTEGER REFERENCES collection_runs(id),

      -- from atproto-scraping state.json
      version TEXT,
      invite_code_required INTEGER,
      is_online INTEGER,
      error_at TEXT,

      -- from describeServer
      did TEXT,
      available_domains TEXT, -- JSON array
      contact TEXT,           -- JSON object
      links TEXT,             -- JSON object

      -- user counts from listRepos
      user_count_total INTEGER,
      user_count_active INTEGER,

      -- geo from ip-api
      ip_address TEXT,
      country TEXT,
      country_code TEXT,
      region TEXT,
      city TEXT,
      latitude REAL,
      longitude REAL,
      isp TEXT,
      org TEXT,
      as_number TEXT,
      hosting_provider TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_pds_snapshots_pds_id
      ON pds_snapshots(pds_id);
    CREATE INDEX IF NOT EXISTS idx_pds_snapshots_collected_at
      ON pds_snapshots(collected_at);
    CREATE INDEX IF NOT EXISTS idx_pds_snapshots_run_id
      ON pds_snapshots(run_id);

    CREATE TABLE IF NOT EXISTS github_stats (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      collected_at TEXT NOT NULL DEFAULT (datetime('now')),
      query TEXT NOT NULL,
      repo_count INTEGER NOT NULL,
      top_repos TEXT NOT NULL  -- JSON: [{name, fullName, stars, url, description}]
    );

    -- Hand-curated geo for PDSes seen in Jetstream/PLC but not in the collector scan
    -- (e.g. Bridgy Fed, EuroSky). Included in world-map and lang-map queries.
    CREATE TABLE IF NOT EXISTS pds_manual_geo (
      url       TEXT PRIMARY KEY,
      city      TEXT,
      country   TEXT,
      latitude  REAL NOT NULL,
      longitude REAL NOT NULL,
      org       TEXT,
      note      TEXT
    );

    CREATE TABLE IF NOT EXISTS firehose_samples (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      sampled_at TEXT NOT NULL DEFAULT (datetime('now')),
      duration_ms INTEGER NOT NULL,
      total_events INTEGER NOT NULL,
      total_interactions INTEGER NOT NULL,
      resolved_interactions INTEGER NOT NULL,
      cross_pds INTEGER NOT NULL,
      same_pds INTEGER NOT NULL,
      events_per_second INTEGER NOT NULL,
      by_type TEXT NOT NULL,              -- JSON: {like: {total, crossPds, samePds}, ...}
      federation TEXT NOT NULL DEFAULT '{}', -- JSON: {bsky-internal, bsky-to-third, ...}
      top_cross_pds_pairs TEXT NOT NULL   -- JSON array
    );
  `);
}
