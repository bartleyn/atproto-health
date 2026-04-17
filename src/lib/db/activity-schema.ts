import Database from "better-sqlite3";
import path from "path";

const DB_PATH = path.join(process.cwd(), "jetstream-activity.db");

let _db: Database.Database | null = null;

export function getActivityDb(): Database.Database {
  if (!_db) {
    _db = new Database(DB_PATH);
    _db.pragma("journal_mode = WAL");
    _db.pragma("synchronous = NORMAL"); // WAL + NORMAL is safe and faster than FULL
    migrate(_db);
  }
  return _db;
}

function migrate(db: Database.Database) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS did_activity_daily (
      did            TEXT NOT NULL,
      date           TEXT NOT NULL,  -- YYYY-MM-DD
      activity_types INTEGER NOT NULL DEFAULT 0,  -- bitmask of collection types seen
      PRIMARY KEY (did, date)
    );

    CREATE INDEX IF NOT EXISTS idx_did_activity_daily_date
      ON did_activity_daily(date);

    CREATE TABLE IF NOT EXISTS delete_events_daily (
      date       TEXT NOT NULL,
      event_type TEXT NOT NULL,  -- 'record:app.bsky.feed.post', 'account:deleted', 'tombstone', etc.
      count      INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (date, event_type)
    );

    CREATE TABLE IF NOT EXISTS jetstream_cursor (
      id         INTEGER PRIMARY KEY CHECK (id = 1),
      cursor     INTEGER NOT NULL,   -- Unix microseconds (Jetstream time_us)
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS starterpack_joins_daily (
      starterpack_uri TEXT NOT NULL,
      date            TEXT NOT NULL,  -- YYYY-MM-DD
      count           INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (starterpack_uri, date)
    );

    CREATE INDEX IF NOT EXISTS idx_starterpack_joins_daily_date
      ON starterpack_joins_daily(date);

    -- Per-DID language usage accumulated from post events.
    -- One row per (did, lang); post_count grows with each flush.
    -- BCP-47 lang tags as set by the posting client (e.g. "en", "ja", "pt-BR").
    CREATE TABLE IF NOT EXISTS did_langs (
      did        TEXT    NOT NULL,
      lang       TEXT    NOT NULL,
      post_count INTEGER NOT NULL DEFAULT 0,
      last_seen  TEXT    NOT NULL,
      PRIMARY KEY (did, lang)
    );

    CREATE INDEX IF NOT EXISTS idx_did_langs_lang ON did_langs(lang);

    -- Running totals for lang tag coverage (% of posts that carry a langs field).
    CREATE TABLE IF NOT EXISTS lang_stats (
      id           INTEGER PRIMARY KEY CHECK (id = 1),
      total_posts  INTEGER NOT NULL DEFAULT 0,
      tagged_posts INTEGER NOT NULL DEFAULT 0,
      updated_at   TEXT    NOT NULL
    );
  `);

  // Add activity_types column to existing DBs that predate this migration
  try {
    db.exec(`ALTER TABLE did_activity_daily ADD COLUMN activity_types INTEGER NOT NULL DEFAULT 0`);
  } catch {
    // column already exists
  }
}
