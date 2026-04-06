import Database from "better-sqlite3";
import path from "path";

const DB_PATH = path.join(process.cwd(), "plc-migrations.db");

let _db: Database.Database | null = null;

export function getPlcDb(): Database.Database {
  if (!_db) {
    _db = new Database(DB_PATH);
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
  `);
}
