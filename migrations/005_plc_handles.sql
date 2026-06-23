-- Schema: plc — current handle per DID (from PLC export alsoKnownAs)
-- Run against: atproto_health database
--   psql atproto_health -f migrations/005_plc_handles.sql
--
-- Backfilled + kept current by src/lib/collectors/plc-handles.ts (own cursor, like
-- plc-creations.ts). Enables handle-pattern / generator-entropy features for the
-- fraud/farm model (see project_fraud_model memory).

CREATE TABLE IF NOT EXISTS plc.plc_did_handle (
  did        TEXT PRIMARY KEY,
  handle     TEXT,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_plc_did_handle_handle ON plc.plc_did_handle(handle);

CREATE TABLE IF NOT EXISTS plc.plc_handles_cursor (
  id         INTEGER PRIMARY KEY CHECK (id = 1),
  after      TEXT        NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);
