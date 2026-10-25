-- Schema: plc (repo status: current state + history)
-- Run against: atproto_health database
--
-- Motivation: plc.did_repo_status was upsert-only — a reactivated account kept its old
-- "deactivated"/"takendown" row forever, and a migrated account kept the status its OLD PDS
-- reports. On 2026-10-24, 232 of 400 feed creators it marked inactive were active live.
--
-- From now on the two questions are separate tables:
--   plc.did_repo_status          CURRENT status: only accounts non-active on their current PDS
--                                as of its latest complete scan (no row = active or unknown)
--   plc.did_repo_status_history  EVERY non-active status ever observed, never pruned:
--                                did this account ever go inactive / get taken down, where, when
-- Maintained by src/lib/collectors/repo-status.ts (scan-pds-status.ts and run.ts).

CREATE TABLE IF NOT EXISTS plc.did_repo_status_history (
  did            TEXT        NOT NULL,
  status         TEXT        NOT NULL,
  pds_url        TEXT        NOT NULL,
  first_seen_at  TIMESTAMPTZ NOT NULL,   -- first scan that reported this status on this PDS
  last_seen_at   TIMESTAMPTZ NOT NULL,   -- latest scan that still reported it
  PRIMARY KEY (did, status, pds_url)
);

CREATE INDEX IF NOT EXISTS idx_did_repo_status_history_status
  ON plc.did_repo_status_history(status);

-- Seed from the current table: every row in it was observed at least once, stale or not.
-- first_seen_at is the row's scanned_at; earlier sightings were overwritten and are unknown.
INSERT INTO plc.did_repo_status_history (did, status, pds_url, first_seen_at, last_seen_at)
SELECT did, status, pds_url, scanned_at, scanned_at FROM plc.did_repo_status
ON CONFLICT (did, status, pds_url) DO NOTHING;

-- Statuses recorded on a PDS the account has since left say nothing about it now.
DELETE FROM plc.did_repo_status s
USING plc.plc_did_pds p
WHERE p.did = s.did AND rtrim(p.pds_url, '/') <> rtrim(s.pds_url, '/');
