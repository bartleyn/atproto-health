-- Hosts known to the AT Protocol relay, from com.atproto.sync.listHosts.
--
-- This is a *current-state* table rather than the snapshot pattern used elsewhere:
-- the relay already exposes its own full host list on every call, so history is
-- re-derivable by re-reading it, and the row count (~6K) makes per-run snapshots
-- pure overhead. first_seen_at/last_seen_at carry the only history we actually
-- use — when a host appeared and whether the relay still lists it.
--
-- `status` is the relay's own view of the host (active / idle / offline /
-- throttled / banned), NOT our reachability scan. A host can be listed by the
-- relay and still fail our own describeServer probe, and vice versa.

CREATE TABLE IF NOT EXISTS health.relay_hosts (
  hostname       TEXT PRIMARY KEY,
  account_count  BIGINT,
  status         TEXT,
  seq            BIGINT,
  first_seen_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS relay_hosts_status_idx    ON health.relay_hosts (status);
CREATE INDEX IF NOT EXISTS relay_hosts_last_seen_idx ON health.relay_hosts (last_seen_at DESC);
