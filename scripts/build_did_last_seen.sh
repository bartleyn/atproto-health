#!/usr/bin/env bash
# Build analysis.did_last_seen — network-wide "last active" date per in-repo DID.
#
# Sources: the DuckDB snapshot's outgoing-action tables (posts/reposts/follows/blocks)
# give last-action <= snapshot_date; live activity.did_activity_daily extends it to now.
# LIKES ARE EXCLUDED — that table OOMs under ~5GB in this snapshot file (see
# reference_duckdb_snapshot memory). Recent likes (>=2026-04-11) are still captured via the
# live merge; only pre-April like-ONLY accounts are missed (they fall in source='none').
#
# Memory-safe: 5GB / 1 thread, spill to disk; heavy aggregation and the actors-join are
# split into two passes so neither peaks. created_at is clamped (dirty far-future values).
#
# Usage: bash scripts/build_did_last_seen.sh
set -euo pipefail

SNAP=${SNAP:-/Volumes/miniext/snapshot.duckdb}
TMP=${TMP:-/Volumes/miniext/duckdb_tmp}
DIDID_CSV=/Volumes/miniext/last_seen_didid.csv
SNAP_CSV=/Volumes/miniext/did_last_seen_snap.csv
PGDB=${PGDB:-atproto_health}
FLOOR='2022-11-01'      # atproto launch floor
CEIL='2026-05-13'       # ~snapshot_date; drops dirty far-future created_at
mkdir -p "$TMP"

echo "[1/3] heavy aggregation (posts+reposts+follows+blocks -> did_id) ..."
duckdb -readonly "$SNAP" <<SQL
SET memory_limit='5GB'; SET threads=1; SET preserve_insertion_order=false;
SET temp_directory='$TMP';
COPY (
  WITH a AS (
    SELECT author_did_id AS did_id, MAX(created_at) AS ls FROM posts
      WHERE created_at BETWEEN TIMESTAMP '$FLOOR' AND TIMESTAMP '$CEIL' GROUP BY 1
    UNION ALL SELECT actor_did_id, MAX(created_at) FROM reposts
      WHERE created_at BETWEEN TIMESTAMP '$FLOOR' AND TIMESTAMP '$CEIL' GROUP BY 1
    UNION ALL SELECT src_did_id, MAX(created_at) FROM follows
      WHERE created_at BETWEEN TIMESTAMP '$FLOOR' AND TIMESTAMP '$CEIL' GROUP BY 1
    UNION ALL SELECT src_did_id, MAX(created_at) FROM blocks
      WHERE created_at BETWEEN TIMESTAMP '$FLOOR' AND TIMESTAMP '$CEIL' GROUP BY 1
  )
  SELECT did_id, MAX(ls) AS last_seen FROM a GROUP BY did_id
) TO '$DIDID_CSV' (FORMAT CSV, HEADER);
SQL

echo "[2/3] light join did_id -> did via actors ..."
duckdb -readonly "$SNAP" <<SQL
SET memory_limit='5GB'; SET threads=2; SET preserve_insertion_order=false;
SET temp_directory='$TMP';
COPY (
  SELECT ac.did, s.last_seen
  FROM read_csv('$DIDID_CSV', header=true, columns={'did_id':'UBIGINT','last_seen':'TIMESTAMP'}) s
  JOIN actors ac ON ac.did_id = s.did_id
) TO '$SNAP_CSV' (FORMAT CSV, HEADER);
SQL

echo "[3/3] load + merge with live did_activity_daily, scope to plc.did_in_repo ..."
psql "$PGDB" -v ON_ERROR_STOP=1 <<SQL
CREATE UNLOGGED TABLE IF NOT EXISTS analysis._ls_snap (did text, last_seen timestamptz);
TRUNCATE analysis._ls_snap;
\copy analysis._ls_snap FROM '$SNAP_CSV' WITH (FORMAT csv, HEADER true)

DROP TABLE IF EXISTS analysis.did_last_seen;
CREATE TABLE analysis.did_last_seen AS
WITH live AS (SELECT did, MAX(date)::date AS live_ls FROM activity.did_activity_daily GROUP BY did),
     snap AS (SELECT did, last_seen::date AS snap_ls FROM analysis._ls_snap)
SELECT r.did,
  GREATEST(s.snap_ls, l.live_ls) AS last_seen,
  CASE WHEN s.snap_ls IS NULL AND l.live_ls IS NULL THEN 'none'
       WHEN l.live_ls IS NULL THEN 'snapshot'
       WHEN s.snap_ls IS NULL THEN 'live'
       WHEN l.live_ls >= s.snap_ls THEN 'live'
       ELSE 'snapshot' END AS source
FROM plc.did_in_repo r
LEFT JOIN snap s ON s.did = r.did
LEFT JOIN live l ON l.did = r.did;

CREATE INDEX idx_dls_did ON analysis.did_last_seen(did);
CREATE INDEX idx_dls_last_seen ON analysis.did_last_seen(last_seen);
DROP TABLE analysis._ls_snap;
ANALYZE analysis.did_last_seen;
SQL
echo "done. analysis.did_last_seen rebuilt."
