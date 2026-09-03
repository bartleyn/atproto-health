"""
ATProto Health — weekly edge-delta rollup DAG.

Turns the continuous firehose edge log (miniext/edges/dt=<date>/…) into ONE deduped, verified delta
Parquet on the Modal `at-snapshot-edges` volume, ready for weekly incremental ER. The delta window is
Airflow's own data interval (start..end), so the DAG is idempotent and backfillable with no external
watermark: re-running a week reprocesses exactly that week.

Pipeline:  build_delta → verify_delta → upload_delta → trigger_er(stub)
                                             └──────→ prune_raw

Requires on the worker: `duckdb` and `uv` on PATH, and env:
  ATPROTO_HEALTH_DIR   (repo dir; only for parity with the other DAGs)
  EDGES_DIR            (default /Volumes/miniext/edges)
  MODAL_EDGES_VOLUME   (default at-snapshot-edges)

Notes:
  * Handles BOTH on-disk layouts: the correct in-partition `dt=<date>/*.parquet` AND any legacy
    top-level `dt=<date>.parquet` files (written before the compactDay fix ships / while the old
    collector build is still running). File discovery is done in Python so empty globs never error.
  * Deletes flow through as op='d' rows; the quarterly `stage --delta-since` scan remains the
    correctness backstop for gaps + removals the firehose missed.
"""
import os
import glob
import subprocess
import tempfile
from datetime import datetime, timedelta

from airflow.sdk import DAG, task, get_current_context

EDGES_DIR = os.environ.get("EDGES_DIR", "/Volumes/miniext/edges")
DELTA_DIR = os.path.join(EDGES_DIR, "_deltas")
MODAL_VOLUME = os.environ.get("MODAL_EDGES_VOLUME", "at-snapshot-edges")
DUCKDB = os.environ.get("DUCKDB_BIN", "/opt/homebrew/bin/duckdb")
RETENTION_DAYS = int(os.environ.get("EDGE_RETENTION_DAYS", "21"))
# The 9 relations edgesFromEvent can emit; verify_delta asserts none silently vanished.
EXPECTED_RELS = {"follow", "block", "repost", "like", "reply", "quote", "listitem", "listblock", "list"}

default_args = {"retries": 1, "retry_delay": timedelta(minutes=5)}


def _duckdb(sql: str) -> str:
    r = subprocess.run([DUCKDB, "-noheader", "-list", "-c", sql],
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"duckdb failed:\n{r.stderr}\n---\n{sql}")
    return r.stdout.strip()


def _window_dates():
    """Inclusive-start, exclusive-end list of YYYY-MM-DD covering the run's data interval."""
    ctx = get_current_context()
    start = ctx["data_interval_start"].date()
    end = ctx["data_interval_end"].date()
    out, d = [], start
    while d < end:
        out.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return out, end.strftime("%Y-%m-%d")


def _files_for(dates):
    files = []
    for dt in dates:
        files += glob.glob(os.path.join(EDGES_DIR, f"dt={dt}", "*.parquet"))  # correct layout
        legacy = os.path.join(EDGES_DIR, f"dt={dt}.parquet")                  # pre-fix top-level
        if os.path.isfile(legacy):
            files.append(legacy)
    return files


def _sql_array(paths):
    return "[" + ", ".join("'" + p.replace("'", "''") + "'" for p in paths) + "]"


with DAG(
    "atproto_edge_rollup",
    description="Weekly rollup of firehose edges into a deduped delta on the Modal edges volume",
    start_date=datetime(2026, 8, 4),          # first Monday on/after edge capture goes live
    schedule="0 3 * * 1",                     # Mondays 03:00 (rolls up the prior 7 complete UTC days)
    catchup=False,
    default_args=default_args,
    tags=["atproto", "edges", "rollup"],
) as dag:

    @task()
    def build_delta() -> str:
        dates, end = _window_dates()
        files = _files_for(dates)
        print(f"[edge-rollup] window {dates[0]}..{dates[-1]} → {len(files)} parquet files")
        if not files:
            # No edges for the whole week — nothing to roll up (collector down all week?). Skip
            # rather than emit an empty delta; the quarterly scan is the backstop.
            from airflow.exceptions import AirflowSkipException
            raise AirflowSkipException("no edge files in window")
        os.makedirs(DELTA_DIR, exist_ok=True)
        delta_path = os.path.join(DELTA_DIR, f"delta_{end}.parquet")
        tmp = delta_path + ".tmp"
        # SELECT * (not named cols) + union_by_name so the ctx column being present in some files and
        # absent in older ones is reconciled. Dedup replay copies on the record's natural key.
        _duckdb(
            f"COPY (SELECT * FROM read_parquet({_sql_array(files)}, union_by_name=true) "
            f"QUALIFY row_number() OVER (PARTITION BY src, rel, rkey, op ORDER BY t) = 1) "
            f"TO '{tmp}' (FORMAT parquet, COMPRESSION zstd);"
        )
        os.replace(tmp, delta_path)           # atomic publish
        print(f"[edge-rollup] wrote {delta_path}")
        return delta_path

    @task()
    def verify_delta(delta_path: str) -> str:
        """Sanity gate (per project working practice): non-empty, all rels present, no bad rows."""
        n = int(_duckdb(f"SELECT count(*) FROM read_parquet('{delta_path}');"))
        if n == 0:
            raise RuntimeError("delta is empty")
        hist = _duckdb(
            f"SELECT rel, op, count(*) FROM read_parquet('{delta_path}') GROUP BY 1,2 ORDER BY 1,2;")
        print(f"[edge-rollup] {n:,} rows\n{hist}")
        rels = set(_duckdb(
            f"SELECT DISTINCT rel FROM read_parquet('{delta_path}');").split("\n"))
        missing = EXPECTED_RELS - rels
        if missing:
            raise RuntimeError(f"delta missing expected relations: {sorted(missing)}")
        bad = int(_duckdb(
            f"SELECT count(*) FROM read_parquet('{delta_path}') "
            f"WHERE (op='c' AND dst IS NULL) OR (op='d' AND dst IS NOT NULL);"))
        if bad:
            raise RuntimeError(f"{bad} rows violate create/delete dst invariant")
        return delta_path

    @task()
    def upload_delta(delta_path: str) -> str:
        remote = f"deltas/{os.path.basename(delta_path)}"
        # System 3.9 modal can't parse the app's `str | None` hints; run modal via uv on 3.12.
        base = ["uv", "run", "--python", "3.12", "--with", "modal", "modal"]
        subprocess.run(base + ["volume", "create", MODAL_VOLUME], capture_output=True, text=True)  # ok if exists
        r = subprocess.run(base + ["volume", "put", MODAL_VOLUME, delta_path, remote, "--force"],
                           capture_output=True, text=True)
        if r.returncode != 0:
            raise RuntimeError(f"modal volume put failed:\n{r.stderr}")
        print(f"[edge-rollup] uploaded → {MODAL_VOLUME}:{remote}")
        return remote

    @task()
    def trigger_er(remote: str) -> None:
        #STUB
        print(f"[edge-rollup] STUB: delta ready at {MODAL_VOLUME}:{remote} — ER trigger not wired yet")

    @task()
    def prune_raw() -> None:
        """Delete raw dt=<date> partitions (and legacy top-level files) older than the retention
        window, after a successful upload, to bound miniext. Never touches _deltas."""
        import shutil
        cutoff = (datetime.utcnow().date() - timedelta(days=RETENTION_DAYS)).strftime("%Y-%m-%d")
        removed = 0
        for p in glob.glob(os.path.join(EDGES_DIR, "dt=*")):
            name = os.path.basename(p)               # dt=YYYY-MM-DD  or  dt=YYYY-MM-DD.parquet
            dt = name[len("dt="):].removesuffix(".parquet")
            if len(dt) == 10 and dt < cutoff:
                (shutil.rmtree if os.path.isdir(p) else os.remove)(p)
                removed += 1
        print(f"[edge-rollup] pruned {removed} partitions older than {cutoff}")

    delta = build_delta()
    verified = verify_delta(delta)
    remote = upload_delta(verified)
    trigger_er(remote)
    remote >> prune_raw()
