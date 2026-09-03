"""
ATProto Health collection pipeline DAGs.

Requires env var: ATPROTO_HEALTH_DIR — path to the atproto-health repo
e.g. export ATPROTO_HEALTH_DIR=/Users/nathanbartley/Documents/personal_dev/github/atproto-health
"""
import os
from datetime import datetime, timedelta
from airflow.sdk import DAG, task

PROJECT_DIR = os.environ.get(
    "ATPROTO_HEALTH_DIR",
    "/Users/nathanbartley/srv/atproto-health",
)

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def npm_run(script: str, extra_args: str = "") -> None:
    import os
    import signal
    import subprocess
    npm = "/opt/homebrew/bin/npm"
    cmd = [npm, "run", script]
    if extra_args:
        cmd += ["--"] + extra_args.split()
    proc = subprocess.Popen(
        cmd, cwd=PROJECT_DIR,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, start_new_session=True,
    )
    try:
        stdout, _ = proc.communicate()
    except BaseException:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        proc.wait()
        raise
    if stdout:
        print(stdout)
    if proc.returncode != 0:
        raise RuntimeError(f"`{' '.join(cmd)}` exited with code {proc.returncode}")


# ---------------------------------------------------------------------------
# DAG 1: PDS collection -- weekly info
# ---------------------------------------------------------------------------
with DAG(
    "atproto_pds_collection",
    description="Collect PDS directory, geo, and user counts",
    start_date=datetime(2026, 1, 1),
    schedule="0 3 * * 0",  # weekly, Sunday 3am
    catchup=False,
    default_args=default_args,
    tags=["atproto", "collection"],
) as pds_dag:
    
    @task()
    def collect():
        npm_run("collect")

    @task()
    def collect_geo():
        npm_run("collect:geo")

    @task()
    def collect_users():
        npm_run("collect:users")

    @task()
    def pds_scan_status():
        npm_run("scan:pds-status", "--include-bsky --concurrency 15")

    # Run in parallel — both write independent fields to pds_snapshots
    collect() >> collect_geo() >> collect_users() >> pds_scan_status()



# ---------------------------------------------------------------------------
# DAG 2: PLC pipeline — fetch + aggregate, runs weekly after pds_collection
# aggregate steps depend on collect:plc completing
# ---------------------------------------------------------------------------
with DAG(
    "atproto_plc_pipeline",
    description="Fetch PLC directory data and run aggregations",
    start_date=datetime(2026, 1, 1),
    schedule="0 7 * * 0",  # weekly, Sunday 7am (after pds_collection)
    catchup=False,
    default_args=default_args,
    tags=["atproto", "plc"],
) as plc_dag:

    @task()
    def collect_plc():
        npm_run("collect:plc")

    @task()
    def aggregate_plc():
        npm_run("aggregate:plc")

    @task()
    def aggregate_active_plc():
        npm_run("aggregate:active-plc")

    plc = collect_plc()
    agg = aggregate_plc()
    active = aggregate_active_plc()

    plc >> agg >> active


# ---------------------------------------------------------------------------
# DAG 3: Dashboard cache — runs daily to keep derived stats fresh
# ---------------------------------------------------------------------------
with DAG(
    "atproto_dashboard_cache",
    description="Refresh dashboard cache from latest collected data",
    start_date=datetime(2026, 1, 1),
    schedule="0 9 * * *",  # daily, 9am
    catchup=False,
    default_args=default_args,
    tags=["atproto", "cache"],
) as cache_dag:

    @task()
    def dashboard_cache():
        npm_run("analysis:dashboard-cache")

    dashboard_cache()
