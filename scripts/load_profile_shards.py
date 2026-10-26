#!/usr/bin/env python3
"""
Load Jetstream v2 archive shards (scripts/jetstream/extract.go) into activity.profile_records.

The archive replay gives current profile state for the whole network in one pass -- see
extract.go's header comment for what it does and does not contain (no pre-2026-08-04 version
history, just current state as of the archive seed). This script only loads the shards it
already produced; it does not talk to Jetstream or Modal itself.

Usage:
  modal run scripts/jetstream/modal_backfill.py --collections app.bsky.actor.profile
  modal volume get jetstream-backfill /profile ./profile-shards
  python3 scripts/load_profile_shards.py ./profile-shards
  python3 scripts/load_profile_shards.py ./profile-shards --dry-run   # count rows, no writes
"""

import argparse
import gzip
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras

CHUNK_SIZE = 5_000


def _clean(value):
    # Postgres text columns reject NUL bytes outright; a handful of archive rows carry them
    # in freeform fields (display_name/description), presumably from clients that don't strip
    # control chars before writing the record.
    return value.replace("\x00", "") if isinstance(value, str) else value


def _parse_ts(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def rows_from_shard(path: Path):
    with gzip.open(path, "rt") as f:
        it = iter(f)
        while True:
            try:
                line = next(it)
            except StopIteration:
                break
            except EOFError:  # shard from a killed run: keep everything before the cut
                print(f"[load] WARN {path.name}: truncated gzip, keeping rows read so far", flush=True)
                break
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:  # partial final line of a truncated shard
                continue
            if r.get("collection") != "app.bsky.actor.profile":
                continue
            if r.get("operation") == "delete":
                # matches the live collector: deletes aren't recorded as profile_records rows,
                # and content_hash (NOT NULL) has nothing to hash for a delete anyway
                continue
            yield (
                r["did"],
                datetime.fromtimestamp(r["time_us"] / 1_000_000, tz=timezone.utc),
                "backfill",
                "jetstream_archive",
                _clean(r.get("display_name")),
                _clean(r.get("description")),
                r.get("avatar_cid"),
                r.get("banner_cid"),
                r.get("joined_via_starterpack_uri"),
                r.get("pinned_post_uri"),
                [_clean(v) for v in r["self_labels"]] if r.get("self_labels") else None,
                _parse_ts(r.get("record_created_at")),
                r["content_hash"],
            )


def write_chunk(conn, rows: list, commit: bool = True) -> None:
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO activity.profile_records
              (did, observed_at, operation, source, display_name, description,
               avatar_cid, banner_cid, joined_via_starterpack_uri, pinned_post_uri,
               self_labels, record_created_at, content_hash)
            VALUES %s
            """,
            rows,
            page_size=CHUNK_SIZE,
        )
    if commit:
        conn.commit()


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("shard_dir", help="directory of part-*.ndjson.gz shards")
    ap.add_argument("--dry-run", action="store_true", help="count rows per shard, write nothing")
    ap.add_argument("--skip-existing", action="store_true",
                    help="skip rows already loaded from the archive (did, observed_at, content_hash)")
    ap.add_argument("--atomic", action="store_true",
                    help="single transaction for the whole run: all rows load or none do")
    ap.add_argument("--window", help="record this run in activity.profile_load_windows (implies --atomic); "
                                     "skip if the window is already recorded")
    args = ap.parse_args()
    if args.window:
        args.atomic = True

    shard_dir = Path(args.shard_dir)
    shards = sorted(shard_dir.glob("**/*.ndjson.gz"))  # chunked runs nest shards per seq window
    if not shards:
        sys.exit(f"no *.ndjson.gz shards found under {shard_dir}")

    conn = None
    if not args.dry_run or args.skip_existing:
        db = os.environ.get("DATABASE_URL")
        if not db:
            sys.exit("DATABASE_URL not set (source .env)")
        conn = psycopg2.connect(db)

    if args.window and not args.dry_run:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS activity.profile_load_windows ("
                        "win text PRIMARY KEY, rows_loaded bigint NOT NULL, "
                        "loaded_at timestamptz NOT NULL DEFAULT now())")
            cur.execute("SELECT 1 FROM activity.profile_load_windows WHERE win = %s", (args.window,))
            if cur.fetchone():
                print(f"[load] window {args.window} already loaded, skipping")
                conn.rollback()
                return
        conn.commit()

    seen_keys = set()
    if args.skip_existing:
        with conn.cursor() as cur:
            cur.execute("SELECT did, observed_at, content_hash FROM activity.profile_records "
                        "WHERE source = 'jetstream_archive'")
            seen_keys = set(cur)
        print(f"[load] {len(seen_keys):,} existing archive rows will be skipped", flush=True)

    total = 0
    for shard in shards:
        chunk, n = [], 0
        for row in rows_from_shard(shard):
            if args.skip_existing:
                key = (row[0], row[1], row[12])
                if key in seen_keys:
                    continue
                seen_keys.add(key)
            n += 1
            if args.dry_run:
                continue
            chunk.append(row)
            if len(chunk) >= CHUNK_SIZE:
                write_chunk(conn, chunk, commit=not args.atomic)
                chunk = []
        if chunk:
            write_chunk(conn, chunk, commit=not args.atomic)
        total += n
        print(f"[load] {shard.relative_to(shard_dir)}: {n:,} profile rows", flush=True)

    print(f"[load] total: {total:,} profile rows across {len(shards)} shards")
    if conn and args.window and not args.dry_run:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO activity.profile_load_windows (win, rows_loaded) VALUES (%s, %s)",
                        (args.window, total))
    if conn and args.atomic:
        conn.commit()
    if conn:
        conn.close()


if __name__ == "__main__":
    main()
