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


def _parse_ts(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def rows_from_shard(path: Path):
    with gzip.open(path, "rt") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            if r.get("collection") != "app.bsky.actor.profile":
                continue
            yield (
                r["did"],
                datetime.fromtimestamp(r["time_us"] / 1_000_000, tz=timezone.utc),
                "backfill",
                "jetstream_archive",
                r.get("display_name"),
                r.get("description"),
                r.get("avatar_cid"),
                r.get("banner_cid"),
                r.get("joined_via_starterpack_uri"),
                r.get("pinned_post_uri"),
                r.get("self_labels") or None,
                _parse_ts(r.get("record_created_at")),
                r["content_hash"],
            )


def write_chunk(conn, rows: list) -> None:
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
    conn.commit()


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("shard_dir", help="directory of part-*.ndjson.gz shards")
    ap.add_argument("--dry-run", action="store_true", help="count rows per shard, write nothing")
    args = ap.parse_args()

    shard_dir = Path(args.shard_dir)
    shards = sorted(shard_dir.glob("*.ndjson.gz"))
    if not shards:
        sys.exit(f"no *.ndjson.gz shards found in {shard_dir}")

    conn = None
    if not args.dry_run:
        db = os.environ.get("DATABASE_URL")
        if not db:
            sys.exit("DATABASE_URL not set (source .env)")
        conn = psycopg2.connect(db)

    total = 0
    for shard in shards:
        chunk, n = [], 0
        for row in rows_from_shard(shard):
            n += 1
            if args.dry_run:
                continue
            chunk.append(row)
            if len(chunk) >= CHUNK_SIZE:
                write_chunk(conn, chunk)
                chunk = []
        if chunk:
            write_chunk(conn, chunk)
        total += n
        print(f"[load] {shard.name}: {n:,} profile rows", flush=True)

    print(f"[load] total: {total:,} profile rows across {len(shards)} shards")
    if conn:
        conn.close()


if __name__ == "__main__":
    main()
