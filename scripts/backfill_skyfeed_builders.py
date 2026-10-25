#!/usr/bin/env python3
"""
Backfill activity.feed_generators.skyfeed_builder for SkyFeed feeds (migration 014).

The jetstream collector stores the recipe from every feed generator create/update from now on;
this reads it for feeds created before that, via com.atproto.repo.getRecord on each creator's
PDS (plc.plc_did_pds). Resumable: only rows still SQL NULL are read. JSON null is written when
the record has no recipe or is gone, so those are not read again.

Usage:
  python3 scripts/backfill_skyfeed_builders.py --dry-run      # count the worklist
  python3 scripts/backfill_skyfeed_builders.py --limit 200    # smoke test
  python3 scripts/backfill_skyfeed_builders.py                # ~70 min for ~89k feeds
"""

from __future__ import annotations  # system python3 here is 3.9

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import psycopg2
import psycopg2.extras

USER_AGENT = "atproto-health-skyfeed-builder-backfill/1.0"
SKYFEED = "did:web:skyfeed.me"
BATCH = 500


def strip_nul(v):
    """Drop U+0000 from every string in a JSON value (jsonb rejects it); escapes stay intact."""
    if isinstance(v, str):
        return v.replace("\x00", "")
    if isinstance(v, list):
        return [strip_nul(x) for x in v]
    if isinstance(v, dict):
        return {strip_nul(k): strip_nul(x) for k, x in v.items()}
    return v


def get_builder(uri: str, pds: str | None, timeout: int):
    """(uri, recipe | None, done). done=False on a transient failure, so the row stays unread."""
    did, rkey = uri.split("/")[2], uri.split("/")[-1]
    q = urllib.parse.urlencode({"repo": did, "collection": "app.bsky.feed.generator", "rkey": rkey})
    url = f"{(pds or 'https://bsky.social').rstrip('/')}/xrpc/com.atproto.repo.getRecord?{q}"
    for attempt in range(3):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": USER_AGENT}),
                                        timeout=timeout) as r:
                return uri, json.load(r).get("value", {}).get("skyfeedBuilder"), True
        except urllib.error.HTTPError as e:
            if e.code == 400:                         # RecordNotFound / repo gone: nothing to read
                return uri, None, True
            if e.code not in (429,) and e.code < 500:
                return uri, None, False
        except Exception:
            pass
        time.sleep(2 * (attempt + 1))
    return uri, None, False


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--limit", type=int)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--timeout", type=int, default=20)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    db = os.environ.get("DATABASE_URL")
    if not db:
        sys.exit("DATABASE_URL not set (source .env)")

    conn = psycopg2.connect(db)
    with conn.cursor() as cur:
        cur.execute("""SELECT f.uri, p.pds_url FROM activity.feed_generators f
                       LEFT JOIN plc.plc_did_pds p ON p.did = f.creator_did
                       WHERE f.deleted_at IS NULL AND f.skyfeed_builder IS NULL AND f.service_did = %s
                       ORDER BY f.uri""", (SKYFEED,))
        todo = cur.fetchall()
    if args.limit:
        todo = todo[:args.limit]
    print(f"[skyfeed] {len(todo):,} SkyFeed feeds without a recipe read")
    if args.dry_run:
        return

    t0, done, found, failed, pending = time.monotonic(), 0, 0, 0, []

    def flush():
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, """
                UPDATE activity.feed_generators f SET skyfeed_builder = v.b::jsonb
                FROM (VALUES %s) AS v(uri, b) WHERE f.uri = v.uri AND f.skyfeed_builder IS NULL""",
                pending, page_size=BATCH)
        conn.commit()
        pending.clear()

    with ThreadPoolExecutor(args.workers) as ex:
        for uri, builder, ok in ex.map(lambda r: get_builder(r[0], r[1], args.timeout), todo):
            done += 1
            if not ok:
                failed += 1
                continue
            found += builder is not None
            pending.append((uri, json.dumps(strip_nul(builder))))
            if len(pending) >= BATCH:
                flush()
                rate = done / (time.monotonic() - t0)
                print(f"  {done:,}/{len(todo):,} | recipes {found:,} | failed {failed:,} | "
                      f"{rate:.0f}/s | ETA {(len(todo) - done) / rate / 60:.0f} min", flush=True)
    if pending:
        flush()
    print(f"[skyfeed] read {done:,}: recipes {found:,}, none {done - found - failed:,}, "
          f"failed {failed:,} (left unread, retried next run)")
    conn.close()


if __name__ == "__main__":
    main()
