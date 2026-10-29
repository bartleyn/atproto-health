#!/usr/bin/env python3
"""
Refresh activity.feed_generators.display_name / description / service_did from the AppView.

The jetstream collector dropped feed generator updates until the skyfeed-recipes change
(2026-10-25), so feeds renamed or moved to a new host before then kept their old values here
(0.6% of a 1,000-feed sample renamed; ~0.1% moved host). The collector records edits from now
on; this catches up the rest. Reads app.bsky.feed.getFeedGenerators, 25 feeds per request;
writes only rows that changed, in URI order (the collector's lock order).

Usage:
  python3 scripts/refresh_feed_generator_records.py --dry-run     # count changes, write nothing
  python3 scripts/refresh_feed_generator_records.py
"""

from __future__ import annotations  # system python3 here is 3.9

import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request

import psycopg2
import psycopg2.extras

APPVIEW = "https://public.api.bsky.app"
USER_AGENT = "atproto-health-feed-record-refresh/1.0"


def fetch(uris: list[str], timeout: int) -> dict:
    """uri -> (displayName, description, did) for up to 25 feeds; unresolvable feeds are omitted."""
    url = f"{APPVIEW}/xrpc/app.bsky.feed.getFeedGenerators?" + urllib.parse.urlencode([("feeds", u) for u in uris])
    for attempt in range(4):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": USER_AGENT}),
                                        timeout=timeout) as r:
                return {f["uri"]: ((f.get("displayName") or "").replace("\x00", ""),   # text rejects NUL
                                   (f.get("description") or "").replace("\x00", ""),
                                   f.get("did"))
                        for f in json.load(r).get("feeds", [])}
        except Exception:                             # noqa: BLE001 - retried, then the batch is skipped
            time.sleep(2 * (attempt + 1))
    return {}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rps", type=float, default=5.0)
    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    db = os.environ.get("DATABASE_URL")
    if not db:
        sys.exit("DATABASE_URL not set (source .env)")

    conn = psycopg2.connect(db)
    with conn.cursor() as cur:
        cur.execute("SELECT uri, display_name, description, service_did FROM activity.feed_generators "
                    "WHERE deleted_at IS NULL ORDER BY uri")
        rows = cur.fetchall()
    have = {u: (n or "", d or "", s) for u, n, d, s in rows}
    uris = [r[0] for r in rows]
    print(f"[records] {len(uris):,} live feeds, {(len(uris) + 24) // 25:,} requests")

    changed, resolved, t0 = [], 0, time.monotonic()
    for i in range(0, len(uris), 25):
        t = time.monotonic()
        got = fetch(uris[i:i + 25], args.timeout)
        resolved += len(got)
        changed += [(u, n, d, s or have[u][2]) for u, (n, d, s) in got.items()
                    if (n, d, s or have[u][2]) != have[u]]   # a missing did never blanks a known one
        if i and i % 25000 == 0:
            print(f"  {i:,}/{len(uris):,} | resolved {resolved:,} | changed {len(changed):,} | "
                  f"{(time.monotonic() - t0) / 60:.0f} min", flush=True)
        time.sleep(max(0.0, 1.0 / args.rps - (time.monotonic() - t)))
    print(f"[records] resolved {resolved:,}; changed {len(changed):,} "
          f"(names {sum(1 for u, n, _, _ in changed if n != have[u][0]):,}, "
          f"descriptions {sum(1 for u, _, d, _ in changed if d != have[u][1]):,}, "
          f"hosts {sum(1 for u, _, _, s in changed if s != have[u][2]):,})")
    if args.dry_run or not changed:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, """
            UPDATE activity.feed_generators f SET display_name = v.n, description = v.d, service_did = v.s
            FROM (VALUES %s) AS v(uri, n, d, s) WHERE f.uri = v.uri""", sorted(changed), page_size=500)
    conn.commit()
    conn.close()
    print(f"[records] updated {len(changed):,} rows")


if __name__ == "__main__":
    main()
