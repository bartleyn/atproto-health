#!/usr/bin/env python3
"""
Backfill activity.feed_generators.service_did for rows collected before the jetstream
collector captured it (see migrations/011_feed_generator_service_did.sql and the
jetstream-activity.ts fix that adds it going forward).

app.bsky.feed.defs#generatorView's `did` field IS the serving DID directly — no DID
resolution step needed, just a batched fetch. app.bsky.feed.getFeedGenerators takes
repeated `feeds=` params in one GET; measured ceiling ~80 URIs/request before a 414
(URL length, not the lexicon's own cap) — see CONSTRUCTION-METHOD-PLAN.md in
feedmoreseemore/feed-discovery, which this backfill directly serves (Layer A / engine_did).

Feeds NOT returned in a batch's response (deleted/unresolvable since they were recorded)
are left service_did = NULL rather than guessed — same convention as feed_health's
"unknown stays unknown" handling elsewhere in this codebase.

Resumable with no separate worklist: progress IS the service_did column. A feed that got
NULL back from a real API response (not an error) is asked again on rerun too, since
that's indistinguishable from "not tried yet" — cheap, since only rows still NULL are
selected each run, and a deleted feed just keeps failing to resolve harmlessly.

Usage:
  python3 scripts/backfill_feed_generator_service_did.py --dry-run
  python3 scripts/backfill_feed_generator_service_did.py
  python3 scripts/backfill_feed_generator_service_did.py --limit 500   # smoke test
"""

from __future__ import annotations   # system python3 here is 3.9 — PEP 604 unions in annotations

import argparse
import json
import os
import signal
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import psycopg2
import psycopg2.extras

APPVIEW = os.environ.get("APPVIEW_URL", "https://public.api.bsky.app")
BATCH_SIZE = 75          # under the measured ~80 URI/request ceiling before a 414
USER_AGENT = "atproto-health-feed-generator-service-did-backfill/1.0"

_stop = False


def _handle_sigint(signum, frame):
    global _stop
    if _stop:
        sys.exit(130)
    _stop = True
    print("\n[backfill] interrupt received — finishing current batch, then exiting", flush=True)


def get_feed_generators(uris: list, timeout: int) -> dict:
    """One getFeedGenerators call. Returns {uri: service_did or None} for every requested
    uri that came back in the response (missing = deleted/unresolvable, left untouched)."""
    params = urllib.parse.urlencode([("feeds", u) for u in uris])
    url = f"{APPVIEW}/xrpc/app.bsky.feed.getFeedGenerators?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = json.load(resp)
    out = {}
    for feed in data.get("feeds", []):
        uri = feed.get("uri")
        if uri:
            out[uri] = feed.get("did") or None
    return out


def fetch_batch(uris: list, timeout: int, backoff: float, max_backoff: float) -> tuple:
    """Returns (result_dict, final_backoff). Retries 429/5xx with growing backoff. URI
    length varies a lot (long rkeys push some 75-feed batches over the 414 line even
    though the ~80 ceiling held on average) — split in half and retry rather than assume
    a fixed count is always safe."""
    while True:
        try:
            return get_feed_generators(uris, timeout), backoff
        except urllib.error.HTTPError as exc:
            if exc.code == 414:
                if len(uris) <= 1:
                    print(f"  HTTP 414 on a single URI ({uris[0]}); skipping", flush=True)
                    return {}, backoff
                mid = len(uris) // 2
                r1, backoff = fetch_batch(uris[:mid], timeout, backoff, max_backoff)
                r2, backoff = fetch_batch(uris[mid:], timeout, backoff, max_backoff)
                return {**r1, **r2}, backoff
            if exc.code == 429 or exc.code >= 500:
                if backoff > max_backoff:
                    print(f"  HTTP {exc.code}; giving up on this batch after backoff", flush=True)
                    return {}, backoff
                print(f"  HTTP {exc.code}; sleeping {backoff:.0f}s", flush=True)
                time.sleep(backoff)
                backoff *= 2
                continue
            print(f"  HTTP {exc.code}: {exc.reason}; skipping batch", flush=True)
            return {}, backoff
        except Exception as exc:
            print(f"  {type(exc).__name__}: {exc}; skipping batch", flush=True)
            return {}, backoff


def write_results(conn, results: dict) -> int:
    """results: {uri: service_did}. Only writes non-null dids — NULL results are left
    for the next run rather than written (a batch write-through of empty values every
    rerun would be pointless work with the same outcome as not touching the row)."""
    rows = [(did, uri) for uri, did in results.items() if did]
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur, "UPDATE activity.feed_generators SET service_did = %s WHERE uri = %s", rows)
    conn.commit()
    return len(rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--backoff", type=float, default=5.0)
    ap.add_argument("--max-backoff", type=float, default=160.0)
    ap.add_argument("--rps-batches", type=float, default=2.0,
                    help="target batch-requests/sec (default 2, each batch = 75 feeds)")
    ap.add_argument("--limit", type=int, help="only process the first N unresolved feeds (smoke test)")
    ap.add_argument("--dry-run", action="store_true", help="show the worklist size, make no calls")
    args = ap.parse_args()

    db = os.environ.get("DATABASE_URL")
    if not db:
        sys.exit("DATABASE_URL not set (source .env)")

    conn = psycopg2.connect(db)
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM activity.feed_generators "
                     "WHERE deleted_at IS NULL AND service_did IS NULL")
        total_unresolved = cur.fetchone()[0]
    print(f"[backfill] {total_unresolved:,} feeds with service_did still unresolved")

    if args.dry_run:
        print(f"[backfill] ~{-(-total_unresolved // BATCH_SIZE):,} requests at batch size {BATCH_SIZE}")
        conn.close()
        return

    signal.signal(signal.SIGINT, _handle_sigint)

    with conn.cursor() as cur:
        q = ("SELECT uri FROM activity.feed_generators "
             "WHERE deleted_at IS NULL AND service_did IS NULL")
        if args.limit:
            q += f" LIMIT {int(args.limit)}"
        cur.execute(q)
        todo = [r[0] for r in cur.fetchall()]
    print(f"[backfill] processing {len(todo):,} feeds in batches of {BATCH_SIZE}")

    interval = 1.0 / args.rps_batches if args.rps_batches > 0 else 0.0
    backoff = args.backoff
    resolved = 0
    t_start = time.monotonic()
    n_batches = -(-len(todo) // BATCH_SIZE)

    for i in range(0, len(todo), BATCH_SIZE):
        if _stop:
            break
        batch = todo[i:i + BATCH_SIZE]
        t0 = time.monotonic()
        results, backoff = fetch_batch(batch, args.timeout, backoff, args.max_backoff)
        resolved += write_results(conn, results)

        batch_num = i // BATCH_SIZE + 1
        if batch_num % 10 == 0 or batch_num == n_batches:
            rate = batch_num / max(time.monotonic() - t_start, 1e-9)
            eta = (n_batches - batch_num) / rate / 60 if rate > 0 else 0
            print(f"  [{batch_num}/{n_batches}] resolved={resolved:,} "
                  f"| {rate * 60:.0f} batches/min | ETA {eta:.1f} min", flush=True)

        elapsed = time.monotonic() - t0
        if interval > elapsed:
            time.sleep(interval - elapsed)

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM activity.feed_generators "
                     "WHERE deleted_at IS NULL AND service_did IS NULL")
        remaining = cur.fetchone()[0]
    print(f"[backfill] stopped: resolved={resolved:,} this run, {remaining:,} still unresolved "
          "(deleted/unresolvable feeds, or interrupted — rerun to continue)")
    conn.close()


if __name__ == "__main__":
    main()
