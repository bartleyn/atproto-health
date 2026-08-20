#!/usr/bin/env python3
"""
Backfill the user x feed like matrix into feeds.feed_likes.

Why this exists: activity.feed_generator_likes_daily only stores a per-(feed,date)
COUNT — the liker is discarded at ingest. So there is no user x feed matrix to
factorise. app.bsky.feed.getLikes DOES work on feed generator records (verified;
the lexicon is written for posts, so this is not obvious), which makes a historical
backfill possible instead of waiting months for forward capture.

Pool comes from the feed-explorer artifacts (external drive by default):
  feed_likes.jsonl   {"uri", "like_count", ...}   — lifetime counts from the AppView
  feed_liveness.csv  uri,status,...               — LIVE | AGING | DEAD

Feeds with like_count = 0 are skipped: getLikes returns nothing for them, so they
are pure wasted requests. As of the 2026-08 snapshot that is 24,878 of 63,691 LIVE
feeds, and skipping them loses ZERO edges (LIVE & likes>=1 and ALL LIVE both hold
1,375,345 edges).

Resumable: per-feed state in feeds.feed_likes_backfill, including the AppView cursor
for a feed that was interrupted mid-pagination. Re-running is idempotent — the edge
table's PK is (feed_uri, liker_did) and inserts are ON CONFLICT DO NOTHING.

Usage:
  python3 scripts/backfill_feed_likes.py --dry-run           # show the worklist, no calls
  python3 scripts/backfill_feed_likes.py --seed-only         # populate the worklist, then stop
  python3 scripts/backfill_feed_likes.py                     # run it (resumes automatically)
  python3 scripts/backfill_feed_likes.py --limit-feeds 20    # smoke test
  python3 scripts/backfill_feed_likes.py --min-likes 100     # high-mass feeds only
"""

from __future__ import annotations   # system python3 here is 3.9 — PEP 604 unions in annotations

import argparse
import json
import math
import os
import signal
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

APPVIEW = os.environ.get("APPVIEW_URL", "https://public.api.bsky.app")
FEEDS_DIR = os.environ.get("FEEDS_DIR", "/Volumes/miniext/feeds")
PAGE_LIMIT = 100          # getLikes max
USER_AGENT = "atproto-health-feed-likes-backfill/1.0"

_stop = False


def _handle_sigint(signum, frame):
    global _stop
    if _stop:                      # second Ctrl-C — give up immediately
        sys.exit(130)
    _stop = True
    print("\n[backfill] interrupt received — finishing current feed, then exiting", flush=True)


def load_pool(feeds_dir: str, min_likes: int, liveness: str | None):
    """Return [(uri, like_count)] from the feed-explorer sidecars, biggest first."""
    likes_path = os.path.join(feeds_dir, "feed_likes.jsonl")
    live_path = os.path.join(feeds_dir, "feed_liveness.csv")
    if not os.path.exists(likes_path):
        sys.exit(f"pool file not found: {likes_path} (set FEEDS_DIR)")

    likes: dict[str, int] = {}
    with open(likes_path) as f:
        for line in f:
            try:
                d = json.loads(line)
            except json.JSONDecodeError:
                continue
            uri = d.get("uri")
            if uri:
                # last row wins — the sidecar is append-only and may hold re-fetches
                likes[uri] = d.get("like_count") or 0

    keep: set[str] | None = None
    if liveness:
        if not os.path.exists(live_path):
            sys.exit(f"liveness file not found: {live_path} (or pass --liveness '')")
        keep = set()
        with open(live_path) as f:
            next(f, None)
            for line in f:
                parts = line.split(",")
                if len(parts) >= 2 and parts[1].strip() == liveness:
                    keep.add(parts[0])

    pool = [
        (uri, c) for uri, c in likes.items()
        if c >= min_likes and (keep is None or uri in keep)
    ]
    pool.sort(key=lambda t: -t[1])       # high-mass feeds first: fail early on the ones that matter
    return pool


def seed_worklist(conn, pool) -> int:
    """Insert the pool into the backfill worklist. Existing rows keep their progress."""
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO feeds.feed_likes_backfill (feed_uri, like_count)
            VALUES %s
            ON CONFLICT (feed_uri) DO UPDATE SET like_count = EXCLUDED.like_count
            """,
            pool,
            page_size=1000,
        )
    conn.commit()
    return len(pool)


def get_likes(uri: str, cursor: str | None, timeout: int):
    """One getLikes page. Returns (likes, next_cursor). Raises on HTTP error."""
    params = {"uri": uri, "limit": str(PAGE_LIMIT)}
    if cursor:
        params["cursor"] = cursor
    url = f"{APPVIEW}/xrpc/app.bsky.feed.getLikes?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = json.load(resp)
    return data.get("likes", []), data.get("cursor")


def _parse_ts(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def write_edges(conn, feed_uri: str, likes: list) -> int:
    rows = []
    for like in likes:
        did = (like.get("actor") or {}).get("did")
        if did:
            rows.append((feed_uri, did, _parse_ts(like.get("createdAt")), "backfill"))
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO feeds.feed_likes (feed_uri, liker_did, created_at, source)
            VALUES %s
            ON CONFLICT (feed_uri, liker_did) DO NOTHING
            """,
            rows,
            page_size=500,
        )
    return len(rows)


def mark(conn, feed_uri, *, status=None, cursor=..., pages=None, likes_seen=None, error=...):
    sets, vals = ["updated_at = now()"], []
    if status is not None:
        sets.append("status = %s"); vals.append(status)
    if cursor is not ...:
        sets.append("cursor = %s"); vals.append(cursor)
    if pages is not None:
        sets.append("pages = %s"); vals.append(pages)
    if likes_seen is not None:
        sets.append("likes_seen = %s"); vals.append(likes_seen)
    if error is not ...:
        sets.append("last_error = %s"); vals.append(error)
    vals.append(feed_uri)
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE feeds.feed_likes_backfill SET {', '.join(sets)} WHERE feed_uri = %s",
            vals,
        )
    conn.commit()


def drain_feed(conn, feed_uri, start_cursor, pages, seen, args) -> tuple[str, str | None]:
    """Paginate one feed to exhaustion. Returns (status, error)."""
    cursor = start_cursor
    interval = 1.0 / args.rps if args.rps > 0 else 0.0
    backoff = args.backoff

    while not _stop:
        t0 = time.monotonic()
        try:
            likes, cursor = get_likes(feed_uri, cursor, args.timeout)
        except urllib.error.HTTPError as exc:
            if exc.code == 429 or exc.code >= 500:
                if backoff > args.max_backoff:
                    return "error", f"HTTP {exc.code} (gave up after backoff)"
                print(f"  HTTP {exc.code}; sleeping {backoff:.0f}s", flush=True)
                time.sleep(backoff)
                backoff *= 2
                continue
            # 400 = deleted/unresolvable record. Terminal, and common in a stale pool.
            return "error", f"HTTP {exc.code}: {exc.reason}"
        except Exception as exc:
            return "error", f"{type(exc).__name__}: {exc}"

        backoff = args.backoff
        seen += write_edges(conn, feed_uri, likes)
        pages += 1
        conn.commit()

        if not cursor or not likes:
            mark(conn, feed_uri, status="done", cursor=None, pages=pages,
                 likes_seen=seen, error=None)
            return "done", None

        # checkpoint mid-feed so a long feed resumes where it stopped
        if pages % args.checkpoint_pages == 0:
            mark(conn, feed_uri, cursor=cursor, pages=pages, likes_seen=seen)

        elapsed = time.monotonic() - t0
        if interval > elapsed:
            time.sleep(interval - elapsed)

    mark(conn, feed_uri, cursor=cursor, pages=pages, likes_seen=seen)
    return "interrupted", None


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--min-likes", type=int, default=1,
                    help="skip feeds below this like_count (default 1; 0-like feeds yield no edges)")
    ap.add_argument("--liveness", default="LIVE",
                    help="liveness status to keep, or '' for all (default LIVE)")
    ap.add_argument("--feeds-dir", default=FEEDS_DIR)
    ap.add_argument("--rps", type=float, default=5.0, help="target requests/sec (default 5)")
    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--backoff", type=float, default=5.0, help="initial retry backoff seconds")
    ap.add_argument("--max-backoff", type=float, default=160.0)
    ap.add_argument("--checkpoint-pages", type=int, default=20)
    ap.add_argument("--limit-feeds", type=int, help="only process N feeds (smoke test)")
    ap.add_argument("--retry-errors", action="store_true", help="re-queue feeds marked error")
    ap.add_argument("--seed-only", action="store_true", help="populate the worklist and exit")
    ap.add_argument("--dry-run", action="store_true", help="show the worklist, make no calls")
    args = ap.parse_args()

    db = os.environ.get("DATABASE_URL")
    if not db:
        sys.exit("DATABASE_URL not set (source .env)")

    pool = load_pool(args.feeds_dir, args.min_likes, args.liveness or None)
    est_pages = sum(max(1, math.ceil(c / PAGE_LIMIT)) for _, c in pool)
    est_edges = sum(c for _, c in pool)
    print(f"[backfill] pool: {len(pool):,} feeds | ~{est_edges:,} edges | ~{est_pages:,} pages")
    if args.rps > 0:
        print(f"[backfill] est. {est_pages / args.rps / 3600:.1f} h at {args.rps} req/s")

    if args.dry_run:
        for uri, c in pool[:10]:
            print(f"  {c:>8,}  {uri}")
        print(f"  ... ({len(pool):,} total)")
        return

    conn = psycopg2.connect(db)
    signal.signal(signal.SIGINT, _handle_sigint)

    n = seed_worklist(conn, pool)
    print(f"[backfill] worklist seeded ({n:,} rows)")
    if args.seed_only:
        conn.close()
        return

    if args.retry_errors:
        with conn.cursor() as cur:
            cur.execute("UPDATE feeds.feed_likes_backfill SET status='pending', last_error=NULL "
                        "WHERE status='error'")
            print(f"[backfill] re-queued {cur.rowcount:,} errored feeds")
        conn.commit()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT feed_uri, cursor, pages, likes_seen
            FROM feeds.feed_likes_backfill
            WHERE status <> 'done' AND like_count >= %s
            ORDER BY like_count DESC
        """, (args.min_likes,))
        todo = cur.fetchall()
    if args.limit_feeds:
        todo = todo[:args.limit_feeds]
    print(f"[backfill] {len(todo):,} feeds to process")

    t_start = time.monotonic()
    done = errors = edges = 0
    for i, (feed_uri, cursor, pages, seen) in enumerate(todo, 1):
        if _stop:
            break
        status, err = drain_feed(conn, feed_uri, cursor, pages, seen, args)
        if status == "done":
            done += 1
        elif status == "error":
            errors += 1
            mark(conn, feed_uri, status="error", error=err)
            print(f"  [{i}/{len(todo)}] ERROR {feed_uri}: {err}", flush=True)
        elif status == "interrupted":
            break

        if i % 25 == 0 or i == len(todo):
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM feeds.feed_likes")
                edges = cur.fetchone()[0]
            rate = i / max(time.monotonic() - t_start, 1e-9)
            eta = (len(todo) - i) / rate / 3600 if rate > 0 else 0
            print(f"  [{i}/{len(todo)}] done={done} err={errors} edges={edges:,} "
                  f"| {rate * 60:.0f} feeds/min | ETA {eta:.1f} h", flush=True)

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM feeds.feed_likes")
        edges = cur.fetchone()[0]
        cur.execute("SELECT count(DISTINCT liker_did) FROM feeds.feed_likes")
        users = cur.fetchone()[0]
    print(f"[backfill] stopped: done={done} errors={errors} "
          f"edges={edges:,} distinct_likers={users:,}")
    conn.close()


if __name__ == "__main__":
    main()
