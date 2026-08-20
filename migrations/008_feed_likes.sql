-- Schema: feeds (feed generator intelligence — like edges, catalog, embeddings)
-- Run against: atproto_health database
--
-- Motivation: activity.feed_generator_likes_daily aggregates likes to a per-(feed,date)
-- COUNT, discarding the liker. The jetstream collector sees the liker on every event
-- (evt.commit.repo) and throws it away. That makes collaborative filtering over feeds
-- impossible — there is no user x feed matrix anywhere.
--
-- feeds.feed_likes is that matrix: one row per (feed, liker). Populated two ways:
--   'backfill'  — scripts/backfill_feed_likes.py, paginating app.bsky.feed.getLikes
--   'jetstream' — live capture (follow-up change to jetstream-activity.ts)
--
-- Sizing (computed from the feed-explorer pool, 2026-08): 38,813 LIVE feeds with >=1
-- like hold ~1.38M edges. Feeds with like_count = 0 are skipped — getLikes returns
-- nothing for them, so they are pure wasted requests (24,878 of them).

CREATE SCHEMA IF NOT EXISTS feeds;

-- The user x feed like matrix. A given actor can like a record at most once, so
-- (feed_uri, liker_did) is a natural key and makes re-ingestion idempotent.
CREATE TABLE IF NOT EXISTS feeds.feed_likes (
  feed_uri   TEXT        NOT NULL,
  liker_did  TEXT        NOT NULL,
  created_at TIMESTAMPTZ,          -- like record createdAt, as reported by the AppView
  source     TEXT        NOT NULL, -- 'backfill' | 'jetstream'
  PRIMARY KEY (feed_uri, liker_did)
);

-- Co-occurrence is built by grouping edges per liker, so the liker-side lookup
-- needs its own index (the PK only serves feed-side scans).
CREATE INDEX IF NOT EXISTS idx_feed_likes_liker
  ON feeds.feed_likes(liker_did);

-- Per-feed backfill progress, so a multi-hour run resumes where it stopped.
-- Cursor is the AppView pagination cursor for a partially-drained feed.
CREATE TABLE IF NOT EXISTS feeds.feed_likes_backfill (
  feed_uri    TEXT        NOT NULL PRIMARY KEY,
  like_count  INTEGER,                          -- expected total, from the pool snapshot
  status      TEXT        NOT NULL DEFAULT 'pending', -- pending | done | error
  cursor      TEXT,
  pages       INTEGER     NOT NULL DEFAULT 0,
  likes_seen  INTEGER     NOT NULL DEFAULT 0,
  last_error  TEXT,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Worklist scan: "give me the next pending feed, biggest first".
CREATE INDEX IF NOT EXISTS idx_feed_likes_backfill_status
  ON feeds.feed_likes_backfill(status, like_count DESC);
