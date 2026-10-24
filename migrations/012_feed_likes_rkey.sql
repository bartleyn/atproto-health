-- Schema: feeds (feed like edges)
-- Run against: atproto_health database
--
-- Motivation: an unlike arrives as a delete of the liker's app.bsky.feed.like record,
-- naming only (did, rkey) — not the liked feed. jetstream-activity.ts now keeps each feed
-- like's rkey so it can match that delete and remove the edge; without it feeds.feed_likes
-- only ever grows and overcounts.
--
-- Backfilled rows stay NULL: getLikes does not return the like record's URI. Their unlikes
-- are caught by scripts/backfill_feed_likes.py --reconcile instead.

ALTER TABLE feeds.feed_likes ADD COLUMN IF NOT EXISTS like_rkey TEXT;

-- Startup load of the (liker, rkey) -> feed map scans only rows that have an rkey.
CREATE INDEX IF NOT EXISTS idx_feed_likes_rkey
  ON feeds.feed_likes(liker_did, like_rkey) WHERE like_rkey IS NOT NULL;
