-- One row per app.bsky.feed.post delete seen on Jetstream, for age-at-deletion analysis.
-- post_created_at is decoded from the rkey TID (NULL for non-TID rkeys).
-- Append-only, no PK: --backfill-hours replays can duplicate rows; dedupe on (did, rkey) at read.
-- Off switch: CAPTURE_POST_DELETE_EVENTS=0 (~200K rows/day).

CREATE TABLE IF NOT EXISTS activity.post_delete_events (
  did             text        NOT NULL,
  rkey            text        NOT NULL,
  post_created_at timestamptz,
  deleted_at      timestamptz NOT NULL
);

-- BRIN: rows arrive in deleted_at order, so range scans are cheap at a fraction of a btree's size.
CREATE INDEX IF NOT EXISTS idx_post_delete_events_deleted_at
  ON activity.post_delete_events USING brin (deleted_at);
