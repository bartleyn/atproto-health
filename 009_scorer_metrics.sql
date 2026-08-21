-- Schema: activity (scorer operational metrics)
-- Run against: atproto_health database
-- DRAFT — move to migrations/009_scorer_metrics.sql before applying.
--
-- Motivation: the only record of scorer round-trip time is a console.log line
-- ("[scorer] API: N posts scored in Xms") that goes to the launchd log and is
-- never aggregated. There is no history of DLQ depth or drop rate at all, so
-- "are we falling behind?" can only be answered by tailing a log by hand.
--
-- Two grains, because the questions have two grains:
--   scorer_latency — one row per API call. Answers "how slow is a batch?"
--   scorer_flush   — one row per flush cycle. Answers "are we keeping up?"
--
-- Sizing (computed from measured throughput 2026-08-21): ~41 posts/sec at
-- SCORE_BATCH_SIZE=50 is 0.82 batches/sec = 71,200 latency rows/day. At ~100
-- bytes/row including tuple overhead and the ts index, that is 7.1 MB/day, so
-- the 30-day retention below costs ~214 MB. scorer_flush is 1,440 rows/day.

-- One row per /score call, live or DLQ retry.
CREATE TABLE IF NOT EXISTS activity.scorer_latency (
  ts        TIMESTAMPTZ NOT NULL,           -- when the call STARTED
  source    TEXT        NOT NULL,           -- 'live' | 'dlq'
  posts     INTEGER     NOT NULL,
  api_ms    INTEGER,                        -- NULL if fetch threw before responding
  upload_ms INTEGER,                        -- NULL if nothing was uploaded
  total_ms  INTEGER     NOT NULL,           -- the round trip the user cares about
  ok        BOOLEAN     NOT NULL
);

-- Every dashboard query is "last N hours", so ts leads. Percentiles are computed
-- with percentile_cont over the window, which is exact at this row count.
CREATE INDEX IF NOT EXISTS idx_scorer_latency_ts
  ON activity.scorer_latency(ts DESC);

-- One row per flushScorer() cycle: the keeping-up numbers.
CREATE TABLE IF NOT EXISTS activity.scorer_flush (
  ts         TIMESTAMPTZ NOT NULL,
  buffered   INTEGER     NOT NULL,  -- posts taken off the buffer this cycle
  succeeded  INTEGER     NOT NULL,
  failed     INTEGER     NOT NULL,  -- posts sent to the DLQ
  dropped    INTEGER     NOT NULL,  -- posts refused at SCORER_MAX_BUFFER — the real loss
  batches    INTEGER     NOT NULL,
  flush_ms   INTEGER     NOT NULL,  -- wall clock; if this exceeds the 60s cycle we are behind
  dlq_rows   INTEGER     NOT NULL,  -- DLQ depth sampled at end of cycle
  dlq_posts  BIGINT                 -- NULL until post_count is backfilled (see below)
);

CREATE INDEX IF NOT EXISTS idx_scorer_flush_ts
  ON activity.scorer_flush(ts DESC);

-- DLQ depth in POSTS, not rows. Summing json_array_length over posts_json means
-- scanning the whole 3.6 GB table, which is far too expensive to do per flush,
-- so record the count at insert time instead. Existing rows stay NULL; a one-time
-- backfill is optional and safe to skip (dashboards fall back to rows).
ALTER TABLE activity.score_dlq
  ADD COLUMN IF NOT EXISTS post_count INTEGER;
