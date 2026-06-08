-- Schema: activity (jetstream-activity.db)
-- Run against: atproto_health database

CREATE SCHEMA IF NOT EXISTS activity;

CREATE TABLE IF NOT EXISTS activity.did_activity_daily (
  did            TEXT    NOT NULL,
  date           TEXT    NOT NULL, -- YYYY-MM-DD
  activity_types INTEGER NOT NULL DEFAULT 0, -- bitmask
  PRIMARY KEY (did, date)
);

-- Covering index: date range scan + did + activity_types without heap lookups
CREATE INDEX IF NOT EXISTS idx_did_activity_daily_covering
  ON activity.did_activity_daily(date, did, activity_types);

-- Secondary index for retention queries that join on (did, date)
CREATE INDEX IF NOT EXISTS idx_did_activity_daily_did
  ON activity.did_activity_daily(did, date);

CREATE TABLE IF NOT EXISTS activity.delete_events_daily (
  date       TEXT    NOT NULL,
  event_type TEXT    NOT NULL,
  count      INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (date, event_type)
);

CREATE TABLE IF NOT EXISTS activity.jetstream_cursor (
  id         INTEGER PRIMARY KEY CHECK (id = 1),
  cursor     BIGINT      NOT NULL, -- Unix microseconds (Jetstream time_us)
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS activity.starterpack_joins_daily (
  starterpack_uri TEXT    NOT NULL,
  date            TEXT    NOT NULL, -- YYYY-MM-DD
  count           INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (starterpack_uri, date)
);

CREATE INDEX IF NOT EXISTS idx_starterpack_joins_daily_date
  ON activity.starterpack_joins_daily(date);

CREATE TABLE IF NOT EXISTS activity.did_langs (
  did        TEXT    NOT NULL,
  lang       TEXT    NOT NULL,
  post_count INTEGER NOT NULL DEFAULT 0,
  last_seen  TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (did, lang)
);

CREATE INDEX IF NOT EXISTS idx_did_langs_lang ON activity.did_langs(lang);

CREATE TABLE IF NOT EXISTS activity.lang_stats (
  date         TEXT        NOT NULL PRIMARY KEY, -- YYYY-MM-DD
  total_posts  INTEGER     NOT NULL DEFAULT 0,
  tagged_posts INTEGER     NOT NULL DEFAULT 0,
  updated_at   TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS activity.collection_activity (
  collection  TEXT    NOT NULL,
  did         TEXT    NOT NULL,
  date        TEXT    NOT NULL, -- YYYY-MM-DD
  event_count INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (collection, did, date)
);

CREATE INDEX IF NOT EXISTS idx_collection_activity_collection
  ON activity.collection_activity(collection);
CREATE INDEX IF NOT EXISTS idx_collection_activity_date
  ON activity.collection_activity(date);
-- Index enabling the cross-schema JOIN with plc.plc_did_pds on did
CREATE INDEX IF NOT EXISTS idx_collection_activity_did
  ON activity.collection_activity(did);

CREATE TABLE IF NOT EXISTS activity.pds_activity_summary (
  pds_url       TEXT        NOT NULL,
  window_days   INTEGER     NOT NULL DEFAULT 30,
  active_dids   INTEGER     NOT NULL,
  poster_dids   INTEGER     NOT NULL,
  liker_dids    INTEGER     NOT NULL,
  reposter_dids INTEGER     NOT NULL,
  follower_dids INTEGER     NOT NULL,
  updated_at    TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (pds_url, window_days)
);

CREATE TABLE IF NOT EXISTS activity.feed_generators (
  uri          TEXT        NOT NULL PRIMARY KEY,
  creator_did  TEXT        NOT NULL,
  display_name TEXT,
  description  TEXT,
  first_seen   TEXT        NOT NULL, -- YYYY-MM-DD
  deleted_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_feed_generators_creator
  ON activity.feed_generators(creator_did);

CREATE TABLE IF NOT EXISTS activity.feed_generator_likes_daily (
  feed_uri TEXT    NOT NULL,
  date     TEXT    NOT NULL, -- YYYY-MM-DD
  likes    INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (feed_uri, date)
);

CREATE INDEX IF NOT EXISTS idx_feed_generator_likes_date
  ON activity.feed_generator_likes_daily(date);

CREATE TABLE IF NOT EXISTS activity.score_dlq (
  id            BIGSERIAL   PRIMARY KEY,
  posts_json    TEXT        NOT NULL,
  failed_at     TIMESTAMPTZ NOT NULL,
  attempts      INTEGER     NOT NULL DEFAULT 0,
  last_error    TEXT,
  next_retry_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_score_dlq_retry
  ON activity.score_dlq(next_retry_at, attempts);
