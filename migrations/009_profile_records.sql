-- Schema: activity — profile record capture (app.bsky.actor.profile)
-- Run against: atproto_health database

CREATE TABLE IF NOT EXISTS activity.profile_records (
  id                         BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  did                        TEXT        NOT NULL,
  observed_at                TIMESTAMPTZ NOT NULL,           -- when WE saw it
  operation                  TEXT        NOT NULL,           -- create | update | backfill
  source                     TEXT        NOT NULL DEFAULT 'jetstream',
  display_name               TEXT,
  description                TEXT,
  avatar_cid                 TEXT,        -- blob ref $link — content hash of the image
  banner_cid                 TEXT,
  joined_via_starterpack_uri TEXT,
  pinned_post_uri            TEXT,
  self_labels                TEXT[],      -- self-applied labels (e.g. !no-unauthenticated)
  record_created_at          TIMESTAMPTZ, -- record.createdAt when the client sets it
  -- md5 over the identity-bearing fields. Lets consumers collapse consecutive identical
  -- versions without re-comparing every column, and lets the collector skip no-op rewrites.
  content_hash               TEXT        NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_profile_records_did_observed
  ON activity.profile_records(did, observed_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_profile_records_avatar_cid
  ON activity.profile_records(avatar_cid) WHERE avatar_cid IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_profile_records_banner_cid
  ON activity.profile_records(banner_cid) WHERE banner_cid IS NOT NULL;

-- Display-name collisions / templated names.
CREATE INDEX IF NOT EXISTS idx_profile_records_display_name
  ON activity.profile_records(display_name) WHERE display_name IS NOT NULL;

-- Backfill progress scans ("which DIDs still need fetching").
CREATE INDEX IF NOT EXISTS idx_profile_records_source
  ON activity.profile_records(source);

CREATE OR REPLACE VIEW activity.profile_latest AS
  SELECT DISTINCT ON (did) *
  FROM activity.profile_records
  ORDER BY did, observed_at DESC, id DESC;
