-- Schema: activity (feed generators)
-- Run against: atproto_health database
--
-- Motivation: app.bsky.feed.generator's record carries a `did` field naming the SERVICE
-- that actually serves the feed's algorithm (a builder platform like SkyFeed, or a
-- bespoke self-hosted generator) — distinct from creator_did, the owning account, which
-- is never shared across feeds. That serving DID is present on every create event the
-- jetstream collector already sees, but jetstream-activity.ts dropped it on the floor
-- (only captured displayName/description/creatorDid). This column is the destination for
-- the collector fix that now captures it going forward.
--
-- Historical rows (collected before this column existed) are NULL here — the raw create
-- event is gone from the firehose, so backfilling those needs a separate batched
-- app.bsky.feed.getFeedGenerators fetch, not this migration.

ALTER TABLE activity.feed_generators ADD COLUMN IF NOT EXISTS service_did TEXT;
