-- Schema: activity (feed generators)
-- Run against: atproto_health database
--
-- Motivation: a SkyFeed feed's app.bsky.feed.generator record carries its whole recipe in
-- `skyfeedBuilder` (inputs: firehose / accounts / lists / tags, regex filters, sort). That says
-- how the feed is built far better than its name: on a 497-feed sample (2026-10-25) types
-- guessed from names matched the recipe 58% of the time. SkyFeed hosts ~89k of ~150k live feeds.
--
-- jetstream-activity.ts captures it from every feed generator create AND update (edits used to
-- be dropped); scripts/backfill_skyfeed_builders.py fills existing rows. JSON null = the record
-- was read and has no recipe; SQL NULL = not read yet.

ALTER TABLE activity.feed_generators ADD COLUMN IF NOT EXISTS skyfeed_builder JSONB;
