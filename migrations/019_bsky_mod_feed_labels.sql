-- Bluesky moderation labels on feed generators (mod.bsky.app, did:plc:ar7c4by46qjdydhdevvrndac):
-- porn / sexual / nudity / graphic-media and actions like !takedown applied to a feed's at:// URI.
-- Current state: the bsky-mod collector upserts a label and deletes the row when it is negated.
-- Backfilled with: npm run collect:bsky-mod -- --subjects --feeds

CREATE TABLE IF NOT EXISTS plc.bsky_mod_feed_labels (
  uri        text NOT NULL,
  label      text NOT NULL,
  labeled_at timestamptz NOT NULL,
  expires_at timestamptz,
  PRIMARY KEY (uri, label)
);
