-- Schema: plc (Bluesky moderation actions on accounts)
-- Run against: atproto_health database
--
-- mod.bsky.app account labels !takedown / !suspend / !hide, CURRENT state only:
-- a negation deletes the row, so no row = not actioned. !suspend carries expires_at;
-- treat a row as active while expires_at IS NULL OR expires_at > now().
-- plc.bsky_mod_labels stays "ever flagged" (spam, impersonation, ...) for model labels.
-- Maintained by src/lib/collectors/skywatch-labels.ts (npm run collect:bsky-mod).

CREATE TABLE IF NOT EXISTS plc.bsky_mod_actions (
  did        TEXT        NOT NULL,
  action     TEXT        NOT NULL,
  labeled_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ,
  PRIMARY KEY (did, action)
);
