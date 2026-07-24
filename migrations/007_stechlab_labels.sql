-- stechlab-labels.bsky.social — behavioral/activity labeler (did:plc:oubsyca6hhgqhmbbk27lvs7c).
-- 25 "inform"-severity activity labels (post/reply volume, 24/7 posting gaps, follow/unfollow churn,
-- profile-metadata churn, same-URL spam). Collected as ACTIVITY PROXY FEATURES, not fraud positives.
-- Same shape as plc.skywatch_labels / plc.bsky_mod_labels: PK (did,label) keeps latest labeled_at.

CREATE TABLE IF NOT EXISTS plc.stechlab_labels (
  did        text NOT NULL,
  label      text NOT NULL,
  labeled_at timestamptz NOT NULL,
  PRIMARY KEY (did, label)
);
CREATE INDEX IF NOT EXISTS idx_stechlab_labels_label ON plc.stechlab_labels (label);

CREATE TABLE IF NOT EXISTS plc.stechlab_labels_cursor (
  id         integer NOT NULL PRIMARY KEY CHECK (id = 1),
  cursor     text NOT NULL,
  updated_at timestamptz NOT NULL
);
