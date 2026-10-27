-- bottags.bsky.social — gracekind's report-driven labeler (did:plc:cdkysjo2taj2jpy3qoostthu).
-- Account labels only: undisclosed-bot (automated, no 'automated' label) and undisclosed-human
-- (human-run, has it). Current state: the collector deletes a row when the label is negated.
-- Same shape as plc.stechlab_labels.

CREATE TABLE IF NOT EXISTS plc.bottags_labels (
  did        text NOT NULL,
  label      text NOT NULL,
  labeled_at timestamptz NOT NULL,
  PRIMARY KEY (did, label)
);

CREATE TABLE IF NOT EXISTS plc.bottags_labels_cursor (
  id         integer NOT NULL PRIMARY KEY CHECK (id = 1),
  cursor     text NOT NULL,
  updated_at timestamptz NOT NULL
);
