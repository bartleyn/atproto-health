-- analysis.cohort_behavior — per-DID early-activity composition for every account that
-- has been ACTIVE in the jetstream window (these are exactly the accounts that show up in
-- retention numerators). Purpose: flag unlabeled, non-farm bots whose "activity" is
-- automated (follow/like-only) so they can be removed from retention denominators.
--
-- We only have per-day action BITMASKS (presence, not counts), so features are composition
-- + active-day shape — which is the strongest "looks active but isn't a real user" signal.
-- Bits: post=1 like=2 repost=4 follow=8 block=16 profile=2048 reply=8192 quote=16384.

DROP TABLE IF EXISTS analysis.cohort_behavior;

CREATE TABLE analysis.cohort_behavior AS
WITH agg AS (
  SELECT did,
         COUNT(*)::int           AS active_days,
         MIN(date)               AS first_active,
         MAX(date)               AS last_active,
         BIT_OR(activity_types)  AS mask,
         -- per-day "engagement-only" = day whose actions are ONLY like/repost/follow/block
         -- (no post/reply/quote/profile) → automated-looking day
         COUNT(*) FILTER (WHERE activity_types & ~(2|4|8|16) = 0)::int AS eng_only_days
  FROM activity.did_activity_daily
  GROUP BY did
)
SELECT
  a.did,
  a.active_days,
  a.first_active, a.last_active,
  (a.last_active::date - a.first_active::date)::int AS span_days,
  -- composition flags (presence over the account's whole active life)
  (a.mask & 1)     <> 0 AS did_post,
  (a.mask & 2)     <> 0 AS did_like,
  (a.mask & 4)     <> 0 AS did_repost,
  (a.mask & 8)     <> 0 AS did_follow,
  (a.mask & 16)    <> 0 AS did_block,
  (a.mask & 2048)  <> 0 AS did_profile,
  (a.mask & 8192)  <> 0 AS did_reply,
  (a.mask & 16384) <> 0 AS did_quote,
  -- human-content signal: ever authored original/reply/quote text
  ((a.mask & (1|8192|16384)) <> 0) AS did_human_content,
  -- count of distinct "human" vs "automatable" action types
  ( ((a.mask&1)<>0)::int + ((a.mask&8192)<>0)::int + ((a.mask&16384)<>0)::int
    + ((a.mask&2048)<>0)::int ) AS n_human_actions,
  ( ((a.mask&2)<>0)::int + ((a.mask&4)<>0)::int + ((a.mask&8)<>0)::int ) AS n_auto_actions,
  a.eng_only_days,
  (a.eng_only_days::float8 / a.active_days) AS eng_only_frac,
  -- tags for training / scoring
  (cb.did IS NOT NULL)                       AS in_cohort,
  cb.created_at,
  (ex.did IS NOT NULL)                       AS is_farm,
  (sky.did IS NOT NULL)                      AS is_skywatch_spam
FROM agg a
LEFT JOIN analysis.cohort_base cb  ON cb.did = a.did
LEFT JOIN analysis.excluded_dids ex ON ex.did = a.did
LEFT JOIN (
  SELECT DISTINCT did FROM plc.skywatch_labels WHERE label IN (
    'suspect-inauthentic','bulk-following','platform-manipulation','inauthentic-fundraising',
    'follow-farming','engagement-abuse','spam','amplifier',
    'repetitive-domain-spam-burst','repetitive-domain-spam-sustained')
) sky ON sky.did = a.did;

CREATE INDEX idx_cohort_behavior_cohort ON analysis.cohort_behavior(in_cohort);
CREATE INDEX idx_cohort_behavior_created ON analysis.cohort_behavior(created_at);
ANALYZE analysis.cohort_behavior;

SELECT COUNT(*) AS active_dids,
       COUNT(*) FILTER (WHERE in_cohort) AS in_cohort,
       COUNT(*) FILTER (WHERE is_farm) AS farms,
       COUNT(*) FILTER (WHERE is_skywatch_spam) AS skywatch_spam,
       COUNT(*) FILTER (WHERE NOT did_human_content) AS no_human_content
FROM analysis.cohort_behavior;
