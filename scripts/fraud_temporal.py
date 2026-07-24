#!/usr/bin/env python3
"""
Temporal-generalization test for the fraud detector. We have ONE graph snapshot
(2026-05-11), so strict 'features-before-label' future prediction isn't possible (most
labels precede the snapshot). Instead we test whether the fraud SIGNATURE is stable over
time: train on accounts skywatch labeled BEFORE a cutoff, test on accounts labeled AFTER.

  train positives: first_labeled <  CUTOFF
  test  positives: first_labeled >= CUTOFF   (disjoint accounts)
  negatives:       random 75/25 split

A test AUC close to the random-split 0.969 means the signature generalizes across labeling
waves (the detector isn't memorizing one wave). A large drop means it's wave-specific.

  CUTOFF=2026-01-01 python3 scripts/fraud_temporal.py
"""
import os, numpy as np, pandas as pd, psycopg2
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline
from sklearn.metrics import roc_auc_score, average_precision_score
np.seterr(divide="ignore", over="ignore", invalid="ignore")

DB = os.environ["DATABASE_URL"]
CUTOFF = os.environ.get("CUTOFF", "2026-01-01")
NEG_RATE = float(os.environ.get("NEG_RATE", "0.15"))

SKY = """('suspect-inauthentic','bulk-following','platform-manipulation','inauthentic-fundraising',
  'follow-farming','engagement-abuse','spam','amplifier',
  'repetitive-domain-spam-burst','repetitive-domain-spam-sustained')"""

COLS = """ff.is_fraud, ff.followers, ff.follows, ff.posts, ff.likes_out, ff.likes_in,
  ff.blocks_out, ff.blocks_in, ff.reposts_out, ff.reposts_in, ff.replies_out, ff.quotes_out,
  ff.quoted_count, ff.follow_ratio, ff.blocks_in_ratio, ff.reply_ratio, ff.repost_ratio,
  ff.like_per_post, ff.likes_in_per_post, ff.account_age_days, ff.is_bsky_pds, ff.creation_burst,
  ff.active_days, ff.recency_days, ff.follows_per_active_day, ff.post_deletes, ff.delete_rate"""

QUERY = f"""
SELECT {COLS},
  (SELECT MIN(s.labeled_at) FROM plc.skywatch_labels s
   WHERE s.did = ff.did AND s.label IN {SKY}) AS first_labeled
FROM analysis.fraud_features ff
WHERE ff.is_fraud OR random() < {NEG_RATE}
"""

with psycopg2.connect(DB) as conn:
    df = pd.read_sql(QUERY, conn)
df["first_labeled"] = pd.to_datetime(df["first_labeled"], utc=True)
cut = pd.Timestamp(CUTOFF, tz="UTC")

# now-anchored features (measured at snapshot/now, not at label time) confound the temporal
# test: set STABLE=1 to drop them and isolate the structural fraud signal.
DROP = {"account_age_days","recency_days","active_days","follows_per_active_day",
        "log_post_deletes","delete_rate"} if os.environ.get("STABLE")=="1" else set()

def feats(d):
    out = pd.DataFrame({
        "log_followers": np.log1p(d.followers), "log_follows": np.log1p(d.follows),
        "log_posts": np.log1p(d.posts), "log_likes_out": np.log1p(d.likes_out),
        "log_likes_in": np.log1p(d.likes_in), "log_blocks_in": np.log1p(d.blocks_in),
        "log_blocks_out": np.log1p(d.blocks_out), "log_reposts_out": np.log1p(d.reposts_out),
        "log_replies_out": np.log1p(d.replies_out), "log_quotes_out": np.log1p(d.quotes_out),
        "follow_ratio": np.log1p(d.follow_ratio), "blocks_in_ratio": np.log1p(d.blocks_in_ratio),
        "reply_ratio": d.reply_ratio.clip(0,50), "repost_ratio": d.repost_ratio,
        "like_per_post": np.log1p(d.like_per_post), "likes_in_per_post": np.log1p(d.likes_in_per_post),
        "account_age_days": d.account_age_days, "is_bsky_pds": d.is_bsky_pds.astype(float),
        "log_creation_burst": np.log1p(d.creation_burst), "active_days": d.active_days,
        "recency_days": d.recency_days.clip(0,9999),
        "follows_per_active_day": np.log1p(d.follows_per_active_day),
        "log_post_deletes": np.log1p(d.post_deletes), "delete_rate": d.delete_rate.clip(0,50),
    }).replace([np.inf,-np.inf], np.nan).fillna(0.0).astype("float64")
    return out.drop(columns=[c for c in DROP if c in out.columns]).values

neg = ~df.is_fraud.values
rng = np.random.default_rng(42)
neg_test = neg & (rng.random(len(df)) < 0.25)
neg_train = neg & ~neg_test
pos_early = df.is_fraud.values & (df.first_labeled < cut).values
pos_late  = df.is_fraud.values & (df.first_labeled >= cut).values

tr = neg_train | pos_early
te = neg_test  | pos_late
X = feats(df); y = df.is_fraud.astype(int).values
print(f"CUTOFF={CUTOFF}  pos_early(train)={pos_early.sum():,}  pos_late(test)={pos_late.sum():,}  "
      f"neg_train={neg_train.sum():,}  neg_test={neg_test.sum():,}")

models = {
    "logistic": make_pipeline(StandardScaler(),
        LogisticRegression(C=1.0, class_weight="balanced", max_iter=2000)),
    "hist_gbm": HistGradientBoostingClassifier(max_iter=300, learning_rate=0.08,
        max_leaf_nodes=31, l2_regularization=1.0, class_weight="balanced", random_state=42),
}
print(f"\n{'model':<10} | {'TEMPORAL AUC':>12} | {'TEMPORAL PR':>11}")
for name, m in models.items():
    m.fit(X[tr], y[tr])
    p = m.predict_proba(X[te])[:,1]
    print(f"{name:<10} | {roc_auc_score(y[te],p):>12.4f} | {average_precision_score(y[te],p):>11.4f}")
print("(compare to random-split GBM 0.969 / 0.398 from fraud_model.py)")
