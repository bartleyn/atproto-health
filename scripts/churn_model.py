#!/usr/bin/env python3
"""
Short-horizon churn model on analysis.churn_features — logistic regression vs gradient
boosting, with an optional TEMPORAL hold-out to gauge how optimistic the random-split
metrics are.

Target: churned == 1 (no activity in [T, T+28)). Population: active before T, farms
excluded, graph-covered only. See churn-features.ts for the table build.

Sampling: stratified 20% per creation cohort, done IN SQL (Python receives ~1M rows,
peak RAM < ~1 GB). FULL=1 trains on all ~5M graph-covered rows.

Validation:
  - Default (single table): random 75/25 split — IN-PERIOD metrics (optimistic).
  - TEMPORAL: set TRAIN_TABLE to an earlier snapshot and TEST_TABLE to a later one, e.g.
        TRAIN_TABLE=analysis.churn_features_20260508 TEST_TABLE=analysis.churn_features
    The model is fit on the earlier T1 and scored on the later T2 (genuinely out-of-period),
    so the in-period-vs-temporal AUC gap quantifies the optimism.

Run:  python3 scripts/churn_model.py
Needs: pandas, scikit-learn, psycopg2-binary; DATABASE_URL in env.
"""
import os
import sys
import numpy as np
import pandas as pd
import psycopg2
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, average_precision_score

# NumPy's Accelerate (macOS) BLAS backend emits spurious matmul RuntimeWarnings inside
# sklearn's solver; they do not affect results.
np.seterr(divide="ignore", over="ignore", invalid="ignore")

DB = os.environ.get("DATABASE_URL")
if not DB:
    sys.exit("DATABASE_URL not set (run with the repo .env loaded)")
FULL = os.environ.get("FULL") == "1"
RATE = 1.0 if FULL else 0.2
TRAIN_TABLE = os.environ.get("TRAIN_TABLE", "analysis.churn_features")
TEST_TABLE = os.environ.get("TEST_TABLE", TRAIN_TABLE)
TEMPORAL = TEST_TABLE != TRAIN_TABLE

COLS = """cf.active_days, cf.recency_days, cf.active_14d, cf.active_h2, cf.active_h1,
  cf.posted_days, cf.liked_days, cf.reposted_days, cf.followed_days, cf.replied_days, cf.quoted_days,
  cf.account_age_days, cf.is_bsky_pds, cf.migrated_ever,
  cf.followers, cf.follows, cf.graph_posts, cf.likes_out, cf.likes_in,
  cf.reposts_out, cf.replies_out, cf.quotes_out, cf.blocks_out, cf.n_langs, cf.churned"""

COHORT = """CASE
      WHEN cb.created_at < '2023-01-01' THEN '1_pre2023'
      WHEN cb.created_at < '2024-01-01' THEN '2_2023'
      WHEN cb.created_at < '2024-11-01' THEN '3_2024_preNov'
      WHEN cb.created_at < '2025-01-01' THEN '4_2024_exodus'
      WHEN cb.created_at < '2025-07-01' THEN '5_2025H1'
      WHEN cb.created_at < '2026-01-01' THEN '6_2025H2'
      WHEN cb.created_at < '2026-04-01' THEN '7_2026Q1'
      ELSE '8_2026Q2' END"""

FEATURE_NAMES = None  # set on first featurize


def load(table, conn):
    base = f"""
      SELECT {COLS}, {COHORT} AS cohort
      FROM {table} cf JOIN analysis.cohort_base cb ON cb.did = cf.did
      WHERE cf.followers IS NOT NULL
    """
    if FULL:
        query = base
    else:
        query = f"""
        SELECT * FROM (
          SELECT b.*,
                 row_number() OVER (PARTITION BY cohort ORDER BY random()) AS _rn,
                 count(*)     OVER (PARTITION BY cohort)                   AS _n
          FROM ({base}) b
        ) s WHERE _rn <= _n * {RATE}
        """
    df = pd.read_sql(query, conn)
    return df.drop(columns=[c for c in ("_rn", "_n") if c in df.columns])


def featurize(df):
    global FEATURE_NAMES
    ad = df["active_days"].clip(lower=1)
    feat = pd.DataFrame({
        "active_days":      df["active_days"],
        "recency_days":     df["recency_days"],
        "active_14d":       df["active_14d"],
        "activity_trend":   df["active_h2"] / (df["active_h1"] + df["active_h2"]).clip(lower=1),
        "post_rate":        df["posted_days"]   / ad,
        "like_rate":        df["liked_days"]    / ad,
        "repost_rate":      df["reposted_days"] / ad,
        "follow_rate":      df["followed_days"] / ad,
        "reply_rate":       df["replied_days"]  / ad,
        "quote_rate":       df["quoted_days"]   / ad,
        "action_diversity": (df[["posted_days","liked_days","reposted_days",
                                 "followed_days","replied_days","quoted_days"]] > 0).sum(axis=1),
        "account_age_days": df["account_age_days"],
        "is_bsky_pds":      df["is_bsky_pds"],
        "migrated_ever":    df["migrated_ever"],
        "log_followers":    np.log1p(df["followers"]),
        "log_follows":      np.log1p(df["follows"]),
        "log_graph_posts":  np.log1p(df["graph_posts"]),
        "log_likes_out":    np.log1p(df["likes_out"]),
        "log_likes_in":     np.log1p(df["likes_in"]),
        "log_reposts_out":  np.log1p(df["reposts_out"]),
        "log_replies_out":  np.log1p(df["replies_out"]),
        "log_quotes_out":   np.log1p(df["quotes_out"]),
        "log_blocks_out":   np.log1p(df["blocks_out"]),
        "followers_per_follow": np.log1p(df["followers"]) - np.log1p(df["follows"]),
        "likes_in_per_post":    np.log1p(df["likes_in"]) - np.log1p(df["graph_posts"]),
        "n_langs":          df["n_langs"],
    }).replace([np.inf, -np.inf], np.nan).fillna(0.0).astype("float64")
    FEATURE_NAMES = list(feat.columns)
    return feat.values, df["churned"].values


print(f"TRAIN={TRAIN_TABLE}  TEST={TEST_TABLE}  ({'TEMPORAL out-of-period' if TEMPORAL else 'random split, in-period only'})")
with psycopg2.connect(DB) as conn:
    train_df = load(TRAIN_TABLE, conn)
    print(f"  train rows: {len(train_df):,}  churn {train_df['churned'].mean():.1%}")
    print("  train cohort mix:",
          {k: f"{v:.0%}" for k, v in train_df.groupby('cohort')['churned'].mean().items()})
    test_df = load(TEST_TABLE, conn) if TEMPORAL else None

Xtr_all, ytr_all = featurize(train_df); del train_df
X_tr, X_ip, y_tr, y_ip = train_test_split(Xtr_all, ytr_all, test_size=0.25,
                                          random_state=42, stratify=ytr_all)
if TEMPORAL:
    X_oop, y_oop = featurize(test_df); del test_df
    print(f"  temporal test rows: {len(y_oop):,}  churn {y_oop.mean():.1%}")

models = {
    "logistic":  make_pipeline(StandardScaler(),
                               LogisticRegression(penalty="l2", C=1.0,
                                                  class_weight="balanced", max_iter=2000)),
    "hist_gbm":  HistGradientBoostingClassifier(max_iter=300, learning_rate=0.08,
                                                max_leaf_nodes=31, l2_regularization=1.0,
                                                class_weight="balanced", random_state=42),
}

print("\n=== AUC / PR-AUC ===")
hdr = f"{'model':<10} | {'in-period AUC':>13} | {'in-period PR':>12}"
if TEMPORAL:
    hdr += f" | {'TEMPORAL AUC':>12} | {'TEMPORAL PR':>11} | {'AUC drop':>8}"
print(hdr)
fitted = {}
for name, m in models.items():
    m.fit(X_tr, y_tr)
    fitted[name] = m
    pip = m.predict_proba(X_ip)[:, 1]
    row = f"{name:<10} | {roc_auc_score(y_ip, pip):>13.4f} | {average_precision_score(y_ip, pip):>12.4f}"
    if TEMPORAL:
        pop = m.predict_proba(X_oop)[:, 1]
        a_ip, a_oop = roc_auc_score(y_ip, pip), roc_auc_score(y_oop, pop)
        row += (f" | {a_oop:>12.4f} | {average_precision_score(y_oop, pop):>11.4f}"
                f" | {a_ip - a_oop:>+8.4f}")
    print(row)

# logistic coefficients (interpretable effect sizes)
lr = fitted["logistic"].named_steps["logisticregression"]
coef = lr.coef_[0]
order = np.argsort(-np.abs(coef))
print("\n=== Logistic standardized coefficients (+ => more likely to CHURN) ===")
for i in order:
    print(f"  {FEATURE_NAMES[i]:<22} {coef[i]:+.3f}")
