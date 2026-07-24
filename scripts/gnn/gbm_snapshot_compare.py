#!/usr/bin/env python3
"""
GOLD-STANDARD fair GBM-vs-GNN comparison: run the tabular model on the SAME 07-07 snapshot features
and the IDENTICAL leak-free split the GNN used, so nothing differs except structure-vs-per-node.

Unlike fraud_strict.py (which pulls analysis.fraud_features from CURRENT Postgres -> features measured
AFTER the post-cutoff labels = time-leakage), this reads node_feats.parquet (frozen 07-07 aggregates,
exactly what the GNN saw) + labels.parquet, and replicates build_pyg_data's masks bit-for-bit (same
seed 42, same boundary) so the TEST set is the identical 1,029 positives -> directly comparable to the
GNN's reported TEST ROC.

Two feature regimes, both 07-07-aligned (no leakage):
  - degree : log1p(followers), log1p(follows)               <- matches the GNN's inputs -> isolates
                                                                the value of message passing itself
  - full   : all 10 raw aggregates + a few snapshot-derived ratios <- best per-node tabular

  python scripts/gnn/gbm_snapshot_compare.py --dir /Volumes/miniext/gnn_export --cutoff 2026-07-07
"""
import argparse, numpy as np, pandas as pd, pyarrow.parquet as pq
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline
from sklearn.metrics import roc_auc_score, average_precision_score

ap = argparse.ArgumentParser()
ap.add_argument("--dir", default="/Volumes/miniext/gnn_export")
ap.add_argument("--cutoff", default="2026-07-07")
ap.add_argument("--train-neg-cap", type=int, default=2_000_000, help="subsample train negs for speed")
ap.add_argument("--seed", type=int, default=42)
args = ap.parse_args()

# --- load snapshot features + labels, replicate build_pyg_data's node order + masks EXACTLY ---------
nf = pq.read_table(f"{args.dir}/node_feats.parquet").to_pandas()
idx_col = "sidx" if "sidx" in nf.columns else "node_idx"
nf = nf.sort_values(idx_col).reset_index(drop=True)          # row order == node_idx (same as GNN)
N = len(nf)

lab = pq.read_table(f"{args.dir}/labels.parquet").to_pandas().drop_duplicates("did").set_index("did")
is_fraud = nf["did"].map(lab["is_fraud"]).fillna(False).to_numpy(bool)
is_excl  = nf["did"].map(lab["is_excluded"]).fillna(False).to_numpy(bool)
first_lab = pd.to_datetime(nf["did"].map(lab["first_labeled"]), utc=True)

# --- masks: identical to build_pyg_data.py (seed 42, >= cutoff, neg 70/15/15) ----------------------
pop = ~is_excl
cut = pd.Timestamp(args.cutoff, tz="UTC")
late = (first_lab >= cut).to_numpy()
rng = np.random.default_rng(seed=args.seed)
r = rng.random(N)
pos, neg = is_fraud & pop, (~is_fraud) & pop
pos_test = pos & late
pos_train = pos & ~late
train_neg = neg & (r < 0.70)
test_mask = pos_test | (neg & (r >= 0.85))                    # SAME test set as the GNN

# subsample train negatives for speed (GBM w/ class_weight handles imbalance; test set untouched)
tn_idx = np.where(train_neg)[0]
if len(tn_idx) > args.train_neg_cap:
    tn_idx = rng.choice(tn_idx, args.train_neg_cap, replace=False)
train_idx = np.concatenate([np.where(pos_train)[0], tn_idx])
rng.shuffle(train_idx)
te_idx = np.where(test_mask)[0]
y = is_fraud.astype(int)
print(f"cutoff={args.cutoff}  N={N:,}  excl={int(is_excl.sum()):,}  "
      f"train_pos={int(pos_train.sum()):,} train_neg={len(tn_idx):,}  "
      f"TEST pos={int(pos_test.sum()):,} neg={int(test_mask.sum()-pos_test.sum()):,}")

# --- feature regimes (all from the 07-07 aggregates -> no time leakage) -----------------------------
f = nf  # columns: followers, follows, posts, likes_out, likes_in, blocks_in, blocks_out, reposts_out, replies_out, quotes_out
eps = 1.0
def build(regime):
    if regime == "degree":
        cols = {"log_followers": np.log1p(f.followers), "log_follows": np.log1p(f.follows)}
    else:
        cols = {
            "log_followers": np.log1p(f.followers), "log_follows": np.log1p(f.follows),
            "log_posts": np.log1p(f.posts), "log_likes_out": np.log1p(f.likes_out),
            "log_likes_in": np.log1p(f.likes_in), "log_blocks_in": np.log1p(f.blocks_in),
            "log_blocks_out": np.log1p(f.blocks_out), "log_reposts_out": np.log1p(f.reposts_out),
            "log_replies_out": np.log1p(f.replies_out), "log_quotes_out": np.log1p(f.quotes_out),
            "follow_ratio": np.log1p(f.follows / (f.followers + eps)),
            "blocks_in_ratio": np.log1p(f.blocks_in / (f.followers + eps)),
            "like_per_post": np.log1p(f.likes_out / (f.posts + eps)),
            "likes_in_per_post": np.log1p(f.likes_in / (f.posts + eps)),
        }
    return pd.DataFrame(cols).replace([np.inf, -np.inf], np.nan).fillna(0.0).to_numpy("float64")

print(f"\n{'regime':<7} {'model':<9} | {'TEST ROC':>9} | {'TEST PR':>9}")
for regime in ("degree", "full"):
    X = build(regime)
    models = {
        "logistic": make_pipeline(StandardScaler(),
            LogisticRegression(C=1.0, class_weight="balanced", max_iter=2000)),
        "hist_gbm": HistGradientBoostingClassifier(max_iter=300, learning_rate=0.08,
            max_leaf_nodes=31, l2_regularization=1.0, class_weight="balanced", random_state=args.seed),
    }
    for name, m in models.items():
        m.fit(X[train_idx], y[train_idx])
        p = m.predict_proba(X[te_idx])[:, 1]
        print(f"{regime:<7} {name:<9} | {roc_auc_score(y[te_idx], p):>9.4f} | "
              f"{average_precision_score(y[te_idx], p):>9.4f}")
print("\n(compare TEST ROC to the GNN structure-only baseline 0.7846 on the identical split)")
