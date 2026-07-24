#!/usr/bin/env python3
"""
FAST, clustering-free embedding evaluation — the standard SSL recipe (linear probe + nearest-class-
mean), so you can iterate on a representation without paying validate_discovery's KMeans(k=20000) +
1.43B-edge streaming every time. Seconds-to-minutes, not hours.

Two leak-free (strict `> cutoff`) numbers, both on the SAVED embeddings only (no edges, no big KMeans):
  * PROBE   — logistic on the embedding (train labels) -> ROC/PR on future fraud. "Does it separate
              fraud linearly?"  (== experiment #1 for the embedding alone)
  * NCM/kNN — score each candidate by cosine to the nearest KNOWN-fraud centroid -> ROC/PR on future.
              "Does the embedding place (future) fraud near (known) fraud?" — the fast discovery proxy.

  python eval_embeddings.py --emb sage_unsup_emb.npy --dir /data --labels /data/labels.parquet \
      --cutoff 2026-07-07 [--device cuda]
"""
import argparse, json, numpy as np, torch
import pyarrow.parquet as pq
from sklearn.cluster import MiniBatchKMeans
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, average_precision_score

ap = argparse.ArgumentParser()
ap.add_argument("--emb", required=True)
ap.add_argument("--dir", required=True)
ap.add_argument("--labels", required=True)
ap.add_argument("--cutoff", default="2026-07-07")
ap.add_argument("--device", default="cuda" if torch.cuda.is_available() else "cpu")
ap.add_argument("--n-centroids", type=int, default=256)
ap.add_argument("--probe-neg-cap", type=int, default=2_000_000)
ap.add_argument("--out", default="emb_eval.json")
args = ap.parse_args()

emb = np.ascontiguousarray(np.load(args.emb)).astype(np.float32)
N = emb.shape[0]
nf = pq.read_table(f"{args.dir}/node_feats.parquet").to_pandas()
nf = nf.sort_values("sidx" if "sidx" in nf.columns else "node_idx").reset_index(drop=True)
import pandas as pd
lab = pq.read_table(args.labels).to_pandas().drop_duplicates("did").set_index("did")
fl = pd.to_datetime(nf["did"].map(lab["first_labeled"]), utc=True)
T = pd.Timestamp(args.cutoff, tz="UTC")
is_fraud = nf["did"].map(lab["is_fraud"]).fillna(False).to_numpy(bool)
excl = nf["did"].map(lab["is_excluded"]).fillna(False).to_numpy(bool)
known = is_fraud & ~excl & (fl <= T).to_numpy()
future = is_fraud & ~excl & (fl > T).to_numpy()
cand = ~excl & ~known
print(f"N={N:,} known={known.sum():,} future={future.sum():,} excluded={excl.sum():,}")

# --- linear PROBE (classification) ------------------------------------------------------------
tr = ~excl & ~future                            # train on everything known at T (incl. known fraud)
tr_idx = np.where(tr)[0]
y = is_fraud.astype(int)
neg = tr_idx[y[tr_idx] == 0]
rng = np.random.default_rng(42)
if len(neg) > args.probe_neg_cap:
    neg = rng.choice(neg, args.probe_neg_cap, replace=False)
fit_idx = np.concatenate([tr_idx[known[tr_idx]], neg])
probe = LogisticRegression(C=1.0, class_weight="balanced", max_iter=2000).fit(emb[fit_idx], y[fit_idx])
ps = probe.predict_proba(emb[cand])[:, 1]
probe_roc = roc_auc_score(future[cand], ps); probe_pr = average_precision_score(future[cand], ps)

# --- nearest-known-fraud-centroid (discovery proxy), cosine, batched on device ----------------
dev = torch.device(args.device)
E = torch.from_numpy(emb)
E = torch.nn.functional.normalize(E, dim=1)                     # cosine space
kf = E[known].to(dev)
km = MiniBatchKMeans(n_clusters=min(args.n_centroids, int(known.sum())), n_init=1,
                     batch_size=10000, random_state=42).fit(kf.cpu().numpy())
C = torch.nn.functional.normalize(torch.from_numpy(km.cluster_centers_).float(), dim=1).to(dev)
score = torch.empty(N)
for i in range(0, N, 1_000_000):
    chunk = E[i:i + 1_000_000].to(dev)
    score[i:i + chunk.shape[0]] = (chunk @ C.T).max(dim=1).values.cpu()   # max cos to any known-fraud centroid
score = score.numpy()
ncm_roc = roc_auc_score(future[cand], score[cand]); ncm_pr = average_precision_score(future[cand], score[cand])

base = future[cand].mean()
print(f"\ncutoff={args.cutoff}  future base rate {base:.4%}")
print(f"PROBE (logistic)          leak-free TEST: ROC {probe_roc:.4f} | PR {probe_pr:.4f}")
print(f"NCM (cos to known-fraud)  leak-free TEST: ROC {ncm_roc:.4f} | PR {ncm_pr:.4f}")
print("(compare supervised follows-only 0.7846 / GBM-full 0.8954)")
json.dump({"cutoff": args.cutoff, "future_base_rate": float(base),
           "probe": {"roc": float(probe_roc), "pr": float(probe_pr)},
           "ncm": {"roc": float(ncm_roc), "pr": float(ncm_pr)}}, open(args.out, "w"), indent=2)
print("wrote", args.out)
