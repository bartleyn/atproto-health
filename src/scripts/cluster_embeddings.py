#!/usr/bin/env python3
"""
M4 discovery: cluster the GNN node embeddings to surface candidate coordinated rings, then rank
clusters by internal follow-edge density x fraud-label enrichment. Emits a small web-ready JSON
(top clusters + sample member DIDs) — NOT the 12.6GB embeddings.

Runs on Modal (CPU + big RAM) reading the gnn-data Volume; see modal_train.py `cluster`.

  python cluster_embeddings.py --emb /data/sage_degree_emb.npy --dir /data \
      --labels /data/labels.parquet --out /data/fraud_clusters.json --k 20000 --top 120
"""
import os
os.environ.setdefault("KMP_DUPLICATE_LIB_OK", "TRUE")   # faiss + torch each ship libomp (macOS clash)
try:                     # import faiss BEFORE torch: on macOS torch-then-faiss segfaults (libomp)
    import faiss
    faiss.omp_set_num_threads(os.cpu_count() or 8)
except ImportError:
    faiss = None
import argparse, json, datetime
import numpy as np
import pyarrow.parquet as pq


def cluster_embeddings(emb, k, seed=42):
    """Multi-threaded k-means. faiss uses all cores + subsamples training points (fast at 25M x 128);
    falls back to sklearn MiniBatchKMeans (largely single-threaded) if faiss isn't installed."""
    if faiss is not None:
        km = faiss.Kmeans(emb.shape[1], k, niter=25, seed=seed, verbose=True)
        km.train(emb)
        return km.index.search(emb, 1)[1].ravel().astype(np.int64)
    from sklearn.cluster import MiniBatchKMeans
    km = MiniBatchKMeans(n_clusters=k, batch_size=100_000, n_init=1, max_iter=50,
                         max_no_improvement=20, random_state=seed)
    return km.fit_predict(emb).astype(np.int64)

ap = argparse.ArgumentParser()
ap.add_argument("--emb", required=True)                       # sage_*_emb.npy (N x hidden)
ap.add_argument("--weights", required=True)                   # sage_*.pt (for the classifier head)
ap.add_argument("--dir", required=True)                       # holds node_feats/edges parquet
ap.add_argument("--labels", required=True)
ap.add_argument("--out", required=True)
ap.add_argument("--k", type=int, default=4000)                # # clusters (avg ring-sized groups)
ap.add_argument("--top", type=int, default=120)               # clusters to emit
ap.add_argument("--samples", type=int, default=25)            # member DIDs per cluster
args = ap.parse_args()

# --- 1) cluster the embeddings -------------------------------------------------------------------
# Load FULLY into RAM (~12.6GB on the 64GB box). DO NOT mmap: the .npy lives on a network-backed
# Modal Volume, and MiniBatchKMeans does random row reads per minibatch -> mmap = pathological
# network I/O (the 2h-timeout bug). Sequential load is fast; in-RAM KMeans is then compute-bound.
emb = np.ascontiguousarray(np.load(args.emb)).astype(np.float32)
N = emb.shape[0]
print(f"clustering {N:,} x {emb.shape[1]} embeddings into k={args.k} ...")
assign = cluster_embeddings(emb, args.k)                      # cluster id per node_idx (multi-threaded)
# per-node model fraud score: apply the trained classifier head to the embeddings (one matmul).
# NB exploratory — a probability the model assigns, NOT a determination; the model can be wrong.
import torch
sd = torch.load(args.weights, map_location="cpu", weights_only=True)
W = sd["head.weight"].numpy().reshape(-1).astype(np.float32); b = float(sd["head.bias"].item())
node_score = 1.0 / (1.0 + np.exp(-(emb.astype(np.float32) @ W + b)))
del emb

# --- 2) labels aligned to node_idx (node_feats row order == node_idx; join labels on did) -------
nf = pq.read_table(f"{args.dir}/node_feats.parquet").to_pandas()
idx_col = "sidx" if "sidx" in nf.columns else "node_idx"
nf = nf.sort_values(idx_col).reset_index(drop=True)
dids = nf["did"].to_numpy()
lab = pq.read_table(args.labels).to_pandas().drop_duplicates("did").set_index("did")
is_fraud = nf["did"].map(lab["is_fraud"]).fillna(False).to_numpy(bool)
is_excl  = nf["did"].map(lab["is_excluded"]).fillna(False).to_numpy(bool)
base_rate = float(is_fraud[~is_excl].mean())

# --- 3) intra-cluster follow-edge density: one streaming pass over edges -------------------------
intra = np.zeros(args.k, dtype=np.int64)
pf = pq.ParquetFile(f"{args.dir}/edges.parquet")
for batch in pf.iter_batches(columns=["src", "dst"], batch_size=20_000_000):
    src = batch.column("src").to_numpy(); dst = batch.column("dst").to_numpy()
    cs = assign[src]; same = cs == assign[dst]
    intra += np.bincount(cs[same], minlength=args.k).astype(np.int64)   # C-level, ~vs slow np.add.at

# --- 4) per-cluster stats + ranking -------------------------------------------------------------
size   = np.bincount(assign, minlength=args.k)
nf_cnt = np.bincount(assign[is_fraud & ~is_excl], minlength=args.k)   # fraud (non-farm) per cluster
density = intra / np.maximum(size, 1)                          # avg intra-cluster follow degree
frate   = nf_cnt / np.maximum(size, 1)
enrich  = frate / max(base_rate, 1e-9)
# candidate rings = dense AND fraud-enriched (their unlabeled members = net-new suspects)
score = enrich * np.log1p(density)
score[size < 8] = 0                                           # ignore tiny clusters
order = np.argsort(-score)[:args.top]

clusters = []
for cid in order:
    members = np.where(assign == cid)[0]
    fr = is_fraud[members] & ~is_excl[members]
    unl = members[~fr]; lbl = members[fr]
    # candidates = unlabeled members the MODEL scores highest (exploratory, can be wrong);
    # anchors = a few already-labeled members for context.
    unl_top = unl[np.argsort(-node_score[unl])][: max(args.samples - 5, 0)]
    lbl_top = lbl[np.argsort(-node_score[lbl])][:5]
    samples = (
        [{"did": str(dids[m]), "labeled": False, "score": round(float(node_score[m]), 3)} for m in unl_top]
        + [{"did": str(dids[m]), "labeled": True, "score": round(float(node_score[m]), 3)} for m in lbl_top]
    )
    clusters.append({
        "id": int(cid), "size": int(size[cid]), "intra_edges": int(intra[cid]),
        "density": round(float(density[cid]), 2), "n_fraud": int(nf_cnt[cid]),
        "fraud_rate": round(float(frate[cid]), 4), "enrichment": round(float(enrich[cid]), 1),
        "n_unlabeled": int(size[cid] - nf_cnt[cid]),
        "samples": samples,
    })

out = {
    "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
    "embedding": args.emb.split("/")[-1], "k": args.k, "n_nodes": int(N),
    "base_fraud_rate": round(base_rate, 6), "clusters": clusters,
}
with open(args.out, "w") as f:
    json.dump(out, f)
print(f"wrote {args.out}: {len(clusters)} clusters (base fraud rate {base_rate:.4%})")
print("top 5:", [(c["size"], c["enrichment"], c["density"]) for c in clusters[:5]])
