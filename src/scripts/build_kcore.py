#!/usr/bin/env python3
"""
Build a k-CORE training subgraph. Most of the 25.75M nodes are low-degree periphery that carries no
coordination signal (degree>=5 keeps 89% of fraud but only 56% of nodes) — training on the dense core
is faster, lower-RAM, and BETTER conditioned (the periphery is trivial link-pred negatives). Because
GraphSAGE is inductive, we train here and embed the FULL graph at inference, so no node is lost.

Emits graph_core.pt (a PyG Data over the k-core). Features are the 10 log1p aggregates standardized
with FULL-graph stats (so a node's features are IDENTICAL in the core and in the full graph → the
inductive inference is consistent). Same labels/masks/cutoff logic as build_pyg_data.

  python build_kcore.py --dir /data --labels /data/labels.parquet --out /data/graph_core.pt \
      --cutoff 2026-07-07 --k 5          # true k-core (iterative peel); --min-degree = single pass
"""
import argparse, numpy as np, pandas as pd, pyarrow.parquet as pq, torch
from torch_geometric.data import Data

ap = argparse.ArgumentParser()
ap.add_argument("--dir", required=True)
ap.add_argument("--labels", default="/Volumes/miniext/gnn_export/labels.parquet")
ap.add_argument("--out", required=True)
ap.add_argument("--cutoff", default="2026-07-07")
ap.add_argument("--k", type=int, default=5, help="k-core: iteratively peel nodes with <k core-neighbors")
ap.add_argument("--min-degree", type=int, default=0,
                help=">0 = single-pass degree>=d filter instead of iterative k-core (cheaper)")
args = ap.parse_args()

et = pq.read_table(f"{args.dir}/edges.parquet")
src = et.column("src").to_numpy().astype(np.int32)   # node_idx < 2^31 -> int32 halves the 1.43B-edge RAM
dst = et.column("dst").to_numpy().astype(np.int32)
nf = pq.read_table(f"{args.dir}/node_feats.parquet").to_pandas()
nf = nf.sort_values("sidx" if "sidx" in nf.columns else "node_idx").reset_index(drop=True)
N = len(nf)
print(f"full graph: {N:,} nodes / {len(src):,} edges")

# --- k-core (or single-pass degree filter) -------------------------------------------------------
alive = np.ones(N, dtype=bool)
if args.min_degree > 0:
    deg = np.bincount(src, minlength=N) + np.bincount(dst, minlength=N)
    alive = deg >= args.min_degree
    print(f"degree>=%d: kept %s" % (args.min_degree, f"{alive.sum():,}"))
else:
    it = 0
    while True:                                     # iterative peel: O(E) per round, few rounds
        m = alive[src] & alive[dst]
        deg = np.bincount(src[m], minlength=N) + np.bincount(dst[m], minlength=N)
        drop = alive & (deg < args.k)
        if not drop.any():
            break
        alive[drop] = False; it += 1
        print(f"  {args.k}-core peel {it}: removed {int(drop.sum()):,} -> {int(alive.sum()):,} alive")
    print(f"{args.k}-core: {int(alive.sum()):,} nodes ({100*alive.mean():.1f}%)")

core = np.where(alive)[0]
M = len(core)
remap = np.full(N, -1, dtype=np.int64); remap[core] = np.arange(M)   # global -> core-local
em = alive[src] & alive[dst]
csrc, cdst = remap[src[em]], remap[dst[em]]
edge_index = torch.from_numpy(np.vstack([csrc, cdst]))

# --- features: FULL-graph standardization, then subset to core (consistent with inference) --------
feat_cols = ["followers", "follows", "posts", "likes_out", "likes_in", "blocks_in",
             "blocks_out", "reposts_out", "replies_out", "quotes_out"]
RAW = nf[feat_cols].to_numpy(np.float32)
Xf = np.log1p(RAW.astype(np.float64))
Xf = (Xf - Xf.mean(0)) / (Xf.std(0) + 1e-8)         # stats over ALL nodes -> matches full graph.pt

lab = pq.read_table(args.labels).to_pandas().drop_duplicates("did").set_index("did")
is_fraud = nf["did"].map(lab["is_fraud"]).fillna(False).to_numpy(bool)
is_excl  = nf["did"].map(lab["is_excluded"]).fillna(False).to_numpy(bool)
first_lab = pd.to_datetime(nf["did"].map(lab["first_labeled"]), utc=True)
cut = pd.Timestamp(args.cutoff, tz="UTC")
late = (first_lab >= cut).to_numpy()
rng = np.random.default_rng(42); r = rng.random(N)
pos, neg = is_fraud & ~is_excl, (~is_fraud) & ~is_excl
train_m = ((pos & ~late & ~(r < 0.15)) | (neg & (r < 0.70)))
val_m   = ((pos & ~late & (r < 0.15)) | (neg & (r >= 0.70) & (r < 0.85)))
test_m  = ((pos & late) | (neg & (r >= 0.85)))

data = Data(x=torch.from_numpy(Xf[core].astype(np.float32)), edge_index=edge_index,
            y=torch.from_numpy(is_fraud[core].astype(np.float32)))
data.x_raw = torch.from_numpy(RAW[core])
data.num_nodes = M
data.train_mask = torch.from_numpy(train_m[core])
data.val_mask = torch.from_numpy(val_m[core])
data.test_mask = torch.from_numpy(test_m[core])
data.core_global_idx = torch.from_numpy(core)       # bookkeeping (not needed for inference)

nfr = int((pos)[core].sum()); allfr = int(pos.sum())
print(data)
print(f"core nodes={M:,}/{N:,} ({100*M/N:.1f}%) edges={edge_index.size(1):,}/{len(src):,} "
      f"({100*edge_index.size(1)/len(src):.1f}%) | fraud kept {nfr:,}/{allfr:,} ({100*nfr/max(allfr,1):.1f}%)")
torch.save(data, args.out)
print("saved", args.out)
