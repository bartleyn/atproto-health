import argparse
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import torch
from torch_geometric.data import Data

ap = argparse.ArgumentParser()
ap.add_argument("--dir", required=True, help="dir with node_feats.parquet + edges.parquet")
ap.add_argument("--labels", default="/Volumes/miniext/gnn_export/labels.parquet")
ap.add_argument("--out", required=True)
ap.add_argument("--cutoff", default="2026-05-11",
                help="temporal split point = snapshot_date; fraud labeled after it = leak-free test")
ap.add_argument("--undirected", action="store_true", help="add reverse edges (M3 ablation)")
args = ap.parse_args()
CUTOFF = args.cutoff   # temporal split point (= the snapshot date the graph was built from)
print(f"CUTOFF (leak-free split) = {CUTOFF}")


nf = pq.read_table(f"{args.dir}/node_feats.parquet").to_pandas()
idx_col = "sidx" if "sidx" in nf.columns else "node_idx"
nf = nf.sort_values(idx_col).reset_index(drop=True)
N = len(nf)

feat_cols = ["followers", "follows", "posts", "likes_out", "likes_in", "blocks_in", "blocks_out", "reposts_out", "replies_out", "quotes_out"]
RAW = nf[feat_cols].to_numpy(dtype=np.float32)             # raw counts, for ratio features
X = np.log1p(RAW.astype(np.float64))                       # 1) log1p: tame the heavy tail
X = (X - X.mean(axis=0)) / (X.std(axis=0) + 1e-8)          # 2) standardize each feature
x = torch.from_numpy(X.astype(np.float32))
x_raw = torch.from_numpy(RAW)


et = pq.read_table(f"{args.dir}/edges.parquet")
src = et.column("src").to_numpy()
dst = et.column("dst").to_numpy()
edge_index = torch.from_numpy(np.vstack([src, dst])).to(torch.long)

if args.undirected:
    from torch_geometric.utils import to_undirected
    edge_index = to_undirected(edge_index, num_nodes=N)

print(f"edges: {edge_index.size(1):,} undirected={args.undirected}")

lab = pq.read_table(args.labels).to_pandas().drop_duplicates("did").set_index("did")
# left-join labels onto node rows BY did. A node with no label row = negative, not a farm.
is_fraud    = nf["did"].map(lab["is_fraud"]).fillna(False).to_numpy(dtype=bool)
is_excluded = nf["did"].map(lab["is_excluded"]).fillna(False).to_numpy(dtype=bool)
first_lab   = pd.to_datetime(nf["did"].map(lab["first_labeled"]), utc=True)  # NaT where missing

y = torch.from_numpy(is_fraud.astype(np.float32))


# add train/test/val masks

pop = ~is_excluded
cut = pd.Timestamp(CUTOFF, tz="UTC")
late = (first_lab >= cut).to_numpy()

rng = np.random.default_rng(seed=42)
r = rng.random(N)


pos = is_fraud & pop
neg = (~is_fraud) & pop


pos_test  = pos & late
pos_early = pos & ~late
pos_val   = pos_early & (r < 0.15)
pos_train = pos_early & ~pos_val
# negatives: random 70/15/15
train_mask = pos_train | (neg & (r < 0.70))
val_mask   = pos_val   | (neg & (r >= 0.70) & (r < 0.85))
test_mask  = pos_test  | (neg & (r >= 0.85))

data = Data(x=X, edge_index=edge_index, y=y)
data.x_raw = x_raw
data.num_nodes = N
data.train_mask = torch.from_numpy(train_mask)
data.val_mask = torch.from_numpy(val_mask)
data.test_mask = torch.from_numpy(test_mask)

data.validate()
print(data)
print(f"nodes={N:,}  edges={edge_index.size(1):,}  positives={int(pos.sum()):,}  "
        f"farms_excluded={int(is_excluded.sum()):,}")
for nm, m in [("train", train_mask), ("val", val_mask), ("test", test_mask)]:
    yv = y.numpy()[m]
    print(f"  {nm:5s}: n={int(m.sum()):>9,}  pos={int(yv.sum()):>6,}  pos_rate={yv.mean()*100:.3f}%")

torch.save(data, args.out)
print("saved", args.out)