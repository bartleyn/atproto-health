#!/usr/bin/env python3
"""
Build a MULTI-RELATIONAL (hetero) PyG graph: one node type ('acct') with the follows AND blocks
relations, so the GNN can message-pass over both. Mirrors build_pyg_data.py's features/labels/masks
exactly (same seed, same cutoff) so the follows+blocks result is comparable to the follows-only run.

  python build_hetero_data.py --dir /data --labels /data/labels.parquet --out /data/graph_hetero.pt \
      --cutoff 2026-07-07 --relations follows,blocks

Node features stored raw+standardized on data['acct']; train_hetero picks feat-mode. Edge types:
  ('acct','follows','acct')  <- edges.parquet
  ('acct','blocks','acct')   <- blocks.parquet
  ('acct','replies','acct')  <- replies.parquet   (replier -> replied-to, deduped directed pairs)
"""
import argparse, numpy as np, pandas as pd, pyarrow.parquet as pq, torch
from torch_geometric.data import HeteroData

REL_FILE = {"follows": "edges.parquet", "blocks": "blocks.parquet", "replies": "replies.parquet"}

ap = argparse.ArgumentParser()
ap.add_argument("--dir", required=True)
ap.add_argument("--labels", default="/Volumes/miniext/gnn_export/labels.parquet")
ap.add_argument("--out", required=True)
ap.add_argument("--cutoff", default="2026-07-07")
ap.add_argument("--relations", default="follows,blocks")
args = ap.parse_args()
rels = args.relations.split(",")
print(f"CUTOFF={args.cutoff}  relations={rels}")

nf = pq.read_table(f"{args.dir}/node_feats.parquet").to_pandas()
idx_col = "sidx" if "sidx" in nf.columns else "node_idx"
nf = nf.sort_values(idx_col).reset_index(drop=True)
N = len(nf)

feat_cols = ["followers", "follows", "posts", "likes_out", "likes_in", "blocks_in",
             "blocks_out", "reposts_out", "replies_out", "quotes_out"]
RAW = nf[feat_cols].to_numpy(dtype=np.float32)
X = np.log1p(RAW.astype(np.float64))
X = ((X - X.mean(0)) / (X.std(0) + 1e-8)).astype(np.float32)

lab = pq.read_table(args.labels).to_pandas().drop_duplicates("did").set_index("did")
is_fraud = nf["did"].map(lab["is_fraud"]).fillna(False).to_numpy(bool)
is_excl  = nf["did"].map(lab["is_excluded"]).fillna(False).to_numpy(bool)
first_lab = pd.to_datetime(nf["did"].map(lab["first_labeled"]), utc=True)

pop = ~is_excl
cut = pd.Timestamp(args.cutoff, tz="UTC")
late = (first_lab >= cut).to_numpy()
rng = np.random.default_rng(seed=42)
r = rng.random(N)
pos, neg = is_fraud & pop, (~is_fraud) & pop
pos_val = (pos & ~late) & (r < 0.15)
train_mask = (pos & ~late & ~(r < 0.15)) | (neg & (r < 0.70))
val_mask   = pos_val | (neg & (r >= 0.70) & (r < 0.85))
test_mask  = (pos & late) | (neg & (r >= 0.85))

data = HeteroData()
data["acct"].x = torch.from_numpy(X)
data["acct"].x_raw = torch.from_numpy(RAW)
data["acct"].y = torch.from_numpy(is_fraud.astype(np.float32))
data["acct"].num_nodes = N
data["acct"].train_mask = torch.from_numpy(train_mask)
data["acct"].val_mask = torch.from_numpy(val_mask)
data["acct"].test_mask = torch.from_numpy(test_mask)

for rel in rels:
    et = pq.read_table(f"{args.dir}/{REL_FILE[rel]}")
    src, dst = et.column("src").to_numpy(), et.column("dst").to_numpy()
    data["acct", rel, "acct"].edge_index = torch.from_numpy(np.vstack([src, dst])).to(torch.long)
    print(f"  {rel}: {len(src):,} edges")

print(data)
print(f"nodes={N:,} pos={int(pos.sum()):,} farms_excluded={int(is_excl.sum()):,}")
for nm, m in [("train", train_mask), ("val", val_mask), ("test", test_mask)]:
    yv = is_fraud[m]
    print(f"  {nm:5s}: n={int(m.sum()):>9,} pos={int(yv.sum()):>6,} pos_rate={yv.mean()*100:.3f}%")
torch.save(data, args.out)
print("saved", args.out)
