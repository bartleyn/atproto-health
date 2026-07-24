#!/usr/bin/env python3
"""
Inductive inference: load trained (self-supervised) SAGE body weights and embed a graph — used to
embed the FULL graph after k-core training, and reusable for scoring brand-new accounts later. Split
out from train_sage_unsup so a slow full-graph pass can't lose the trained weights on timeout.

Fits the logistic PROBE on the resulting embeddings (train labels) and saves it as head.weight/bias,
so cluster_embeddings / validate_discovery / eval_embeddings work unchanged.

  python infer_embeddings.py --weights /data/sage_unsup.pt --graph /data/graph.pt \
      --feat-mode full --out-stem /data/sage_unsup [--num-workers 16] [--device cuda]
"""
import argparse, numpy as np, torch
import torch.multiprocessing as _mp
_mp.set_sharing_strategy("file_system")   # worker shared tensors -> disk, not tiny /dev/shm
from torch_geometric.loader import NeighborLoader
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, average_precision_score
from train_sage_unsup import SAGEBody         # same architecture the weights were trained with
from train_sage import build_features

ap = argparse.ArgumentParser()
ap.add_argument("--weights", required=True)
ap.add_argument("--graph", required=True)
ap.add_argument("--feat-mode", choices=["full", "degree", "stable"], default="full")
ap.add_argument("--out-stem", required=True)
ap.add_argument("--hidden", type=int, default=128)
ap.add_argument("--aggr", choices=["mean", "max", "sum"], default="mean")
ap.add_argument("--device", default="cuda" if torch.cuda.is_available() else "cpu")
ap.add_argument("--num-workers", type=int, default=0)
ap.add_argument("--probe-neg-cap", type=int, default=2_000_000)
args = ap.parse_args()
device = torch.device(args.device)

data = torch.load(args.graph, weights_only=False)
data.x = torch.as_tensor(build_features(data, args.feat_mode)).float()
model = SAGEBody(data.x.size(1), args.hidden, aggr=args.aggr).to(device)
sd = torch.load(args.weights, map_location="cpu", weights_only=True)
model.load_state_dict({k: v for k, v in sd.items() if not k.startswith("head.")})  # body only
model.eval()
print(f"inductive inference on {args.graph}: {data.num_nodes:,} nodes | device {device}", flush=True)


@torch.no_grad()
def all_embeddings():
    full = NeighborLoader(data, num_neighbors=[15, 10], batch_size=4096, input_nodes=None,
                          shuffle=False, num_workers=args.num_workers, pin_memory=True)
    embs = torch.empty(data.num_nodes, args.hidden)
    for batch in full:
        emb = model(batch.x.to(device), batch.edge_index.to(device))
        n = batch.batch_size
        embs[batch.n_id[:n].cpu()] = emb[:n].cpu()
    return embs.numpy()


emb = all_embeddings()

# probe (train labels) -> node_score head + the embedding-alone classification number
y = data.y.numpy().astype(int)
tr, te = data.train_mask.numpy(), data.test_mask.numpy()
tr_idx = np.where(tr)[0]
neg = tr_idx[y[tr_idx] == 0]
rng = np.random.default_rng(42)
if len(neg) > args.probe_neg_cap:
    neg = rng.choice(neg, args.probe_neg_cap, replace=False)
fit_idx = np.concatenate([tr_idx[y[tr_idx] == 1], neg])
probe = LogisticRegression(C=1.0, class_weight="balanced", max_iter=2000).fit(emb[fit_idx], y[fit_idx])
p = probe.predict_proba(emb[te])[:, 1]
print(f"PROBE leak-free TEST: ROC {roc_auc_score(y[te], p):.4f} | PR {average_precision_score(y[te], p):.4f}"
      f"  (vs supervised follows-only 0.7846 / GBM-full 0.8954)")

out = model.state_dict()
out["head.weight"] = torch.tensor(probe.coef_, dtype=torch.float32)
out["head.bias"] = torch.tensor(probe.intercept_, dtype=torch.float32)
torch.save(out, f"{args.out_stem}.pt")
np.save(f"{args.out_stem}_emb.npy", emb)
print(f"saved {args.out_stem}.pt and {args.out_stem}_emb.npy")
