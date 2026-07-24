#!/usr/bin/env python3
"""
SELF-SUPERVISED GraphSAGE (link-prediction objective). Learns node embeddings from graph STRUCTURE
ALONE — no fraud labels touch the representation — so the embedding is non-circular, reusable, and
free of the label-wave shortcut-learning that caps the supervised model. Downstream: cluster it for
discovery, or feed it (± tabular features) to a GBM for scoring.

Objective (Hamilton et al.): real edges = positives, random node pairs = negatives; score = dot
product of endpoint embeddings; BCE. LinkNeighborLoader samples the supervision edges + their
neighborhoods, so it scales with neighbor sampling like the supervised trainer.

At the end it fits a cheap LOGISTIC PROBE on the embeddings (train-split labels only) and saves it as
head.weight/head.bias — this is standard linear-probe evaluation (does the label-free embedding
separate fraud?) AND keeps cluster_embeddings/validate_discovery working unchanged.

  python train_sage_unsup.py --graph /data/graph.pt --feat-mode degree --device cuda
Saves sage_unsup.pt (body + probe head) and sage_unsup_emb.npy.
"""
import argparse, os, numpy as np, torch
import torch.multiprocessing as _mp
_mp.set_sharing_strategy("file_system")   # route worker shared tensors to disk, not tiny /dev/shm
import torch.nn.functional as F
from torch_geometric.loader import LinkNeighborLoader, NeighborLoader
from torch_geometric.nn import SAGEConv
from sklearn.metrics import roc_auc_score, average_precision_score
from train_sage import build_features   # identical feature construction as the supervised trainer


class SAGEBody(torch.nn.Module):
    """2-layer SAGE, no classification head — returns the embedding."""
    def __init__(self, in_dim, hidden, aggr="mean"):
        super().__init__()
        self.conv1 = SAGEConv(in_dim, hidden, aggr=aggr)
        self.conv2 = SAGEConv(hidden, hidden, aggr=aggr)

    def forward(self, x, edge_index):
        x = F.relu(self.conv1(x, edge_index))
        x = F.dropout(x, p=0.4, training=self.training)
        return self.conv2(x, edge_index)   # NO final relu: dot-product link scores need to go negative


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--graph", required=True, help="training graph (e.g. graph_core.pt = k-core)")
    ap.add_argument("--infer-graph", default=None,
                    help="embed THIS graph with the trained weights (inductive; e.g. full graph.pt). "
                         "Use feat-mode full so features match across core/full.")
    ap.add_argument("--feat-mode", choices=["full", "degree", "stable"], default="degree")
    ap.add_argument("--epochs", type=int, default=3)
    ap.add_argument("--hidden", type=int, default=128)
    ap.add_argument("--aggr", choices=["mean", "max", "sum"], default="mean")
    ap.add_argument("--device", default="mps" if torch.backends.mps.is_available() else "cpu")
    ap.add_argument("--edges-per-epoch", type=int, default=30_000_000,
                    help="random subsample of positive edges per epoch (repr. learning needs a big "
                         "sample, not all 1.4B)")
    ap.add_argument("--batch-size", type=int, default=8192)
    ap.add_argument("--temperature", type=float, default=0.1, help="cosine temperature for link score")
    ap.add_argument("--probe-neg-cap", type=int, default=2_000_000)
    ap.add_argument("--num-workers", type=int, default=0,
                    help="parallel neighbor-sampling workers (LinkNeighborLoader is CPU-bound; set to "
                         "the box's core count on GPU runs — this is the main throughput lever)")
    ap.add_argument("--no-infer", action="store_true",
                    help="train + save body weights only; run infer_embeddings.py separately (so a slow "
                         "full-graph inference can't lose the trained weights on timeout)")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    device = torch.device(args.device)
    torch.manual_seed(args.seed); gen = torch.Generator().manual_seed(args.seed)

    data = torch.load(args.graph, weights_only=False)
    data.x = torch.as_tensor(build_features(data, args.feat_mode)).float()
    in_dim = data.x.size(1)
    E = data.edge_index.size(1)
    print(data, "| device:", device, "| edges", f"{E:,}")

    model = SAGEBody(in_dim, args.hidden, aggr=args.aggr).to(device)
    opt = torch.optim.Adam(model.parameters(), lr=0.005, weight_decay=5e-4)

    # ONE loader over all edges (CSC built + shared ONCE — recreating per epoch re-shares the CSC and
    # exhausts /dev/shm). shuffle=True reshuffles each epoch; we take edges_per_epoch worth of batches.
    loader = LinkNeighborLoader(
        data, num_neighbors=[15, 10], batch_size=args.batch_size,
        edge_label_index=data.edge_index, neg_sampling_ratio=1.0, shuffle=True,
        num_workers=args.num_workers, persistent_workers=args.num_workers > 0, pin_memory=True)
    steps = max(1, min(args.edges_per_epoch, E) // args.batch_size)

    def train_epoch():
        model.train(); total = seen = 0.0
        for i, batch in enumerate(loader):
            if i >= steps:
                break
            batch = batch.to(device); opt.zero_grad()
            z = model(batch.x, batch.edge_index)
            s, d = batch.edge_label_index
            # cosine sim / temperature: bounds the score so BCE stays stable (raw dot products of
            # unnormalized embeddings diverge). Standard contrastive-learning normalization.
            score = (F.normalize(z[s], dim=-1) * F.normalize(z[d], dim=-1)).sum(-1) / args.temperature
            loss = F.binary_cross_entropy_with_logits(score, batch.edge_label)
            loss.backward(); opt.step()
            bs = batch.edge_label.numel()
            total += float(loss.detach()) * bs; seen += bs
        return total / max(seen, 1)

    for epoch in range(1, args.epochs + 1):
        print(f"epoch {epoch:02d} | link-pred loss {train_epoch():.4f}", flush=True)

    stem = f"{os.path.dirname(args.graph)}/sage_unsup"
    if args.no_infer:                                          # persist weights NOW, infer separately
        torch.save(model.state_dict(), f"{stem}.pt")
        print(f"saved {stem}.pt (body only) — run infer_embeddings.py for emb + probe head")
        return

    # INDUCTIVE inference: embed the full graph with the (core-trained) weights.
    infer_data = data
    if args.infer_graph:
        infer_data = torch.load(args.infer_graph, weights_only=False)
        infer_data.x = torch.as_tensor(build_features(infer_data, args.feat_mode)).float()
        print(f"inductive inference on {args.infer_graph}: {infer_data.num_nodes:,} nodes", flush=True)

    @torch.no_grad()
    def all_embeddings(g):
        model.eval()
        full = NeighborLoader(g, num_neighbors=[15, 10], batch_size=4096,
                              input_nodes=None, shuffle=False,
                              num_workers=args.num_workers, pin_memory=True)
        embs = torch.empty(g.num_nodes, args.hidden)
        for batch in full:
            emb = model(batch.x.to(device), batch.edge_index.to(device))
            n = batch.batch_size
            embs[batch.n_id[:n].cpu()] = emb[:n].cpu()
        return embs

    emb = all_embeddings(infer_data).numpy()

    # --- linear probe: does the label-free embedding separate fraud? (also = node_score for ranking) ---
    from sklearn.linear_model import LogisticRegression
    y = infer_data.y.numpy().astype(int)
    tr, te = infer_data.train_mask.numpy(), infer_data.test_mask.numpy()
    tr_idx = np.where(tr)[0]
    neg = tr_idx[y[tr_idx] == 0]
    if len(neg) > args.probe_neg_cap:
        neg = np.random.default_rng(args.seed).choice(neg, args.probe_neg_cap, replace=False)
    fit_idx = np.concatenate([tr_idx[y[tr_idx] == 1], neg])
    probe = LogisticRegression(C=1.0, class_weight="balanced", max_iter=2000)
    probe.fit(emb[fit_idx], y[fit_idx])
    p = probe.predict_proba(emb[te])[:, 1]
    print(f"\nPROBE (logistic on self-sup emb) leak-free TEST: "
          f"ROC {roc_auc_score(y[te], p):.4f} | PR {average_precision_score(y[te], p):.4f}  "
          f"(compare follows-only supervised 0.7846 / GBM-full 0.8954)")

    stem = f"{os.path.dirname(args.graph)}/sage_unsup"
    sd = model.state_dict()
    sd["head.weight"] = torch.tensor(probe.coef_, dtype=torch.float32)      # [1, hidden]
    sd["head.bias"] = torch.tensor(probe.intercept_, dtype=torch.float32)   # [1]
    torch.save(sd, f"{stem}.pt")
    np.save(f"{stem}_emb.npy", emb)
    print(f"saved {stem}.pt and {stem}_emb.npy")


if __name__ == "__main__":
    main()
