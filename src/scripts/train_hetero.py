#!/usr/bin/env python3
"""
Train a MULTI-RELATIONAL GraphSAGE (follows + blocks) via to_hetero, to test whether the block
relation adds discovery/classification signal over follows-only. Reuses train_sage's negative
subsampling + focal loss so the only difference vs the homogeneous baseline is the extra relation.

  python train_hetero.py --graph /data/graph_hetero.pt --feat-mode degree --device cuda

Saves sage_hetero.pt (state_dict incl. head.weight/head.bias so cluster/validate load unchanged) +
sage_hetero_emb.npy (per-node 'acct' embeddings, ordered by node_idx).
"""
import argparse, os, numpy as np, torch
import torch.multiprocessing as _mp
_mp.set_sharing_strategy("file_system")   # route worker shared tensors to disk, not tiny /dev/shm
import torch.nn.functional as F
from torch_geometric.loader import NeighborLoader
from torch_geometric.nn import SAGEConv, to_hetero
from torch_geometric.utils import degree
from sklearn.metrics import roc_auc_score, average_precision_score
from train_sage import build_train_inputs, focal_loss   # shared, unit-tested helpers


class SAGEBody(torch.nn.Module):
    """Homogeneous 2-layer body; to_hetero() replicates it per relation and aggregates across them."""
    def __init__(self, hidden, aggr="mean"):
        super().__init__()
        self.conv1 = SAGEConv((-1, -1), hidden, aggr=aggr)
        self.conv2 = SAGEConv((-1, -1), hidden, aggr=aggr)

    def forward(self, x, edge_index):
        x = F.relu(self.conv1(x, edge_index))
        x = F.dropout(x, p=0.4, training=self.training)
        return F.relu(self.conv2(x, edge_index))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--graph", required=True)
    ap.add_argument("--feat-mode", choices=["full", "degree"], default="degree")
    ap.add_argument("--epochs", type=int, default=3)
    ap.add_argument("--hidden", type=int, default=128)
    ap.add_argument("--device", default="mps" if torch.backends.mps.is_available() else "cpu")
    ap.add_argument("--neg-ratio", type=float, default=20.0)
    ap.add_argument("--hard-frac", type=float, default=0.7)
    ap.add_argument("--loss", choices=["bce", "focal"], default="bce")
    ap.add_argument("--focal-gamma", type=float, default=2.0)
    ap.add_argument("--aggr", choices=["mean", "max", "sum"], default="mean")   # cross-relation aggr
    ap.add_argument("--num-workers", type=int, default=0,
                    help="parallel neighbor-sampling workers (hetero sampling walks every relation and "
                         "is CPU-bound; set to the box's core count on GPU runs — main throughput lever)")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    device = torch.device(args.device)
    gen = torch.Generator().manual_seed(args.seed); torch.manual_seed(args.seed)

    data = torch.load(args.graph, weights_only=False)
    rels = [et for et in data.edge_types]
    print(data, "| relations:", rels, "| device:", device)

    # feature mode: degree = follows in/out degree (== the follows-only baseline's inputs, so the
    # only new signal is the block relation); full = the standardized 10 aggregates.
    fe = data["acct", "follows", "acct"].edge_index
    N = data["acct"].num_nodes
    out_deg = degree(fe[0], N); in_deg = degree(fe[1], N)
    if args.feat_mode == "degree":
        data["acct"].x = torch.stack([torch.log1p(out_deg), torch.log1p(in_deg)], 1).float()
    else:
        data["acct"].x = torch.as_tensor(data["acct"].x).float()
    in_dim = data["acct"].x.size(1)

    y = data["acct"].y
    train_input, n_pos, n_neg = build_train_inputs(
        y, data["acct"].train_mask, out_deg, in_deg, args.neg_ratio, args.hard_frac, generator=gen)
    pos_weight = torch.tensor([n_neg / max(n_pos, 1.0)], device=device)
    print(f"train targets: pos={n_pos} neg={n_neg} ({n_neg/max(n_pos,1):.1f}:1) | "
          f"pos_weight={pos_weight.item():.1f} loss={args.loss} cross-rel-aggr={args.aggr}")

    def loader(nodes, shuffle):
        return NeighborLoader(data, num_neighbors=[15, 10], batch_size=2048,
                              input_nodes=("acct", nodes), shuffle=shuffle,
                              num_workers=args.num_workers,
                              persistent_workers=args.num_workers > 0, pin_memory=True)
    train_loader = loader(train_input, True)
    val_loader = loader(data["acct"].val_mask, False)
    test_loader = loader(data["acct"].test_mask, False)

    body = to_hetero(SAGEBody(args.hidden), data.metadata(), aggr=args.aggr).to(device)
    head = torch.nn.Linear(args.hidden, 1).to(device)
    # lazy SAGEConv((-1,-1)) has NO params until the first forward -> init before building the
    # optimizer, else it gets an empty param list and nothing trains.
    _init = next(iter(train_loader)).to(device)
    with torch.no_grad():
        body(_init.x_dict, _init.edge_index_dict)
    opt = torch.optim.Adam(list(body.parameters()) + list(head.parameters()), lr=0.005, weight_decay=5e-4)

    def emb_and_logit(batch):
        emb = body(batch.x_dict, batch.edge_index_dict)["acct"]
        bs = batch["acct"].batch_size
        return emb, head(emb[:bs]).squeeze(-1)

    def compute_loss(logit, target):
        if args.loss == "focal":
            return focal_loss(logit, target, args.focal_gamma, pos_weight)
        return F.binary_cross_entropy_with_logits(logit, target, pos_weight=pos_weight)

    def train_epoch():
        body.train(); head.train(); total = 0.0
        for batch in train_loader:
            batch = batch.to(device); opt.zero_grad()
            _, logit = emb_and_logit(batch)
            loss = compute_loss(logit, batch["acct"].y[:batch["acct"].batch_size].float())
            loss.backward(); opt.step()
            total += float(loss.detach()) * batch["acct"].batch_size
        return total / max(int(train_input.numel()), 1)

    @torch.no_grad()
    def evaluate(loader):
        body.eval(); head.eval(); scores, ys = [], []
        for batch in loader:
            batch = batch.to(device); _, logit = emb_and_logit(batch)
            scores.append(torch.sigmoid(logit).cpu().numpy())
            ys.append(batch["acct"].y[:batch["acct"].batch_size].cpu().numpy())
        s, yv = np.concatenate(scores), np.concatenate(ys)
        return roc_auc_score(yv, s), average_precision_score(yv, s)

    for epoch in range(1, args.epochs + 1):
        loss = train_epoch(); v_roc, v_pr = evaluate(val_loader)
        print(f"epoch {epoch:02d} | loss {loss:.4f} | val ROC {v_roc:.4f} | val PR {v_pr:.4f}")
    t_roc, t_pr = evaluate(test_loader)
    print(f"\nTEST  ROC-AUC {t_roc:.4f} | PR-AUC {t_pr:.4f}  (relations={rels}; "
          f"compare to follows-only 0.7846)")

    @torch.no_grad()
    def all_embeddings():
        body.eval()
        full = NeighborLoader(data, num_neighbors=[15, 10], batch_size=4096,
                              input_nodes=("acct", None), shuffle=False,
                              num_workers=args.num_workers, pin_memory=True)
        embs = torch.empty(N, args.hidden)
        for batch in full:
            batch = batch.to(device)                       # inputs must be on the model's device
            emb = body(batch.x_dict, batch.edge_index_dict)["acct"]
            bs = batch["acct"].batch_size
            embs[batch["acct"].n_id[:bs].cpu()] = emb[:bs].cpu()   # n_id back to cpu to index embs
        return embs

    stem = f"{os.path.dirname(args.graph)}/sage_hetero"
    sd = body.state_dict()
    sd["head.weight"] = head.weight.detach().cpu()      # keep keys cluster/validate expect
    sd["head.bias"] = head.bias.detach().cpu()
    torch.save(sd, f"{stem}.pt")
    np.save(f"{stem}_emb.npy", all_embeddings().numpy())
    print(f"saved {stem}.pt and {stem}_emb.npy")


if __name__ == "__main__":
    main()
