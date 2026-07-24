import argparse, os, numpy as np, torch
import torch.nn.functional as F
from torch_geometric.loader import NeighborLoader
from torch_geometric.nn import SAGEConv
from torch_geometric.utils import degree
from sklearn.metrics import roc_auc_score, average_precision_score


# ------------------------- new: imbalance handling (pure, unit-testable) -------------------------

def build_train_inputs(y, train_mask, out_deg, in_deg, neg_ratio, hard_frac, generator=None):
    """Subsample training TARGETS (not the message-passing graph) to `neg_ratio` negatives per
    positive, biasing `hard_frac` of the negatives toward the STRUCTURE positives occupy.

    Fraud is ~0.03-0.1% -> a random train_mask is ~all trivially-easy negatives; the boundary is
    set by uninformative points and every epoch scans ~24.6M of them. We keep all positives, draw
    `neg_ratio * n_pos` negatives, and weight the "hard" share by the positive density in a 2-D
    (log out-degree, log in-degree) histogram. Structure ONLY (degree) -> does NOT reintroduce the
    skywatch-heuristic circularity that engineered features caused (project theme). Returns the
    input-node index tensor + the realized (n_pos, n_neg) for pos_weight.
    """
    pos_idx = torch.nonzero(train_mask & (y == 1), as_tuple=True)[0]
    neg_idx = torch.nonzero(train_mask & (y == 0), as_tuple=True)[0]
    n_pos = int(pos_idx.numel())
    n_take = min(int(neg_idx.numel()), int(round(neg_ratio * n_pos)))
    if n_take <= 0 or n_pos == 0:
        return torch.cat([pos_idx, neg_idx]), n_pos, int(neg_idx.numel())

    lo, li = torch.log1p(out_deg.float()), torch.log1p(in_deg.float())
    nb = 24

    def binize(v):
        vmin, vmax = float(v.min()), float(v.max())
        return torch.clamp(((v - vmin) / (vmax - vmin + 1e-9) * nb).long(), 0, nb - 1)

    bo, bi = binize(lo), binize(li)
    hist = torch.zeros(nb, nb)
    hist.index_put_((bo[pos_idx], bi[pos_idx]), torch.ones(n_pos), accumulate=True)
    hist /= hist.sum().clamp_min(1e-9)
    neg_hard_w = hist[bo[neg_idx], bi[neg_idx]] + 1e-6            # positive-density at each neg

    n_hard = min(int(round(hard_frac * n_take)), int(neg_idx.numel()))
    n_easy = n_take - n_hard
    # weighted sampling WITHOUT replacement via Gumbel-top-k (O(n), scale-safe at 24.6M)
    u = torch.rand(neg_hard_w.shape, generator=generator).clamp_min(1e-12)
    gumbel = -torch.log(-torch.log(u))
    hard_local = torch.topk(torch.log(neg_hard_w) + gumbel, n_hard).indices
    if n_easy > 0:
        remaining = torch.ones(neg_idx.numel(), dtype=torch.bool)
        remaining[hard_local] = False
        rem = torch.nonzero(remaining, as_tuple=True)[0]
        perm = torch.randperm(rem.numel(), generator=generator)[:n_easy]
        sel_local = torch.cat([hard_local, rem[perm]])
    else:
        sel_local = hard_local
    neg_take = neg_idx[sel_local]
    train_input = torch.cat([pos_idx, neg_take])
    return train_input, n_pos, int(neg_take.numel())


def focal_loss(logit, target, gamma, pos_weight):
    """Focal BCE: (1-p_t)^gamma * CE, with pos_weight as the class-balance (alpha) term. gamma=0
    reduces exactly to weighted BCE. Down-weights easy examples -> ablation knob; expected to help
    val and NOT the leak-free test (chasing hard positives = the training-wave overfit direction)."""
    ce = F.binary_cross_entropy_with_logits(logit, target, pos_weight=pos_weight, reduction="none")
    p = torch.sigmoid(logit)
    p_t = p * target + (1 - p) * (1 - target)
    return (((1 - p_t) ** gamma) * ce).mean()


# ------------------------------------------ features ---------------------------------------------

def build_features(data, feat_mode):
    if feat_mode == "full":
        return data.x
    if feat_mode == "stable":
        r = data.x_raw   # followers,follows,posts,likes_out,likes_in,blocks_in,blocks_out,reposts_out,replies_out,quotes_out
        followers, follows, posts = r[:, 0], r[:, 1], r[:, 2]
        likes_out, likes_in, blocks_in = r[:, 3], r[:, 4], r[:, 5]
        reposts_out, replies_out = r[:, 7], r[:, 8]
        eps = 1.0
        ratios = torch.stack([
            follows / (followers + eps),                    # follow_ratio  (bulk-following)
            blocks_in / (followers + eps),                  # blocks_in_ratio
            replies_out / (posts + eps),                    # reply_ratio
            reposts_out / (posts + reposts_out + eps),      # repost_ratio  (amplifier)
            likes_out / (posts + eps),                      # like_per_post
            likes_in / (posts + eps),                       # likes_in_per_post
        ], dim=1)
        src, dst = data.edge_index
        deg = torch.stack([torch.log1p(degree(src, data.num_nodes)),
                           torch.log1p(degree(dst, data.num_nodes))], dim=1)
        feats = torch.cat([deg, torch.log1p(ratios)], dim=1)
        return (feats - feats.mean(0)) / (feats.std(0) + 1e-8)
    # degree
    src, dst = data.edge_index
    x = torch.stack([torch.log1p(degree(src, data.num_nodes)),
                     torch.log1p(degree(dst, data.num_nodes))], dim=1)
    return x


class SAGE(torch.nn.Module):
    def __init__(self, in_dim, hidden, activation="relu", aggr="mean"):
        super().__init__()
        # aggr: mean is sample-invariant (SAGE's design); sum/max are magnitude-aware but note the
        # NeighborLoader cap ([15,10]) truncates sum over high-degree nodes -> ablation, not free win.
        self.conv1 = SAGEConv(in_dim, hidden, aggr=aggr)
        self.conv2 = SAGEConv(hidden, hidden, aggr=aggr)
        self.head = torch.nn.Linear(hidden, 1)
        # PReLU is stateful (learnable negative slope) -> a distinct module per site, not F.relu.
        # A separate instance after each conv so each layer learns its own slope.
        def act():
            return torch.nn.PReLU() if activation == "prelu" else torch.nn.ReLU()
        self.act1, self.act2 = act(), act()

    def forward(self, x, edge_index, return_emb=False):
        x = self.act1(self.conv1(x, edge_index))
        x = F.dropout(x, p=0.4, training=self.training)
        emb = self.act2(self.conv2(x, edge_index))
        x = F.dropout(emb, p=0.4, training=self.training)
        return (self.head(x).squeeze(-1), emb) if return_emb else self.head(x).squeeze(-1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--graph", required=True)
    ap.add_argument("--feat-mode", choices=["full", "degree", "stable"], default="full")
    ap.add_argument("--epochs", type=int, default=3)          # converges by epoch 1; was 6
    ap.add_argument("--hidden", type=int, default=128)
    ap.add_argument("--device", default="mps" if torch.backends.mps.is_available() else "cpu")
    ap.add_argument("--undirected", action="store_true", help="symmetrize edges for message passing")
    ap.add_argument("--neg-ratio", type=float, default=20.0,
                    help="negatives per positive in the train target set (0 = use all, old behavior)")
    ap.add_argument("--hard-frac", type=float, default=0.7,
                    help="fraction of sampled negatives drawn by structural hardness vs uniform")
    ap.add_argument("--loss", choices=["bce", "focal"], default="bce")
    ap.add_argument("--focal-gamma", type=float, default=2.0)
    ap.add_argument("--activation", choices=["relu", "prelu"], default="relu")
    ap.add_argument("--aggr", choices=["mean", "max", "sum"], default="mean")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    device = torch.device(args.device)
    gen = torch.Generator().manual_seed(args.seed)
    torch.manual_seed(args.seed)

    data = torch.load(args.graph, weights_only=False)
    print(data, "| device:", device)

    data.x = torch.as_tensor(build_features(data, args.feat_mode)).float()
    src, dst = data.edge_index
    out_deg = degree(src, data.num_nodes)                     # directed degrees for hard-neg bins
    in_deg = degree(dst, data.num_nodes)
    if args.undirected:
        from torch_geometric.utils import to_undirected
        data.edge_index = to_undirected(data.edge_index, num_nodes=data.num_nodes)
        print("symmetrized edge_index for message passing (degree features stay directed)")
    in_dim = data.x.size(1)

    # ---- training target set: subsample negatives (or keep all if --neg-ratio 0) ----
    if args.neg_ratio and args.neg_ratio > 0:
        train_input, n_pos, n_neg = build_train_inputs(
            data.y, data.train_mask, out_deg, in_deg, args.neg_ratio, args.hard_frac, generator=gen)
        input_nodes = train_input
        print(f"train targets subsampled: pos={n_pos} neg={n_neg} "
              f"(ratio {n_neg/max(n_pos,1):.1f}:1, hard_frac={args.hard_frac})")
    else:
        yt = data.y[data.train_mask]
        n_pos, n_neg = int(yt.sum()), int(len(yt) - yt.sum())
        input_nodes = data.train_mask
        print(f"train targets: ALL pos={n_pos} neg={n_neg}")
    n_train_targets = int(input_nodes.numel()) if input_nodes.dtype != torch.bool else int(input_nodes.sum())
    pos_weight = torch.tensor([n_neg / max(n_pos, 1.0)], device=device)
    print(f"pos_weight={pos_weight.item():.1f} | loss={args.loss}"
          + (f" gamma={args.focal_gamma}" if args.loss == "focal" else ""))

    def loader(nodes, shuffle):
        return NeighborLoader(data, num_neighbors=[15, 10], batch_size=2048,
                              input_nodes=nodes, shuffle=shuffle)

    train_loader = loader(input_nodes, shuffle=True)
    val_loader = loader(data.val_mask, shuffle=False)
    test_loader = loader(data.test_mask, shuffle=False)

    model = SAGE(in_dim, args.hidden, activation=args.activation, aggr=args.aggr).to(device)
    opt = torch.optim.Adam(model.parameters(), lr=0.005, weight_decay=5e-4)

    def compute_loss(logit, target):
        if args.loss == "focal":
            return focal_loss(logit, target, args.focal_gamma, pos_weight)
        return F.binary_cross_entropy_with_logits(logit, target, pos_weight=pos_weight)

    def train_epoch():
        model.train()
        total = 0.0
        for batch in train_loader:
            batch = batch.to(device)                          # (was a no-op typo -> broke on cuda)
            opt.zero_grad()
            logit = model(batch.x, batch.edge_index)[:batch.batch_size]
            loss = compute_loss(logit, batch.y[:batch.batch_size].float())
            loss.backward()
            opt.step()
            total += float(loss.detach()) * batch.batch_size
        return total / max(n_train_targets, 1)

    @torch.no_grad()
    def evaluate(loader):
        model.eval()
        scores, ys = [], []
        for batch in loader:
            batch = batch.to(device)
            logit = model(batch.x, batch.edge_index)[:batch.batch_size]
            scores.append(torch.sigmoid(logit).cpu().numpy())
            ys.append(batch.y[:batch.batch_size].cpu().numpy())
        s, y = np.concatenate(scores), np.concatenate(ys)
        return roc_auc_score(y, s), average_precision_score(y, s)

    for epoch in range(1, args.epochs + 1):
        loss = train_epoch()
        v_roc, v_pr = evaluate(val_loader)
        print(f"epoch {epoch:02d} | loss {loss:.4f} | val ROC {v_roc:.4f} | val PR {v_pr:.4f}")

    t_roc, t_pr = evaluate(test_loader)
    print(f"\nTEST  ROC-AUC {t_roc:.4f} | PR-AUC {t_pr:.4f}  "
          f"(feat_mode={args.feat_mode}; compare ROC to GBM 0.65-0.69)")

    @torch.no_grad()
    def all_embeddings():
        model.eval()
        full = NeighborLoader(data, num_neighbors=[15, 10], batch_size=4096,
                              input_nodes=None, shuffle=False)
        embs = torch.empty(data.num_nodes, args.hidden)
        for batch in full:
            _, emb = model(batch.x.to(device), batch.edge_index.to(device), return_emb=True)
            n = batch.batch_size
            embs[batch.n_id[:n]] = emb[:n].cpu()
        return embs

    stem = f"{os.path.dirname(args.graph)}/sage_{args.feat_mode}"
    torch.save(model.state_dict(), f"{stem}.pt")
    np.save(f"{stem}_emb.npy", all_embeddings().numpy())
    print(f"saved {stem}.pt and {stem}_emb.npy")


if __name__ == "__main__":
    main()
