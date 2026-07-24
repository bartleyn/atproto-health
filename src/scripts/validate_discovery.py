#!/usr/bin/env python3
"""
#2 FUTURE-LABEL VALIDATION for the fraud-cluster DISCOVERY pipeline.

The GNN lost as a classifier; its job now is DISCOVERY — surfacing coordinated accounts the
labelers/per-node models miss. The only honest test of discovery is temporal, mirroring what
temporal-CV did for the classifier: cluster/score using ONLY labels known at a cutoff T, then
check whether the accounts we surface get INDEPENDENTLY flagged AFTER T.

Split the label set at T (default 2026-05-11, the graph-snapshot date — after which the
embeddings' supervised head could no longer see labels):
    known    = is_fraud & first_labeled <= T   -> drives cluster enrichment (what we "knew")
    future   = is_fraud & first_labeled  > T    -> HELD-OUT ground truth (did we discover it?)
    excluded = farms                            -> dropped from the population entirely
Candidates = non-excluded nodes NOT known at T.  is_hit = future  (future ⊂ candidates).

Metrics (all vs the future-fraud base rate among candidates = lift):
  * NODE level   — rank candidates by model score sigmoid(emb·head); precision/recall@k.
  * CLUSTER level — rank clusters by KNOWN-only enrichment (x log density if --edges given);
                    do the previously-unlabeled members of enriched/dense clusters become
                    future-fraud above base rate? This is the "ring discovery" claim.
  * NOVELTY      — dense clusters with LOW known-rate: do THEIR members catch future fraud?
                   (the net-new-ring claim the production ranking currently fights.)

Runs on Modal (embeddings live on the gnn-data Volume); see modal_train.py::validate. Locally
it runs on the 1.37M-node sample (`/Volumes/miniext/gnn_export/sample/`).

  python validate_discovery.py --emb /data/sage_degree_emb.npy --weights /data/sage_degree.pt \
      --dir /data --labels /data/labels.parquet --cutoff 2026-05-11 --k 20000 \
      [--edges /data/edges.parquet] [--assign /data/assign_degree.npy] --out /data/discovery_val.json
"""
import os
os.environ.setdefault("KMP_DUPLICATE_LIB_OK", "TRUE")   # faiss + torch each ship libomp (macOS clash)
try:                     # import faiss BEFORE torch: on macOS torch-then-faiss segfaults (libomp)
    import faiss
    faiss.omp_set_num_threads(os.cpu_count() or 8)
except ImportError:
    faiss = None
import argparse, json, datetime, sys
import numpy as np


def _kmeans(emb, k, seed=42):
    """Multi-threaded k-means (faiss, all cores); falls back to single-threaded MiniBatchKMeans."""
    if faiss is not None:
        km = faiss.Kmeans(emb.shape[1], k, niter=25, seed=seed, verbose=True)
        km.train(emb.astype(np.float32))
        return km.index.search(emb.astype(np.float32), 1)[1].ravel().astype(np.int64)
    from sklearn.cluster import MiniBatchKMeans
    return MiniBatchKMeans(n_clusters=k, batch_size=100_000, n_init=1, max_iter=50,
                           max_no_improvement=20, random_state=seed).fit_predict(emb).astype(np.int64)


# --------------------------- pure, unit-testable metric helpers ----------------------------------

def precision_recall_at_k(score: np.ndarray, is_hit: np.ndarray, ks):
    """Rank items by `score` desc; return precision@k, recall@k, lift@k vs the base hit rate.

    score, is_hit are 1-D arrays over the CANDIDATE set (same length). is_hit is bool.
    """
    n = score.shape[0]
    total_hits = int(is_hit.sum())
    base = total_hits / n if n else 0.0
    order = np.argsort(-score, kind="stable")
    hit_sorted = is_hit[order].astype(np.int64)
    cum = np.cumsum(hit_sorted)
    out = []
    for k in ks:
        k = min(k, n)
        if k == 0:
            continue
        tp = int(cum[k - 1])
        prec = tp / k
        rec = tp / total_hits if total_hits else 0.0
        out.append({"k": k, "precision": round(prec, 5), "recall": round(rec, 4),
                    "lift": round(prec / base, 2) if base else None, "tp": tp})
    return {"n_candidates": n, "n_future_hits": total_hits, "base_rate": round(base, 6), "at_k": out}


def cluster_arrays(assign: np.ndarray, known: np.ndarray, future: np.ndarray,
                   excluded: np.ndarray, k: int, intra: np.ndarray | None = None):
    """Per-cluster stats using KNOWN-only enrichment. Returns a dict of length-k arrays.

    cand = ~excluded & ~known (unlabeled-at-T, non-farm). future ⊂ cand.
    known_enrich = (known/size) / base_known ;  future_rate = future/cand (the discovery target).
    """
    cand = (~excluded) & (~known)
    size = np.bincount(assign, minlength=k).astype(np.int64)
    known_cnt = np.bincount(assign[known], minlength=k).astype(np.int64)
    cand_cnt = np.bincount(assign[cand], minlength=k).astype(np.int64)
    fut_cnt = np.bincount(assign[future], minlength=k).astype(np.int64)
    nonexcl = int((~excluded).sum())
    base_known = known.sum() / nonexcl if nonexcl else 0.0
    known_rate = known_cnt / np.maximum(size, 1)
    known_enrich = known_rate / max(base_known, 1e-12)
    future_rate = fut_cnt / np.maximum(cand_cnt, 1)  # among previously-unlabeled members
    density = (intra / np.maximum(size, 1)) if intra is not None else np.zeros(k)
    return {"size": size, "known_cnt": known_cnt, "cand_cnt": cand_cnt, "fut_cnt": fut_cnt,
            "known_enrich": known_enrich, "future_rate": future_rate, "density": density,
            "base_known": base_known}


def leakfree_classification(node_score, future, known, excluded):
    """Strict-boundary leak-free CLASSIFICATION ROC/PR from the saved embeddings' head score.
    Positives = future (fraud first-labeled strictly AFTER the cutoff); negatives = all non-excluded,
    non-known candidates. Unlike train_sage's test mask (which uses `>= cutoff` at UTC-midnight and a
    random 15% of negatives), this uses validate's strict `> cutoff` boundary and the FULL candidate
    pool -> the honest, conservative version of the reported classification number. ROC is directly
    comparable; PR-AUC prevalence differs (full pool vs 15% holdout), so read PR as lift over base."""
    from sklearn.metrics import roc_auc_score, average_precision_score
    cand = (~excluded) & (~known)
    y, s = future[cand].astype(int), node_score[cand]
    n_pos = int(y.sum())
    if n_pos == 0 or n_pos == len(y):
        return {"n_future_pos": n_pos, "n_candidates": int(len(y)), "roc_auc": None, "pr_auc": None}
    return {"n_future_pos": n_pos, "n_candidates": int(len(y)),
            "prevalence": round(y.mean(), 6),
            "roc_auc": round(float(roc_auc_score(y, s)), 4),
            "pr_auc": round(float(average_precision_score(y, s)), 4)}


def cluster_candidate_ranking(assign, node_score, known, future, excluded, ca, min_size=8,
                              use_density=False, novelty=False):
    """Mirror the page's selection: pick clusters (by known-enrichment x logdensity, or by
    density with LOW known-rate for `novelty`), then rank their unlabeled members by node_score.
    Return precision/recall@k of that surfaced candidate stream against future fraud."""
    k = ca["size"].shape[0]
    if novelty:
        # dense but under-labeled: high density, known_enrich <= 1 (not already a known ring)
        rank = np.log1p(ca["density"]) * (ca["known_enrich"] <= 1.0)
    else:
        rank = ca["known_enrich"] * (np.log1p(ca["density"]) if use_density else 1.0)
    rank = rank.copy()
    rank[ca["size"] < min_size] = -1.0
    cluster_order = np.argsort(-rank, kind="stable")
    cand = (~excluded) & (~known)
    # walk clusters best-first, emit their candidate members ordered by node_score within cluster
    cluster_rank_of = np.empty(k, dtype=np.int64)
    cluster_rank_of[cluster_order] = np.arange(k)
    cand_idx = np.where(cand)[0]
    # sort candidates by (their cluster's rank, then -node_score)
    primary = cluster_rank_of[assign[cand_idx]]
    order = np.lexsort((-node_score[cand_idx], primary))
    ordered = cand_idx[order]
    is_hit = future[ordered]
    ks = [100, 500, 1000, 5000, 10000, 50000]
    return precision_recall_at_k(np.arange(len(ordered), 0, -1).astype(np.float64), is_hit, ks)


# --------------------------------- self test (synthetic) -----------------------------------------

def self_test():
    rng = np.random.default_rng(0)
    N, K = 60000, 300
    assign = rng.integers(0, K, N)
    excluded = rng.random(N) < 0.05
    hot = {7, 42, 99}
    is_hot = np.array([a in hot for a in assign])
    # realistic discovery premise: hot clusters are PARTIALLY-known rings at T (elevated known)
    # that keep growing (elevated future). Both the known signal and the future target concentrate
    # in the same clusters -> known-enrichment ranking should surface them.
    known = ((rng.random(N) < np.where(is_hot, 0.10, 0.015)) & ~excluded)
    latent = rng.random(N)
    future = np.zeros(N, bool)
    for i in range(N):
        if excluded[i] or known[i]:
            continue
        p = 0.15 if is_hot[i] else 0.001
        p *= (0.3 + latent[i])            # score-correlated
        future[i] = rng.random() < p
    # real head scores fraud-STRUCTURED nodes high (embedding encodes the ring) -> correlated with
    # cluster membership, plus a per-node component. noisy, not oracle.
    node_score = 0.6 * is_hot + 0.4 * latent + rng.normal(0, 0.15, N)

    cand = (~excluded) & (~known)
    base = future[cand].mean()
    node_m = precision_recall_at_k(node_score[cand], future[cand], [100, 1000, 10000])
    assert node_m["at_k"][0]["lift"] > 3, f"node lift too low: {node_m}"          # score works
    ca = cluster_arrays(assign, known, future, excluded, K)
    # the 3 hot clusters must be the most future-enriched among sizeable clusters
    big = ca["cand_cnt"] >= 20
    top3 = set(np.argsort(-np.where(big, ca["future_rate"], 0))[:3].tolist())
    assert hot <= top3, f"cluster future_rate failed to surface planted rings: {top3}"
    cl_m = cluster_candidate_ranking(assign, node_score, known, future, excluded, ca, min_size=8)
    assert cl_m["at_k"][0]["lift"] > 3, f"cluster-ranked lift too low: {cl_m}"
    # base-rate sanity
    assert abs(node_m["base_rate"] - base) < 1e-6
    print(f"self-test OK: base_rate={base:.4%} node_lift@100={node_m['at_k'][0]['lift']} "
          f"cluster_lift@100={cl_m['at_k'][0]['lift']}")


# ---------------------------------------- real run -----------------------------------------------

def load_node_dids(dir_path):
    import pyarrow.parquet as pq
    nf = pq.read_table(f"{dir_path}/node_feats.parquet").to_pandas()
    idx_col = "sidx" if "sidx" in nf.columns else "node_idx"
    nf = nf.sort_values(idx_col).reset_index(drop=True)  # row i == emb row i
    return nf["did"].to_numpy()


def load_masks(dids, labels_path, cutoff):
    import pyarrow.parquet as pq, pandas as pd
    lab = pq.read_table(labels_path).to_pandas().drop_duplicates("did").set_index("did")
    fl = pd.to_datetime(lab["first_labeled"], utc=True, errors="coerce")
    T = pd.Timestamp(cutoff, tz="UTC")
    is_fraud = lab["is_fraud"].astype(bool)
    is_excl = lab["is_excluded"].astype(bool)
    known_did = is_fraud & ~is_excl & (fl <= T)
    future_did = is_fraud & ~is_excl & (fl > T)
    s = pd.Series(dids)
    excluded = s.map(is_excl).fillna(False).to_numpy(bool)
    known = s.map(known_did).fillna(False).to_numpy(bool)
    future = s.map(future_did).fillna(False).to_numpy(bool)
    return known, future, excluded


def stream_intra_edges(edges_path, assign, k):
    import pyarrow.parquet as pq
    intra = np.zeros(k, dtype=np.int64)
    pf = pq.ParquetFile(edges_path)
    for b in pf.iter_batches(columns=["src", "dst"], batch_size=20_000_000):
        cs = assign[b.column("src").to_numpy()]
        same = cs == assign[b.column("dst").to_numpy()]
        intra += np.bincount(cs[same], minlength=k).astype(np.int64)   # C-level vs slow np.add.at
    return intra


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--emb"); ap.add_argument("--weights"); ap.add_argument("--dir")
    ap.add_argument("--labels"); ap.add_argument("--cutoff", default="2026-05-11")
    ap.add_argument("--k", type=int, default=20000)
    ap.add_argument("--assign", help="precomputed cluster assignment npy (skip re-clustering)")
    ap.add_argument("--edges", help="edges.parquet for intra-cluster density (optional)")
    ap.add_argument("--out", default="discovery_val.json")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()

    if args.self_test:
        self_test(); return
    for req in ("emb", "weights", "dir", "labels"):
        if not getattr(args, req):
            ap.error(f"--{req} required (or use --self-test)")

    print(f"loading embeddings {args.emb} ...", flush=True)
    emb = np.ascontiguousarray(np.load(args.emb)).astype(np.float32)
    N = emb.shape[0]
    dids = load_node_dids(args.dir)
    assert len(dids) == N, f"dids {len(dids)} != emb rows {N}"
    known, future, excluded = load_masks(dids, args.labels, args.cutoff)
    print(f"N={N:,}  known={known.sum():,}  future={future.sum():,}  excluded={excluded.sum():,}",
          flush=True)

    import torch
    sd = torch.load(args.weights, map_location="cpu", weights_only=True)
    W = sd["head.weight"].numpy().reshape(-1).astype(np.float32); b = float(sd["head.bias"].item())
    node_score = 1.0 / (1.0 + np.exp(-(emb @ W + b)))

    if args.assign:
        assign = np.load(args.assign).astype(np.int64); k = int(assign.max()) + 1
    else:
        print(f"clustering into k={args.k} (faiss, multi-threaded) ...", flush=True)
        assign = _kmeans(emb, args.k); k = args.k
    del emb

    intra = stream_intra_edges(args.edges, assign, k) if args.edges else None
    ca = cluster_arrays(assign, known, future, excluded, k, intra=intra)

    cand = (~excluded) & (~known)
    clf_m = leakfree_classification(node_score, future, known, excluded)
    node_m = precision_recall_at_k(node_score[cand], future[cand], [100, 500, 1000, 5000, 10000, 50000])
    cl_m = cluster_candidate_ranking(assign, node_score, known, future, excluded, ca,
                                     use_density=intra is not None)
    nov_m = cluster_candidate_ranking(assign, node_score, known, future, excluded, ca,
                                      use_density=True, novelty=True) if intra is not None else None

    result = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "embedding": args.emb.split("/")[-1], "cutoff": args.cutoff, "k": k, "n_nodes": int(N),
        "counts": {"known": int(known.sum()), "future": int(future.sum()),
                   "excluded": int(excluded.sum()), "candidates": int(cand.sum())},
        "leakfree_classification": clf_m,
        "node_ranking": node_m,
        "cluster_ranking": cl_m,
        "novelty_ranking": nov_m,
    }
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\nwrote {args.out}")
    print(f"LEAK-FREE CLASSIFICATION (strict > {args.cutoff}): ROC {clf_m['roc_auc']} | "
          f"PR {clf_m['pr_auc']}  ({clf_m['n_future_pos']} future pos / {clf_m['n_candidates']:,} cand)")
    print(f"future base rate among candidates: {node_m['base_rate']:.4%}")
    print("NODE ranking  ", [(x["k"], x["lift"]) for x in node_m["at_k"]])
    print("CLUSTER ranking", [(x["k"], x["lift"]) for x in cl_m["at_k"]])
    if nov_m:
        print("NOVELTY ranking", [(x["k"], x["lift"]) for x in nov_m["at_k"]])


if __name__ == "__main__":
    main()
