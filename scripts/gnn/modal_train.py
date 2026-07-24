"""
Modal wrapper for the full-graph GNN run. Builds graph.pt from parquet in a Volume, trains
GraphSAGE on a GPU, writes weights + embeddings back to the Volume. The actual logic lives in
build_pyg_data.py / train_sage.py (added to the image, run as subprocesses) — unchanged.

One-time data upload (from the Mac, the 3 files build_pyg_data needs):
    modal volume create gnn-data   # if it doesn't exist
    modal volume put gnn-data /Volumes/miniext/gnn_export/node_feats.parquet /node_feats.parquet
    modal volume put gnn-data /Volumes/miniext/gnn_export/edges.parquet      /edges.parquet
    modal volume put gnn-data /Volumes/miniext/gnn_export/labels.parquet     /labels.parquet

Run (from repo root):
    modal run scripts/gnn/modal_train.py --feat-mode degree

Fetch results:
    modal volume get gnn-data /sage_degree.pt     ./
    modal volume get gnn-data /sage_degree_emb.npy ./
"""
import subprocess
import modal

# --- the container image: CUDA torch + pyg-lib (the fiddly part), built once and cached --------
# cu126 verified to have BOTH torch 2.12.1 (matches local) and pyg-lib 0.7.0+pt212cu126 wheels.
# (cu121 tops out at torch 2.5.1; torch 2.12 only ships on cu126+.) If you bump torch, re-check
# the matching CUDA tag at https://download.pytorch.org/whl/ and https://data.pyg.org/whl/ .
image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install("torch==2.12.1", index_url="https://download.pytorch.org/whl/cu126")
    .pip_install("pyg-lib", find_links="https://data.pyg.org/whl/torch-2.12.0+cu126.html")
    .pip_install("torch_geometric==2.8.0", "ogb", "scikit-learn", "pyarrow", "pandas", "numpy",
                 "faiss-cpu")
    .add_local_file("src/scripts/build_pyg_data.py", "/root/build_pyg_data.py")
    .add_local_file("src/scripts/train_sage.py", "/root/train_sage.py")
    .add_local_file("src/scripts/cluster_embeddings.py", "/root/cluster_embeddings.py")
    .add_local_file("src/scripts/validate_discovery.py", "/root/validate_discovery.py")
    .add_local_file("src/scripts/build_hetero_data.py", "/root/build_hetero_data.py")
    .add_local_file("src/scripts/train_hetero.py", "/root/train_hetero.py")
    .add_local_file("src/scripts/train_sage_unsup.py", "/root/train_sage_unsup.py")
    .add_local_file("src/scripts/eval_embeddings.py", "/root/eval_embeddings.py")
    .add_local_file("src/scripts/build_kcore.py", "/root/build_kcore.py")
    .add_local_file("src/scripts/infer_embeddings.py", "/root/infer_embeddings.py")
)

app = modal.App("gnn-fraud")
vol = modal.Volume.from_name("gnn-data", create_if_missing=True)

# A10G (24GB) is enough — neighbor sampling keeps GPU memory low. The real constraint is HOST RAM
# (the 21.6GB int64 edge tensor), so request 64GB. timeout 2h covers build + train comfortably.
def _peak_gb():
    import resource
    return resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss / 1024 / 1024

# BUILD: CPU-only, big RAM (the 115GB peak is torch.save doubling the 21.6GB edge tensor).
# Run only when parquet/labels change; writes /data/graph.pt to the Volume.
@app.function(image=image, memory=147456, timeout=7200, volumes={"/data": vol})
def build(cutoff: str = "2026-05-11"):
    subprocess.run([
        "python", "/root/build_pyg_data.py",
        "--dir", "/data", "--labels", "/data/labels.parquet", "--out", "/data/graph.pt",
        "--cutoff", cutoff,
    ], check=True)
    print(f"PEAK child RSS (build): {_peak_gb():.1f} GB")
    vol.commit()

# TRAIN: GPU, modest RAM — just loads the existing /data/graph.pt (no rebuild).
@app.function(image=image, gpu="A10G", memory=65536, timeout=7200, volumes={"/data": vol})
def train(feat_mode: str = "degree", epochs: int = 6):
    subprocess.run([
        "python", "/root/train_sage.py",
        "--graph", "/data/graph.pt", "--feat-mode", feat_mode, "--device", "cuda",
        "--epochs", str(epochs),
    ], check=True)
    print(f"PEAK child RSS (train): {_peak_gb():.1f} GB")
    vol.commit()   # persist sage_*.pt + sage_*_emb.npy
    print("done — fetch sage_%s.pt / sage_%s_emb.npy from the gnn-data volume" % (feat_mode, feat_mode))


# M4 DISCOVERY: cluster embeddings -> ranked candidate rings (CPU + big RAM, no GPU).
# Emits /data/fraud_clusters.json (small) for the web subpage. cpu=16 so faiss k-means + the edge
# density pass use all cores (was single-threaded: MiniBatchKMeans loop + np.add.at).
@app.function(image=image, cpu=16, memory=65536, timeout=7200, volumes={"/data": vol})
def cluster(embedding: str = "degree", k: int = 20000, top: int = 120):
    subprocess.run([
        "python", "/root/cluster_embeddings.py",
        "--emb", f"/data/sage_{embedding}_emb.npy", "--weights", f"/data/sage_{embedding}.pt",
        "--dir", "/data", "--labels", "/data/labels.parquet", "--out", "/data/fraud_clusters.json",
        "--k", str(k), "--top", str(top),
    ], check=True)
    print(f"PEAK child RSS (cluster): {_peak_gb():.1f} GB")
    vol.commit()   # persist fraud_clusters.json


# (run cluster directly: modal run scripts/gnn/modal_train.py::cluster --embedding degree)


# #2 DISCOVERY VALIDATION: temporal future-label test of the discovery pipeline (CPU + big RAM).
# Splits labels at --cutoff (graph snapshot date), ranks candidates using KNOWN-only labels, and
# measures precision/recall/lift against fraud flagged AFTER the cutoff. Emits discovery_val.json.
@app.function(image=image, cpu=16, memory=65536, timeout=7200, volumes={"/data": vol})
def validate(embedding: str = "degree", k: int = 20000, cutoff: str = "2026-05-11"):
    subprocess.run([
        "python", "/root/validate_discovery.py",
        "--emb", f"/data/sage_{embedding}_emb.npy", "--weights", f"/data/sage_{embedding}.pt",
        "--dir", "/data", "--labels", "/data/labels.parquet", "--edges", "/data/edges.parquet",
        "--cutoff", cutoff, "--k", str(k), "--out", f"/data/discovery_val_{embedding}.json",
    ], check=True)
    print(f"PEAK child RSS (validate): {_peak_gb():.1f} GB")
    vol.commit()   # persist discovery_val_*.json

# (run: modal run scripts/gnn/modal_train.py::validate --embedding degree)


# HETERO EXPERIMENT: multi-relational (follows + blocks) GraphSAGE. build_hetero (CPU big RAM) then
# train_hetero (GPU). Needs blocks.parquet uploaded to the Volume alongside the other parquets.
@app.function(image=image, memory=147456, timeout=7200, volumes={"/data": vol})
def build_hetero(cutoff: str = "2026-07-07", relations: str = "follows,blocks"):
    subprocess.run([
        "python", "/root/build_hetero_data.py", "--dir", "/data",
        "--labels", "/data/labels.parquet", "--out", "/data/graph_hetero.pt",
        "--cutoff", cutoff, "--relations", relations,
    ], check=True)
    print(f"PEAK child RSS (build_hetero): {_peak_gb():.1f} GB")
    vol.commit()

@app.function(image=image, gpu="A10G", memory=98304, timeout=7200, volumes={"/data": vol})
def train_hetero(feat_mode: str = "degree", epochs: int = 3, aggr: str = "mean"):
    # num-workers=0: this script builds SEPARATE train/val/test loaders (like train_sage.py). With
    # workers, each NeighborLoader share_memory's its OWN full-graph CSC and the 2nd exhausts the
    # backing store on the 3-relation graph. Serial sampling instead; use the shared-sampler refactor
    # (one NeighborSampler reused across splits) if sampling ever becomes the bottleneck.
    subprocess.run([
        "python", "/root/train_hetero.py", "--graph", "/data/graph_hetero.pt",
        "--feat-mode", feat_mode, "--device", "cuda", "--epochs", str(epochs), "--aggr", aggr,
        "--num-workers", "0",
    ], check=True)
    print(f"PEAK child RSS (train_hetero): {_peak_gb():.1f} GB")
    vol.commit()   # persist sage_hetero.pt + sage_hetero_emb.npy

# k-CORE training subgraph (CPU, big RAM: the 1.43B-edge int32 peel). Emits graph_core.pt.
@app.function(image=image, memory=98304, timeout=7200, volumes={"/data": vol})
def build_kcore(cutoff: str = "2026-07-07", k: int = 5, min_degree: int = 0):
    cmd = ["python", "/root/build_kcore.py", "--dir", "/data", "--labels", "/data/labels.parquet",
           "--out", "/data/graph_core.pt", "--cutoff", cutoff, "--k", str(k)]
    if min_degree > 0:
        cmd += ["--min-degree", str(min_degree)]
    subprocess.run(cmd, check=True)
    print(f"PEAK child RSS (build_kcore): {_peak_gb():.1f} GB")
    vol.commit()

# SELF-SUPERVISED embedding (link-prediction, no labels). Trains + saves BODY WEIGHTS only
# (--no-infer) so a slow full-graph inference can't lose the training on timeout. cpu=16 +
# num_workers = parallel neighbor sampling (the throughput fix for the 2h timeout).
@app.function(image=image, gpu="A10G", cpu=16, memory=98304, timeout=14400, volumes={"/data": vol})
def train_unsup(feat_mode: str = "full", epochs: int = 3, edges_per_epoch: int = 15_000_000,
                graph: str = "graph_core.pt"):
    subprocess.run([
        "python", "/root/train_sage_unsup.py", "--graph", f"/data/{graph}",
        "--feat-mode", feat_mode, "--device", "cuda", "--epochs", str(epochs),
        "--edges-per-epoch", str(edges_per_epoch), "--num-workers", "16", "--no-infer",
    ], check=True)
    print(f"PEAK child RSS (train_unsup): {_peak_gb():.1f} GB")
    vol.commit()   # persist sage_unsup.pt (body weights)

# INDUCTIVE inference: embed the full graph with the trained body weights (separate step, own timeout).
@app.function(image=image, gpu="A10G", cpu=16, memory=98304, timeout=14400, volumes={"/data": vol})
def infer_unsup(feat_mode: str = "full", graph: str = "graph.pt", stem: str = "sage_unsup"):
    subprocess.run([
        "python", "/root/infer_embeddings.py", "--weights", f"/data/{stem}.pt",
        "--graph", f"/data/{graph}", "--feat-mode", feat_mode, "--out-stem", f"/data/{stem}",
        "--device", "cuda", "--num-workers", "16",
    ], check=True)
    print(f"PEAK child RSS (infer_unsup): {_peak_gb():.1f} GB")
    vol.commit()   # persist sage_unsup.pt (+head) + sage_unsup_emb.npy

# FAST clustering-free embedding eval (CPU, cheap) — linear probe + nearest-known-fraud-centroid.
# Use this to iterate; it replaces validate's KMeans(20000)+1.43B-edge streaming (the timeout).
@app.function(image=image, memory=65536, timeout=3600, volumes={"/data": vol})
def eval_emb(embedding: str = "unsup", cutoff: str = "2026-07-07"):
    subprocess.run([
        "python", "/root/eval_embeddings.py", "--emb", f"/data/sage_{embedding}_emb.npy",
        "--dir", "/data", "--labels", "/data/labels.parquet", "--cutoff", cutoff,
        "--device", "cpu", "--out", f"/data/emb_eval_{embedding}.json",
    ], check=True)
    vol.commit()
# k-core self-supervised run (train small, embed all — each step persists independently):
#   modal run scripts/gnn/modal_train.py::build_kcore --k 5 --cutoff 2026-07-07
#   modal run scripts/gnn/modal_train.py::train_unsup --graph graph_core.pt        # trains, saves weights
#   modal run scripts/gnn/modal_train.py::infer_unsup --graph graph.pt             # embeds full graph
#   modal run scripts/gnn/modal_train.py::eval_emb   --embedding unsup --cutoff 2026-07-07

# hetero run:  modal run scripts/gnn/modal_train.py::build_hetero --cutoff 2026-07-07
#         then modal run scripts/gnn/modal_train.py::train_hetero --feat-mode degree
#         then ::cluster/::validate with --embedding hetero


@app.local_entrypoint()
def main(feat_mode: str = "degree", epochs: int = 3, do_build: bool = False,
         cutoff: str = "2026-05-11"):
    # graph.pt already on the Volume -> default skips the 115GB rebuild. Pass --do-build after a
    # snapshot/label refresh to regenerate it first; --cutoff must be the new snapshot_date.
    if do_build:
        build.remote(cutoff=cutoff)
    train.remote(feat_mode=feat_mode, epochs=epochs)
