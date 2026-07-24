# GNN fraud detection — runbook

GraphSAGE fraud detector on the atproto follow graph (24.68M nodes, 1.35B edges). Built as a
learning exercise alongside the tabular models (`scripts/fraud_model.py`). Plan:
`~/.claude/plans/ticklish-beaming-noodle.md`.

**Why a GNN:** the tabular models use per-node aggregates (`follow_ratio`, `blocks_in`) and throw away
edge structure. Honest stable AUC there is ~0.65–0.69. A GNN consumes the relational structure directly
via message passing. The eval is explicitly designed to test whether it learns more than the ratios
(skywatch's labels *are* graph heuristics → circularity risk).

## Environment

```bash
python3.12 -m venv /tmp/gnn_venv
/tmp/gnn_venv/bin/pip install torch torch_geometric ogb scikit-learn
# NeighborLoader needs a sampling backend (pyg-lib). Mac arm64 / torch 2.12:
/tmp/gnn_venv/bin/pip install pyg-lib -f https://data.pyg.org/whl/torch-2.12.0+cpu.html
# On the GPU box (Linux+CUDA) use the matching CUDA index instead, e.g.:
#   pip install pyg-lib -f https://data.pyg.org/whl/torch-2.12.0+cu121.html
```

Versions: torch 2.12.1, pyg 2.8.0, pyg-lib 0.7.0+pt212. MPS used locally; CUDA on the spot box.

Two gotchas:
- torch ≥ 2.6 defaults `torch.load(weights_only=True)` → OGB's cached graph fails to unpickle. Fix:
  `torch.serialization.add_safe_globals([DataEdgeAttr, DataTensorAttr, GlobalStorage])`.
- `NeighborLoader` raises `requires pyg-lib or torch-sparse` until the wheel above is installed.

## Data artifacts (M1 — built from the DuckDB snapshot)

Source: `/Volumes/miniext/snapshot.duckdb` (constellation backfill, snapshot_date 2026-05-11).
Output dir: `/Volumes/miniext/gnn_export/` (external drive — Mac internal is too small).

| file | rows | notes |
|---|---|---|
| `node_map.parquet` | 24,678,767 | **canonical** `node_idx` 0..N-1 (row_number over `actors`). Everything joins to this. |
| `node_feats.parquet` | 24.68M × 11 | raw `actor_aggs` feats, ordered by `node_idx` (row position == idx) |
| `edges.parquet` | 1,348,852,443 | remapped **int32** `(src, dst)`; 0 dropped |
| `labels.parquet` | 3,491,097 | `did, is_fraud, is_excluded, first_labeled` |

Key counts: 25,790 fraud positives present in the graph and not farms (~0.10%); 3.45M excluded farms
(2.15M of them exist as sparse graph nodes, masked out via `is_excluded`).

### Rebuild the artifacts

The remapping is done **in DuckDB** (not Python) so edges export pre-remapped int32 — avoids a 21 GB
int64 array in RAM. DuckDB pragmas: `memory_limit='10GB'; threads=4; preserve_insertion_order=false;
temp_directory='/Volumes/miniext/duckdb_tmp'`.

1. `node_map.parquet`: `row_number() OVER (ORDER BY did_id) - 1` over `actors`.
2. `node_feats.parquet`: `node_map ⋈ actor_aggs USING(did_id)`, `ORDER BY node_idx`.
3. `edges.parquet`: `follows ⋈ node_map (src) ⋈ node_map (dst)`, emit `int32`, **no ORDER BY**.
4. `labels.parquet`: `psql \copy` the fraud-union (reuse `scripts/build_fraud_features.sql:14-26` +
   `first_labeled` = `MIN(skywatch.labeled_at)` from `scripts/fraud_temporal.py`) → CSV → DuckDB parquet.

## Milestones

- **M0 ✅** mechanics warm-up on ogbn-arxiv (`src/scripts/m0_toy_sage.py`). Lesson: follow edges are
  **directed & asymmetric** — out-degree (bulk-following) is the farm tell, in-degree is reputation.
  Do not blindly `ToUndirected` for fraud.
- **M1 ✅** export the graph (above).
- **M2** `scripts/gnn/build_pyg_data.py` — build PyG `Data`: log1p+standardize feats, attach `y` via
  `labels ⋈ node_map` on `did`, masks (`is_excluded` dropped; temporal train/test from `first_labeled`),
  decide directed vs undirected.
- **M3** `scripts/gnn/train_sage.py` — supervised GraphSAGE on the GPU spot box. **Circularity ablation:**
  (a) structure-only vs (b) structure+aggregates, both vs the GBM baseline 0.65–0.69 on the temporal test.
- **M4** `scripts/gnn/cluster_embeddings.py` — cluster embeddings → candidate rings, cross-labeler validation.

## Full-graph run on Modal

Provider: **Modal** (serverless GPU, per-second billing, auto-shutdown — no box to forget). Driver:
`scripts/gnn/modal_train.py` (A10G + 64 GB host RAM; A10G is enough since neighbor sampling keeps GPU mem
low — the binding constraint is the 21.6 GB int64 edge tensor in host RAM). Locked config: **directed,
`--feat-mode degree`, 2-hop [15,10], CUTOFF=2026-05-11.**

**CUDA wheels (the one fiddly part, already solved):** torch 2.12 only ships on **cu126**+ (cu121 froze at
2.5.1), and `pyg-lib 0.7.0+pt212cu126` matches it. The image pins both. If you bump torch, re-verify the
tag has BOTH at https://download.pytorch.org/whl/ and https://data.pyg.org/whl/ .

Sequence (run AFTER the label pull settles):

1. Refresh labels locally (graph artifacts unchanged, only labels): `bash scripts/gnn/export_labels.sh`.
2. Upload the 3 parquets build_pyg_data needs to the Volume (once; `edges.parquet` 5.2 GB is the slow one):
   ```bash
   modal volume create gnn-data
   modal volume put gnn-data /Volumes/miniext/gnn_export/node_feats.parquet /node_feats.parquet
   modal volume put gnn-data /Volumes/miniext/gnn_export/edges.parquet      /edges.parquet
   modal volume put gnn-data /Volumes/miniext/gnn_export/labels.parquet     /labels.parquet
   ```
3. **Dry run** (build + 1 epoch — validates image/CUDA, 64 GB RAM build, GPU loader for <$0.50):
   ```bash
   modal run scripts/gnn/modal_train.py --feat-mode degree --epochs 1
   ```
   build prints ~24.68M nodes / 1.35B edges / ~901+ leak-free test pos. OOM → bump `memory=98304`.
4. **Full run:** `modal run scripts/gnn/modal_train.py --feat-mode degree`
5. Fetch results: `modal volume get gnn-data /sage_degree.pt ./` and `/sage_degree_emb.npy` (for M4).

Approx cost: a few $ per run. (Fill exact instance/$ after first run.)

## Phase 2 — inductive refresh pipeline (design only; build AFTER M3 beats the baseline)

GraphSAGE is **inductive**: it embeds a brand-new account via `f(current features, current neighbors)`
using frozen weights — no retraining. Three refresh clocks:

| clock | refreshes | cadence | compute |
|---|---|---|---|
| fast | edges + degree features in PG | continuous (jetstream) | tiny |
| on-demand | score a new account | per request / nightly | **CPU, seconds** |
| slow | GraphSAGE weights | monthly / quarterly | **GPU spot** |

**Inference never needs the GPU.** New PG tables: `analysis.gnn_node_map` (living, append-only
`node_idx`), `analysis.follow_edges(src_idx, dst_idx, rkey, created_at)`, `analysis.node_features`.

The one genuinely new collector: **capture follow *edges* from jetstream** (today only the activity bit
is stored, not the src→dst target).
- CREATE `app.bsky.graph.follow`: `src=commit.repo`, `dst=commit.record.subject`, `rkey`, → +edge,
  increment `src.follows` / `dst.followers`.
- DELETE carries **only `repo`+`rkey`** → you **must** store `rkey` per edge to know what to remove.
- Snapshot edges have no rkey → treat the snapshot as an **immutable base layer**; only live edges are
  deletable; the monthly re-backfill resets drift. Volume is millions/day → batch (65,534-param limit).
- Rides on the planned `wantedCollections`-filter removal; the GCP backfill is the scale alternative.

Inference job (nightly): new DIDs (last 30d) → resolve/append `node_idx` → `NeighborLoader` over the
current `follow_edges` → `model.eval()` forward pass → write `analysis.gnn_scores(did, score,
model_version, scored_at)`.

## Phase 2 — behavioral/content detector (complementary view)

The structure GNN detects **coordinated graph fraud only** (follow-farming, bulk-following, amplifier,
engagement-abuse). It is **blind** to content/LLM bots, link/domain spam (`repetitive-domain-spam-*` are
content, not structure), sleeper socks, and posting-time automation. Those need a **second detector on a
behavioral view**, then fuse: `structural embedding + behavioral features → final classifier`.

Behavioral signals already available (no new ingestion): `posts.source` (client app — bot tell), posting
cadence/burstiness/24-7 timing, reply/quote/broadcast ratios, `activity.post_deletes_daily` (ephemeral),
`activity.did_langs`, engagement-received trend. **Post text is stored nowhere** → text/LLM-gen detection
is a separate, larger ingestion — deferred.

Two rules, learned from M3:
1. **Don't mix behavioral features into the GNN's node inputs.** The M3 ablation showed features redundant
   with skywatch's heuristic cause shortcut-learning and wreck temporal generalization. Behavioral features
   are *orthogonal* to skywatch, so they *may* add non-circular lift — but keep them in a **separate
   channel** and prove it on the **temporal test AUC, never val**.
2. **Labels skew graph/engagement**, so content fraud likely has no supervision → **unsupervised/anomaly**
   (cluster a behavioral embedding, like M4 but on the behavioral view).
