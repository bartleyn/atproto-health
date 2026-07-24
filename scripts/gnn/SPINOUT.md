# Spinning the fraud work into its own repo

The tabular + GNN fraud work is standalone Python/SQL/bash with **zero imports** from the main repo's
TypeScript. It couples to the main repo only through **data** (the shared Postgres + the DuckDB snapshot),
so extraction is a move-and-reconfigure job, not an untangle.

## Decisions (resolved)

1. **DB strategy — keep reading the shared `atproto_health` Postgres** via a dedicated **read-only role**
   (`fraud_ro`). Simplest, zero data duplication. Revisit "own DB" only if/when the main repo's schema
   churns under us. The boundary is clean: **main repo = ingestion/warehouse; fraud repo = modeling.**
2. **`fraud_features` ownership — the new repo OWNS it** (`data/build_fraud_features.sql`). It depends only
   on base tables the main repo populates (label tables + the derived analysis tables), not on any main-repo
   code. This keeps the fraud repo self-sufficient to rebuild its own feature table.
3. **Git history — fresh start.** The scripts are young and were untracked; a clean `git init` is simpler
   than `git filter-repo`, and nothing of value is lost.
4. **Name + packaging — `atproto-fraud`, loose scripts to start** (a `pyproject.toml` for deps + a small
   `config.py`, but no installable package until the layout stabilizes).

## Target layout (`atproto-fraud/`)

```
atproto-fraud/
  README.md  pyproject.toml  .env.example  config.py
  data/      export_graph.sql  export_labels.sh  build_fraud_features.sql
  tabular/   fraud_model.py  fraud_temporal.py  fraud_strict.py  fraud_diag.py
  gnn/       build_pyg_data.py  train_sage.py  cluster_embeddings.py  modal_train.py
  inference/ score_did.py
  examples/  m0_toy_sage.py
  docs/      DATA_CONTRACT.md  RESULTS.md
```

## File move map (current → new)

| current (in atproto-health) | new (in atproto-fraud) |
|---|---|
| `scripts/build_fraud_features.sql` | `data/build_fraud_features.sql` |
| `scripts/gnn/export_labels.sh` | `data/export_labels.sh` |
| *(M1 DuckDB export, currently inline)* → extract to | `data/export_graph.sql` |
| `scripts/fraud_model.py` `fraud_temporal.py` `fraud_strict.py` `fraud_diag.py` | `tabular/` |
| `src/scripts/build_pyg_data.py` `train_sage.py` | `gnn/` |
| `scripts/gnn/modal_train.py` `requirements.txt` `README.md` | `gnn/` + repo root |
| `src/scripts/m0_toy_sage.py` | `examples/` |
| *(M4, to be written)* | `gnn/cluster_embeddings.py` |
| *(Tier-1 inference, to be written)* | `inference/score_did.py` |

## Data contract (what the repo READS — goes in `docs/DATA_CONTRACT.md`)

Postgres (`atproto_health`, read-only):
- `plc.bsky_mod_labels`, `plc.skywatch_labels`, `plc.hailey_labels` — labels (`did, label, labeled_at`)
- `analysis.excluded_dids` — farm/handle-generator DIDs (population exclusion)
- `analysis.account_graph_snapshot`, `analysis.cohort_base` — derived; **built by the main repo**
- `activity.did_activity_daily`, `activity.post_deletes_daily` — live activity tempo
- `plc.plc_account_creations` — creation-burst feature

DuckDB snapshot (file): `/Volumes/miniext/snapshot.duckdb` → `actors`, `actor_aggs`, `follows`.

Modal: `gnn-data` Volume (account-scoped; carries over as-is).

These derived/base tables are the contract surface — if the main repo renames them, the fraud repo breaks.
List them in `DATA_CONTRACT.md` so the dependency is explicit.

## Phased steps

1. **Generate `DATA_CONTRACT.md`** by grepping the SQL for `schema.table` references (mechanical).
2. **Scaffold** `atproto-fraud` (the layout above) + `pyproject.toml` (torch_geometric, ogb, sklearn,
   pyarrow, pandas, psycopg2-binary; torch + pyg-lib via the cu126 wheel indexes — see gnn/README).
3. **Move** the scripts per the map; create `config.py` for `DATABASE_URL`, `SNAPSHOT_PATH`, `EXPORT_DIR`.
4. **Decouple config** — replace `export_labels.sh`'s `git rev-parse --show-toplevel` `.env` lookup and the
   hardcoded `/Volumes/...` paths with `config.py` / env vars.
5. **Provision** the `fraud_ro` read-only Postgres role; point `.env` at it.
6. **README runbook** — build_features → export_graph/labels → build_pyg_data → train_sage/modal_train →
   fraud_strict → score_did, end to end.

## Sequencing note

**Finish the science before packaging** so `RESULTS.md` ships with the final verdict:
- run the queued GNN `full` + `stable` on the strict full graph (does structure+features beat tabular 0.70?)
- then M4 discovery, then Tier-1 inference.
Bundle the standing results: tabular GBM-full **0.700** vs GNN structure-only **0.651** on the strict
leak-free split (989 test pos); the directed/feature/cutoff ablations; the inductive-refresh + inference
tiers.
