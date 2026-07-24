#!/usr/bin/env bash
# Export the GNN graph artifacts (node_map + node_feats + edges) from a DuckDB snapshot.
# Re-run whenever the snapshot is regenerated; then run export_labels.sh, upload, build, train.
#   bash scripts/gnn/export_graph.sh [SNAPSHOT_DB] [OUT_DIR]
# Defaults match the project layout on the external drive.
#
# The did_id->node_idx remap is done IN DuckDB (not Python) so edges.parquet exports pre-remapped
# int32 -> build_pyg_data reads straight to tensors, avoiding a 21 GB int64 array in RAM.
set -euo pipefail
DB="${1:-/Volumes/miniext/snapshot.duckdb}"
OUT="${2:-/Volumes/miniext/gnn_export}"
TMP="/Volumes/miniext/duckdb_tmp"
mkdir -p "$OUT" "$TMP"

SNAP=$(duckdb "$DB" -readonly -noheader -list -c "SELECT snapshot_date FROM snapshot_metadata;")
echo "snapshot_date = $SNAP   (this is the leak-free CUTOFF for build + validate)"

duckdb "$DB" -readonly <<SQL
SET memory_limit='10GB'; SET threads=4; SET preserve_insertion_order=false;
SET temp_directory='$TMP';

-- 1) node_map: canonical contiguous node_idx 0..N-1 (row_number over actors). SACRED join key.
COPY (
  SELECT (row_number() OVER (ORDER BY did_id) - 1)::INT AS node_idx, did_id, did
  FROM actors
) TO '$OUT/node_map.parquet' (FORMAT parquet);

-- 2) node_feats: node_map join actor_aggs, ordered by node_idx (row position == node_idx).
--    Column order MUST match build_pyg_data.feat_cols.
COPY (
  SELECT m.node_idx, m.did,
         a.followers, a.follows, a.posts, a.likes_out, a.likes_in,
         a.blocks_in, a.blocks_out, a.reposts_out, a.replies_out, a.quotes_out
  FROM read_parquet('$OUT/node_map.parquet') m
  JOIN actor_aggs a USING (did_id)
  ORDER BY m.node_idx
) TO '$OUT/node_feats.parquet' (FORMAT parquet);

-- 3) edges: follows remapped to int32 (src, dst). No ORDER BY (build_pyg_data doesn't need it).
COPY (
  SELECT ms.node_idx::INT AS src, md.node_idx::INT AS dst
  FROM follows f
  JOIN read_parquet('$OUT/node_map.parquet') ms ON ms.did_id = f.src_did_id
  JOIN read_parquet('$OUT/node_map.parquet') md ON md.did_id = f.dst_did_id
) TO '$OUT/edges.parquet' (FORMAT parquet);

-- 4) blocks: second relation for the multi-relational (hetero) graph. src=blocker -> dst=blocked.
--    Same int32 remap; ~1/11th the size of follows.
COPY (
  SELECT ms.node_idx::INT AS src, md.node_idx::INT AS dst
  FROM blocks b
  JOIN read_parquet('$OUT/node_map.parquet') ms ON ms.did_id = b.src_did_id
  JOIN read_parquet('$OUT/node_map.parquet') md ON md.did_id = b.dst_did_id
) TO '$OUT/blocks.parquet' (FORMAT parquet);

-- 5) replies: third relation. Interaction graph src=replier -> dst=replied-to. Projected by
--    self-joining posts on reply_parent_uri_id -> parent post's author. Deduped to unique directed
--    pairs (consistent with follows/blocks; reply-count weighting is a possible future enhancement)
--    and self-loops dropped.
COPY (
  SELECT DISTINCT ms.node_idx::INT AS src, md.node_idx::INT AS dst
  FROM posts p
  JOIN posts par ON par.uri_id = p.reply_parent_uri_id
  JOIN read_parquet('$OUT/node_map.parquet') ms ON ms.did_id = p.author_did_id
  JOIN read_parquet('$OUT/node_map.parquet') md ON md.did_id = par.author_did_id
  WHERE p.reply_parent_uri_id IS NOT NULL
    AND p.author_did_id <> par.author_did_id
) TO '$OUT/replies.parquet' (FORMAT parquet);
SQL

echo "== exported to $OUT =="
duckdb -noheader -list -c "
SELECT 'node_map  ' || COUNT(*) FROM read_parquet('$OUT/node_map.parquet')
UNION ALL SELECT 'node_feats ' || COUNT(*) FROM read_parquet('$OUT/node_feats.parquet')
UNION ALL SELECT 'edges     ' || COUNT(*) FROM read_parquet('$OUT/edges.parquet')
UNION ALL SELECT 'blocks    ' || COUNT(*) FROM read_parquet('$OUT/blocks.parquet')
UNION ALL SELECT 'replies   ' || COUNT(*) FROM read_parquet('$OUT/replies.parquet');"
echo
echo "next:"
echo "  bash scripts/gnn/export_labels.sh $OUT $SNAP     # labels.parquet from Postgres"
echo "  # then upload the 4 parquets to the gnn-data Volume and:"
echo "  modal run scripts/gnn/modal_train.py --feat-mode degree --do-build --cutoff $SNAP"
