#!/usr/bin/env bash
# Regenerate labels.parquet with a HIGH-PRECISION, NON-CIRCULAR positive class:
#   (1) bsky_mod  = Bluesky's OWN moderation labeler (spam/impersonation) -- DATED via labeled_at
#   (2) takedowns = confirmed per-DID repo takedowns (plc.did_repo_status). The scan has no true
#                   takedown DATE (upsert-latest), so we PROXY it with the account's LAST DAY OF
#                   ACTIVITY (MAX activity.did_activity_daily.date). This is leak-SAFE: a repo stops
#                   emitting once takendown, so last_active <= takedown_date -> last_active > cutoff
#                   guarantees the real takedown is post-snapshot too (can only under-count test pos,
#                   never leak a pre-snapshot fraud in). Takedowns with no captured activity -> NULL
#                   -> train. (did_last_seen was stale at 2026-06-20; did_activity_daily is fresh.)
# Skywatch + hailey GRAPH-HEURISTIC labels are intentionally DROPPED from the positive class: training
# and testing a follow-graph model on graph-derived labels is circular. bsky_mod + takedown are
# moderation decisions independent of the follow graph. (The old skywatch-heavy positive union is in
# git history if you want to A/B it -- keep SQL comments OUT of the \copy body: psql flattens \copy to
# one line so a '--' comments out the rest of the query.) Farms stay masked via is_excluded -> the
# ~2.9M farm takedowns drop out of the population.
#
# first_labeled = MIN(bsky_mod labeled_at); NULL for takedown-only DIDs. build_pyg_data routes
# first_labeled >= CUTOFF -> leak-free TEST; NaT/undated & pre-cutoff -> train/val. So undated
# takedowns become train positives and only dated bsky_mod-post-cutoff form the leak-free test set.
#
# Re-run after refreshing labels, then rebuild graph.pt + retrain.
#   bash scripts/gnn/export_labels.sh [OUT_DIR] [SNAPSHOT_DATE]
# OUT_DIR defaults to the GNN export dir; SNAPSHOT_DATE (for the leak-free count) defaults to the
# date recorded in node_map's sibling snapshot, else pass it explicitly (export_graph.sh prints it).
set -euo pipefail
OUT="${1:-/Volumes/miniext/gnn_export}"
SNAPSHOT="${2:-2026-05-11}"
DB=$(grep '^DATABASE_URL=' "$(git rev-parse --show-toplevel)/.env" | cut -d= -f2-)

psql "$DB" -c "\copy (
  WITH
    mod_labels AS (
      SELECT did, labeled_at FROM plc.bsky_mod_labels WHERE label IN ('spam','impersonation')
    ),
    takedowns AS (
      SELECT t.did, MAX(d.date::date)::timestamptz AS labeled_at
      FROM plc.did_repo_status t
      LEFT JOIN activity.did_activity_daily d ON d.did = t.did
      WHERE t.status IN ('takendown','takedown')
      GROUP BY t.did
    ),
    fraud_labels AS (
      SELECT did, labeled_at, 'bsky_mod'::text AS source FROM mod_labels
      UNION ALL
      SELECT did, labeled_at, 'takedown'::text AS source FROM takedowns
    ),
    fraud AS (SELECT DISTINCT did FROM fraud_labels),
    dated AS (SELECT did, MIN(labeled_at) AS first_labeled FROM fraud_labels GROUP BY did),
    src   AS (
      SELECT did, CASE WHEN bool_or(source = 'bsky_mod') THEN 'bsky_mod' ELSE 'takedown' END AS label_source
      FROM fraud_labels GROUP BY did
    ),
    ids AS (SELECT did FROM fraud UNION SELECT did FROM analysis.excluded_dids)
  SELECT i.did,
         (f.did IS NOT NULL) AS is_fraud,
         (e.did IS NOT NULL) AS is_excluded,
         d.first_labeled,
         s.label_source
  FROM ids i
  LEFT JOIN fraud f               ON f.did = i.did
  LEFT JOIN analysis.excluded_dids e ON e.did = i.did
  LEFT JOIN dated d               ON d.did = i.did
  LEFT JOIN src s                 ON s.did = i.did
) TO '$OUT/labels.csv' WITH CSV HEADER"

duckdb -c "COPY (SELECT * FROM read_csv_auto('$OUT/labels.csv', header=true))
  TO '$OUT/labels.parquet' (FORMAT parquet);"

# verification: population, provenance split, and the all-important leak-free (dated) positive count
duckdb -c "
WITH n AS (SELECT did FROM read_parquet('$OUT/node_map.parquet')),
     l AS (SELECT * FROM read_parquet('$OUT/labels.parquet'))
SELECT COUNT(*) FILTER (WHERE is_fraud AND NOT is_excluded)                                      AS fraud_pop,
       COUNT(*) FILTER (WHERE is_fraud AND NOT is_excluded AND label_source='bsky_mod')          AS bsky_mod_pop,
       COUNT(*) FILTER (WHERE is_fraud AND NOT is_excluded AND label_source='takedown')          AS takedown_pop,
       COUNT(*) FILTER (WHERE is_fraud AND NOT is_excluded AND first_labeled > DATE '$SNAPSHOT') AS leakfree_pos,
       MAX(first_labeled) AS latest
FROM l JOIN n USING(did);"
echo "labels.parquet refreshed -> now rebuild graph.pt + retrain."
