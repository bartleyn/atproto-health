import { getPlcDb } from "../db/plc-schema";

// SQLite expression: Monday of the ISO week containing a datetime column.
// strftime('%w') returns 0=Sunday…6=Saturday; (dow+6)%7 gives days since Monday.
const WEEK_EXPR = `date(created_at, '-' || ((strftime('%w', created_at) + 6) % 7) || ' days')`;
const MIGRATED_WEEK_EXPR = `date(migrated_at, '-' || ((strftime('%w', migrated_at) + 6) % 7) || ' days')`;

export function aggregatePlc() {
  const db = getPlcDb();

  // ── Monthly ────────────────────────────────────────────────────────────────
  const monthlyCursor = db
    .prepare(`SELECT creations_cursor, migrations_cursor FROM plc_aggregation_cursor WHERE id = 1`)
    .get() as { creations_cursor: string; migrations_cursor: string } | undefined;

  const monthlyCreationsCursor  = monthlyCursor?.creations_cursor  ?? "2020-01-01T00:00:00Z";
  const monthlyMigrationsCursor = monthlyCursor?.migrations_cursor ?? "2020-01-01T00:00:00Z";

  db.prepare(`
    INSERT INTO plc_creation_monthly (pds_url, month, count)
    SELECT pds_url, substr(created_at, 1, 7) AS month, COUNT(*) AS count
    FROM plc_account_creations
    WHERE created_at > ?
    GROUP BY pds_url, month
    ON CONFLICT (pds_url, month) DO UPDATE SET count = count + excluded.count
  `).run(monthlyCreationsCursor);

  db.prepare(`
    INSERT INTO plc_migration_monthly (from_pds, to_pds, month, count)
    SELECT from_pds, to_pds, substr(migrated_at, 1, 7) AS month, COUNT(*) AS count
    FROM plc_migrations
    WHERE migrated_at > ?
    GROUP BY from_pds, to_pds, month
    ON CONFLICT (from_pds, to_pds, month) DO UPDATE SET count = count + excluded.count
  `).run(monthlyMigrationsCursor);

  // ── Weekly ─────────────────────────────────────────────────────────────────
  const weeklyCursor = db
    .prepare(`SELECT creations_cursor, migrations_cursor FROM plc_aggregation_weekly_cursor WHERE id = 1`)
    .get() as { creations_cursor: string; migrations_cursor: string } | undefined;

  const weeklyCreationsCursor  = weeklyCursor?.creations_cursor  ?? "2020-01-01T00:00:00Z";
  const weeklyMigrationsCursor = weeklyCursor?.migrations_cursor ?? "2020-01-01T00:00:00Z";

  db.prepare(`
    INSERT INTO plc_creation_weekly (pds_url, week, count)
    SELECT pds_url, ${WEEK_EXPR} AS week, COUNT(*) AS count
    FROM plc_account_creations
    WHERE created_at > ?
    GROUP BY pds_url, week
    ON CONFLICT (pds_url, week) DO UPDATE SET count = count + excluded.count
  `).run(weeklyCreationsCursor);

  db.prepare(`
    INSERT INTO plc_migration_weekly (from_pds, to_pds, week, count)
    SELECT from_pds, to_pds, ${MIGRATED_WEEK_EXPR} AS week, COUNT(*) AS count
    FROM plc_migrations
    WHERE migrated_at > ?
    GROUP BY from_pds, to_pds, week
    ON CONFLICT (from_pds, to_pds, week) DO UPDATE SET count = count + excluded.count
  `).run(weeklyMigrationsCursor);

  // ── Update cursors ─────────────────────────────────────────────────────────
  const now = new Date().toISOString();

  db.prepare(`
    INSERT INTO plc_aggregation_cursor (id, creations_cursor, migrations_cursor, updated_at)
    VALUES (1, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      creations_cursor = excluded.creations_cursor,
      migrations_cursor = excluded.migrations_cursor,
      updated_at = excluded.updated_at
  `).run(now, now, now);

  db.prepare(`
    INSERT INTO plc_aggregation_weekly_cursor (id, creations_cursor, migrations_cursor, updated_at)
    VALUES (1, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      creations_cursor = excluded.creations_cursor,
      migrations_cursor = excluded.migrations_cursor,
      updated_at = excluded.updated_at
  `).run(now, now, now);

  // ── Stats cache (full recompute from did_in_repo) ─────────────────────────
  // did_in_repo has 42M rows — COUNT(*) takes ~45s live, so we precompute here.
  db.prepare(`
    INSERT INTO plc_stats_cache (id, total_dids, bsky_concentration_pct, updated_at)
    SELECT
      1,
      COUNT(*),
      ROUND(100.0 * SUM(CASE WHEN pds_url LIKE '%bsky.network' OR pds_url = 'https://bsky.social' THEN 1 ELSE 0 END) / COUNT(*), 1),
      ?
    FROM did_in_repo
    ON CONFLICT(id) DO UPDATE SET
      total_dids             = excluded.total_dids,
      bsky_concentration_pct = excluded.bsky_concentration_pct,
      updated_at             = excluded.updated_at
  `).run(now);

  // ── Trajectory edges (full recompute) ─────────────────────────────────────
  // Multi-step migration paths for the Sankey chart. DELETE + INSERT since hop
  // assignments depend on each DID's full history and can't be done incrementally.
  db.exec(`
    DELETE FROM plc_trajectory_edges;

    INSERT INTO plc_trajectory_edges (source, target, value)
    WITH
      verified AS (SELECT DISTINCT pds_url FROM pds_repo_status_snapshots),
      normalized AS (
        SELECT
          did,
          CASE WHEN from_pds LIKE '%bsky.network' OR from_pds = 'https://bsky.social' THEN 'bsky.network' ELSE from_pds END AS from_pds,
          CASE WHEN to_pds LIKE '%bsky.network' OR to_pds = 'https://bsky.social' THEN 'bsky.network' ELSE to_pds END AS to_pds,
          migrated_at
        FROM plc_migrations
        WHERE NOT (
          (from_pds LIKE '%bsky.network' OR from_pds = 'https://bsky.social')
          AND (to_pds LIKE '%bsky.network' OR to_pds = 'https://bsky.social')
        )
      ),
      deduped AS (
        SELECT did, from_pds, to_pds, MIN(migrated_at) AS migrated_at
        FROM normalized GROUP BY did, from_pds, to_pds
      ),
      hops AS (
        SELECT did, from_pds, to_pds, migrated_at,
          ROW_NUMBER() OVER (PARTITION BY did ORDER BY migrated_at) AS hop
        FROM deduped
        WHERE (from_pds = 'bsky.network' OR from_pds IN (SELECT pds_url FROM verified))
          AND (to_pds = 'bsky.network' OR to_pds IN (SELECT pds_url FROM verified))
      ),
      hop2 AS (SELECT * FROM hops WHERE hop <= 2),
      top1 AS (SELECT to_pds FROM hop2 WHERE hop = 1 GROUP BY to_pds HAVING COUNT(*) >= 5 ORDER BY COUNT(*) DESC LIMIT 10),
      top2 AS (SELECT to_pds FROM hop2 WHERE hop = 2 GROUP BY to_pds HAVING COUNT(*) >= 5 ORDER BY COUNT(*) DESC LIMIT 10),
      top0 AS (SELECT from_pds FROM hop2 WHERE hop = 1 GROUP BY from_pds HAVING COUNT(*) >= 5 ORDER BY COUNT(*) DESC LIMIT 10),
      edges AS (
        SELECT
          CASE WHEN h.from_pds IN (SELECT from_pds FROM top0) THEN h.from_pds ELSE 'Other' END || '@0' AS source,
          CASE WHEN h.to_pds IN (SELECT to_pds FROM top1) THEN h.to_pds ELSE 'Other' END || '@1' AS target
        FROM hop2 h WHERE h.hop = 1
        UNION ALL
        SELECT
          CASE WHEN h.from_pds IN (SELECT to_pds FROM top1) THEN h.from_pds ELSE 'Other' END || '@1' AS source,
          CASE WHEN h.to_pds IN (SELECT to_pds FROM top2) THEN h.to_pds ELSE 'Other' END || '@2' AS target
        FROM hop2 h WHERE h.hop = 2
      )
    SELECT source, target, COUNT(*) AS value
    FROM edges GROUP BY source, target HAVING COUNT(*) >= 5;
  `);

  console.log(`Aggregated PLC data (monthly + weekly + trajectories) up to ${now}`);
}
