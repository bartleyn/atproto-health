import path from "path";
import { getPlcDb } from "../db/plc-schema";

// SQLite expression: Monday of the ISO week containing a datetime column.
// strftime('%w') returns 0=Sunday…6=Saturday; (dow+6)%7 gives days since Monday.
const WEEK_EXPR = `date(created_at, '-' || ((strftime('%w', created_at) + 6) % 7) || ' days')`;
const MIGRATED_WEEK_EXPR = `date(migrated_at, '-' || ((strftime('%w', migrated_at) + 6) % 7) || ' days')`;

const forceHeavy = process.argv.includes("--force");

function step(label: string, fn: () => void) {
  const t0 = Date.now();
  fn();
  console.log(`  ${label}: ${((Date.now() - t0) / 1000).toFixed(1)}s`);
}

export function aggregatePlc() {
  const db = getPlcDb();

  // ── Monthly ────────────────────────────────────────────────────────────────
  const monthlyCursor = db
    .prepare(`SELECT creations_cursor, migrations_cursor FROM plc_aggregation_cursor WHERE id = 1`)
    .get() as { creations_cursor: string; migrations_cursor: string } | undefined;

  const monthlyCreationsCursor  = monthlyCursor?.creations_cursor  ?? "2020-01-01T00:00:00Z";
  const monthlyMigrationsCursor = monthlyCursor?.migrations_cursor ?? "2020-01-01T00:00:00Z";

  step("monthly creations", () => db.prepare(`
    INSERT INTO plc_creation_monthly (pds_url, month, count)
    SELECT pds_url, substr(created_at, 1, 7) AS month, COUNT(*) AS count
    FROM plc_account_creations
    WHERE created_at > ?
    GROUP BY pds_url, month
    ON CONFLICT (pds_url, month) DO UPDATE SET count = count + excluded.count
  `).run(monthlyCreationsCursor));

  step("monthly migrations", () => db.prepare(`
    INSERT INTO plc_migration_monthly (from_pds, to_pds, month, count)
    SELECT from_pds, to_pds, substr(migrated_at, 1, 7) AS month, COUNT(*) AS count
    FROM plc_migrations
    WHERE migrated_at > ?
    GROUP BY from_pds, to_pds, month
    ON CONFLICT (from_pds, to_pds, month) DO UPDATE SET count = count + excluded.count
  `).run(monthlyMigrationsCursor));

  // ── Weekly ─────────────────────────────────────────────────────────────────
  const weeklyCursor = db
    .prepare(`SELECT creations_cursor, migrations_cursor FROM plc_aggregation_weekly_cursor WHERE id = 1`)
    .get() as { creations_cursor: string; migrations_cursor: string } | undefined;

  const weeklyCreationsCursor  = weeklyCursor?.creations_cursor  ?? "2020-01-01T00:00:00Z";
  const weeklyMigrationsCursor = weeklyCursor?.migrations_cursor ?? "2020-01-01T00:00:00Z";

  step("weekly creations", () => db.prepare(`
    INSERT INTO plc_creation_weekly (pds_url, week, count)
    SELECT pds_url, ${WEEK_EXPR} AS week, COUNT(*) AS count
    FROM plc_account_creations
    WHERE created_at > ?
    GROUP BY pds_url, week
    ON CONFLICT (pds_url, week) DO UPDATE SET count = count + excluded.count
  `).run(weeklyCreationsCursor));

  step("weekly migrations", () => db.prepare(`
    INSERT INTO plc_migration_weekly (from_pds, to_pds, week, count)
    SELECT from_pds, to_pds, ${MIGRATED_WEEK_EXPR} AS week, COUNT(*) AS count
    FROM plc_migrations
    WHERE migrated_at > ?
    GROUP BY from_pds, to_pds, week
    ON CONFLICT (from_pds, to_pds, week) DO UPDATE SET count = count + excluded.count
  `).run(weeklyMigrationsCursor));

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

  // ── Stats cache + heavy recomputes: skip when nothing has changed ─────────
  // did_in_repo has 42M rows — COUNT(*) takes ~45s live, so we precompute here.
  // Trajectory/hop recomputes are expensive; skip if migrations haven't advanced.

  const { max_migrated_at } = db.prepare(
    `SELECT MAX(migrated_at) AS max_migrated_at FROM plc_migrations`
  ).get() as { max_migrated_at: string };

  const { max_scanned_at } = db.prepare(
    `SELECT MAX(scanned_at) AS max_scanned_at FROM did_in_repo`
  ).get() as { max_scanned_at: string };

  const heavyCursor = db.prepare(
    `SELECT migrations_cursor, did_scanned_cursor FROM plc_heavy_recompute_cursor WHERE id = 1`
  ).get() as { migrations_cursor: string; did_scanned_cursor: string } | undefined;

  const migrationsNew = !heavyCursor || max_migrated_at > heavyCursor.migrations_cursor;
  const didScanNew    = !heavyCursor || max_scanned_at  > heavyCursor.did_scanned_cursor;

  if (!forceHeavy && !didScanNew) {
    console.log(`  stats cache: skipped (did_in_repo unchanged since ${heavyCursor?.did_scanned_cursor})`);
  } else {
    // SQLite upsert (ON CONFLICT DO UPDATE) only works with INSERT … VALUES, not INSERT … SELECT.
    // Use INSERT OR REPLACE instead — same effect since this table always has exactly one row.
    step("stats cache", () => db.prepare(`
      INSERT OR REPLACE INTO plc_stats_cache (id, total_dids, bsky_concentration_pct, updated_at)
      SELECT
        1,
        COUNT(*),
        ROUND(100.0 * SUM(CASE WHEN pds_url LIKE '%bsky.network' OR pds_url = 'https://bsky.social' THEN 1 ELSE 0 END) / COUNT(*), 1),
        ?
      FROM did_in_repo
    `).run(now));
  }

  // ── Trajectory edges ───────────────────────────────────────────────────────
  // Origin→current: for each migrating DID, maps first-ever source PDS to current PDS.
  // Flattens multi-hop paths (x→y→z becomes x→z). DELETE+INSERT since it depends
  // on full per-DID history and can't be done incrementally.
  if (!forceHeavy && !migrationsNew) {
    console.log(`  trajectory edges: skipped (migrations unchanged since ${heavyCursor?.migrations_cursor})`);
  } else step("trajectory edges", () => db.exec(`
    DELETE FROM plc_trajectory_edges;

    INSERT INTO plc_trajectory_edges (source, target, value)
    WITH
      verified AS (SELECT DISTINCT pds_url FROM pds_repo_status_snapshots),
      normalized AS (
        SELECT did,
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
      filtered AS (
        SELECT did, from_pds, to_pds, migrated_at FROM deduped
        WHERE (from_pds = 'bsky.network' OR from_pds IN (SELECT pds_url FROM verified))
          AND (to_pds = 'bsky.network' OR to_pds IN (SELECT pds_url FROM verified))
      ),
      ranked AS (
        SELECT did, from_pds, to_pds,
          ROW_NUMBER() OVER (PARTITION BY did ORDER BY migrated_at ASC)  AS rn_asc,
          ROW_NUMBER() OVER (PARTITION BY did ORDER BY migrated_at DESC) AS rn_desc
        FROM filtered
      ),
      journeys AS (
        SELECT
          MAX(CASE WHEN rn_asc  = 1 THEN from_pds END) AS origin_pds,
          MAX(CASE WHEN rn_desc = 1 THEN to_pds   END) AS current_pds
        FROM ranked GROUP BY did
      ),
      top_origins AS (SELECT origin_pds  FROM journeys GROUP BY origin_pds  HAVING COUNT(*) >= 5 ORDER BY COUNT(*) DESC LIMIT 10),
      top_current AS (SELECT current_pds FROM journeys GROUP BY current_pds HAVING COUNT(*) >= 5 ORDER BY COUNT(*) DESC LIMIT 10),
      labeled AS (
        SELECT
          CASE WHEN j.origin_pds  IN (SELECT origin_pds  FROM top_origins) THEN j.origin_pds  ELSE 'Other' END || '@0' AS source,
          CASE WHEN j.current_pds IN (SELECT current_pds FROM top_current) THEN j.current_pds ELSE 'Other' END || '@1' AS target
        FROM journeys j
      )
    SELECT source, target, COUNT(*) AS value
    FROM labeled GROUP BY source, target HAVING COUNT(*) >= 5;
  `));

  // ── Per-hop migration edges ────────────────────────────────────────────────
  // Preserves actual per-hop transitions (A→B, B→C) rather than collapsing to origin→current.
  // step = 0-indexed hop number; limited to first 3 hops (covers the vast majority of migrants).
  if (!forceHeavy && !migrationsNew) {
    console.log(`  migration hops: skipped (migrations unchanged since ${heavyCursor?.migrations_cursor})`);
  } else step("migration hops", () => db.exec(`
    DELETE FROM plc_migration_hops;

    INSERT INTO plc_migration_hops (source, target, value)
    WITH
      verified AS (SELECT DISTINCT pds_url FROM pds_repo_status_snapshots),
      normalized AS (
        SELECT did,
          CASE WHEN from_pds LIKE '%bsky.network' OR from_pds = 'https://bsky.social' THEN 'bsky.network' ELSE from_pds END AS from_pds,
          CASE WHEN to_pds   LIKE '%bsky.network' OR to_pds   = 'https://bsky.social' THEN 'bsky.network' ELSE to_pds   END AS to_pds,
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
      filtered AS (
        SELECT did, from_pds, to_pds, migrated_at FROM deduped
        WHERE (from_pds = 'bsky.network' OR from_pds IN (SELECT pds_url FROM verified))
          AND (to_pds   = 'bsky.network' OR to_pds   IN (SELECT pds_url FROM verified))
      ),
      ranked AS (
        SELECT did, from_pds, to_pds,
          ROW_NUMBER() OVER (PARTITION BY did ORDER BY migrated_at ASC) - 1 AS step
        FROM filtered
      ),
      top_pdses AS (
        SELECT pds FROM (
          SELECT from_pds AS pds FROM ranked UNION ALL SELECT to_pds AS pds FROM ranked
        ) GROUP BY pds HAVING COUNT(*) >= 10 ORDER BY COUNT(*) DESC LIMIT 12
      ),
      labeled AS (
        SELECT
          CASE WHEN from_pds IN (SELECT pds FROM top_pdses) THEN from_pds ELSE 'Other' END || '@' || step       AS source,
          CASE WHEN to_pds   IN (SELECT pds FROM top_pdses) THEN to_pds   ELSE 'Other' END || '@' || (step + 1) AS target
        FROM ranked
        WHERE step < 3
      )
    SELECT source, target, COUNT(*) AS value
    FROM labeled GROUP BY source, target HAVING COUNT(*) >= 5;
  `));

  // ── Update heavy recompute cursor ─────────────────────────────────────────
  if (forceHeavy || migrationsNew || didScanNew) {
    db.prepare(`
      INSERT INTO plc_heavy_recompute_cursor (id, migrations_cursor, did_scanned_cursor, updated_at)
      VALUES (1, ?, ?, ?)
      ON CONFLICT (id) DO UPDATE SET
        migrations_cursor  = excluded.migrations_cursor,
        did_scanned_cursor = excluded.did_scanned_cursor,
        updated_at         = excluded.updated_at
    `).run(max_migrated_at, max_scanned_at, now);
  }

  // Checkpoint the WAL so it doesn't grow unboundedly while the dev server holds
  // open read transactions. TRUNCATE mode writes WAL pages back to the main file
  // and truncates the WAL to 0 bytes.
  db.pragma("wal_checkpoint(TRUNCATE)");

  console.log(`Aggregated PLC data (monthly + weekly + trajectories) up to ${now}`);
}

/**
 * Aggregate per-PDS language breakdown from jetstream-activity.db.
 *
 * Joins did_langs (activity DB) with did_in_repo (plc DB) to get the current
 * PDS for each DID. did_in_repo is populated by listRepos scans so bsky shards
 * appear as individual URLs (https://morel.us-east.host.bsky.network etc.).
 * BCP-47 subtags are collapsed to base tag (en-US → en, zh-TW → zh).
 * Fully recomputed each run (DELETE + INSERT), skipped if did_in_repo is unchanged.
 */
export function aggregateLangs() {
  const db = getPlcDb();
  const activityDbPath = path.join(process.cwd(), "jetstream-activity.db");

  const { max_scanned_at } = db.prepare(
    `SELECT MAX(scanned_at) AS max_scanned_at FROM did_in_repo`
  ).get() as { max_scanned_at: string };

  const heavyCursor = db.prepare(
    `SELECT did_scanned_cursor FROM plc_heavy_recompute_cursor WHERE id = 1`
  ).get() as { did_scanned_cursor: string } | undefined;

  if (!forceHeavy && heavyCursor && max_scanned_at <= heavyCursor.did_scanned_cursor) {
    console.log(`  lang aggregation: skipped (did_in_repo unchanged since ${heavyCursor.did_scanned_cursor})`);
    return;
  }

  // ATTACH the activity DB as a read-only alias
  db.exec(`ATTACH DATABASE '${activityDbPath}' AS activity`);

  try {
    step("lang aggregation (cross-DB join)", () => db.exec(`
      DELETE FROM pds_lang_summary;

      INSERT INTO pds_lang_summary (pds_url, lang, dids, post_count)
      SELECT
        RTRIM(p.pds_url, '/') AS pds_url,
        substr(dl.lang, 1, instr(dl.lang || '-', '-') - 1) AS lang,
        COUNT(DISTINCT dl.did) AS dids,
        SUM(dl.post_count) AS post_count
      FROM activity.did_langs dl
      JOIN did_in_repo p ON dl.did = p.did
      WHERE dl.lang != ''
      GROUP BY 1, 2
      HAVING dids >= 2;
    `));
  } finally {
    db.exec(`DETACH DATABASE activity`);
  }

  const now = new Date().toISOString();
  db.prepare(`
    INSERT INTO plc_heavy_recompute_cursor (id, migrations_cursor, did_scanned_cursor, updated_at)
    VALUES (1, COALESCE((SELECT migrations_cursor FROM plc_heavy_recompute_cursor WHERE id = 1), '1970-01-01'), ?, ?)
    ON CONFLICT (id) DO UPDATE SET did_scanned_cursor = excluded.did_scanned_cursor, updated_at = excluded.updated_at
  `).run(max_scanned_at, now);

  const rowCount = (db.prepare(`SELECT COUNT(*) AS n FROM pds_lang_summary`).get() as { n: number }).n;
  console.log(`Language aggregation complete: ${rowCount} (pds_url, lang) pairs`);
}
