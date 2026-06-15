import sql from "../db/pg";

const forceHeavy = process.argv.includes("--force");

async function step(label: string, fn: () => Promise<void>): Promise<void> {
  const t0 = Date.now();
  await fn();
  console.log(`  ${label}: ${((Date.now() - t0) / 1000).toFixed(1)}s`);
}

export async function aggregatePlc(): Promise<void> {
  // ── Monthly ────────────────────────────────────────────────────────────────
  const [monthlyCursor] = await sql<{ creations_cursor: string; migrations_cursor: string }[]>`
    SELECT creations_cursor, migrations_cursor FROM plc.plc_aggregation_cursor WHERE id = 1
  `;
  const monthlyCreationsCursor  = monthlyCursor?.creations_cursor  ?? "2020-01-01T00:00:00Z";
  const monthlyMigrationsCursor = monthlyCursor?.migrations_cursor ?? "2020-01-01T00:00:00Z";

  await step("monthly creations", async () => { await sql`
    INSERT INTO plc.plc_creation_monthly (pds_url, month, count)
    SELECT pds_url, TO_CHAR(created_at, 'YYYY-MM') AS month, COUNT(*)::int AS count
    FROM plc.plc_account_creations
    WHERE created_at > ${monthlyCreationsCursor}
    GROUP BY pds_url, month
    ON CONFLICT (pds_url, month) DO UPDATE SET count = plc_creation_monthly.count + EXCLUDED.count
  `; });

  await step("monthly migrations", async () => { await sql`
    INSERT INTO plc.plc_migration_monthly (from_pds, to_pds, month, count)
    SELECT from_pds, to_pds, TO_CHAR(migrated_at, 'YYYY-MM') AS month, COUNT(*)::int AS count
    FROM plc.plc_migrations
    WHERE migrated_at > ${monthlyMigrationsCursor}
    GROUP BY from_pds, to_pds, month
    ON CONFLICT (from_pds, to_pds, month) DO UPDATE SET count = plc_migration_monthly.count + EXCLUDED.count
  `; });

  // ── Weekly ─────────────────────────────────────────────────────────────────
  const [weeklyCursor] = await sql<{ creations_cursor: string; migrations_cursor: string }[]>`
    SELECT creations_cursor, migrations_cursor FROM plc.plc_aggregation_weekly_cursor WHERE id = 1
  `;
  const weeklyCreationsCursor  = weeklyCursor?.creations_cursor  ?? "2020-01-01T00:00:00Z";
  const weeklyMigrationsCursor = weeklyCursor?.migrations_cursor ?? "2020-01-01T00:00:00Z";

  // DATE_TRUNC('week', ...) in PG truncates to Monday (ISO 8601).
  await step("weekly creations", async () => { await sql`
    INSERT INTO plc.plc_creation_weekly (pds_url, week, count)
    SELECT pds_url, DATE_TRUNC('week', created_at)::date::text AS week, COUNT(*)::int AS count
    FROM plc.plc_account_creations
    WHERE created_at > ${weeklyCreationsCursor}
    GROUP BY pds_url, week
    ON CONFLICT (pds_url, week) DO UPDATE SET count = plc_creation_weekly.count + EXCLUDED.count
  `; });

  await step("weekly migrations", async () => { await sql`
    INSERT INTO plc.plc_migration_weekly (from_pds, to_pds, week, count)
    SELECT from_pds, to_pds, DATE_TRUNC('week', migrated_at)::date::text AS week, COUNT(*)::int AS count
    FROM plc.plc_migrations
    WHERE migrated_at > ${weeklyMigrationsCursor}
    GROUP BY from_pds, to_pds, week
    ON CONFLICT (from_pds, to_pds, week) DO UPDATE SET count = plc_migration_weekly.count + EXCLUDED.count
  `; });

  // ── Update aggregation cursors ─────────────────────────────────────────────
  const now = new Date().toISOString();

  await sql`
    INSERT INTO plc.plc_aggregation_cursor (id, creations_cursor, migrations_cursor, updated_at)
    VALUES (1, ${now}, ${now}, NOW())
    ON CONFLICT (id) DO UPDATE SET
      creations_cursor  = EXCLUDED.creations_cursor,
      migrations_cursor = EXCLUDED.migrations_cursor,
      updated_at        = NOW()
  `;
  await sql`
    INSERT INTO plc.plc_aggregation_weekly_cursor (id, creations_cursor, migrations_cursor, updated_at)
    VALUES (1, ${now}, ${now}, NOW())
    ON CONFLICT (id) DO UPDATE SET
      creations_cursor  = EXCLUDED.creations_cursor,
      migrations_cursor = EXCLUDED.migrations_cursor,
      updated_at        = NOW()
  `;

  // ── Stats cache + heavy recomputes: skip when nothing has changed ──────────
  const [[maxMigratedRow], [maxScannedRow], [heavyCursor]] = await Promise.all([
    sql<{ v: string | null }[]>`SELECT MAX(migrated_at)::text AS v FROM plc.plc_migrations`,
    sql<{ v: string | null }[]>`SELECT MAX(scanned_at)::text AS v FROM plc.did_in_repo`,
    sql<{ migrations_cursor: string; did_scanned_cursor: string }[]>`
      SELECT migrations_cursor, did_scanned_cursor FROM plc.plc_heavy_recompute_cursor WHERE id = 1
    `,
  ]);
  const max_migrated_at = maxMigratedRow?.v ?? now;
  const max_scanned_at  = maxScannedRow?.v  ?? now;

  const migrationsNew = !heavyCursor || max_migrated_at > heavyCursor.migrations_cursor;
  const didScanNew    = !heavyCursor || max_scanned_at  > heavyCursor.did_scanned_cursor;

  if (!forceHeavy && !didScanNew && !migrationsNew) {
    console.log(`  stats cache: skipped (did_in_repo and migrations unchanged)`);
  } else {
    await step("stats cache", async () => { await sql`
      INSERT INTO plc.plc_stats_cache (id, total_dids, bsky_concentration_pct, unique_migrating_dids, updated_at)
      SELECT
        1,
        (SELECT COUNT(*)::int FROM plc.did_in_repo),
        (SELECT ROUND((100.0 * SUM(CASE WHEN pds_url LIKE '%bsky.network' OR pds_url = 'https://bsky.social' THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0))::numeric, 1)::float8 FROM plc.did_in_repo),
        (SELECT COUNT(DISTINCT did)::int FROM plc.plc_migrations WHERE from_pds NOT LIKE '%bsky.social'),
        NOW()
      ON CONFLICT (id) DO UPDATE SET
        total_dids             = EXCLUDED.total_dids,
        bsky_concentration_pct = EXCLUDED.bsky_concentration_pct,
        unique_migrating_dids  = EXCLUDED.unique_migrating_dids,
        updated_at             = NOW()
    `; });
  }

  // ── Trajectory edges ───────────────────────────────────────────────────────
  if (!forceHeavy && !migrationsNew) {
    console.log(`  trajectory edges: skipped (migrations unchanged since ${heavyCursor?.migrations_cursor})`);
  } else {
    await step("trajectory edges", async () => {
      await sql`DELETE FROM plc.plc_trajectory_edges`;
      await sql`
        INSERT INTO plc.plc_trajectory_edges (source, target, value)
        WITH
          verified AS (SELECT DISTINCT pds_url FROM plc.pds_repo_status_snapshots),
          normalized AS (
            SELECT did,
              CASE WHEN from_pds LIKE '%bsky.network' OR from_pds = 'https://bsky.social' THEN 'bsky.network' ELSE from_pds END AS from_pds,
              CASE WHEN to_pds   LIKE '%bsky.network' OR to_pds   = 'https://bsky.social' THEN 'bsky.network' ELSE to_pds   END AS to_pds,
              migrated_at
            FROM plc.plc_migrations
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
          top_origins AS (SELECT origin_pds  FROM journeys GROUP BY origin_pds  HAVING COUNT(*) >= 1 ORDER BY COUNT(*) DESC LIMIT 10),
          top_current AS (SELECT current_pds FROM journeys GROUP BY current_pds HAVING COUNT(*) >= 1 ORDER BY COUNT(*) DESC LIMIT 10),
          labeled AS (
            SELECT
              CASE WHEN j.origin_pds  IN (SELECT origin_pds  FROM top_origins) THEN j.origin_pds  ELSE 'Other' END || '@0' AS source,
              CASE WHEN j.current_pds IN (SELECT current_pds FROM top_current) THEN j.current_pds ELSE 'Other' END || '@1' AS target
            FROM journeys j
          )
        SELECT source, target, COUNT(*)::int AS value
        FROM labeled GROUP BY source, target HAVING COUNT(*) >= 1
      `;
    });
  }

  // ── Per-hop migration edges ────────────────────────────────────────────────
  if (!forceHeavy && !migrationsNew) {
    console.log(`  migration hops: skipped (migrations unchanged since ${heavyCursor?.migrations_cursor})`);
  } else {
    await step("migration hops", async () => {
      await sql`DELETE FROM plc.plc_migration_hops`;
      await sql`
        INSERT INTO plc.plc_migration_hops (source, target, value)
        WITH
          verified AS (SELECT DISTINCT pds_url FROM plc.pds_repo_status_snapshots),
          normalized AS (
            SELECT did,
              CASE WHEN from_pds LIKE '%bsky.network' OR from_pds = 'https://bsky.social' THEN 'bsky.network' ELSE from_pds END AS from_pds,
              CASE WHEN to_pds   LIKE '%bsky.network' OR to_pds   = 'https://bsky.social' THEN 'bsky.network' ELSE to_pds   END AS to_pds,
              migrated_at
            FROM plc.plc_migrations
            WHERE NOT (
              (from_pds LIKE '%bsky.network' OR from_pds = 'https://bsky.social')
              AND (to_pds LIKE '%bsky.network' OR to_pds = 'https://bsky.social')
            )
          ),
          filtered AS (
            SELECT did, from_pds, to_pds, migrated_at FROM normalized
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
            ) t GROUP BY pds HAVING COUNT(*) >= 11 ORDER BY COUNT(*) DESC LIMIT 15
          ),
          final_pds_per_did AS (
            SELECT r.did, r.to_pds AS final_pds
            FROM ranked r
            JOIN (SELECT did, MAX(step) AS ms FROM ranked GROUP BY did) m
              ON r.did = m.did AND r.step = m.ms
          ),
          labeled AS (
            SELECT
              CASE WHEN from_pds IN (SELECT pds FROM top_pdses) THEN from_pds ELSE 'Other' END || '@' || step       AS source,
              CASE WHEN to_pds   IN (SELECT pds FROM top_pdses) THEN to_pds   ELSE 'Other' END || '@' || (step + 1) AS target
            FROM ranked
            WHERE step < 5

            UNION ALL

            SELECT
              CASE WHEN r.from_pds  IN (SELECT pds FROM top_pdses) THEN r.from_pds  ELSE 'Other' END || '@5' AS source,
              CASE WHEN f.final_pds IN (SELECT pds FROM top_pdses) THEN f.final_pds ELSE 'Other' END || '@6' AS target
            FROM ranked r
            JOIN final_pds_per_did f ON r.did = f.did
            WHERE r.step = 5
          )
        SELECT source, target, COUNT(*)::int AS value
        FROM labeled GROUP BY source, target HAVING COUNT(*) >= 1
      `;
    });
  }

  // ── Update heavy recompute cursor ──────────────────────────────────────────
  if (forceHeavy || migrationsNew || didScanNew) {
    await sql`
      INSERT INTO plc.plc_heavy_recompute_cursor (id, migrations_cursor, did_scanned_cursor, updated_at)
      VALUES (1, ${max_migrated_at}, ${max_scanned_at}, NOW())
      ON CONFLICT (id) DO UPDATE SET
        migrations_cursor  = EXCLUDED.migrations_cursor,
        did_scanned_cursor = EXCLUDED.did_scanned_cursor,
        updated_at         = NOW()
    `;
  }

  console.log(`Aggregated PLC data (monthly + weekly + trajectories) up to ${now}`);
}

/**
 * Aggregate per-PDS language breakdown.
 *
 * Joins activity.did_langs with plc.did_in_repo (cross-schema, no ATTACH needed).
 * BCP-47 subtags collapsed to base tag (en-US → en). Fully recomputed each run,
 * skipped if did_in_repo is unchanged.
 */
export async function aggregateLangs(): Promise<void> {
  const [[maxScannedRow], [heavyCursor]] = await Promise.all([
    sql<{ v: string | null }[]>`SELECT MAX(scanned_at)::text AS v FROM plc.did_in_repo`,
    sql<{ did_scanned_cursor: string }[]>`
      SELECT did_scanned_cursor FROM plc.plc_heavy_recompute_cursor WHERE id = 1
    `,
  ]);
  const max_scanned_at = maxScannedRow?.v;

  if (!forceHeavy && heavyCursor && max_scanned_at && max_scanned_at <= heavyCursor.did_scanned_cursor) {
    console.log(`  lang aggregation: skipped (did_in_repo unchanged since ${heavyCursor.did_scanned_cursor})`);
    return;
  }

  await step("lang aggregation (cross-schema join)", async () => {
    await sql`DELETE FROM plc.pds_lang_summary`;
    await sql`
      INSERT INTO plc.pds_lang_summary (pds_url, lang, dids, post_count)
      SELECT
        RTRIM(p.pds_url, '/') AS pds_url,
        SPLIT_PART(dl.lang, '-', 1) AS lang,
        COUNT(DISTINCT dl.did)::int AS dids,
        SUM(dl.post_count)::int AS post_count
      FROM activity.did_langs dl
      JOIN plc.did_in_repo p ON dl.did = p.did
      WHERE dl.lang != ''
      GROUP BY 1, 2
      HAVING COUNT(DISTINCT dl.did) >= 2
    `;
  });

  const now = new Date().toISOString();
  await sql`
    INSERT INTO plc.plc_heavy_recompute_cursor (id, migrations_cursor, did_scanned_cursor, updated_at)
    VALUES (1, COALESCE((SELECT migrations_cursor FROM plc.plc_heavy_recompute_cursor WHERE id = 1), '1970-01-01'), ${max_scanned_at ?? now}, NOW())
    ON CONFLICT (id) DO UPDATE SET
      did_scanned_cursor = EXCLUDED.did_scanned_cursor,
      updated_at         = NOW()
  `;

  const [{ n }] = await sql<{ n: number }[]>`SELECT COUNT(*)::int AS n FROM plc.pds_lang_summary`;
  console.log(`Language aggregation complete: ${n} (pds_url, lang) pairs`);
}
