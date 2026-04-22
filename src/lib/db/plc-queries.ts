import { getPlcDb } from "./plc-schema";

export interface TimeseriesRow {
  period: string; // month (YYYY-MM) or week (YYYY-MM-DD Monday)
  pds_url: string;
  count: number;
}

/** @deprecated use TimeseriesRow */
export interface MonthlyRow {
  month: string;
  pds_url: string;
  count: number;
}

export interface PdsStatusRow {
  pds_url: string;
  snapshot_date: string;
  active: number;
  deactivated: number;
  deleted: number;
  takendown: number;
  suspended: number;
  other: number;
  total_scanned: number;
}

export interface EcosystemStats {
  total_dids: number;
  total_dids_ex_trump: number;
  total_migrations: number;
  unique_migrating_dids: number;
  independent_pds_count: number;
  independent_pds_account_pct: number;
  bsky_concentration_pct: number;
  earliest_creation: string;
  latest_creation: string;
}

const BSKY_NETWORK_LABEL = "bsky.network";
const TRUMP_PDS = "https://pds.trump.com";
const TOP_N = 10;

// Exclude localhost/loopback dev artifacts, reserved TLDs, private IPs, and malformed URLs.
// .dev is a real IANA TLD (Google) — do NOT filter it.
// Pass a table alias (e.g. "w") to avoid ambiguity in joined queries.
function junkPdsFilter(col = "pds_url") {
  return `
    ${col} NOT LIKE '%localhost%'
    AND ${col} NOT LIKE '%127.0.0.1%'
    AND ${col} NOT LIKE '%0.0.0.0%'
    AND ${col} NOT LIKE '%192.168.%'
    AND ${col} NOT LIKE '%10.0.%'
    AND ${col} NOT LIKE '%172.16.%'
    AND (${col} LIKE 'http://%' OR ${col} LIKE 'https://%')
    AND INSTR(SUBSTR(${col}, INSTR(${col}, '://')+3), '.') > 0
    AND ${col} NOT LIKE '%ngrok%'
    AND ${col} NOT LIKE '%.surge.sh%'
    AND ${col} NOT LIKE '%plc.surge.sh%'
    AND ${col} NOT LIKE '%.uwu%'
    AND ${col} NOT LIKE '%uwu.%'
    AND ${col} NOT LIKE '%.test'
    AND ${col} NOT LIKE '%.test/%'
    AND ${col} NOT LIKE '%.test:%'
    AND ${col} NOT LIKE '%//example.%'
    AND ${col} NOT LIKE '%.example'
    AND ${col} NOT LIKE '%.example/%'
    AND ${col} NOT LIKE '%.example:%'
    AND ${col} NOT LIKE '%.invalid'
    AND ${col} NOT LIKE '%.invalid/%'
    AND ${col} NOT LIKE '%.local'
    AND ${col} NOT LIKE '%.local/%'
    AND ${col} NOT LIKE '%.local:%'
    AND ${col} NOT LIKE '%.internal'
    AND ${col} NOT LIKE '%.internal/%'
    AND ${col} NOT LIKE '%.internal:%'
    AND ${col} NOT LIKE '%.lan'
    AND ${col} NOT LIKE '%.lan/%'
    AND ${col} NOT LIKE '%.lan:%'
    AND ${col} NOT LIKE '%.home'
    AND ${col} NOT LIKE '%.home/%'
    AND ${col} NOT LIKE '%.home:%'
    AND length(${col}) <= 200
  `;
}
const JUNK_PDS_FILTER = junkPdsFilter();

export function getCreationTimeseries(): MonthlyRow[] {
  const db = getPlcDb();
  return db.prepare(`
    WITH collapsed AS (
      SELECT
        CASE WHEN pds_url LIKE '%bsky.network' THEN '${BSKY_NETWORK_LABEL}' ELSE pds_url END AS pds_url,
        month,
        SUM(count) AS count
      FROM plc_creation_monthly
      GROUP BY 1, 2
    ),
    top_pds AS (
      SELECT pds_url
      FROM collapsed
      GROUP BY pds_url
      ORDER BY SUM(count) DESC
      LIMIT ${TOP_N}
    ),
    labeled AS (
      SELECT
        month,
        CASE WHEN c.pds_url IN (SELECT pds_url FROM top_pds) THEN c.pds_url ELSE 'Other' END AS pds_url,
        count
      FROM collapsed c
    )
    SELECT month, pds_url, SUM(count) AS count
    FROM labeled
    GROUP BY month, pds_url
    ORDER BY month, pds_url
  `).all() as MonthlyRow[];
}

export function getCreationTimeseriesWeekly(includeTrump = false, hideBsky = false): TimeseriesRow[] {
  const db = getPlcDb();
  const trumpFilter = includeTrump ? "" : `AND w.pds_url != '${TRUMP_PDS}'`;
  const bskyFilter  = hideBsky
    ? `AND w.pds_url NOT LIKE '%bsky.network' AND w.pds_url != 'https://bsky.social'`
    : "";
  const bskyVerifiedExempt = `OR w.pds_url LIKE '%bsky.network' OR w.pds_url = 'https://bsky.social'`;
  const verifiedFilter = includeTrump
    ? `AND (v.pds_url IS NOT NULL OR w.pds_url = '${TRUMP_PDS}' ${bskyVerifiedExempt})`
    : `AND (v.pds_url IS NOT NULL ${bskyVerifiedExempt})`;
  return db.prepare(`
    WITH
    verified AS (SELECT DISTINCT pds_url FROM pds_repo_status_snapshots),
    collapsed AS (
      SELECT
        CASE WHEN w.pds_url LIKE '%bsky.network' OR w.pds_url = 'https://bsky.social' THEN '${BSKY_NETWORK_LABEL}' ELSE w.pds_url END AS pds_url,
        week AS period,
        SUM(w.count) AS count
      FROM plc_creation_weekly w
      LEFT JOIN verified v ON w.pds_url = v.pds_url
      WHERE ${junkPdsFilter("w.pds_url")} ${trumpFilter} ${bskyFilter} ${verifiedFilter}
      GROUP BY 1, 2
    ),
    top_pds AS (
      SELECT pds_url FROM collapsed GROUP BY pds_url ORDER BY SUM(count) DESC LIMIT ${TOP_N}
    ),
    labeled AS (
      SELECT period,
        CASE WHEN c.pds_url IN (SELECT pds_url FROM top_pds) THEN c.pds_url ELSE 'Other' END AS pds_url,
        count
      FROM collapsed c
    )
    SELECT period, pds_url, SUM(count) AS count
    FROM labeled
    GROUP BY period, pds_url
    ORDER BY period, pds_url
  `).all() as TimeseriesRow[];
}

export function getActiveCreationTimeseriesWeekly(hideBsky = false): TimeseriesRow[] {
  const db = getPlcDb();
  const bskyFilter = hideBsky ? `AND w.pds_url NOT LIKE '%bsky.network'` : "";
  return db.prepare(`
    WITH
    collapsed AS (
      SELECT
        CASE WHEN w.pds_url LIKE '%bsky.network' THEN '${BSKY_NETWORK_LABEL}' ELSE w.pds_url END AS pds_url,
        week AS period,
        SUM(w.count) AS count
      FROM active_creation_weekly w
      WHERE ${junkPdsFilter("w.pds_url")} ${bskyFilter}
      GROUP BY 1, 2
    ),
    top_pds AS (
      SELECT pds_url FROM collapsed GROUP BY pds_url ORDER BY SUM(count) DESC LIMIT ${TOP_N}
    ),
    labeled AS (
      SELECT period,
        CASE WHEN c.pds_url IN (SELECT pds_url FROM top_pds) THEN c.pds_url ELSE 'Other' END AS pds_url,
        count
      FROM collapsed c
    )
    SELECT period, pds_url, SUM(count) AS count
    FROM labeled
    GROUP BY period, pds_url
    ORDER BY period, pds_url
  `).all() as TimeseriesRow[];
}

export function getMigrationTimeseriesWeekly(): TimeseriesRow[] {
  const db = getPlcDb();
  return db.prepare(`
    WITH collapsed AS (
      SELECT
        to_pds AS pds_url,
        week AS period,
        SUM(count) AS count
      FROM plc_migration_weekly
      WHERE to_pds NOT LIKE '%bsky.network'
      GROUP BY 1, 2
    ),
    top_pds AS (
      SELECT pds_url FROM collapsed GROUP BY pds_url ORDER BY SUM(count) DESC LIMIT ${TOP_N}
    ),
    labeled AS (
      SELECT period,
        CASE WHEN c.pds_url IN (SELECT pds_url FROM top_pds) THEN c.pds_url ELSE 'Other' END AS pds_url,
        count
      FROM collapsed c
    )
    SELECT period, pds_url, SUM(count) AS count
    FROM labeled
    GROUP BY period, pds_url
    ORDER BY period, pds_url
  `).all() as TimeseriesRow[];
}

export function getMigrationTimeseries(): MonthlyRow[] {
  const db = getPlcDb();
  return db.prepare(`
    WITH collapsed AS (
      SELECT
        to_pds AS pds_url,
        month,
        SUM(count) AS count
      FROM plc_migration_monthly
      WHERE to_pds NOT LIKE '%bsky.network'
      GROUP BY 1, 2
    ),
    top_pds AS (
      SELECT pds_url
      FROM collapsed
      GROUP BY pds_url
      ORDER BY SUM(count) DESC
      LIMIT ${TOP_N}
    ),
    labeled AS (
      SELECT
        month,
        CASE WHEN c.pds_url IN (SELECT pds_url FROM top_pds) THEN c.pds_url ELSE 'Other' END AS pds_url,
        count
      FROM collapsed c
    )
    SELECT month, pds_url, SUM(count) AS count
    FROM labeled
    GROUP BY month, pds_url
    ORDER BY month, pds_url
  `).all() as MonthlyRow[];
}

export interface MigrationFlow {
  source: string;
  target: string;
  value: number;
}

export function getMigrationFlows(): MigrationFlow[] {
  const db = getPlcDb();
  // Reads collapsed origin→current trajectories: where did accounts ultimately land?
  return db.prepare(`
    SELECT
      replace(source, '@0', '') AS source,
      replace(target, '@1', '') AS target,
      value
    FROM plc_trajectory_edges
    WHERE replace(source, '@0', '') != replace(target, '@1', '')
    ORDER BY value DESC
  `).all() as MigrationFlow[];
}

export interface WeeklyMigrationRow {
  week: string;
  to_pds: string;
  count: number;
}

// Returns per-week migration counts for the last 18 months, broken down by the same
// top-10 destinations used in the Sankey (so sink names match for cross-highlighting).
export function getMigrationWeeklyBreakdown(topN = 10): WeeklyMigrationRow[] {
  const db = getPlcDb();
  return db.prepare(`
    WITH
      verified AS (SELECT DISTINCT pds_url FROM pds_repo_status_snapshots),
      top_targets AS (
        SELECT CASE WHEN to_pds LIKE '%bsky.network' THEN '${BSKY_NETWORK_LABEL}' ELSE to_pds END AS target
        FROM plc_migration_monthly
        WHERE NOT (from_pds LIKE '%bsky.network' AND to_pds LIKE '%bsky.network')
          AND (to_pds LIKE '%bsky.network' OR to_pds IN (SELECT pds_url FROM verified))
          AND (from_pds LIKE '%bsky.network' OR from_pds IN (SELECT pds_url FROM verified))
        GROUP BY target
        ORDER BY SUM(count) DESC
        LIMIT ${topN}
      ),
      labeled AS (
        SELECT
          w.week,
          CASE
            WHEN w.to_pds LIKE '%bsky.network' THEN '${BSKY_NETWORK_LABEL}'
            WHEN w.to_pds IN (SELECT target FROM top_targets) THEN w.to_pds
            ELSE 'Other destinations'
          END AS to_pds,
          w.count
        FROM plc_migration_weekly w
        WHERE NOT (w.from_pds LIKE '%bsky.network' AND w.to_pds LIKE '%bsky.network')
          AND (w.to_pds LIKE '%bsky.network' OR w.to_pds IN (SELECT pds_url FROM verified))
          AND (w.from_pds LIKE '%bsky.network' OR w.from_pds IN (SELECT pds_url FROM verified))
          AND w.week >= date('now', '-18 months')
      )
    SELECT week, to_pds, SUM(count) AS count
    FROM labeled
    GROUP BY week, to_pds
    ORDER BY week, to_pds
  `).all() as WeeklyMigrationRow[];
}

export function getLatestPdsStatusSnapshot(): PdsStatusRow[] {
  const db = getPlcDb();
  return db.prepare(`
    WITH latest AS (
      SELECT pds_url, MAX(snapshot_date) AS snapshot_date
      FROM pds_repo_status_snapshots
      GROUP BY pds_url
    )
    SELECT s.pds_url, s.snapshot_date, s.active, s.deactivated, s.deleted,
           s.takendown, s.suspended, s.other, s.total_scanned
    FROM pds_repo_status_snapshots s
    JOIN latest l ON s.pds_url = l.pds_url AND s.snapshot_date = l.snapshot_date
    ORDER BY s.total_scanned DESC
  `).all() as PdsStatusRow[];
}

export function getPlcDataTimestamp(): { collected_through: string; aggregated_at: string } | null {
  const db = getPlcDb();
  const cursor = db.prepare(`SELECT after AS collected_through FROM plc_cursor WHERE id = 1`).get() as { collected_through: string } | undefined;
  const agg    = db.prepare(`SELECT updated_at AS aggregated_at FROM plc_aggregation_cursor WHERE id = 1`).get() as { aggregated_at: string } | undefined;
  if (!cursor) return null;
  return { collected_through: cursor.collected_through, aggregated_at: agg?.aggregated_at ?? cursor.collected_through };
}

export function getEcosystemStats(hideBsky = false): EcosystemStats {
  const db = getPlcDb();
  const bskyFilter = hideBsky ? `AND m.pds_url NOT LIKE '%bsky.network'` : "";

  // Read total_dids from the stats cache (precomputed by aggregate-plc.ts).
  // did_in_repo has 42M rows — a live COUNT(*) takes ~45s.
  const statsCache = db.prepare(`
    SELECT total_dids, bsky_concentration_pct FROM plc_stats_cache WHERE id = 1
  `).get() as { total_dids: number; bsky_concentration_pct: number } | undefined;
  const totals = {
    total_dids: statsCache?.total_dids ?? 0,
    total_dids_ex_trump: statsCache?.total_dids ?? 0,
  };

  // Match the Sankey filter: verified PDSes only, exclude internal bsky resharding.
  // Includes returns to bsky.network. When hideBsky, also exclude bsky as source or destination.
  const bskyMigrationFilter = hideBsky
    ? `AND from_pds NOT LIKE '%bsky.network' AND to_pds NOT LIKE '%bsky.network'`
    : `AND NOT (from_pds LIKE '%bsky.network' AND to_pds LIKE '%bsky.network')`;
  const migrations = db.prepare(`
    SELECT SUM(count) AS total_migrations FROM plc_migration_monthly
    WHERE (from_pds IN (SELECT pds_url FROM pds_repo_status_snapshots) OR from_pds LIKE '%bsky.network')
      AND (to_pds IN (SELECT pds_url FROM pds_repo_status_snapshots) OR to_pds LIKE '%bsky.network')
      ${bskyMigrationFilter}
  `).get() as { total_migrations: number };

  const indep = db.prepare(`
    SELECT COUNT(DISTINCT pds_url) AS independent_pds_count
    FROM pds_repo_status_snapshots
  `).get() as { independent_pds_count: number };

  const dates = db.prepare(`
    SELECT MIN(month) || '-01' AS earliest_creation, MAX(month) || '-01' AS latest_creation
    FROM plc_creation_monthly
    WHERE pds_url != '${TRUMP_PDS}'
  `).get() as { earliest_creation: string; latest_creation: string };

  const uniqueMigrators = db.prepare(`
    SELECT COUNT(DISTINCT did) AS unique_migrating_dids
    FROM plc_migrations
    WHERE from_pds NOT LIKE '%bsky.social'
  `).get() as { unique_migrating_dids: number };

  // Read from stats cache — live query over 42M rows takes ~45s.
  const concentration = { bsky_pct: statsCache?.bsky_concentration_pct ?? 0 };

  return {
    total_dids: totals.total_dids ?? 0,
    total_dids_ex_trump: totals.total_dids_ex_trump ?? 0,
    total_migrations: migrations.total_migrations ?? 0,
    unique_migrating_dids: uniqueMigrators?.unique_migrating_dids ?? 0,
    independent_pds_count: indep?.independent_pds_count ?? 0,
    independent_pds_account_pct: 0,
    bsky_concentration_pct: concentration?.bsky_pct ?? 0,
    earliest_creation: dates?.earliest_creation ?? "",
    latest_creation: dates?.latest_creation ?? "",
  };
}

export interface TrajectoryEdge {
  source: string; // "pds_url@step" e.g. "bsky.network@0"
  target: string; // "pds_url@step" e.g. "eurosky.social@1"
  value: number;
}

// Reads per-hop migration edges from plc_migration_hops.
// Populated by aggregate-plc.ts — run npm run aggregate:plc to refresh.
export function getMigrationTrajectories(): TrajectoryEdge[] {
  const db = getPlcDb();
  return db.prepare(`
    SELECT source, target, value FROM plc_migration_hops ORDER BY value DESC
  `).all() as TrajectoryEdge[];
}

// ── Longevity queries ───────────────────────────────────────────────────

export interface PdsAgeRow {
  pds_url: string;
  first_week: string;    // YYYY-MM-DD (Monday of ISO week)
  total_accounts: number;
}

// First seen date per PDS in the PLC directory: earliest of first account creation week OR first
// migration arrival. Uses plc_creation_weekly (pre-aggregated) to avoid scanning 86M-row table.
// Collapses bsky shards. Excludes junk and pds.trump.com. Requires ≥ 10 accounts.
export function getPdsAgeData(): PdsAgeRow[] {
  const db = getPlcDb();
  return db.prepare(`
    WITH
      creation_first AS (
        SELECT
          CASE WHEN pds_url LIKE '%bsky.network' OR pds_url = 'https://bsky.social'
               THEN 'bsky.network' ELSE pds_url END AS pds_url,
          MIN(week) AS first_seen,
          SUM(count) AS total_accounts
        FROM plc_creation_weekly
        WHERE ${JUNK_PDS_FILTER} AND pds_url != '${TRUMP_PDS}'
        GROUP BY 1
      ),
      migration_first AS (
        SELECT to_pds AS pds_url, MIN(migrated_at) AS first_seen
        FROM plc_migrations
        WHERE ${junkPdsFilter('to_pds')} AND to_pds != '${TRUMP_PDS}'
        GROUP BY to_pds
      )
    SELECT
      c.pds_url,
      date(MIN(c.first_seen, COALESCE(m.first_seen, c.first_seen))) AS first_week,
      c.total_accounts
    FROM creation_first c
    LEFT JOIN migration_first m ON c.pds_url = m.pds_url
    WHERE c.total_accounts >= 10
    ORDER BY first_week
  `).all() as PdsAgeRow[];
}

export interface AccountAgeMonthRow {
  month: string;  // YYYY-MM-DD (week date, aliased for compatibility with age-bucket logic)
  count: number;
}

// Repo-backed account creations per week (active_creation_weekly). Age-bucket math done in JS.
export function getAccountAgeHistogram(): AccountAgeMonthRow[] {
  const db = getPlcDb();
  return db.prepare(`
    SELECT week AS month, SUM(count) AS count
    FROM active_creation_weekly
    WHERE pds_url != '${TRUMP_PDS}'
    GROUP BY week
    ORDER BY week
  `).all() as AccountAgeMonthRow[];
}

// ── Language queries ────────────────────────────────────────────────────

export interface PdsLangRow {
  pds_url: string;
  lang: string;
  dids: number;
  post_count: number;
}

// All (pds_url, lang) pairs from the precomputed summary table.
// pds_lang_summary is populated by aggregate-plc.ts; returns [] if not yet run.
export function getPdsLangSummary(): PdsLangRow[] {
  const db = getPlcDb();
  return db.prepare(`
    SELECT pds_url, lang, dids, post_count
    FROM pds_lang_summary
    ORDER BY pds_url, dids DESC
  `).all() as PdsLangRow[];
}

export interface LangTotal {
  lang: string;
  total_dids: number;
  pds_count: number;
}

// Top languages by distinct-DID count. Includes bsky.network row.
// Callers can filter by pds_url to get community-only or all-inclusive totals.
export function getTopLangs(limit = 25): LangTotal[] {
  const db = getPlcDb();
  return db.prepare(`
    SELECT lang, SUM(dids) AS total_dids, COUNT(DISTINCT pds_url) AS pds_count
    FROM pds_lang_summary
    GROUP BY lang
    ORDER BY total_dids DESC
    LIMIT ?
  `).all(limit) as LangTotal[];
}

export function getLastScanTime(): string | null {
  const db = getPlcDb();
  const row = db.prepare(
    `SELECT MAX(scanned_at) AS last_scan FROM pds_repo_status_snapshots WHERE is_partial = 0`
  ).get() as { last_scan: string | null } | undefined;
  return row?.last_scan ?? null;
}
