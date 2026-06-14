import sql from "./pg";

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

const CACHE_TTL_MS = 60 * 60 * 1000; // 1 hour
const topPdsCache      = new Map<boolean, { data: ScannedTopPds[];  expires: number }>();
const langSummaryCache = new Map<boolean, { data: PdsLangRow[];    expires: number }>();
const topLangsCache    = new Map<boolean, { data: LangTotal[];      expires: number }>();
let lastScanTimeCache: { value: string | null; expires: number } | null = null;
let bskyShardCountsCache: { value: Map<string, number>; expires: number } | null = null;

// Exclude localhost/loopback dev artifacts, reserved TLDs, private IPs, and malformed URLs.
// .dev is a real IANA TLD (Google) — do NOT filter it.
// Pass a table alias (e.g. "w") to avoid ambiguity in joined queries.
export function junkPdsFilter(col = "pds_url") {
  return `
    ${col} NOT LIKE '%localhost%'
    AND ${col} NOT LIKE '%127.0.0.1%'
    AND ${col} NOT LIKE '%0.0.0.0%'
    AND ${col} NOT LIKE '%192.168.%'
    AND ${col} NOT LIKE '%10.0.%'
    AND ${col} NOT LIKE '%172.16.%'
    AND (${col} LIKE 'http://%' OR ${col} LIKE 'https://%')
    AND POSITION('.' IN SUBSTRING(${col} FROM POSITION('://' IN ${col})+3)) > 0
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

export async function getCreationTimeseries(): Promise<MonthlyRow[]> {
  return await sql.unsafe(`
    WITH collapsed AS (
      SELECT
        CASE WHEN pds_url LIKE '%bsky.network' THEN '${BSKY_NETWORK_LABEL}' ELSE pds_url END AS pds_url,
        month,
        SUM(count) AS count
      FROM plc.plc_creation_monthly
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
  `) as unknown as MonthlyRow[];
}

export async function getCreationTimeseriesWeekly(includeTrump = false, hideBsky = false): Promise<TimeseriesRow[]> {
  const trumpFilter = includeTrump ? "" : `AND w.pds_url != '${TRUMP_PDS}'`;
  const bskyFilter  = hideBsky
    ? `AND w.pds_url NOT LIKE '%bsky.network' AND w.pds_url != 'https://bsky.social'`
    : "";
  const bskyVerifiedExempt = `OR w.pds_url LIKE '%bsky.network' OR w.pds_url = 'https://bsky.social'`;
  const verifiedFilter = includeTrump
    ? `AND (v.pds_url IS NOT NULL OR w.pds_url = '${TRUMP_PDS}' ${bskyVerifiedExempt})`
    : `AND (v.pds_url IS NOT NULL ${bskyVerifiedExempt})`;
  return await sql.unsafe(`
    WITH
    verified AS (SELECT DISTINCT pds_url FROM plc.pds_repo_status_snapshots),
    collapsed AS (
      SELECT
        CASE WHEN w.pds_url LIKE '%bsky.network' OR w.pds_url = 'https://bsky.social' THEN '${BSKY_NETWORK_LABEL}' ELSE w.pds_url END AS pds_url,
        week AS period,
        SUM(w.count) AS count
      FROM plc.plc_creation_weekly w
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
  `) as unknown as TimeseriesRow[];
}

export async function getActiveCreationTimeseriesWeekly(hideBsky = false): Promise<TimeseriesRow[]> {
  const bskyFilter = hideBsky ? `AND w.pds_url NOT LIKE '%bsky.network'` : "";
  return await sql.unsafe(`
    WITH
    collapsed AS (
      SELECT
        CASE
          WHEN w.pds_url LIKE '%bsky.network' THEN '${BSKY_NETWORK_LABEL}'
          WHEN w.pds_url = 'https://myatproto.social' THEN 'https://blacksky.app'
          ELSE w.pds_url
        END AS pds_url,
        week AS period,
        SUM(w.count) AS count
      FROM plc.active_creation_weekly w
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
  `) as unknown as TimeseriesRow[];
}

export async function getMigrationTimeseriesWeekly(): Promise<TimeseriesRow[]> {
  return await sql.unsafe(`
    WITH collapsed AS (
      SELECT
        to_pds AS pds_url,
        week AS period,
        SUM(count) AS count
      FROM plc.plc_migration_weekly
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
  `) as unknown as TimeseriesRow[];
}

export async function getMigrationTimeseries(): Promise<MonthlyRow[]> {
  return await sql.unsafe(`
    WITH collapsed AS (
      SELECT
        to_pds AS pds_url,
        month,
        SUM(count) AS count
      FROM plc.plc_migration_monthly
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
  `) as unknown as MonthlyRow[];
}

export interface MigrationFlow {
  source: string;
  target: string;
  value: number;
}

export async function getMigrationFlows(): Promise<MigrationFlow[]> {
  return await sql.unsafe(`
    SELECT
      replace(source, '@0', '') AS source,
      replace(target, '@1', '') AS target,
      value
    FROM plc.plc_trajectory_edges
    WHERE replace(source, '@0', '') != replace(target, '@1', '')
    ORDER BY value DESC
  `) as unknown as MigrationFlow[];
}

export interface WeeklyMigrationRow {
  week: string;
  to_pds: string;
  count: number;
}

// Returns per-week migration counts for the last 18 months, broken down by the same
// top-10 destinations used in the Sankey (so sink names match for cross-highlighting).
export async function getMigrationWeeklyBreakdown(topN = 10): Promise<WeeklyMigrationRow[]> {
  return await sql.unsafe(`
    WITH
      verified AS (SELECT DISTINCT pds_url FROM plc.pds_repo_status_snapshots),
      top_targets AS (
        SELECT CASE WHEN to_pds LIKE '%bsky.network' THEN '${BSKY_NETWORK_LABEL}' ELSE to_pds END AS target
        FROM plc.plc_migration_monthly
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
        FROM plc.plc_migration_weekly w
        WHERE NOT (w.from_pds LIKE '%bsky.network' AND w.to_pds LIKE '%bsky.network')
          AND (w.to_pds LIKE '%bsky.network' OR w.to_pds IN (SELECT pds_url FROM verified))
          AND (w.from_pds LIKE '%bsky.network' OR w.from_pds IN (SELECT pds_url FROM verified))
          AND w.week >= TO_CHAR(CURRENT_DATE - INTERVAL '18 months', 'YYYY-MM-DD')
      )
    SELECT week, to_pds, SUM(count) AS count
    FROM labeled
    GROUP BY week, to_pds
    ORDER BY week, to_pds
  `) as unknown as WeeklyMigrationRow[];
}

export async function getLatestPdsStatusSnapshot(): Promise<PdsStatusRow[]> {
  return await sql.unsafe(`
    WITH latest AS (
      SELECT pds_url, MAX(snapshot_date) AS snapshot_date
      FROM plc.pds_repo_status_snapshots
      GROUP BY pds_url
    )
    SELECT s.pds_url, s.snapshot_date, s.active, s.deactivated, s.deleted,
           s.takendown, s.suspended, s.other, s.total_scanned
    FROM plc.pds_repo_status_snapshots s
    JOIN latest l ON s.pds_url = l.pds_url AND s.snapshot_date = l.snapshot_date
    ORDER BY s.total_scanned DESC
  `) as unknown as PdsStatusRow[];
}

export async function getPlcDataTimestamp(): Promise<{ collected_through: string; aggregated_at: string } | null> {
  const [cursor, agg] = await Promise.all([
    sql`SELECT after AS collected_through FROM plc.plc_cursor WHERE id = 1`,
    sql`SELECT updated_at AS aggregated_at FROM plc.plc_aggregation_cursor WHERE id = 1`,
  ]);
  if (!cursor[0]) return null;
  return {
    collected_through: cursor[0].collected_through as string,
    aggregated_at: (agg[0]?.aggregated_at as string | undefined) ?? cursor[0].collected_through as string,
  };
}

export async function getActiveCreationLastRun(): Promise<string | null> {
  const rows = await sql`SELECT updated_at FROM plc.active_creation_cursor WHERE id = 1`;
  return (rows[0]?.updated_at as string | undefined) ?? null;
}

export async function getEcosystemStats(hideBsky = false): Promise<EcosystemStats> {
  const bskyMigrationFilter = hideBsky
    ? `AND from_pds NOT LIKE '%bsky.network' AND to_pds NOT LIKE '%bsky.network'
       AND from_pds != 'https://bsky.social' AND to_pds != 'https://bsky.social'`
    : `AND NOT (from_pds LIKE '%bsky.network' AND to_pds LIKE '%bsky.network')
       AND from_pds != 'https://bsky.social' AND to_pds != 'https://bsky.social'`;

  const [statsRows, migrationsRows, indepRows, datesRows] = await Promise.all([
    sql`SELECT total_dids, bsky_concentration_pct, unique_migrating_dids FROM plc.plc_stats_cache WHERE id = 1`,
    sql.unsafe<{ total_migrations: string | null }[]>(`
      SELECT SUM(count)::bigint AS total_migrations FROM plc.plc_migration_monthly
      WHERE (from_pds IN (SELECT pds_url FROM plc.pds_repo_status_snapshots) OR from_pds LIKE '%bsky.network')
        AND (to_pds IN (SELECT pds_url FROM plc.pds_repo_status_snapshots) OR to_pds LIKE '%bsky.network')
        ${bskyMigrationFilter}
    `),
    sql`SELECT COUNT(DISTINCT pds_url)::int AS independent_pds_count FROM plc.pds_repo_status_snapshots`,
    sql`
      SELECT MIN(month) || '-01' AS earliest_creation, MAX(month) || '-01' AS latest_creation
      FROM plc.plc_creation_monthly
      WHERE pds_url != ${TRUMP_PDS}
    `,
  ]);

  const statsCache = statsRows[0] as { total_dids: number; bsky_concentration_pct: number; unique_migrating_dids: number } | undefined;
  const migrations = migrationsRows[0] as { total_migrations: string | null } | undefined;
  const indep = indepRows[0] as { independent_pds_count: number } | undefined;
  const dates = datesRows[0] as { earliest_creation: string; latest_creation: string } | undefined;

  return {
    total_dids: statsCache?.total_dids ?? 0,
    total_dids_ex_trump: statsCache?.total_dids ?? 0,
    total_migrations: Number(migrations?.total_migrations ?? 0),
    unique_migrating_dids: statsCache?.unique_migrating_dids ?? 0,
    independent_pds_count: indep?.independent_pds_count ?? 0,
    independent_pds_account_pct: 0,
    bsky_concentration_pct: statsCache?.bsky_concentration_pct ?? 0,
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
export async function getMigrationTrajectories(): Promise<TrajectoryEdge[]> {
  return await sql`
    SELECT source, target, value FROM plc.plc_migration_hops ORDER BY value DESC
  ` as unknown as TrajectoryEdge[];
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
export async function getPdsAgeData(minAccounts = 1): Promise<PdsAgeRow[]> {
  return await sql.unsafe(`
    WITH
      latest_scan AS (
        SELECT RTRIM(pds_url, '/') AS pds_url, MAX(active) AS total_accounts
        FROM plc.pds_repo_status_snapshots s
        WHERE snapshot_date = (
          SELECT MAX(s2.snapshot_date) FROM plc.pds_repo_status_snapshots s2
          WHERE s2.pds_url = s.pds_url
        )
        GROUP BY RTRIM(pds_url, '/')
      ),
      known_mirrors AS (
        SELECT 'https://dev.blacksky.app' AS pds_url
        UNION ALL SELECT 'https://cryptoanarchy.network'
      ),
      creation_first AS (
        SELECT RTRIM(pds_url, '/') AS pds_url, MIN(week) AS first_seen
        FROM plc.plc_creation_weekly
        WHERE ${JUNK_PDS_FILTER} AND pds_url != '${TRUMP_PDS}'
          AND RTRIM(pds_url, '/') IN (SELECT pds_url FROM latest_scan)
          AND RTRIM(pds_url, '/') NOT IN (SELECT pds_url FROM known_mirrors)
        GROUP BY RTRIM(pds_url, '/')
      ),
      migration_first AS (
        SELECT RTRIM(to_pds, '/') AS pds_url, MIN(migrated_at) AS first_seen
        FROM plc.plc_migrations
        WHERE ${junkPdsFilter('to_pds')} AND to_pds != '${TRUMP_PDS}'
        GROUP BY RTRIM(to_pds, '/')
      )
    SELECT
      c.pds_url,
      LEAST(c.first_seen::date, COALESCE(m.first_seen::date, c.first_seen::date))::text AS first_week,
      ls.total_accounts
    FROM creation_first c
    JOIN latest_scan ls ON c.pds_url = ls.pds_url AND ls.total_accounts >= ${minAccounts}
    LEFT JOIN migration_first m ON c.pds_url = m.pds_url
    ORDER BY first_week
  `) as unknown as PdsAgeRow[];
}

export async function getActivePdsCount(): Promise<number> {
  const rows = await sql.unsafe<{ cnt: number }[]>(`
    SELECT COUNT(*)::int AS cnt FROM (
      SELECT RTRIM(pds_url, '/') AS pds_url
      FROM plc.pds_repo_status_snapshots
      WHERE ${JUNK_PDS_FILTER}
      GROUP BY RTRIM(pds_url, '/')
      HAVING MAX(total_scanned) > 0
    ) sub
  `);
  return rows[0].cnt;
}

export async function getScannedPdsCount(): Promise<number> {
  const rows = await sql.unsafe<{ cnt: number }[]>(`
    SELECT COUNT(DISTINCT RTRIM(pds_url, '/'))::int AS cnt
    FROM plc.pds_repo_status_snapshots s
    WHERE snapshot_date = (
      SELECT MAX(s2.snapshot_date) FROM plc.pds_repo_status_snapshots s2 WHERE s2.pds_url = s.pds_url
    )
  `);
  return rows[0].cnt;
}

export interface ScannedTopPds {
  url: string;
  repoCount: number;
  activeCount: number;
  snapshot_date: string;
}

/** Top PDSes by repo count from the most recent scan:pds-status run.
 *  Collapses bsky.network shards (SUM — genuinely separate backends) and
 *  same-IP non-bsky aliases (MAX — same repos served under multiple hostnames). */
export async function getTopPdsByScan(limit = 15, hideBsky = false): Promise<ScannedTopPds[]> {
  const cached = topPdsCache.get(hideBsky);
  if (cached && Date.now() < cached.expires) return cached.data.slice(0, limit);

  const rows = await sql.unsafe(`
    WITH latest AS (
      SELECT RTRIM(pds_url, '/') AS norm_url, MAX(snapshot_date) AS snap_date
      FROM plc.pds_repo_status_snapshots
      GROUP BY norm_url
    ),
    best AS (
      SELECT
        RTRIM(s.pds_url, '/') AS url,
        s.ip_address,
        MAX(s.total_scanned) AS total_scanned,
        s.active,
        l.snap_date AS snapshot_date
      FROM plc.pds_repo_status_snapshots s
      JOIN latest l ON RTRIM(s.pds_url, '/') = l.norm_url AND s.snapshot_date = l.snap_date
      WHERE s.total_scanned > 0
      GROUP BY RTRIM(s.pds_url, '/'), s.ip_address, s.active, l.snap_date
    )
    SELECT url, ip_address, total_scanned, active, snapshot_date FROM best
  `) as unknown as { url: string; ip_address: string | null; total_scanned: number; active: number; snapshot_date: string }[];

  const isBsky = (u: string) => u.includes(".host.bsky.network") || /bsky\.social/.test(u);
  const hostname = (u: string) => u.replace(/^https?:\/\//, "");

  let bskyRepos = 0, bskyActive = 0, bskyDate = "";
  const ipGroups = new Map<string, { url: string; repoCount: number; activeCount: number; snapshot_date: string }>();

  for (const r of rows) {
    if (isBsky(r.url)) {
      bskyRepos  += r.total_scanned;
      bskyActive += r.active;
      if (r.snapshot_date > bskyDate) bskyDate = r.snapshot_date;
      continue;
    }
    const key = r.ip_address ?? r.url;
    const existing = ipGroups.get(key);
    if (!existing) {
      ipGroups.set(key, { url: r.url, repoCount: r.total_scanned, activeCount: r.active, snapshot_date: r.snapshot_date });
    } else {
      if (hostname(r.url).length < hostname(existing.url).length) existing.url = r.url;
      if (r.total_scanned > existing.repoCount) {
        existing.repoCount  = r.total_scanned;
        existing.activeCount = r.active;
      }
      if (r.snapshot_date > existing.snapshot_date) existing.snapshot_date = r.snapshot_date;
    }
  }

  const results: ScannedTopPds[] = [...ipGroups.values()].map(g => ({
    url: g.url,
    repoCount: g.repoCount,
    activeCount: g.activeCount,
    snapshot_date: g.snapshot_date,
  }));

  if (!hideBsky && bskyRepos > 0) {
    results.push({ url: "https://bsky.social", repoCount: bskyRepos, activeCount: bskyActive, snapshot_date: bskyDate });
  }

  const sorted = results.sort((a, b) => b.activeCount - a.activeCount);
  topPdsCache.set(hideBsky, { data: sorted, expires: Date.now() + CACHE_TTL_MS });
  return sorted.slice(0, limit);
}

/** Per-shard repo counts for bsky.network shards, keyed by normalized URL. */
export async function getBskyShardCounts(): Promise<Map<string, number>> {
  if (bskyShardCountsCache && Date.now() < bskyShardCountsCache.expires) return bskyShardCountsCache.value;
  const rows = await sql.unsafe(`
    WITH latest AS (
      SELECT RTRIM(pds_url, '/') AS norm_url, MAX(snapshot_date) AS snap_date
      FROM plc.pds_repo_status_snapshots
      WHERE pds_url LIKE '%.host.bsky.network%'
      GROUP BY norm_url
    )
    SELECT RTRIM(s.pds_url, '/') AS url, MAX(s.total_scanned)::int AS repo_count
    FROM plc.pds_repo_status_snapshots s
    JOIN latest l ON RTRIM(s.pds_url, '/') = l.norm_url AND s.snapshot_date = l.snap_date
    GROUP BY RTRIM(s.pds_url, '/')
  `) as unknown as { url: string; repo_count: number }[];
  const value = new Map(rows.map(r => [r.url, r.repo_count]));
  bskyShardCountsCache = { value, expires: Date.now() + CACHE_TTL_MS };
  return value;
}

export interface AccountCohortRow {
  cohort: string;
  count: number;
}

// Repo-backed account creations bucketed by the same cohorts used in the analysis script.
export async function getAccountCohortCounts(): Promise<AccountCohortRow[]> {
  return await sql.unsafe(`
    SELECT
      CASE
        WHEN week < '2023-01-01' THEN 'pre-2023'
        WHEN week < '2024-01-01' THEN '2023'
        WHEN week < '2024-11-01' THEN '2024 (pre-Nov)'
        WHEN week < '2025-01-01' THEN 'Nov–Dec 2024 (exodus)'
        WHEN week < '2025-07-01' THEN '2025 H1'
        WHEN week < '2026-01-01' THEN '2025 H2'
        WHEN week < '2026-04-11' THEN '2026 (pre-analysis)'
        ELSE '2026 (in-window)'
      END AS cohort,
      SUM(count)::bigint AS count
    FROM plc.active_creation_weekly
    WHERE pds_url != '${TRUMP_PDS}'
    GROUP BY cohort
    ORDER BY MIN(week)
  `) as unknown as AccountCohortRow[];
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
export async function getPdsLangSummary(hideBsky = false): Promise<PdsLangRow[]> {
  const cached = langSummaryCache.get(hideBsky);
  if (cached && Date.now() < cached.expires) return cached.data;

  const bskyFilter = hideBsky
    ? `WHERE pds_url NOT LIKE '%bsky.network%' AND pds_url != 'https://bsky.social'`
    : "";
  const data = await sql.unsafe(`
    SELECT
      CASE WHEN pds_url = 'https://myatproto.social' THEN 'https://blacksky.app' ELSE pds_url END AS pds_url,
      lang, dids, post_count
    FROM plc.pds_lang_summary
    ${bskyFilter}
    ORDER BY pds_url, dids DESC
  `) as unknown as PdsLangRow[];
  langSummaryCache.set(hideBsky, { data, expires: Date.now() + CACHE_TTL_MS });
  return data;
}

export interface LangTotal {
  lang: string;
  total_dids: number;
  pds_count: number;
}

// Top languages by distinct-DID count.
export async function getTopLangs(limit = 25, hideBsky = false): Promise<LangTotal[]> {
  const cached = topLangsCache.get(hideBsky);
  if (cached && Date.now() < cached.expires) return cached.data.slice(0, limit);

  const bskyFilter = hideBsky
    ? `WHERE pds_url NOT LIKE '%bsky.network%' AND pds_url != 'https://bsky.social'`
    : "";
  const data = await sql.unsafe(`
    SELECT lang, SUM(dids)::int AS total_dids, COUNT(DISTINCT pds_url)::int AS pds_count
    FROM plc.pds_lang_summary
    ${bskyFilter}
    GROUP BY lang
    ORDER BY total_dids DESC
  `) as unknown as LangTotal[];
  topLangsCache.set(hideBsky, { data, expires: Date.now() + CACHE_TTL_MS });
  return data.slice(0, limit);
}

export interface MigrationJourneyStats {
  buckets: { label: string; users: number }[];
  maxMigrations: number;
  medianMigrations: number;
  pctMultiple: number; // % of migrants with 2+ migrations
  totalMigrants: number;
}

export async function getMigrationJourneyStats(): Promise<MigrationJourneyStats> {
  const rows = await sql.unsafe(`
    SELECT migration_count, COUNT(*)::int AS users
    FROM (
      SELECT did, COUNT(*) AS migration_count
      FROM plc.plc_migrations
      WHERE NOT (
        (from_pds LIKE '%bsky.network' OR from_pds = 'https://bsky.social')
        AND (to_pds LIKE '%bsky.network' OR to_pds = 'https://bsky.social')
      )
      GROUP BY did
      HAVING COUNT(*) >= 1
    ) sub
    GROUP BY migration_count
    ORDER BY migration_count
  `) as unknown as { migration_count: number; users: number }[];

  if (rows.length === 0) return { buckets: [], maxMigrations: 0, medianMigrations: 1, pctMultiple: 0, totalMigrants: 0 };

  const totalMigrants = rows.reduce((s, r) => s + r.users, 0);
  const maxMigrations = rows[rows.length - 1].migration_count;
  const multipleUsers = rows.filter(r => r.migration_count >= 2).reduce((s, r) => s + r.users, 0);
  const pctMultiple = totalMigrants > 0 ? (multipleUsers / totalMigrants) * 100 : 0;

  let cum = 0;
  let medianMigrations = 1;
  const half = totalMigrants / 2;
  for (const r of rows) {
    cum += r.users;
    if (cum >= half) { medianMigrations = r.migration_count; break; }
  }

  const buckets = [
    { label: "1", users: 0 },
    { label: "2", users: 0 },
    { label: "3", users: 0 },
    { label: "4", users: 0 },
    { label: "5–9", users: 0 },
    { label: "10+", users: 0 },
  ];
  for (const r of rows) {
    const n = r.migration_count;
    if      (n === 1)           buckets[0].users += r.users;
    else if (n === 2)           buckets[1].users += r.users;
    else if (n === 3)           buckets[2].users += r.users;
    else if (n === 4)           buckets[3].users += r.users;
    else if (n <= 9)            buckets[4].users += r.users;
    else                        buckets[5].users += r.users;
  }

  return { buckets, maxMigrations, medianMigrations, pctMultiple, totalMigrants };
}

export async function getLastScanTime(): Promise<string | null> {
  if (lastScanTimeCache && Date.now() < lastScanTimeCache.expires) return lastScanTimeCache.value;
  const rows = await sql`
    SELECT MAX(scanned_at) AS last_scan FROM plc.pds_repo_status_snapshots WHERE is_partial = 0
  `;
  const value = (rows[0]?.last_scan as string | null) ?? null;
  lastScanTimeCache = { value, expires: Date.now() + CACHE_TTL_MS };
  return value;
}

// ── Dashboard queries (migrated from queries.ts / atproto-health.db) ──────────

const BSKY_SNAP_FILTER = `pds_url NOT LIKE '%host.bsky.network%' AND pds_url NOT LIKE '%bsky.social%'`;

// Returns a CTE selecting the latest non-partial snapshot per PDS,
// optionally filtering out bsky infrastructure.
function latestSnapshotCte(hideBsky: boolean) {
  const bskyFilter = hideBsky ? `AND ${BSKY_SNAP_FILTER}` : "";
  return `
    WITH latest AS (
      SELECT RTRIM(pds_url, '/') AS pds_url,
             MAX(snapshot_date)  AS snap_date,
             MAX(total_scanned)  AS best_total_scanned,
             MAX(active)         AS best_active
      FROM plc.pds_repo_status_snapshots
      WHERE is_partial = 0 AND ${JUNK_PDS_FILTER} ${bskyFilter}
      GROUP BY RTRIM(pds_url, '/')
    ),
    pds_raw AS (
      SELECT s.*
      FROM plc.pds_repo_status_snapshots s
      JOIN latest l ON RTRIM(s.pds_url, '/') = l.pds_url AND s.snapshot_date = l.snap_date
    ),
    latest_geo_date AS (
      SELECT RTRIM(pds_url, '/') AS norm_url, MAX(snapshot_date) AS snap_date
      FROM plc.pds_repo_status_snapshots WHERE latitude IS NOT NULL OR org IS NOT NULL
      GROUP BY RTRIM(pds_url, '/')
    ),
    latest_geo AS (
      SELECT RTRIM(s.pds_url, '/') AS norm_url,
             s.org, s.city, s.country, s.country_code, s.region,
             s.latitude, s.longitude, s.isp, s.as_number
      FROM plc.pds_repo_status_snapshots s
      JOIN latest_geo_date d ON RTRIM(s.pds_url, '/') = d.norm_url AND s.snapshot_date = d.snap_date
    ),
    latest_version_date AS (
      SELECT RTRIM(pds_url, '/') AS norm_url, MAX(snapshot_date) AS snap_date
      FROM plc.pds_repo_status_snapshots WHERE version IS NOT NULL
      GROUP BY RTRIM(pds_url, '/')
    ),
    latest_version AS (
      SELECT RTRIM(s.pds_url, '/') AS norm_url, s.version
      FROM plc.pds_repo_status_snapshots s
      JOIN latest_version_date d ON RTRIM(s.pds_url, '/') = d.norm_url AND s.snapshot_date = d.snap_date
    ),
    dir_pds AS (
      SELECT RTRIM(pds_url, '/') AS norm_url
      FROM plc.pds_repo_status_snapshots
      WHERE in_directory = 1
      GROUP BY RTRIM(pds_url, '/')
    ),
    pds_latest AS (
      SELECT
        r.id, r.pds_url, r.snapshot_date, r.active, r.deactivated, r.deleted,
        r.takendown, r.suspended, r.other, r.total_scanned, r.is_sampled,
        r.did_plc_count, r.did_web_count, r.is_partial, r.scanned_at, r.ip_address,
        l.best_total_scanned, l.best_active,
        COALESCE(r.version, v.version)           AS version,
        r.invite_code_required, r.is_online,
        COALESCE(r.country,      g.country)      AS country,
        COALESCE(r.country_code, g.country_code) AS country_code,
        COALESCE(r.region,       g.region)       AS region,
        COALESCE(r.city,         g.city)         AS city,
        COALESCE(r.latitude,     g.latitude)     AS latitude,
        COALESCE(r.longitude,    g.longitude)    AS longitude,
        COALESCE(r.org,          g.org)          AS org,
        COALESCE(r.isp,          g.isp)          AS isp,
        COALESCE(r.as_number,    g.as_number)    AS as_number,
        CASE WHEN d.norm_url IS NOT NULL THEN 1 ELSE 0 END AS in_directory
      FROM pds_raw r
      JOIN latest l ON RTRIM(r.pds_url, '/') = l.pds_url
      LEFT JOIN latest_geo g ON RTRIM(r.pds_url, '/') = g.norm_url
      LEFT JOIN latest_version v ON RTRIM(r.pds_url, '/') = v.norm_url
      LEFT JOIN dir_pds d ON RTRIM(r.pds_url, '/') = d.norm_url
    )
  `;
}

export const PROVIDER_NORMALIZE_SQL = `
  CASE
    WHEN org LIKE '%Cloudflare%' THEN 'Behind Cloudflare (host unknown)'
    WHEN org LIKE '%DigitalOcean%' OR org LIKE '%Digital Ocean%' THEN 'DigitalOcean'
    WHEN org LIKE '%Hetzner%' OR org LIKE '%HETZNER%' THEN 'Hetzner'
    WHEN org LIKE '%OVH%' THEN 'OVH'
    WHEN org LIKE '%AWS%' OR org LIKE '%Amazon%' THEN 'AWS'
    WHEN org LIKE '%Google%' THEN 'Google Cloud'
    WHEN org LIKE '%Microsoft%' OR org LIKE '%Azure%' THEN 'Azure'
    WHEN org LIKE '%Linode%' OR org LIKE '%Akamai%' THEN 'Akamai/Linode'
    WHEN org LIKE '%Vultr%' THEN 'Vultr'
    WHEN org LIKE '%i3Dnet%' OR org LIKE '%i3D.net%' THEN 'i3D.net'
    WHEN org LIKE '%Scaleway%' THEN 'Scaleway'
    WHEN org LIKE '%Oracle%' THEN 'Oracle Cloud'
    WHEN org LIKE '%Contabo%' THEN 'Contabo'
    WHEN org LIKE '%Fastly%' THEN 'Fastly (CDN)'
    WHEN org LIKE '%Fly.io%' OR org LIKE '%Fly IO%' THEN 'Fly.io'
    WHEN org LIKE '%netcup%' OR org LIKE '%NETCUP%' THEN 'netcup'
    WHEN org LIKE '%RackNerd%' THEN 'RackNerd'
    WHEN org LIKE '%IONOS%' OR org LIKE '%Ionos%' THEN 'IONOS'
    WHEN org IS NULL OR org = '' THEN 'Unknown'
    ELSE org
  END
`;

export interface OverviewStats {
  total: number;
  online: number;
  offline: number;
  openReg: number;
  inviteOnly: number;
  countries: number;
  totalUsers: number;
  activeUsers: number;
}

export async function getOverviewStats(hideBsky = false): Promise<OverviewStats> {
  const bskyFilter = hideBsky ? `AND ${BSKY_SNAP_FILTER}` : "";

  const [dirStatsRows, repoStatsRows] = await Promise.all([
    sql.unsafe<{ total: number; online: number; offline: number; openReg: number; inviteOnly: number }[]>(`
      SELECT
        COUNT(*)::int as total,
        SUM(CASE WHEN is_online = 1 THEN 1 ELSE 0 END)::int as online,
        SUM(CASE WHEN is_online = 0 THEN 1 ELSE 0 END)::int as offline,
        SUM(CASE WHEN invite_code_required = 0 THEN 1 ELSE 0 END)::int as "openReg",
        SUM(CASE WHEN invite_code_required != 0 OR invite_code_required IS NULL THEN 1 ELSE 0 END)::int as "inviteOnly"
      FROM (
        SELECT RTRIM(pds_url, '/') AS norm_url, is_online, invite_code_required,
               ROW_NUMBER() OVER (PARTITION BY RTRIM(pds_url, '/') ORDER BY snapshot_date DESC) AS rn
        FROM plc.pds_repo_status_snapshots
        WHERE in_directory = 1 AND is_partial = 0 AND ${JUNK_PDS_FILTER} ${bskyFilter}
      ) sub WHERE rn = 1
    `),
    sql.unsafe<{ countries: number; totalUsers: string; activeUsers: string }[]>(`
      ${latestSnapshotCte(hideBsky)},
      ever_had_repos AS (
        SELECT RTRIM(pds_url, '/') AS norm_url
        FROM plc.pds_repo_status_snapshots
        WHERE ${JUNK_PDS_FILTER} ${bskyFilter}
        GROUP BY RTRIM(pds_url, '/')
        HAVING MAX(total_scanned) >= 0
      ),
      backend_deduped AS (
        SELECT
          CASE
            WHEN org LIKE '%Cloudflare%' THEN RTRIM(pds_url, '/')
            ELSE COALESCE(ip_address, RTRIM(pds_url, '/'))
          END AS backend,
          MAX(best_total_scanned) AS total_scanned,
          MAX(best_active)        AS active
        FROM pds_latest
        GROUP BY CASE
          WHEN org LIKE '%Cloudflare%' THEN RTRIM(pds_url, '/')
          ELSE COALESCE(ip_address, RTRIM(pds_url, '/'))
        END
      )
      SELECT
        (SELECT COUNT(DISTINCT country_code)::int
         FROM latest_geo g
         JOIN ever_had_repos e ON g.norm_url = e.norm_url
         WHERE country_code IS NOT NULL) as countries,
        COALESCE(SUM(total_scanned), 0)::bigint as "totalUsers",
        COALESCE(SUM(active), 0)::bigint as "activeUsers"
      FROM backend_deduped
    `),
  ]);

  const dirStats = dirStatsRows[0];
  const repoStats = repoStatsRows[0];

  return {
    total: dirStats.total,
    online: dirStats.online,
    offline: dirStats.offline,
    openReg: dirStats.openReg,
    inviteOnly: dirStats.inviteOnly,
    countries: repoStats.countries,
    totalUsers: Number(repoStats.totalUsers),
    activeUsers: Number(repoStats.activeUsers),
  };
}

export interface CountryCount {
  country: string;
  countryCode: string;
  count: number;
}

export async function getCountryDistribution(hideBsky = false): Promise<CountryCount[]> {
  return await sql.unsafe(`
    ${latestSnapshotCte(hideBsky)}
    SELECT country, country_code as "countryCode", COUNT(*)::int as count
    FROM pds_latest
    WHERE country IS NOT NULL
    GROUP BY country_code, country ORDER BY count DESC
  `) as unknown as CountryCount[];
}

export interface CountryRepoCount {
  country: string;
  countryCode: string;
  repoCount: number;
}

export async function getReposByCountry(hideBsky = false): Promise<CountryRepoCount[]> {
  return await sql.unsafe(`
    ${latestSnapshotCte(hideBsky)}
    SELECT country, country_code as "countryCode", SUM(total_scanned)::bigint as "repoCount"
    FROM pds_latest
    WHERE country IS NOT NULL AND total_scanned > 0
    GROUP BY country_code, country ORDER BY "repoCount" DESC
  `) as unknown as CountryRepoCount[];
}

export interface VersionCount {
  version: string;
  count: number;
}

export async function getVersionDistribution(hideBsky = false): Promise<VersionCount[]> {
  return await sql.unsafe(`
    ${latestSnapshotCte(hideBsky)}
    SELECT COALESCE(version, 'unknown') as version, COUNT(*)::int as count
    FROM pds_latest
    WHERE in_directory = 1
    GROUP BY version ORDER BY count DESC
  `) as unknown as VersionCount[];
}

export interface HostingProviderCount {
  provider: string;
  count: number;
  isCdn: boolean;
}

export async function getHostingProviders(hideBsky = false): Promise<HostingProviderCount[]> {
  return await sql.unsafe(`
    ${latestSnapshotCte(hideBsky)}
    SELECT
      ${PROVIDER_NORMALIZE_SQL} as provider,
      COUNT(*)::int as count,
      CASE WHEN org LIKE '%Cloudflare%' OR org LIKE '%Fastly%' THEN true ELSE false END as "isCdn"
    FROM pds_latest
    WHERE in_directory = 1
    GROUP BY provider, "isCdn" ORDER BY count DESC
  `) as unknown as HostingProviderCount[];
}

export async function getCloudflareBreakdown(hideBsky = false): Promise<{ behindCdn: number; directHosting: number; unknown: number }> {
  const rows = await sql.unsafe<{ behindCdn: number; directHosting: number; unknown: number }[]>(`
    ${latestSnapshotCte(hideBsky)}
    SELECT
      SUM(CASE WHEN org LIKE '%Cloudflare%' OR org LIKE '%Fastly%' THEN 1 ELSE 0 END)::int as "behindCdn",
      SUM(CASE WHEN org IS NOT NULL AND org != '' AND org NOT LIKE '%Cloudflare%' AND org NOT LIKE '%Fastly%' THEN 1 ELSE 0 END)::int as "directHosting",
      SUM(CASE WHEN org IS NULL OR org = '' THEN 1 ELSE 0 END)::int as unknown
    FROM pds_latest
    WHERE in_directory = 1
  `);
  return rows[0];
}

export interface UserDistBucket {
  range: string;
  count: number;
  sortKey: number;
}

export async function getUserDistribution(hideBsky = false): Promise<UserDistBucket[]> {
  const rows = await sql.unsafe(`
    ${latestSnapshotCte(hideBsky)}
    SELECT total_scanned as users FROM pds_latest WHERE total_scanned IS NOT NULL
  `) as unknown as { users: number }[];

  const buckets: Record<string, { count: number; sortKey: number }> = {
    "0":      { count: 0, sortKey: 0 },
    "1":      { count: 0, sortKey: 1 },
    "2-5":    { count: 0, sortKey: 2 },
    "6-10":   { count: 0, sortKey: 3 },
    "11-50":  { count: 0, sortKey: 4 },
    "51-100": { count: 0, sortKey: 5 },
    "101-500":{ count: 0, sortKey: 6 },
    "501-1K": { count: 0, sortKey: 7 },
    "1K+":    { count: 0, sortKey: 8 },
  };
  for (const { users } of rows) {
    if      (users === 0)   buckets["0"].count++;
    else if (users === 1)   buckets["1"].count++;
    else if (users <= 5)    buckets["2-5"].count++;
    else if (users <= 10)   buckets["6-10"].count++;
    else if (users <= 50)   buckets["11-50"].count++;
    else if (users <= 100)  buckets["51-100"].count++;
    else if (users <= 500)  buckets["101-500"].count++;
    else if (users <= 1000) buckets["501-1K"].count++;
    else                    buckets["1K+"].count++;
  }
  return Object.entries(buckets).map(([range, { count, sortKey }]) => ({ range, count, sortKey }));
}

export interface ConcentrationStats {
  top1Pct: number;
  top5Pct: number;
  top10Pct: number;
  totalWithData: number;
}

export async function getConcentrationStats(hideBsky = false): Promise<ConcentrationStats> {
  const [statsCacheRows, totalWithDataRows] = await Promise.all([
    sql<{ bsky_concentration_pct: number }[]>`SELECT bsky_concentration_pct FROM plc.plc_stats_cache WHERE id = 1`,
    sql.unsafe<{ n: number }[]>(`
      SELECT COUNT(*)::int as n FROM (
        SELECT RTRIM(pds_url, '/') AS pds_url
        FROM plc.pds_repo_status_snapshots
        WHERE ${JUNK_PDS_FILTER}
        GROUP BY RTRIM(pds_url, '/')
        HAVING MAX(total_scanned) > 0
      ) sub
    `),
  ]);

  const statsCache = statsCacheRows[0];
  const top1Pct = hideBsky ? 0 : (statsCache?.bsky_concentration_pct ?? 0);
  const totalWithData = totalWithDataRows[0].n;

  return { top1Pct, top5Pct: 0, top10Pct: 0, totalWithData };
}

export interface CityCluster {
  latitude: number;
  longitude: number;
  city: string | null;
  country: string | null;
  pdsCount: number;
}

export async function getPdsLocations(hideBsky = false): Promise<CityCluster[]> {
  const cte = latestSnapshotCte(hideBsky);
  return await sql.unsafe(`
    ${cte},
    placed AS (
      SELECT
        CASE WHEN org LIKE '%Cloudflare%' OR org LIKE '%Fastly%' THEN 38.0  ELSE latitude  END AS latitude,
        CASE WHEN org LIKE '%Cloudflare%' OR org LIKE '%Fastly%' THEN -30.0 ELSE longitude END AS longitude,
        CASE WHEN org LIKE '%Cloudflare%' OR org LIKE '%Fastly%' THEN 'CDN / Host Unknown' ELSE city    END AS city,
        CASE WHEN org LIKE '%Cloudflare%' OR org LIKE '%Fastly%' THEN NULL                 ELSE country END AS country
      FROM pds_latest
      WHERE latitude IS NOT NULL AND longitude IS NOT NULL
      UNION ALL
      SELECT latitude, longitude, city, country FROM plc.pds_manual_geo
    )
    SELECT AVG(latitude) AS latitude, AVG(longitude) AS longitude,
           city, country, COUNT(*)::int AS "pdsCount"
    FROM placed
    GROUP BY city, country
    ORDER BY "pdsCount" DESC
  `) as unknown as CityCluster[];
}

export interface PdsProviderLocation {
  url: string;
  latitude: number;
  longitude: number;
  city: string | null;
  country: string | null;
  provider: string;
}

export async function getPdsLocationsWithProvider(hideBsky = false): Promise<PdsProviderLocation[]> {
  const cte = latestSnapshotCte(hideBsky);
  return await sql.unsafe(`
    ${cte},
    combined AS (
      SELECT
        pds_url AS url,
        CASE WHEN org LIKE '%Cloudflare%' THEN 38.0  ELSE latitude  END AS latitude,
        CASE WHEN org LIKE '%Cloudflare%' THEN -30.0 ELSE longitude END AS longitude,
        CASE WHEN org LIKE '%Cloudflare%' THEN 'Cloudflare Network' ELSE city    END AS city,
        CASE WHEN org LIKE '%Cloudflare%' THEN NULL                  ELSE country END AS country,
        ${PROVIDER_NORMALIZE_SQL} as provider
      FROM pds_latest
      WHERE in_directory = 1 AND (latitude IS NOT NULL OR org IS NOT NULL)
      UNION ALL
      SELECT url, latitude, longitude, city, country, COALESCE(org, 'Self-hosted') AS provider
      FROM plc.pds_manual_geo
    )
    SELECT * FROM combined
  `) as unknown as PdsProviderLocation[];
}
