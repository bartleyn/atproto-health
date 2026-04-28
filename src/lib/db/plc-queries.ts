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
export function getPdsAgeData(minAccounts = 1): PdsAgeRow[] {
  const db = getPlcDb();
  return db.prepare(`
    WITH
      latest_scan AS (
        SELECT RTRIM(pds_url, '/') AS pds_url, MAX(active) AS total_accounts
        FROM pds_repo_status_snapshots s
        WHERE snapshot_date = (
          SELECT MAX(s2.snapshot_date) FROM pds_repo_status_snapshots s2
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
        FROM plc_creation_weekly
        WHERE ${JUNK_PDS_FILTER} AND pds_url != '${TRUMP_PDS}'
          AND RTRIM(pds_url, '/') IN (SELECT pds_url FROM latest_scan)
          AND RTRIM(pds_url, '/') NOT IN (SELECT pds_url FROM known_mirrors)
        GROUP BY RTRIM(pds_url, '/')
      ),
      migration_first AS (
        SELECT RTRIM(to_pds, '/') AS pds_url, MIN(migrated_at) AS first_seen
        FROM plc_migrations
        WHERE ${junkPdsFilter('to_pds')} AND to_pds != '${TRUMP_PDS}'
        GROUP BY RTRIM(to_pds, '/')
      )
    SELECT
      c.pds_url,
      date(MIN(c.first_seen, COALESCE(m.first_seen, c.first_seen))) AS first_week,
      ls.total_accounts
    FROM creation_first c
    JOIN latest_scan ls ON c.pds_url = ls.pds_url AND ls.total_accounts >= ${minAccounts}
    LEFT JOIN migration_first m ON c.pds_url = m.pds_url
    ORDER BY first_week
  `).all() as PdsAgeRow[];
}

export function getScannedPdsCount(): number {
  const db = getPlcDb();
  const row = db.prepare(`
    SELECT COUNT(DISTINCT RTRIM(pds_url, '/')) AS cnt
    FROM pds_repo_status_snapshots s
    WHERE snapshot_date = (
      SELECT MAX(s2.snapshot_date) FROM pds_repo_status_snapshots s2 WHERE s2.pds_url = s.pds_url
    )
  `).get() as { cnt: number };
  return row.cnt;
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
export function getTopPdsByScan(limit = 15, hideBsky = false): ScannedTopPds[] {
  const db = getPlcDb();

  const rows = db.prepare(`
    WITH latest AS (
      SELECT RTRIM(pds_url, '/') AS norm_url, MAX(snapshot_date) AS snap_date
      FROM pds_repo_status_snapshots
      GROUP BY norm_url
    )
    SELECT
      RTRIM(s.pds_url, '/') AS url,
      s.ip_address,
      s.total_scanned,
      s.active,
      l.snap_date AS snapshot_date
    FROM pds_repo_status_snapshots s
    JOIN latest l ON RTRIM(s.pds_url, '/') = l.norm_url AND s.snapshot_date = l.snap_date
    WHERE s.total_scanned > 0
  `).all() as { url: string; ip_address: string | null; total_scanned: number; active: number; snapshot_date: string }[];

  const isBsky = (u: string) => u.includes(".host.bsky.network") || /bsky\.social/.test(u);
  const hostname = (u: string) => u.replace(/^https?:\/\//, "");

  // Bsky shards: sum across all shards (each has genuinely different repos).
  let bskyRepos = 0, bskyActive = 0, bskyDate = "";
  // Non-bsky: group by IP. URLs with no IP resolved stay as their own group.
  const ipGroups = new Map<string, { url: string; repoCount: number; activeCount: number; snapshot_date: string }>();

  for (const r of rows) {
    if (isBsky(r.url)) {
      bskyRepos  += r.total_scanned;
      bskyActive += r.active;
      if (r.snapshot_date > bskyDate) bskyDate = r.snapshot_date;
      continue;
    }
    // Group key: IP if resolved, otherwise URL (each is its own group).
    const key = r.ip_address ?? r.url;
    const existing = ipGroups.get(key);
    if (!existing) {
      ipGroups.set(key, { url: r.url, repoCount: r.total_scanned, activeCount: r.active, snapshot_date: r.snapshot_date });
    } else {
      // Same backend: prefer the shortest hostname as canonical (least likely to be a subdomain alias).
      if (hostname(r.url).length < hostname(existing.url).length) existing.url = r.url;
      // Same repos on same backend — take the freshest/highest count.
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

  return results
    .sort((a, b) => b.repoCount - a.repoCount)
    .slice(0, limit);
}

export interface AccountCohortRow {
  cohort: string;
  count: number;
}

// Repo-backed account creations bucketed by the same cohorts used in the analysis script.
export function getAccountCohortCounts(): AccountCohortRow[] {
  const db = getPlcDb();
  return db.prepare(`
    SELECT
      CASE
        WHEN week < '2023-01-01' THEN 'pre-2023'
        WHEN week < '2024-01-01' THEN '2023'
        WHEN week < '2024-11-01' THEN '2024 (pre-Nov)'
        WHEN week < '2025-01-01' THEN 'Nov–Dec 2024 (exodus)'
        WHEN week < '2025-07-01' THEN '2025 H1'
        WHEN week < '2026-01-01' THEN '2025 H2'
        ELSE '2026+'
      END AS cohort,
      SUM(count) AS count
    FROM active_creation_weekly
    WHERE pds_url != '${TRUMP_PDS}'
    GROUP BY cohort
    ORDER BY MIN(week)
  `).all() as AccountCohortRow[];
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

export interface MigrationJourneyStats {
  buckets: { label: string; users: number }[];
  maxMigrations: number;
  medianMigrations: number;
  pctMultiple: number; // % of migrants with 2+ migrations
  totalMigrants: number;
}

export function getMigrationJourneyStats(): MigrationJourneyStats {
  const db = getPlcDb();

  const rows = db.prepare(`
    SELECT migration_count, COUNT(*) AS users
    FROM (
      SELECT did, COUNT(*) AS migration_count
      FROM plc_migrations
      WHERE NOT (
        (from_pds LIKE '%bsky.network' OR from_pds = 'https://bsky.social')
        AND (to_pds LIKE '%bsky.network' OR to_pds = 'https://bsky.social')
      )
      GROUP BY did
      HAVING COUNT(*) >= 1
    )
    GROUP BY migration_count
    ORDER BY migration_count
  `).all() as { migration_count: number; users: number }[];

  if (rows.length === 0) return { buckets: [], maxMigrations: 0, medianMigrations: 1, pctMultiple: 0, totalMigrants: 0 };

  const totalMigrants = rows.reduce((s, r) => s + r.users, 0);
  const maxMigrations = rows[rows.length - 1].migration_count;
  const multipleUsers = rows.filter(r => r.migration_count >= 2).reduce((s, r) => s + r.users, 0);
  const pctMultiple = totalMigrants > 0 ? (multipleUsers / totalMigrants) * 100 : 0;

  // Median: find the migration_count where cumulative users crosses 50%
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

export function getLastScanTime(): string | null {
  const db = getPlcDb();
  const row = db.prepare(
    `SELECT MAX(scanned_at) AS last_scan FROM pds_repo_status_snapshots WHERE is_partial = 0`
  ).get() as { last_scan: string | null } | undefined;
  return row?.last_scan ?? null;
}
