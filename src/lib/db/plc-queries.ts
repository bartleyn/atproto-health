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
  independent_pds_count: number;
  independent_pds_account_pct: number;
  earliest_creation: string;
  latest_creation: string;
}

const BSKY_NETWORK_LABEL = "bsky.network";
const TRUMP_PDS = "https://pds.trump.com";
const TOP_N = 10;

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

export function getCreationTimeseriesWeekly(includeTrump = false): TimeseriesRow[] {
  const db = getPlcDb();
  const trumpFilter = includeTrump ? "" : `AND pds_url != '${TRUMP_PDS}'`;
  return db.prepare(`
    WITH collapsed AS (
      SELECT
        CASE WHEN pds_url LIKE '%bsky.network' THEN '${BSKY_NETWORK_LABEL}' ELSE pds_url END AS pds_url,
        week AS period,
        SUM(count) AS count
      FROM plc_creation_weekly
      WHERE 1=1 ${trumpFilter}
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

export function getMigrationFlows(topN = 10): MigrationFlow[] {
  const db = getPlcDb();
  return db.prepare(`
    WITH
      collapsed AS (
        SELECT
          CASE WHEN from_pds LIKE '%bsky.network' THEN '${BSKY_NETWORK_LABEL}' ELSE from_pds END AS source,
          to_pds AS target,
          SUM(count) AS value
        FROM plc_migration_monthly
        WHERE to_pds NOT LIKE '%bsky.network'
        GROUP BY 1, 2
      ),
      top_sources AS (
        SELECT source FROM collapsed GROUP BY source ORDER BY SUM(value) DESC LIMIT ${topN}
      ),
      top_targets AS (
        SELECT target FROM collapsed GROUP BY target ORDER BY SUM(value) DESC LIMIT ${topN}
      ),
      labeled AS (
        SELECT
          CASE WHEN source IN (SELECT source FROM top_sources) THEN source ELSE 'Other sources' END AS source,
          CASE WHEN target IN (SELECT target FROM top_targets) THEN target ELSE 'Other destinations' END AS target,
          value
        FROM collapsed
      )
    SELECT source, target, SUM(value) AS value
    FROM labeled
    GROUP BY source, target
    ORDER BY value DESC
  `).all() as MigrationFlow[];
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

export function getEcosystemStats(): EcosystemStats {
  const db = getPlcDb();

  // Use aggregated tables — much faster than scanning 83M raw rows.
  // plc_creation_monthly is small and already grouped by pds_url.
  const totals = db.prepare(`
    SELECT
      SUM(count) AS total_dids,
      SUM(CASE WHEN pds_url != '${TRUMP_PDS}' THEN count ELSE 0 END) AS total_dids_ex_trump
    FROM plc_creation_monthly
  `).get() as { total_dids: number; total_dids_ex_trump: number };

  const migrations = db.prepare(`
    SELECT SUM(count) AS total_migrations FROM plc_migration_monthly
  `).get() as { total_migrations: number };

  const indep = db.prepare(`
    WITH ex_trump_total AS (
      SELECT SUM(count) AS n FROM plc_creation_monthly
      WHERE pds_url != '${TRUMP_PDS}'
    ),
    indep AS (
      SELECT COUNT(DISTINCT pds_url) AS cnt, SUM(count) AS n
      FROM plc_creation_monthly
      WHERE pds_url != '${TRUMP_PDS}'
        AND pds_url NOT LIKE '%bsky.network'
    )
    SELECT
      indep.cnt AS independent_pds_count,
      ROUND(100.0 * indep.n / ex_trump_total.n, 2) AS independent_pds_account_pct
    FROM indep, ex_trump_total
  `).get() as { independent_pds_count: number; independent_pds_account_pct: number };

  const dates = db.prepare(`
    SELECT MIN(month) || '-01' AS earliest_creation, MAX(month) || '-01' AS latest_creation
    FROM plc_creation_monthly
    WHERE pds_url != '${TRUMP_PDS}'
  `).get() as { earliest_creation: string; latest_creation: string };

  return {
    total_dids: totals.total_dids ?? 0,
    total_dids_ex_trump: totals.total_dids_ex_trump ?? 0,
    total_migrations: migrations.total_migrations ?? 0,
    independent_pds_count: indep?.independent_pds_count ?? 0,
    independent_pds_account_pct: indep?.independent_pds_account_pct ?? 0,
    earliest_creation: dates?.earliest_creation ?? "",
    latest_creation: dates?.latest_creation ?? "",
  };
}
