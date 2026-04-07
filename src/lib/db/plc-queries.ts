import { getPlcDb } from "./plc-schema";

export interface MonthlyRow {
  month: string;
  pds_url: string;
  count: number;
}

const BSKY_NETWORK_LABEL = "bsky.network";
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
