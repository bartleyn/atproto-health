import path from "path";
import { existsSync, readFileSync, writeFileSync, mkdirSync } from "fs";
import sql from "./pg";

const CACHE_TTL_MS = 60 * 60 * 1000;
const DISK_CACHE_MAX_AGE_MS = 24 * 60 * 60 * 1000; // 24 hours
const collectionPdsCache = new Map<boolean, { data: CollectionPdsRow[]; expires: number }>();

export type CollectionPdsRow = { collection: string; pds_url: string; unique_dids: number };

function collectionDiskPath(hideBsky: boolean) {
  return path.join(process.cwd(), "cache", hideBsky ? "collection-pds-hidebsky.json" : "collection-pds.json");
}

function tryLoadCollectionDiskCache(hideBsky: boolean): CollectionPdsRow[] | null {
  try {
    const f = collectionDiskPath(hideBsky);
    if (!existsSync(f)) return null;
    const { data, writtenAt } = JSON.parse(readFileSync(f, "utf8"));
    if (Date.now() - new Date(writtenAt).getTime() > DISK_CACHE_MAX_AGE_MS) return null;
    return data;
  } catch { return null; }
}

// Called from the analysis:dashboard-cache script to compute and persist.
// Cross-schema join replaces the SQLite ATTACH DATABASE pattern.
export async function computeAndSaveCollectionPdsData(hideBsky: boolean): Promise<CollectionPdsRow[]> {
  const bskyFilter = hideBsky
    ? `AND p.pds_url NOT LIKE '%bsky.network%' AND p.pds_url != 'https://bsky.social'`
    : "";
  // Aggregate at the namespace-root grain (first two NSID parts, e.g. "site.standard")
  const data = await sql.unsafe(`
    SELECT split_part(ca.collection, '.', 1) || '.' || split_part(ca.collection, '.', 2) AS collection,
           p.pds_url, COUNT(DISTINCT ca.did)::int AS unique_dids
    FROM activity.collection_activity ca
    JOIN plc.plc_did_pds p ON ca.did = p.did
    WHERE ca.collection NOT LIKE 'app.bsky.%'
      AND ca.collection NOT LIKE 'chat.bsky.%'
      ${bskyFilter}
    GROUP BY 1, 2
  `) as unknown as CollectionPdsRow[];
  mkdirSync(path.join(process.cwd(), "cache"), { recursive: true });
  writeFileSync(collectionDiskPath(hideBsky), JSON.stringify({ data, writtenAt: new Date().toISOString() }, null, 0));
  return data;
}

export interface PdsActivityRow {
  pds_url: string;
  active_dids: number;
  poster_dids: number;
  liker_dids: number;
  reposter_dids: number;
  follower_dids: number;
  updated_at: string;
}

export async function getPdsActivitySummary(windowDays = 30, limit = 15): Promise<PdsActivityRow[]> {
  try {
    const rows = await sql`
      SELECT pds_url, active_dids, poster_dids, liker_dids, reposter_dids, follower_dids, updated_at
      FROM activity.pds_activity_summary
      WHERE window_days = ${windowDays}
        AND pds_url != 'bsky.network'
        AND active_dids > 0
      ORDER BY active_dids DESC
      LIMIT ${limit}
    `;
    return rows as unknown as PdsActivityRow[];
  } catch {
    return [];
  }
}

export async function getPdsActivityUpdatedAt(windowDays = 30): Promise<string | null> {
  try {
    const rows = await sql`
      SELECT MAX(updated_at) AS ts FROM activity.pds_activity_summary WHERE window_days = ${windowDays}
    `;
    return (rows[0]?.ts as string | null) ?? null;
  } catch {
    return null;
  }
}

// Reads from disk cache written by analysis:dashboard-cache. Returns [] if no cache.
// Stays synchronous — no DB query, disk I/O only.
export function getCollectionPdsData(hideBsky = false): CollectionPdsRow[] {
  const cached = collectionPdsCache.get(hideBsky);
  if (cached && Date.now() < cached.expires) return cached.data;

  const disk = tryLoadCollectionDiskCache(hideBsky);
  if (disk) {
    collectionPdsCache.set(hideBsky, { data: disk, expires: Date.now() + CACHE_TTL_MS });
    return disk;
  }

  return [];
}
