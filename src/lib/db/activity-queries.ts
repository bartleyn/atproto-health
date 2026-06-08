import path from "path";
import { existsSync, readFileSync, writeFileSync, mkdirSync } from "fs";
import { getActivityDbReadonly as getActivityDb } from "./activity-schema";

const CACHE_TTL_MS = 60 * 60 * 1000;
const DISK_CACHE_MAX_AGE_MS = 24 * 60 * 60 * 1000; // 24 hours
const collectionPdsCache = new Map<boolean, { data: { collection: string; pds_url: string; unique_dids: number }[]; expires: number }>();

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
export function computeAndSaveCollectionPdsData(hideBsky: boolean): CollectionPdsRow[] {
  const db = getActivityDb();
  const plcPath = path.join(process.cwd(), "plc-migrations.db");
  try { db.exec(`ATTACH DATABASE '${plcPath}' AS plc`); } catch { /* already attached */ }
  const bskyFilter = hideBsky
    ? `AND p.pds_url NOT LIKE '%bsky.network%' AND p.pds_url != 'https://bsky.social'`
    : "";
  const data = db.prepare(`
    SELECT ca.collection, p.pds_url, COUNT(DISTINCT ca.did) AS unique_dids
    FROM collection_activity ca
    JOIN plc.plc_did_pds p ON ca.did = p.did
    WHERE 1=1 ${bskyFilter}
    GROUP BY ca.collection, p.pds_url
  `).all() as CollectionPdsRow[];
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

/**
 * Returns top indie PDSes by unique active DIDs over the last windowDays.
 * bsky.network is excluded — this is the indie-PDS view.
 * Returns [] if aggregate:activity-pds hasn't been run yet.
 */
export function getPdsActivitySummary(windowDays = 30, limit = 15): PdsActivityRow[] {
  try {
    const db = getActivityDb();
    return db.prepare(`
      SELECT pds_url, active_dids, poster_dids, liker_dids, reposter_dids, follower_dids, updated_at
      FROM pds_activity_summary
      WHERE window_days = ?
        AND pds_url != 'bsky.network'
        AND active_dids > 0
      ORDER BY active_dids DESC
      LIMIT ?
    `).all(windowDays, limit) as PdsActivityRow[];
  } catch {
    return [];
  }
}

export function getPdsActivityUpdatedAt(windowDays = 30): string | null {
  try {
    const db = getActivityDb();
    const row = db.prepare(
      `SELECT MAX(updated_at) AS ts FROM pds_activity_summary WHERE window_days = ?`
    ).get(windowDays) as { ts: string | null };
    return row?.ts ?? null;
  } catch {
    return null;
  }
}

// Returns per-(collection, pds_url) unique DID counts.
// Reads from disk cache written by analysis:dashboard-cache. Returns [] if no cache.
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
