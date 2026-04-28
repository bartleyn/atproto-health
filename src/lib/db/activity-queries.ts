import path from "path";
import { getActivityDb } from "./activity-schema";

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

export interface CollectionPdsRow {
  collection: string;
  pds_url: string;
  unique_dids: number;
}

// Returns per-(collection, pds_url) unique DID counts by cross-joining
// collection_activity with plc_did_pds. Used for the namespace map overlay.
export function getCollectionPdsData(): CollectionPdsRow[] {
  try {
    const db = getActivityDb();
    const plcPath = path.join(process.cwd(), "plc-migrations.db");
    try { db.exec(`ATTACH DATABASE '${plcPath}' AS plc`); } catch { /* already attached */ }
    return db.prepare(`
      SELECT ca.collection, p.pds_url, COUNT(DISTINCT ca.did) AS unique_dids
      FROM collection_activity ca
      JOIN plc.plc_did_pds p ON ca.did = p.did
      GROUP BY ca.collection, p.pds_url
    `).all() as CollectionPdsRow[];
  } catch {
    return [];
  }
}
