/**
 * Record one PDS scan's repo statuses into plc.did_repo_status (current state) and
 * plc.did_repo_status_history (every non-active status ever seen). See migration 013.
 *
 * Call after the scan's did_in_repo rows are written: a complete scan clears current rows
 * for accounts it listed as active, using did_in_repo to know which DIDs it listed.
 */
import sql, { withDbRetry } from "../db/pg";
import type { NonActiveRepo } from "./pds-repos";

const BATCH = 5000;

export async function recordRepoStatuses(
  pdsUrl: string,
  nonActive: NonActiveRepo[],
  scannedAt: string,
  complete: boolean,
): Promise<void> {
  for (let i = 0; i < nonActive.length; i += BATCH) {
    const batch = nonActive.slice(i, i + BATCH);
    const dids = batch.map(r => r.did), statuses = batch.map(r => r.status);
    await withDbRetry(() => sql`
      INSERT INTO plc.did_repo_status_history (did, status, pds_url, first_seen_at, last_seen_at)
      SELECT did, status, ${pdsUrl}, ${scannedAt}, ${scannedAt}
      FROM unnest(${dids}::text[], ${statuses}::text[]) AS u(did, status)
      ON CONFLICT (did, status, pds_url) DO UPDATE SET last_seen_at = EXCLUDED.last_seen_at
    `, "did_repo_status_history");
    // current status only from the account's current PDS (a migrated account reads deactivated on the old one)
    await withDbRetry(() => sql`
      INSERT INTO plc.did_repo_status (did, status, pds_url, scanned_at)
      SELECT u.did, u.status, ${pdsUrl}, ${scannedAt}
      FROM unnest(${dids}::text[], ${statuses}::text[]) AS u(did, status)
      LEFT JOIN plc.plc_did_pds p ON p.did = u.did
      WHERE p.did IS NULL OR rtrim(p.pds_url, '/') = rtrim(${pdsUrl}, '/')
      ON CONFLICT (did) DO UPDATE SET
        status = EXCLUDED.status, pds_url = EXCLUDED.pds_url, scanned_at = EXCLUDED.scanned_at
    `, "did_repo_status");
  }

  if (!complete) return;   // a partial listing can't prove anyone active
  // listed by this scan but not as non-active => active now
  await withDbRetry(() => sql`
    DELETE FROM plc.did_repo_status s
    USING plc.did_in_repo r
    WHERE r.did = s.did
      AND rtrim(s.pds_url, '/') = rtrim(${pdsUrl}, '/')
      AND rtrim(r.pds_url, '/') = rtrim(${pdsUrl}, '/')
      AND r.scanned_at = ${scannedAt}
      AND s.scanned_at < ${scannedAt}
  `, "did_repo_status clear");
  // rows from this PDS for accounts that have since moved elsewhere
  await withDbRetry(() => sql`
    DELETE FROM plc.did_repo_status s
    USING plc.plc_did_pds p
    WHERE p.did = s.did
      AND rtrim(s.pds_url, '/') = rtrim(${pdsUrl}, '/')
      AND rtrim(p.pds_url, '/') <> rtrim(${pdsUrl}, '/')
  `, "did_repo_status moved");
}
