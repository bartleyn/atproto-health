/**
 * CLI runner for data collection.
 *
 * Usage:
 *   npm run collect          # Full collection (directory + geo + users)
 *   npm run collect:geo      # Directory + geo only (faster)
 *   npm run collect:users    # Directory + user counts only
 */

import { promises as dns } from "dns";
import { getDb } from "../db/schema";
import { getPlcDb } from "../db/plc-schema";
import { fetchPdsDirectory, type PdsDirectoryEntry } from "./pds-directory";
import { geolocatePdses, type GeoIpResult } from "./geo-ip";
import { fetchAllPdsDetails, type PdsDetails, type RepoInfo } from "./pds-details";

const args = new Set(process.argv.slice(2));
const geoOnly = args.has("--geo");
const usersOnly = args.has("--users");
const fullRun = !geoOnly && !usersOnly;

const DID_BATCH_SIZE = 10_000;

async function resolveIp(pdsUrl: string): Promise<string | null> {
  try {
    const hostname = new URL(pdsUrl).hostname;
    const { address } = await dns.lookup(hostname, { family: 4 });
    return address;
  } catch {
    return null;
  }
}

async function main() {
  const db = getDb();
  const plcDb = getPlcDb();
  const mode = geoOnly ? "geo" : usersOnly ? "users" : "full";
  console.log(`\n=== ATProto Health Collection (${mode}) ===\n`);

  const run = db
    .prepare(`INSERT INTO collection_runs (source, status) VALUES (?, 'running') RETURNING id`)
    .get(`collect:${mode}`) as { id: number };

  try {
    // 1. Fetch PDS directory (source of truth for both collect and scan)
    const directory = await fetchPdsDirectory();
    // Normalize URLs: strip trailing slashes and force https — matches scan-pds-status format
    const normalizeUrl = (u: string) => u.replace(/\/+$/, "").replace(/^http:\/\//, "https://");
    const allUrls = directory.map((e) => normalizeUrl(e.url));

    // Upsert PDS instances
    const upsertPds = db.prepare(`INSERT INTO pds_instances (url) VALUES (?) ON CONFLICT(url) DO NOTHING`);
    const getPdsId = db.prepare(`SELECT id FROM pds_instances WHERE url = ?`);
    db.transaction(() => { for (const entry of directory) upsertPds.run(entry.url); })();

    // 2. Geo enrichment
    const geoResults = fullRun || geoOnly ? await geolocatePdses(allUrls) : null;

    // 3. Repo scan — all directory PDSes (online + offline; offline ones return 0 repos naturally)
    let detailResults: Map<string, PdsDetails> | null = null;

    if (fullRun || usersOnly) {
      const today = new Date().toISOString().slice(0, 10);
      const scannedAt = new Date().toISOString();

      // Load cached IPs from previous scans to avoid redundant DNS lookups
      const cachedIps = plcDb.prepare(`
        SELECT pds_url, ip_address FROM (
          SELECT pds_url, ip_address,
            ROW_NUMBER() OVER (PARTITION BY pds_url ORDER BY snapshot_date DESC) AS rn
          FROM pds_repo_status_snapshots WHERE ip_address IS NOT NULL
        ) WHERE rn = 1
      `).all() as { pds_url: string; ip_address: string }[];

      const ipMap = new Map<string, string | null>(cachedIps.map(r => [r.pds_url, r.ip_address]));

      const needResolve = allUrls.filter(url => !ipMap.has(url));
      if (needResolve.length > 0) {
        console.log(`Resolving IPs for ${needResolve.length} new PDSes (${allUrls.length - needResolve.length} cached)...`);
        await Promise.all(needResolve.map(async url => {
          ipMap.set(url, await resolveIp(url));
        }));
      }

      // Skip PDSes whose scan already completed successfully today (allow re-runs to resume)
      const alreadyDone = new Set(
        (plcDb.prepare(`SELECT pds_url FROM pds_repo_status_snapshots WHERE snapshot_date = ? AND is_partial = 0`)
          .all(today) as { pds_url: string }[]).map(r => r.pds_url)
      );

      // Skip PDSes that had 0 repos in the last 7 days (re-check weekly)
      const emptyRecently = new Set(
        (plcDb.prepare(`
          SELECT pds_url FROM (
            SELECT pds_url, total_scanned, snapshot_date,
              ROW_NUMBER() OVER (PARTITION BY pds_url ORDER BY snapshot_date DESC) AS rn
            FROM pds_repo_status_snapshots WHERE is_partial = 0
          )
          WHERE rn = 1 AND total_scanned = 0 AND snapshot_date >= date('now', '-7 days')
        `).all() as { pds_url: string }[]).map(r => r.pds_url)
      );

      const toScan = allUrls.filter(url => !alreadyDone.has(url) && !emptyRecently.has(url));
      if (alreadyDone.size > 0 || emptyRecently.size > 0) {
        console.log(`Resuming: ${alreadyDone.size} done today, ${emptyRecently.size} skipped (empty <7d), ${toScan.length} remaining\n`);
      }

      // Build lookup maps for geo + directory data available in onPdsDone
      const directoryMap = new Map<string, PdsDirectoryEntry>(
        directory.map((e) => [normalizeUrl(e.url), e])
      );

      // For usersOnly runs (no geoResults), load last known geo from plcDb
      type CachedGeo = { country: string | null; country_code: string | null; region: string | null;
        city: string | null; latitude: number | null; longitude: number | null;
        isp: string | null; org: string | null; as_number: string | null; };
      let cachedGeoMap = new Map<string, CachedGeo>();
      if (!geoResults) {
        const rows = plcDb.prepare(`
          SELECT pds_url, country, country_code, region, city, latitude, longitude, isp, org, as_number
          FROM (
            SELECT pds_url, country, country_code, region, city, latitude, longitude, isp, org, as_number,
              ROW_NUMBER() OVER (PARTITION BY pds_url ORDER BY snapshot_date DESC) AS rn
            FROM pds_repo_status_snapshots WHERE country IS NOT NULL
          ) WHERE rn = 1
        `).all() as ({ pds_url: string } & CachedGeo)[];
        cachedGeoMap = new Map(rows.map(r => [r.pds_url, r]));
      }

      // Prepare plcDb write statements
      const upsertSnapshot = plcDb.prepare(`
        INSERT INTO pds_repo_status_snapshots
          (pds_url, snapshot_date, active, deactivated, deleted, takendown, suspended, other,
           total_scanned, is_sampled, did_plc_count, did_web_count, is_partial, scanned_at, ip_address,
           country, country_code, region, city, latitude, longitude, isp, org, as_number, hosting_provider,
           version, invite_code_required, is_online, in_directory)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, 1)
        ON CONFLICT(pds_url, snapshot_date) DO UPDATE SET
          active = excluded.active, deactivated = excluded.deactivated,
          deleted = excluded.deleted, takendown = excluded.takendown,
          suspended = excluded.suspended, other = excluded.other,
          total_scanned = excluded.total_scanned,
          did_plc_count = excluded.did_plc_count, did_web_count = excluded.did_web_count,
          is_partial = excluded.is_partial, scanned_at = excluded.scanned_at,
          ip_address = excluded.ip_address,
          country = excluded.country, country_code = excluded.country_code,
          region = excluded.region, city = excluded.city,
          latitude = excluded.latitude, longitude = excluded.longitude,
          isp = excluded.isp, org = excluded.org, as_number = excluded.as_number,
          hosting_provider = excluded.hosting_provider,
          version = excluded.version, invite_code_required = excluded.invite_code_required,
          is_online = excluded.is_online,
          in_directory = 1
      `);

      const upsertDidInRepo = plcDb.prepare(`
        INSERT INTO did_in_repo (did, pds_url, scanned_at, first_scanned_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(did) DO UPDATE SET pds_url = excluded.pds_url, scanned_at = excluded.scanned_at
      `);

      const upsertDidStatus = plcDb.prepare(`
        INSERT INTO did_repo_status (did, status, pds_url, scanned_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(did) DO UPDATE SET
          status = excluded.status, pds_url = excluded.pds_url, scanned_at = excluded.scanned_at
      `);

      const writeDidBatch = plcDb.transaction((pdsUrl: string, batch: RepoInfo[]) => {
        for (const r of batch) upsertDidInRepo.run(r.did, pdsUrl, scannedAt, scannedAt);
      });

      const writeNonActive = plcDb.transaction((pdsUrl: string, nonActive: { did: string; status: string }[]) => {
        for (const r of nonActive) upsertDidStatus.run(r.did, r.status, pdsUrl, scannedAt);
      });

      // Per-PDS DID batch buffers (multiple PDSes scanned concurrently)
      const repoBatches = new Map<string, RepoInfo[]>();

      const onRepo = (pdsUrl: string, repo: RepoInfo) => {
        if (alreadyDone.has(pdsUrl)) return;
        let batch = repoBatches.get(pdsUrl);
        if (!batch) { batch = []; repoBatches.set(pdsUrl, batch); }
        batch.push(repo);
        if (batch.length >= DID_BATCH_SIZE) {
          writeDidBatch(pdsUrl, batch);
          repoBatches.set(pdsUrl, []);
        }
      };

      const onPdsDone = (pdsUrl: string, details: PdsDetails) => {
        if (alreadyDone.has(pdsUrl)) return;

        // Flush remaining DID batch
        const remaining = repoBatches.get(pdsUrl) ?? [];
        if (remaining.length > 0) {
          writeDidBatch(pdsUrl, remaining);
          repoBatches.delete(pdsUrl);
        }

        const c = details.statusCounts;
        const rawGeo = geoResults?.get(pdsUrl);
        const cachedGeo = cachedGeoMap.get(pdsUrl);
        const countryCode = rawGeo?.countryCode ?? cachedGeo?.country_code ?? null;
        const lat = rawGeo?.lat ?? cachedGeo?.latitude ?? null;
        const lon = rawGeo?.lon ?? cachedGeo?.longitude ?? null;
        const asNumber = rawGeo?.asNumber ?? cachedGeo?.as_number ?? null;
        const country = rawGeo?.country ?? cachedGeo?.country ?? null;
        const region = rawGeo?.region ?? cachedGeo?.region ?? null;
        const city = rawGeo?.city ?? cachedGeo?.city ?? null;
        const isp = rawGeo?.isp ?? cachedGeo?.isp ?? null;
        const org = rawGeo?.org ?? cachedGeo?.org ?? null;
        const dirEntry = directoryMap.get(pdsUrl);
        // Write a row for every PDS in the directory — including offline ones (total=0).
        // Offline PDSes still carry geo + invite_code_required + is_online=0 from Mary's state.json.
        const active = c?.active ?? 0;
        const total = c?.total ?? 0;
        upsertSnapshot.run(
          pdsUrl, today,
          active, c?.deactivated ?? 0, c?.deleted ?? 0, c?.takendown ?? 0, c?.suspended ?? 0, c?.other ?? 0,
          total, c?.didPlc ?? 0, c?.didWeb ?? 0,
          details.partial ? 1 : 0, scannedAt, ipMap.get(pdsUrl) ?? null,
          country, countryCode, region, city, lat, lon, isp, org, asNumber, org,
          dirEntry?.version ?? null,
          dirEntry?.inviteCodeRequired ? 1 : 0,
          dirEntry?.isOnline ? 1 : 0,
        );

        if (details.nonActive && details.nonActive.length > 0) {
          writeNonActive(pdsUrl, details.nonActive);
        }
      };

      detailResults = await fetchAllPdsDetails(toScan, { onRepo, onPdsDone });
    }

    // 4. Write pds_snapshots (atproto-health.db) — existing behaviour unchanged
    const insertSnapshot = db.prepare(`
      INSERT INTO pds_snapshots (
        pds_id, run_id,
        version, invite_code_required, is_online, error_at,
        did, available_domains, contact, links,
        user_count_total, user_count_active,
        ip_address, country, country_code, region, city,
        latitude, longitude, isp, org, as_number, hosting_provider
      ) VALUES (
        ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?,
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?
      )
    `);

    db.transaction(() => {
      for (const entry of directory) {
        const pdsRow = getPdsId.get(entry.url) as { id: number };
        const normUrl = normalizeUrl(entry.url);
        const geo = geoResults?.get(normUrl);
        const detail = detailResults?.get(normUrl);

        insertSnapshot.run(
          pdsRow.id, run.id,
          entry.version,
          entry.inviteCodeRequired ? 1 : 0,
          entry.isOnline ? 1 : 0,
          entry.errorAt ? new Date(entry.errorAt).toISOString() : null,
          detail?.did ?? null,
          detail?.availableDomains ? JSON.stringify(detail.availableDomains) : null,
          detail?.contact ? JSON.stringify(detail.contact) : null,
          detail?.links ? JSON.stringify(detail.links) : null,
          detail?.userCountTotal ?? null,
          detail?.userCountActive ?? null,
          geo?.ip ?? null,
          geo?.country ?? null,
          geo?.countryCode ?? null,
          geo?.region ?? null,
          geo?.city ?? null,
          geo?.lat ?? null,
          geo?.lon ?? null,
          geo?.isp ?? null,
          geo?.org ?? null,
          geo?.asNumber ?? null,
          geo?.org ?? null,
        );
      }
    })();

    db.prepare(`UPDATE collection_runs SET completed_at = datetime('now'), status = 'completed' WHERE id = ?`).run(run.id);

    // Summary
    const totalUsers = detailResults
      ? [...detailResults.values()].reduce((sum, d) => sum + (d.userCountActive ?? 0), 0)
      : null;

    console.log(`\n=== Collection Complete ===`);
    console.log(`  PDS instances: ${directory.length}`);
    console.log(`  Online: ${directory.filter((e) => e.isOnline).length}`);
    if (geoResults) {
      console.log(`  Geolocated: ${[...geoResults.values()].filter(g => g.country).length}`);
    }
    if (totalUsers !== null) {
      console.log(`  Total active users (third-party): ${totalUsers}`);
    }
    console.log(`  Run ID: ${run.id}\n`);
  } catch (err) {
    db.prepare(`UPDATE collection_runs SET completed_at = datetime('now'), status = 'failed', metadata = ? WHERE id = ?`).run(String(err), run.id);
    throw err;
  }
}

main().catch((err) => {
  console.error("Collection failed:", err);
  process.exit(1);
});
