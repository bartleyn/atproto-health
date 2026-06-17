/**
 * CLI runner for data collection.
 *
 * Usage:
 *   npm run collect          # Full collection (directory + geo + users)
 *   npm run collect:geo      # Directory + geo only (faster)
 *   npm run collect:users    # Directory + user counts only
 */

import { promises as dns } from "dns";
import sql, { withDbRetry } from "../db/pg";
import { fetchPdsDirectory, type PdsDirectoryEntry } from "./pds-directory";
import { geolocatePdses, type GeoIpResult } from "./geo-ip";
import { fetchAllPdsDetails, type PdsDetails, type RepoInfo } from "./pds-details";

const args = new Set(process.argv.slice(2));
const geoOnly   = args.has("--geo");
const usersOnly = args.has("--users");
const fullRun   = !geoOnly && !usersOnly;

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
  const mode = geoOnly ? "geo" : usersOnly ? "users" : "full";
  console.log(`\n=== ATProto Health Collection (${mode}) ===\n`);

  const [runRow] = await sql<{ id: bigint }[]>`
    INSERT INTO health.collection_runs (source, status) VALUES (${"collect:" + mode}, 'running') RETURNING id
  `;
  const runId = runRow.id;

  try {
    // 1. Fetch PDS directory
    const directory = await fetchPdsDirectory();
    const normalizeUrl = (u: string) => u.replace(/\/+$/, "").replace(/^http:\/\//, "https://");
    const allUrls = directory.map((e) => normalizeUrl(e.url));

    // Upsert PDS instances and load their IDs
    await sql`
      INSERT INTO health.pds_instances (url)
      SELECT UNNEST(${allUrls}::text[]) AS url
      ON CONFLICT (url) DO NOTHING
    `;
    const pdsIdRows = await sql<{ id: bigint; url: string }[]>`
      SELECT id, url FROM health.pds_instances WHERE url = ANY(${allUrls})
    `;
    const pdsIdMap = new Map(pdsIdRows.map(r => [r.url, r.id]));

    // 2. Geo enrichment
    const geoResults = fullRun || geoOnly ? await geolocatePdses(allUrls) : null;

    // 3. Repo scan
    let detailResults: Map<string, PdsDetails> | null = null;

    if (fullRun || usersOnly) {
      const today = new Date().toISOString().slice(0, 10);
      const scannedAt = new Date().toISOString();

      // Load cached IPs from previous scans
      const cachedIpRows = await sql<{ pds_url: string; ip_address: string }[]>`
        SELECT pds_url, ip_address FROM (
          SELECT pds_url, ip_address,
            ROW_NUMBER() OVER (PARTITION BY pds_url ORDER BY snapshot_date DESC) AS rn
          FROM plc.pds_repo_status_snapshots WHERE ip_address IS NOT NULL
        ) t WHERE rn = 1
      `;
      const ipMap = new Map<string, string | null>(cachedIpRows.map(r => [r.pds_url, r.ip_address]));

      const needResolve = allUrls.filter(url => !ipMap.has(url));
      if (needResolve.length > 0) {
        console.log(`Resolving IPs for ${needResolve.length} new PDSes (${allUrls.length - needResolve.length} cached)...`);
        await Promise.all(needResolve.map(async url => { ipMap.set(url, await resolveIp(url)); }));
      }

      // Skip PDSes already fully scanned today
      const alreadyDoneRows = await sql<{ pds_url: string }[]>`
        SELECT pds_url FROM plc.pds_repo_status_snapshots
        WHERE snapshot_date = ${today} AND is_partial = 0
      `;
      const alreadyDone = new Set(alreadyDoneRows.map(r => r.pds_url));

      // Skip PDSes empty within last 7 days
      const emptyRecentlyRows = await sql<{ pds_url: string }[]>`
        SELECT pds_url FROM (
          SELECT pds_url, total_scanned, snapshot_date,
            ROW_NUMBER() OVER (PARTITION BY pds_url ORDER BY snapshot_date DESC) AS rn
          FROM plc.pds_repo_status_snapshots WHERE is_partial = 0
        ) t
        WHERE rn = 1 AND total_scanned = 0 AND snapshot_date >= (CURRENT_DATE - 7)::text
      `;
      const emptyRecently = new Set(emptyRecentlyRows.map(r => r.pds_url));

      const toScan = allUrls.filter(url => !alreadyDone.has(url) && !emptyRecently.has(url));
      if (alreadyDone.size > 0 || emptyRecently.size > 0) {
        console.log(`Resuming: ${alreadyDone.size} done today, ${emptyRecently.size} skipped (empty <7d), ${toScan.length} remaining\n`);
      }

      const directoryMap = new Map<string, PdsDirectoryEntry>(
        directory.map((e) => [normalizeUrl(e.url), e])
      );

      // For usersOnly runs (no geoResults), load last known geo from DB
      type CachedGeo = {
        country: string | null; country_code: string | null; region: string | null;
        city: string | null; latitude: number | null; longitude: number | null;
        isp: string | null; org: string | null; as_number: string | null;
      };
      let cachedGeoMap = new Map<string, CachedGeo>();
      if (!geoResults) {
        const rows = await sql<({ pds_url: string } & CachedGeo)[]>`
          SELECT pds_url, country, country_code, region, city, latitude, longitude, isp, org, as_number
          FROM (
            SELECT pds_url, country, country_code, region, city, latitude, longitude, isp, org, as_number,
              ROW_NUMBER() OVER (PARTITION BY pds_url ORDER BY snapshot_date DESC) AS rn
            FROM plc.pds_repo_status_snapshots WHERE country IS NOT NULL
          ) t WHERE rn = 1
        `;
        cachedGeoMap = new Map(rows.map(r => [r.pds_url, r]));
      }

      // Per-PDS DID batch buffers
      const repoBatches = new Map<string, RepoInfo[]>();

      const onRepo = async (pdsUrl: string, repo: RepoInfo) => {
        if (alreadyDone.has(pdsUrl)) return;
        let batch = repoBatches.get(pdsUrl);
        if (!batch) { batch = []; repoBatches.set(pdsUrl, batch); }
        batch.push(repo);
        if (batch.length >= DID_BATCH_SIZE) {
          const flush = batch.splice(0);
          repoBatches.set(pdsUrl, batch);
          await withDbRetry(() => sql`
            INSERT INTO plc.did_in_repo ${sql(
              flush.map(r => ({ did: r.did, pds_url: pdsUrl, scanned_at: scannedAt, first_scanned_at: scannedAt })),
              "did", "pds_url", "scanned_at", "first_scanned_at"
            )}
            ON CONFLICT (did) DO UPDATE SET pds_url = EXCLUDED.pds_url, scanned_at = EXCLUDED.scanned_at
          `, "did_in_repo");
        }
      };

      const onPdsDone = async (pdsUrl: string, details: PdsDetails) => {
        if (alreadyDone.has(pdsUrl)) return;

        // Flush remaining DID batch
        const remaining = repoBatches.get(pdsUrl) ?? [];
        if (remaining.length > 0) {
          repoBatches.delete(pdsUrl);
          await withDbRetry(() => sql`
            INSERT INTO plc.did_in_repo ${sql(
              remaining.map(r => ({ did: r.did, pds_url: pdsUrl, scanned_at: scannedAt, first_scanned_at: scannedAt })),
              "did", "pds_url", "scanned_at", "first_scanned_at"
            )}
            ON CONFLICT (did) DO UPDATE SET pds_url = EXCLUDED.pds_url, scanned_at = EXCLUDED.scanned_at
          `, "did_in_repo");
        }

        const c = details.statusCounts;
        const rawGeo = geoResults?.get(pdsUrl);
        const cachedGeo = cachedGeoMap.get(pdsUrl);
        const dirEntry = directoryMap.get(pdsUrl);

        await withDbRetry(() => sql`
          INSERT INTO plc.pds_repo_status_snapshots
            (pds_url, snapshot_date, active, deactivated, deleted, takendown, suspended, other,
             total_scanned, is_sampled, did_plc_count, did_web_count, is_partial, scanned_at, ip_address,
             country, country_code, region, city, latitude, longitude, isp, org, as_number, hosting_provider,
             version, invite_code_required, is_online, in_directory)
          VALUES (
            ${pdsUrl}, ${today},
            ${c?.active ?? 0}, ${c?.deactivated ?? 0}, ${c?.deleted ?? 0},
            ${c?.takendown ?? 0}, ${c?.suspended ?? 0}, ${c?.other ?? 0},
            ${c?.total ?? 0}, 0, ${c?.didPlc ?? 0}, ${c?.didWeb ?? 0},
            ${details.partial ? 1 : 0}, ${scannedAt}, ${ipMap.get(pdsUrl) ?? null},
            ${rawGeo?.country ?? cachedGeo?.country ?? null},
            ${rawGeo?.countryCode ?? cachedGeo?.country_code ?? null},
            ${rawGeo?.region ?? cachedGeo?.region ?? null},
            ${rawGeo?.city ?? cachedGeo?.city ?? null},
            ${rawGeo?.lat ?? cachedGeo?.latitude ?? null},
            ${rawGeo?.lon ?? cachedGeo?.longitude ?? null},
            ${rawGeo?.isp ?? cachedGeo?.isp ?? null},
            ${rawGeo?.org ?? cachedGeo?.org ?? null},
            ${rawGeo?.asNumber ?? cachedGeo?.as_number ?? null},
            ${rawGeo?.org ?? cachedGeo?.org ?? null},
            ${dirEntry?.version ?? null},
            ${dirEntry?.inviteCodeRequired ? 1 : 0},
            ${dirEntry?.isOnline ? 1 : 0},
            1
          )
          ON CONFLICT (pds_url, snapshot_date) DO UPDATE SET
            active = EXCLUDED.active, deactivated = EXCLUDED.deactivated,
            deleted = EXCLUDED.deleted, takendown = EXCLUDED.takendown,
            suspended = EXCLUDED.suspended, other = EXCLUDED.other,
            total_scanned = EXCLUDED.total_scanned,
            did_plc_count = EXCLUDED.did_plc_count, did_web_count = EXCLUDED.did_web_count,
            is_partial = EXCLUDED.is_partial, scanned_at = EXCLUDED.scanned_at,
            ip_address = EXCLUDED.ip_address,
            country = EXCLUDED.country, country_code = EXCLUDED.country_code,
            region = EXCLUDED.region, city = EXCLUDED.city,
            latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude,
            isp = EXCLUDED.isp, org = EXCLUDED.org, as_number = EXCLUDED.as_number,
            hosting_provider = EXCLUDED.hosting_provider,
            version = EXCLUDED.version, invite_code_required = EXCLUDED.invite_code_required,
            is_online = EXCLUDED.is_online, in_directory = 1
        `, "pds_repo_status_snapshots");

        if (details.nonActive && details.nonActive.length > 0) {
          // Batched to stay under Postgres's 65,534-parameter limit (4 cols/row).
          for (let i = 0; i < details.nonActive.length; i += DID_BATCH_SIZE) {
            const batch = details.nonActive.slice(i, i + DID_BATCH_SIZE);
            await withDbRetry(() => sql`
              INSERT INTO plc.did_repo_status ${sql(
                batch.map(r => ({ did: r.did, status: r.status, pds_url: pdsUrl, scanned_at: scannedAt })),
                "did", "status", "pds_url", "scanned_at"
              )}
              ON CONFLICT (did) DO UPDATE SET
                status = EXCLUDED.status, pds_url = EXCLUDED.pds_url, scanned_at = EXCLUDED.scanned_at
            `, "did_repo_status");
          }
        }
      };

      detailResults = await fetchAllPdsDetails(toScan, { onRepo, onPdsDone });
    }

    // 4. Write pds_snapshots (health schema)
    const snapshots = directory.map(entry => {
      const normUrl = normalizeUrl(entry.url);
      const geo = geoResults?.get(normUrl);
      const detail = detailResults?.get(normUrl);
      return {
        pds_id:               pdsIdMap.get(normUrl) ?? pdsIdMap.get(entry.url)!,
        run_id:               runId,
        version:              entry.version ?? null,
        invite_code_required: entry.inviteCodeRequired ? 1 : 0,
        is_online:            entry.isOnline ? 1 : 0,
        error_at:             entry.errorAt ? new Date(entry.errorAt).toISOString() : null,
        did:                  detail?.did ?? null,
        available_domains:    detail?.availableDomains ? JSON.stringify(detail.availableDomains) : null,
        contact:              detail?.contact ? JSON.stringify(detail.contact) : null,
        links:                detail?.links ? JSON.stringify(detail.links) : null,
        user_count_total:     detail?.userCountTotal ?? null,
        user_count_active:    detail?.userCountActive ?? null,
        ip_address:           geo?.ip ?? null,
        country:              geo?.country ?? null,
        country_code:         geo?.countryCode ?? null,
        region:               geo?.region ?? null,
        city:                 geo?.city ?? null,
        latitude:             geo?.lat ?? null,
        longitude:            geo?.lon ?? null,
        isp:                  geo?.isp ?? null,
        org:                  geo?.org ?? null,
        as_number:            geo?.asNumber ?? null,
        hosting_provider:     geo?.org ?? null,
      };
    });

    // Batched to stay under Postgres's 65,534-parameter limit (23 cols/row).
    const SNAPSHOT_BATCH_SIZE = 1000;
    for (let i = 0; i < snapshots.length; i += SNAPSHOT_BATCH_SIZE) {
      const batch = snapshots.slice(i, i + SNAPSHOT_BATCH_SIZE);
      await withDbRetry(() => sql`INSERT INTO health.pds_snapshots ${sql(batch,
        "pds_id", "run_id", "version", "invite_code_required", "is_online", "error_at",
        "did", "available_domains", "contact", "links",
        "user_count_total", "user_count_active",
        "ip_address", "country", "country_code", "region", "city",
        "latitude", "longitude", "isp", "org", "as_number", "hosting_provider"
      )}`, "pds_snapshots");
    }

    await sql`
      UPDATE health.collection_runs
      SET completed_at = NOW(), status = 'completed'
      WHERE id = ${runId}
    `;

    const totalUsers = detailResults
      ? [...detailResults.values()].reduce((sum, d) => sum + (d.userCountActive ?? 0), 0)
      : null;

    console.log(`\n=== Collection Complete ===`);
    console.log(`  PDS instances: ${directory.length}`);
    console.log(`  Online: ${directory.filter((e) => e.isOnline).length}`);
    if (geoResults) console.log(`  Geolocated: ${[...geoResults.values()].filter(g => g.country).length}`);
    if (totalUsers !== null) console.log(`  Total active users (third-party): ${totalUsers}`);
    console.log(`  Run ID: ${runId}\n`);

  } catch (err) {
    await sql`
      UPDATE health.collection_runs
      SET completed_at = NOW(), status = 'failed', metadata = ${String(err)}
      WHERE id = ${runId}
    `;
    throw err;
  } finally {
    await sql.end();
  }
}

main().catch((err) => {
  console.error("Collection failed:", err);
  process.exit(1);
});
