/**
 * Scans each PDS via com.atproto.sync.listRepos and records a snapshot of
 * repo statuses (active / deactivated / deleted / takendown / suspended).
 *
 * NOTE: npm run collect now also writes pds_repo_status_snapshots / did_in_repo
 * for directory PDSes. Use this script when you need to scan PDSes derived from
 * PLC (broader set) or need flags like --include-bsky / --include-trump.
 *
 * Usage:
 *   npx tsx src/lib/collectors/scan-pds-status.ts
 *   npx tsx src/lib/collectors/scan-pds-status.ts --include-bsky     # also scan bsky.network shards (~hours)
 *   npx tsx src/lib/collectors/scan-pds-status.ts --include-trump     # also scan pds.trump.com (~hours)
 *   npx tsx src/lib/collectors/scan-pds-status.ts --only pds_url,...  # comma-separated list of specific PDSes
 *   npx tsx src/lib/collectors/scan-pds-status.ts --concurrency 5
 */

import { promises as dns } from "dns";
import sql, { withDbRetry } from "../db/pg";
import { junkPdsFilter } from "../db/plc-queries";
import { scanPdsRepos, type RepoInfo, type StatusCounts, type NonActiveRepo } from "./pds-repos";
import { describeServer } from "./pds-details";

const DEFAULT_CONCURRENCY = 5;

// ── CLI args ──────────────────────────────────────────────────────────────────
const args = process.argv.slice(2);
const includeBsky  = args.includes("--include-bsky");
const includeTrump = args.includes("--include-trump");
const onlyIdx      = args.indexOf("--only");
const onlyList     = onlyIdx >= 0 ? args[onlyIdx + 1].split(",").map(s => s.trim()) : null;
const concIdx      = args.indexOf("--concurrency");
const CONCURRENCY  = concIdx >= 0 ? parseInt(args[concIdx + 1], 10) : DEFAULT_CONCURRENCY;

function isBskyShard(url: string) { return url.includes(".host.bsky.network"); }
function isTrump(url: string)     { return url.includes("pds.trump.com"); }

// ── Version fetch ─────────────────────────────────────────────────────────────
async function fetchVersion(pdsUrl: string): Promise<string | null> {
  try {
    const url = `${pdsUrl.replace(/\/$/, "")}/xrpc/_health`;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 10000);
    try {
      const res = await fetch(url, { signal: controller.signal });
      if (!res.ok) return null;
      const body = await res.json() as { version?: string };
      return body.version ?? null;
    } finally {
      clearTimeout(timer);
    }
  } catch {
    return null;
  }
}

// ── DNS resolution ────────────────────────────────────────────────────────────
async function resolveIp(pdsUrl: string): Promise<string | null> {
  try {
    const hostname = new URL(pdsUrl).hostname;
    const { address } = await dns.lookup(hostname, { family: 4 });
    return address;
  } catch {
    return null;
  }
}

async function resolveAll(urls: string[]): Promise<Map<string, string | null>> {
  const map = new Map<string, string | null>();
  await Promise.all(urls.map(async url => {
    map.set(url, await resolveIp(url));
  }));
  return map;
}

// ── Concurrency pool ──────────────────────────────────────────────────────────
async function runWithConcurrency<T>(
  items: T[],
  fn: (item: T, idx: number) => Promise<void>,
  concurrency: number
): Promise<void> {
  let i = 0;
  async function worker() {
    while (i < items.length) {
      const idx = i++;
      await fn(items[idx], idx);
    }
  }
  await Promise.all(Array.from({ length: concurrency }, worker));
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  const today = new Date().toISOString().slice(0, 10);

  console.log(`\n=== PDS Repo Status Scanner ===`);
  console.log(`Snapshot date: ${today}`);
  console.log(`Concurrency:   ${CONCURRENCY}`);

  let pdsList: string[];
  if (onlyList) {
    pdsList = onlyList;
    console.log(`Mode: specific PDSes (${pdsList.length})\n`);
  } else {
    const [plcRows, didWebRows] = await Promise.all([
      sql.unsafe<{ pds_url: string }[]>(
        `SELECT pds_url FROM plc.plc_did_pds WHERE ${junkPdsFilter()} GROUP BY pds_url ORDER BY COUNT(*) DESC`
      ),
      sql.unsafe<{ pds_url: string }[]>(
        `SELECT DISTINCT pds_url FROM plc.did_web_pds WHERE pds_url IS NOT NULL AND ${junkPdsFilter()}`
      ),
    ]);

    const pdsSet = new Map<string, number>();
    for (const r of plcRows) {
      pdsSet.set(r.pds_url.replace(/\/+$/, "").replace(/^http:\/\//, "https://"), 0);
    }
    let didWebAdded = 0;
    for (const r of didWebRows) {
      const url = r.pds_url.replace(/\/+$/, "").replace(/^http:\/\//, "https://");
      if (!pdsSet.has(url)) { pdsSet.set(url, 0); didWebAdded++; }
    }
    console.log(`PDS sources: ${plcRows.length.toLocaleString()} from PLC did:plc + ${didWebAdded.toLocaleString()} additional from did:web discovery`);

    pdsList = [...pdsSet.keys()].filter(url => {
      if (isBskyShard(url) && !includeBsky)  return false;
      if (isTrump(url)     && !includeTrump) return false;
      return true;
    });
    const skipped = pdsSet.size - pdsList.length;
    console.log(`PDSes to scan: ${pdsList.length.toLocaleString()} (${skipped.toLocaleString()} skipped — pass --include-bsky / --include-trump to include)\n`);
  }

  // Skip PDSes already snapshotted today
  const alreadyDoneRows = await sql<{ pds_url: string }[]>`
    SELECT pds_url FROM plc.pds_repo_status_snapshots
    WHERE snapshot_date = ${today} AND is_partial = 0
  `;
  const alreadyDone = new Set(alreadyDoneRows.map(r => r.pds_url));

  // Skip PDSes whose most recent completed scan returned 0 repos AND was within 7 days
  const emptyRecentlyRows = await sql<{ pds_url: string }[]>`
    SELECT pds_url FROM (
      SELECT pds_url, total_scanned, snapshot_date,
        ROW_NUMBER() OVER (PARTITION BY pds_url ORDER BY snapshot_date DESC) AS rn
      FROM plc.pds_repo_status_snapshots WHERE is_partial = 0
    ) t
    WHERE rn = 1 AND total_scanned = 0
      AND snapshot_date >= (CURRENT_DATE - 7)::text
  `;
  const emptyRecently = new Set(emptyRecentlyRows.map(r => r.pds_url));

  const toScan = pdsList.filter(p => !alreadyDone.has(p) && !emptyRecently.has(p));
  const skippedEmpty = pdsList.filter(p => !alreadyDone.has(p) && emptyRecently.has(p)).length;

  if (alreadyDone.size > 0 || skippedEmpty > 0) {
    console.log(`Resuming: ${alreadyDone.size.toLocaleString()} done today, ${skippedEmpty.toLocaleString()} skipped (empty <7d ago), ${toScan.length.toLocaleString()} remaining\n`);
  }

  // Build IP map from cached previous snapshots
  const cachedIpRows = await sql<{ pds_url: string; ip_address: string }[]>`
    SELECT pds_url, ip_address FROM (
      SELECT pds_url, ip_address,
        ROW_NUMBER() OVER (PARTITION BY pds_url ORDER BY snapshot_date DESC) AS rn
      FROM plc.pds_repo_status_snapshots WHERE ip_address IS NOT NULL
    ) t WHERE rn = 1
  `;
  const ipMap = new Map<string, string | null>(cachedIpRows.map(r => [r.pds_url, r.ip_address]));

  const needResolve = toScan.filter(url => !ipMap.has(url));
  if (needResolve.length > 0) {
    console.log(`Resolving IPs for ${needResolve.length.toLocaleString()} new PDSes (${toScan.length - needResolve.length} cached)...`);
    const freshIps = await resolveAll(needResolve);
    for (const [url, ip] of freshIps) ipMap.set(url, ip);
  } else {
    console.log(`IPs: all ${toScan.length.toLocaleString()} cached from previous scans`);
  }
  const resolved = toScan.filter(url => ipMap.get(url)).length;
  console.log(`  ${resolved.toLocaleString()} of ${toScan.length.toLocaleString()} resolved\n`);

  const DID_BATCH_SIZE = 10_000;
  let done = 0;
  let errors = 0;
  let totalActive = 0;
  let totalDeleted = 0;
  let totalNonActive = 0;
  let totalDidInRepo = 0;
  const startTime = Date.now();
  const scannedAt = new Date().toISOString();

  await runWithConcurrency(toScan, async (pdsUrl, _idx) => {
    let counts: StatusCounts | null = null;
    let nonActive: NonActiveRepo[] = [];
    let partial = false;
    let repoBatch: RepoInfo[] = [];

    const flushBatch = async () => {
      if (repoBatch.length === 0) return;
      const batch = repoBatch.splice(0);
      await withDbRetry(() => sql`
        INSERT INTO plc.did_in_repo ${sql(
          batch.map(r => ({ did: r.did, pds_url: pdsUrl, scanned_at: scannedAt, first_scanned_at: scannedAt })),
          "did", "pds_url", "scanned_at", "first_scanned_at"
        )}
        ON CONFLICT (did) DO UPDATE SET pds_url = EXCLUDED.pds_url, scanned_at = EXCLUDED.scanned_at
      `, "did_in_repo");
      totalDidInRepo += batch.length;
    };

    const onRepo = async (repo: RepoInfo) => {
      repoBatch.push(repo);
      if (repoBatch.length >= DID_BATCH_SIZE) await flushBatch();
    };

    let inviteCodeRequired: number | null = null;
    let version: string | null = null;
    let isOnline = 0;

    try {
      const [result, desc, ver] = await Promise.all([
        scanPdsRepos(pdsUrl, onRepo),
        describeServer(pdsUrl),
        fetchVersion(pdsUrl),
      ]);
      counts = result.counts;
      nonActive = result.nonActive;
      partial = result.partial;
      if (desc?.inviteCodeRequired !== undefined) {
        inviteCodeRequired = desc.inviteCodeRequired ? 1 : 0;
      }
      version = ver;
      isOnline = 1;
      if (partial) {
        errors++;
        if (errors <= 20) console.error(`  ⚠ ${pdsUrl}: partial scan (${counts.total.toLocaleString()} repos)`);
      }
    } catch (err: unknown) {
      errors++;
      if (errors <= 20) console.error(`  ✗ ${pdsUrl}: ${err}`);
      await withDbRetry(() => sql`
        INSERT INTO plc.pds_repo_status_snapshots
          (pds_url, snapshot_date, active, deactivated, deleted, takendown, suspended, other,
           total_scanned, is_sampled, did_plc_count, did_web_count, is_partial, scanned_at, ip_address,
           invite_code_required, is_online, version)
        VALUES (${pdsUrl}, ${today}, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ${scannedAt},
                ${ipMap.get(pdsUrl) ?? null}, ${inviteCodeRequired}, 0, ${version})
        ON CONFLICT (pds_url, snapshot_date) DO UPDATE SET
          active = 0, deactivated = 0, deleted = 0, takendown = 0, suspended = 0, other = 0,
          total_scanned = 0, is_partial = 0, scanned_at = EXCLUDED.scanned_at,
          ip_address = EXCLUDED.ip_address, invite_code_required = EXCLUDED.invite_code_required,
          is_online = 0, version = COALESCE(EXCLUDED.version, plc.pds_repo_status_snapshots.version)
      `, "pds_repo_status_snapshots");
      return;
    }

    await flushBatch();

    await withDbRetry(() => sql`
      INSERT INTO plc.pds_repo_status_snapshots
        (pds_url, snapshot_date, active, deactivated, deleted, takendown, suspended, other,
         total_scanned, is_sampled, did_plc_count, did_web_count, is_partial, scanned_at, ip_address,
         invite_code_required, is_online, version)
      VALUES (
        ${pdsUrl}, ${today},
        ${counts?.active ?? 0}, ${counts?.deactivated ?? 0}, ${counts?.deleted ?? 0},
        ${counts?.takendown ?? 0}, ${counts?.suspended ?? 0}, ${counts?.other ?? 0},
        ${counts?.total ?? 0}, 0, ${counts?.didPlc ?? 0}, ${counts?.didWeb ?? 0},
        ${partial ? 1 : 0}, ${scannedAt}, ${ipMap.get(pdsUrl) ?? null},
        ${inviteCodeRequired}, ${isOnline}, ${version}
      )
      ON CONFLICT (pds_url, snapshot_date) DO UPDATE SET
        active = EXCLUDED.active, deactivated = EXCLUDED.deactivated,
        deleted = EXCLUDED.deleted, takendown = EXCLUDED.takendown,
        suspended = EXCLUDED.suspended, other = EXCLUDED.other,
        total_scanned = EXCLUDED.total_scanned,
        did_plc_count = EXCLUDED.did_plc_count, did_web_count = EXCLUDED.did_web_count,
        is_partial = EXCLUDED.is_partial, scanned_at = EXCLUDED.scanned_at,
        ip_address = EXCLUDED.ip_address,
        invite_code_required = EXCLUDED.invite_code_required,
        is_online = EXCLUDED.is_online,
        version = COALESCE(EXCLUDED.version, plc.pds_repo_status_snapshots.version)
    `, "pds_repo_status_snapshots");

    if (nonActive.length > 0) {
      // Batched to stay under Postgres's 65,534-parameter limit (4 cols/row).
      for (let i = 0; i < nonActive.length; i += DID_BATCH_SIZE) {
        const batch = nonActive.slice(i, i + DID_BATCH_SIZE);
        await withDbRetry(() => sql`
          INSERT INTO plc.did_repo_status ${sql(
            batch.map(r => ({ did: r.did, status: r.status, pds_url: pdsUrl, scanned_at: scannedAt })),
            "did", "status", "pds_url", "scanned_at"
          )}
          ON CONFLICT (did) DO UPDATE SET
            status = EXCLUDED.status, pds_url = EXCLUDED.pds_url, scanned_at = EXCLUDED.scanned_at
        `, "did_repo_status");
      }
      totalNonActive += nonActive.length;
    }

    if (counts) {
      totalActive  += counts.active;
      totalDeleted += counts.deleted + counts.deactivated + counts.takendown + counts.suspended;
    }
    done++;

    if (done % 50 === 0 || (counts && counts.active > 1000)) {
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
      console.log(
        `[${done}/${toScan.length}] ${pdsUrl.replace("https://", "")} — ` +
        `${(counts?.active ?? 0).toLocaleString()} active${partial ? " (partial)" : ""} ` +
        `(${elapsed}s elapsed)`
      );
    }
  }, CONCURRENCY);

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\n=== Done in ${elapsed}m ===`);
  console.log(`  PDSes scanned:    ${done.toLocaleString()}`);
  console.log(`  Errors/skipped:   ${errors.toLocaleString()}`);
  console.log(`  Total active:     ${totalActive.toLocaleString()}`);
  console.log(`  Total inactive:   ${totalDeleted.toLocaleString()}`);
  console.log(`  Non-active saved: ${totalNonActive.toLocaleString()} DIDs written to did_repo_status`);
  console.log(`  All repos saved:  ${totalDidInRepo.toLocaleString()} DIDs written to did_in_repo`);
  if (totalActive + totalDeleted > 0) {
    const rate = ((totalDeleted / (totalActive + totalDeleted)) * 100).toFixed(1);
    console.log(`  Deletion rate:   ${rate}%`);
  }
  console.log(`\nQuery results:`);
  console.log(`  SELECT * FROM plc.pds_repo_status_snapshots WHERE snapshot_date = '${today}' ORDER BY active DESC;`);

  await sql.end();
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
