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
import { getPlcDb } from "../db/plc-schema";
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
  const db = getPlcDb();
  const today = new Date().toISOString().slice(0, 10);

  console.log(`\n=== PDS Repo Status Scanner ===`);
  console.log(`Snapshot date: ${today}`);
  console.log(`Concurrency:   ${CONCURRENCY}`);

  // Get all PDSes from our DB, ordered by account count desc
  let pdsList: string[];
  if (onlyList) {
    pdsList = onlyList;
    console.log(`Mode: specific PDSes (${pdsList.length})\n`);
  } else {
    // PDS sources: did:plc accounts (plc_did_pds) + did:web accounts (did_web_pds)
    const plcRows = db
      .prepare(`SELECT pds_url FROM plc_did_pds GROUP BY pds_url ORDER BY COUNT(*) DESC`)
      .all() as { pds_url: string }[];
    const pdsSet = new Map<string, number>(); // url → approx account count
    for (const r of plcRows) {
      pdsSet.set(r.pds_url.replace(/\/+$/, "").replace(/^http:\/\//, "https://"), 0);
    }

    const didWebRows = db
      .prepare(`SELECT DISTINCT pds_url FROM did_web_pds WHERE pds_url IS NOT NULL`)
      .all() as { pds_url: string }[];
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

    const total = pdsSet.size;
    const skipped = total - pdsList.length;
    console.log(`PDSes to scan: ${pdsList.length.toLocaleString()} (${skipped.toLocaleString()} skipped — pass --include-bsky / --include-trump to include)\n`);
  }

  // Skip PDSes already snapshotted today
  const alreadyDone = new Set(
    (db.prepare(`SELECT pds_url FROM pds_repo_status_snapshots WHERE snapshot_date = ? AND is_partial = 0`).all(today) as { pds_url: string }[])
      .map(r => r.pds_url)
  );

  // Skip PDSes whose most recent completed scan returned 0 repos AND was within
  // the last 7 days — re-check them weekly in case they come back online.
  const emptyRecently = new Set(
    (db.prepare(`
      SELECT pds_url FROM (
        SELECT pds_url, total_scanned, snapshot_date,
          ROW_NUMBER() OVER (PARTITION BY pds_url ORDER BY snapshot_date DESC) AS rn
        FROM pds_repo_status_snapshots WHERE is_partial = 0
      )
      WHERE rn = 1 AND total_scanned = 0
        AND snapshot_date >= date('now', '-7 days')
    `).all() as { pds_url: string }[]).map(r => r.pds_url)
  );

  const toScan = pdsList.filter(p => !alreadyDone.has(p) && !emptyRecently.has(p));
  const skippedEmpty = pdsList.filter(p => !alreadyDone.has(p) && emptyRecently.has(p)).length;

  if (alreadyDone.size > 0 || skippedEmpty > 0) {
    console.log(`Resuming: ${alreadyDone.size.toLocaleString()} done today, ${skippedEmpty.toLocaleString()} skipped (empty <7d ago), ${toScan.length.toLocaleString()} remaining\n`);
  }

  // Build IP map: reuse cached IPs from previous snapshots, only DNS-resolve unknowns.
  const cachedIps = db.prepare(`
    SELECT pds_url, ip_address FROM (
      SELECT pds_url, ip_address,
        ROW_NUMBER() OVER (PARTITION BY pds_url ORDER BY snapshot_date DESC) AS rn
      FROM pds_repo_status_snapshots WHERE ip_address IS NOT NULL
    ) WHERE rn = 1
  `).all() as { pds_url: string; ip_address: string }[];

  const ipMap = new Map<string, string | null>(cachedIps.map(r => [r.pds_url, r.ip_address]));
  const needResolve = toScan.filter(url => !ipMap.has(url));

  if (needResolve.length > 0) {
    console.log(`Resolving IPs for ${needResolve.length.toLocaleString()} new PDSes (${toScan.length - needResolve.length} cached)...`);
    const freshIps = await resolveAll(needResolve);
    for (const [url, ip] of freshIps) ipMap.set(url, ip);
  } else {
    console.log(`IPs: all ${toScan.length.toLocaleString()} cached from previous scans`);
  }
  const resolved = [...ipMap.values()].filter(Boolean).length;
  console.log(`  ${resolved.toLocaleString()} of ${toScan.length.toLocaleString()} resolved\n`);

  const upsert = db.prepare(`
    INSERT INTO pds_repo_status_snapshots
      (pds_url, snapshot_date, active, deactivated, deleted, takendown, suspended, other, total_scanned, is_sampled, did_plc_count, did_web_count, is_partial, scanned_at, ip_address,
       invite_code_required, is_online)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(pds_url, snapshot_date) DO UPDATE SET
      active = excluded.active, deactivated = excluded.deactivated,
      deleted = excluded.deleted, takendown = excluded.takendown,
      suspended = excluded.suspended, other = excluded.other,
      total_scanned = excluded.total_scanned,
      did_plc_count = excluded.did_plc_count,
      did_web_count = excluded.did_web_count,
      is_partial = excluded.is_partial,
      scanned_at = excluded.scanned_at,
      ip_address = excluded.ip_address,
      invite_code_required = excluded.invite_code_required,
      is_online = excluded.is_online
  `);

  const upsertDidStatus = db.prepare(`
    INSERT INTO did_repo_status (did, status, pds_url, scanned_at)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(did) DO UPDATE SET
      status     = excluded.status,
      pds_url    = excluded.pds_url,
      scanned_at = excluded.scanned_at
  `);

  const upsertDidInRepo = db.prepare(`
    INSERT INTO did_in_repo (did, pds_url, scanned_at, first_scanned_at)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(did) DO UPDATE SET
      pds_url    = excluded.pds_url,
      scanned_at = excluded.scanned_at
  `);

  const DID_BATCH_SIZE = 10_000;

  const writeDidBatch = db.transaction((pdsUrl: string, batch: RepoInfo[]) => {
    for (const r of batch) {
      upsertDidInRepo.run(r.did, pdsUrl, scannedAt, scannedAt);
    }
  });

  const writeNonActive = db.transaction((pdsUrl: string, nonActive: NonActiveRepo[]) => {
    for (const r of nonActive) {
      upsertDidStatus.run(r.did, r.status, pdsUrl, scannedAt);
    }
  });

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

    const flushBatch = () => {
      if (repoBatch.length > 0) {
        writeDidBatch(pdsUrl, repoBatch);
        totalDidInRepo += repoBatch.length;
        repoBatch = [];
      }
    };

    const onRepo = (repo: RepoInfo) => {
      repoBatch.push(repo);
      if (repoBatch.length >= DID_BATCH_SIZE) flushBatch();
    };

    let inviteCodeRequired: number | null = null;

    let isOnline = 0;

    try {
      const [result, desc] = await Promise.all([
        scanPdsRepos(pdsUrl, onRepo),
        describeServer(pdsUrl),
      ]);
      counts = result.counts;
      nonActive = result.nonActive;
      partial = result.partial;
      if (desc?.inviteCodeRequired !== undefined) {
        inviteCodeRequired = desc.inviteCodeRequired ? 1 : 0;
      }
      isOnline = 1; // got a response from the PDS
      if (partial) {
        errors++;
        if (errors <= 20) {
          console.error(`  ⚠ ${pdsUrl}: partial scan (${counts.total.toLocaleString()} repos)`);
        }
      }
    } catch (err: any) {
      errors++;
      if (errors <= 20) {
        console.error(`  ✗ ${pdsUrl}: ${err}`);
      }
      // Write an offline row so the dashboard reflects current status
      upsert.run(pdsUrl, today, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, scannedAt, ipMap.get(pdsUrl) ?? null, inviteCodeRequired, 0);
      return;
    }

    flushBatch(); // write any remaining DIDs

    upsert.run(
      pdsUrl, today,
      counts?.active ?? 0, counts?.deactivated ?? 0, counts?.deleted ?? 0,
      counts?.takendown ?? 0, counts?.suspended ?? 0, counts?.other ?? 0,
      counts?.total ?? 0, counts?.didPlc ?? 0, counts?.didWeb ?? 0,
      partial ? 1 : 0, scannedAt, ipMap.get(pdsUrl) ?? null,
      inviteCodeRequired, isOnline
    );

    if (nonActive.length > 0) {
      writeNonActive(pdsUrl, nonActive);
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
  console.log(`  npx tsx src/lib/collectors/scan-pds-status.ts --only <pds_url>`);
  console.log(`  Or query: SELECT * FROM pds_repo_status_snapshots WHERE snapshot_date = '${today}' ORDER BY active DESC;`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
