/**
 * Scans each PDS via com.atproto.sync.listRepos and records a snapshot of
 * repo statuses (active / deactivated / deleted / takendown / suspended).
 *
 * Run periodically to build a timeseries of active account counts per PDS.
 *
 * Usage:
 *   npx tsx src/lib/collectors/scan-pds-status.ts
 *   npx tsx src/lib/collectors/scan-pds-status.ts --include-bsky     # also scan bsky.network shards (~hours)
 *   npx tsx src/lib/collectors/scan-pds-status.ts --include-trump     # also scan pds.trump.com (~hours)
 *   npx tsx src/lib/collectors/scan-pds-status.ts --only pds_url,...  # comma-separated list of specific PDSes
 *   npx tsx src/lib/collectors/scan-pds-status.ts --concurrency 5
 */

import path from "path";
import { promises as dns } from "dns";
import Database from "better-sqlite3";
import { getPlcDb } from "../db/plc-schema";

const LIST_REPOS_LIMIT = 1000;
const DEFAULT_CONCURRENCY = 3;
const COURTESY_DELAY_MS = 100;
const REQUEST_TIMEOUT_MS = 20_000;

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

// ── listRepos paging ──────────────────────────────────────────────────────────
interface RepoInfo {
  did: string;
  active: boolean;
  status?: "deactivated" | "deleted" | "takendown" | "suspended" | string;
}

interface ListReposResponse {
  cursor?: string;
  repos: RepoInfo[];
}

const PAGE_RETRIES = 3;
const RETRY_DELAY_MS = 2_000;

async function fetchPage(url: string): Promise<ListReposResponse | null> {
  for (let attempt = 1; attempt <= PAGE_RETRIES; attempt++) {
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS) });
      if (res.status === 404 || res.status === 501) return null; // unsupported
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json() as ListReposResponse;
    } catch (err) {
      if (attempt === PAGE_RETRIES) throw err;
      await new Promise(r => setTimeout(r, RETRY_DELAY_MS * attempt));
    }
  }
  throw new Error("unreachable");
}

async function* listAllRepos(pdsUrl: string): AsyncGenerator<RepoInfo> {
  let cursor: string | undefined;
  const base = pdsUrl.replace(/\/$/, "");

  while (true) {
    const params = new URLSearchParams({ limit: String(LIST_REPOS_LIMIT) });
    if (cursor) params.set("cursor", cursor);

    const url = `${base}/xrpc/com.atproto.sync.listRepos?${params}`;
    const body = await fetchPage(url);
    if (!body) return; // PDS doesn't support listRepos

    for (const repo of body.repos) yield repo;

    if (!body.cursor || body.repos.length < LIST_REPOS_LIMIT) break;
    cursor = body.cursor;
    await new Promise(r => setTimeout(r, COURTESY_DELAY_MS));
  }
}

// ── Status counting ───────────────────────────────────────────────────────────
interface StatusCounts {
  active: number;
  deactivated: number;
  deleted: number;
  takendown: number;
  suspended: number;
  other: number;
  total: number;
  didPlc: number;
  didWeb: number;
}

interface NonActiveRepo {
  did: string;
  status: string;
}

interface ScanResult {
  counts: StatusCounts;
  nonActive: NonActiveRepo[];
  partial: boolean; // true if scan errored mid-way
}

async function scanPds(
  pdsUrl: string,
  onRepo?: (repo: RepoInfo) => void,
): Promise<ScanResult> {
  const counts: StatusCounts = {
    active: 0, deactivated: 0, deleted: 0,
    takendown: 0, suspended: 0, other: 0, total: 0,
    didPlc: 0, didWeb: 0,
  };
  const nonActive: NonActiveRepo[] = [];

  try {
    for await (const repo of listAllRepos(pdsUrl)) {
      onRepo?.(repo);
      counts.total++;
      if (repo.did.startsWith("did:plc:"))      counts.didPlc++;
      else if (repo.did.startsWith("did:web:")) counts.didWeb++;
      if (repo.active) {
        counts.active++;
      } else {
        const status = repo.status ?? "other";
        switch (status) {
          case "deactivated": counts.deactivated++; break;
          case "deleted":     counts.deleted++;     break;
          case "takendown":   counts.takendown++;   break;
          case "suspended":   counts.suspended++;   break;
          default:            counts.other++;        break;
        }
        nonActive.push({ did: repo.did, status });
      }
    }
    return { counts, nonActive, partial: false };
  } catch (err) {
    if (counts.total > 0) {
      throw Object.assign(err as Error, { partialCounts: counts, partialNonActive: nonActive });
    }
    throw err;
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
    // Start with PLC-derived PDS list (PDSes that actual accounts point to)
    const plcRows = db
      .prepare(`SELECT pds_url FROM plc_did_pds GROUP BY pds_url ORDER BY COUNT(*) DESC`)
      .all() as { pds_url: string }[];
    const pdsSet = new Map<string, number>(); // url → approx account count
    for (const r of plcRows) {
      pdsSet.set(r.pds_url.replace(/\/+$/, "").replace(/^http:\/\//, "https://"), 0);
    }

    // Also union in PDSes from the main atproto-health.db directory so both
    // pipelines scan the same set and produce aligned total account counts.
    const mainDbPath = path.join(process.cwd(), "atproto-health.db");
    try {
      const mainDb = new Database(mainDbPath, { readonly: true, timeout: 5000 });
      const mainRows = mainDb.prepare(`SELECT url FROM pds_instances`).all() as { url: string }[];
      mainDb.close();
      let added = 0;
      for (const r of mainRows) {
        const url = r.url.replace(/\/+$/, "").replace(/^http:\/\//, "https://");
        if (!pdsSet.has(url)) { pdsSet.set(url, 0); added++; }
      }
      console.log(`PDS sources: ${plcRows.length.toLocaleString()} from PLC + ${added.toLocaleString()} additional from atproto-health.db directory`);
    } catch {
      console.log(`PDS sources: ${plcRows.length.toLocaleString()} from PLC (atproto-health.db not found, skipping directory merge)`);
    }

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
  const toScan = pdsList.filter(p => !alreadyDone.has(p));
  if (alreadyDone.size > 0) {
    console.log(`Resuming: ${alreadyDone.size.toLocaleString()} already done today, ${toScan.length.toLocaleString()} remaining\n`);
  }

  // Resolve IPs for all PDSes to be scanned (used to detect same-backend aliases).
  console.log(`Resolving IPs for ${toScan.length.toLocaleString()} PDSes...`);
  const ipMap = await resolveAll(toScan);
  const resolved = [...ipMap.values()].filter(Boolean).length;
  console.log(`  ${resolved.toLocaleString()} of ${toScan.length.toLocaleString()} resolved\n`);

  const upsert = db.prepare(`
    INSERT INTO pds_repo_status_snapshots
      (pds_url, snapshot_date, active, deactivated, deleted, takendown, suspended, other, total_scanned, is_sampled, did_plc_count, did_web_count, is_partial, scanned_at, ip_address)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?)
    ON CONFLICT(pds_url, snapshot_date) DO UPDATE SET
      active = excluded.active, deactivated = excluded.deactivated,
      deleted = excluded.deleted, takendown = excluded.takendown,
      suspended = excluded.suspended, other = excluded.other,
      total_scanned = excluded.total_scanned,
      did_plc_count = excluded.did_plc_count,
      did_web_count = excluded.did_web_count,
      is_partial = excluded.is_partial,
      scanned_at = excluded.scanned_at,
      ip_address = excluded.ip_address
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

  const writeDidBatch = db.transaction((pdsUrl: string, scannedAt: string, batch: RepoInfo[]) => {
    for (const r of batch) {
      upsertDidInRepo.run(r.did, pdsUrl, scannedAt, scannedAt);
    }
  });

  const writeNonActive = db.transaction((pdsUrl: string, scannedAt: string, nonActive: NonActiveRepo[]) => {
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
        writeDidBatch(pdsUrl, scannedAt, repoBatch);
        totalDidInRepo += repoBatch.length;
        repoBatch = [];
      }
    };

    const onRepo = (repo: RepoInfo) => {
      repoBatch.push(repo);
      if (repoBatch.length >= DID_BATCH_SIZE) flushBatch();
    };

    try {
      const result = await scanPds(pdsUrl, onRepo);
      counts = result.counts;
      nonActive = result.nonActive;
    } catch (err: any) {
      if (err?.partialCounts?.total > 0) {
        counts = err.partialCounts;
        nonActive = err.partialNonActive ?? [];
        partial = true;
        errors++;
        if (errors <= 20) {
          console.error(`  ⚠ ${pdsUrl}: partial scan (${counts!.total.toLocaleString()} repos) — ${err.message}`);
        }
      } else {
        errors++;
        if (errors <= 20) {
          console.error(`  ✗ ${pdsUrl}: ${err}`);
        }
        return;
      }
    }

    flushBatch(); // write any remaining DIDs

    if (!counts || counts.total === 0) return; // PDS offline or doesn't support listRepos

    upsert.run(
      pdsUrl, today,
      counts.active, counts.deactivated, counts.deleted,
      counts.takendown, counts.suspended, counts.other,
      counts.total, counts.didPlc, counts.didWeb,
      partial ? 1 : 0, scannedAt, ipMap.get(pdsUrl) ?? null
    );

    if (nonActive.length > 0) {
      writeNonActive(pdsUrl, scannedAt, nonActive);
      totalNonActive += nonActive.length;
    }

    totalActive  += counts.active;
    totalDeleted += counts.deleted + counts.deactivated + counts.takendown + counts.suspended;
    done++;

    if (done % 50 === 0 || counts.active > 1000) {
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
      console.log(
        `[${done}/${toScan.length}] ${pdsUrl.replace("https://", "")} — ` +
        `${counts.active.toLocaleString()} active${partial ? " (partial)" : ""} ` +
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
