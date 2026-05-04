/**
 * Shared listRepos pagination and repo-status counting logic.
 * Used by both run.ts (collect) and scan-pds-status.ts.
 */

const LIST_REPOS_LIMIT = 1000;
const COURTESY_DELAY_MS = 75;
const REQUEST_TIMEOUT_MS = 20_000;
const PAGE_RETRIES = 3;
const RETRY_DELAY_MS = 2_000;

export interface RepoInfo {
  did: string;
  active: boolean;
  status?: "deactivated" | "deleted" | "takendown" | "suspended" | string;
}

interface ListReposResponse {
  cursor?: string;
  repos: RepoInfo[];
}

export interface StatusCounts {
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

export interface NonActiveRepo {
  did: string;
  status: string;
}

export interface ScanResult {
  counts: StatusCounts;
  nonActive: NonActiveRepo[];
  partial: boolean;
}

async function fetchPage(url: string): Promise<ListReposResponse | null> {
  for (let attempt = 1; attempt <= PAGE_RETRIES; attempt++) {
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS) });
      if (res.status === 404 || res.status === 501) return null;
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json() as ListReposResponse;
    } catch (err) {
      if (attempt === PAGE_RETRIES) throw err;
      await new Promise(r => setTimeout(r, RETRY_DELAY_MS * attempt));
    }
  }
  throw new Error("unreachable");
}

export async function* listAllRepos(pdsUrl: string): AsyncGenerator<RepoInfo> {
  let cursor: string | undefined;
  const base = pdsUrl.replace(/\/$/, "");

  while (true) {
    const params = new URLSearchParams({ limit: String(LIST_REPOS_LIMIT) });
    if (cursor) params.set("cursor", cursor);

    const url = `${base}/xrpc/com.atproto.sync.listRepos?${params}`;
    const body = await fetchPage(url);
    if (!body) return;

    for (const repo of body.repos) yield repo;

    if (!body.cursor || body.repos.length < LIST_REPOS_LIMIT) break;
    cursor = body.cursor;
    await new Promise(r => setTimeout(r, COURTESY_DELAY_MS));
  }
}

/**
 * Full listRepos scan for one PDS. Returns counts + non-active DIDs.
 * On mid-scan error, returns a partial result (partial: true) rather than throwing.
 * Only throws on complete failure (0 repos seen before the error).
 */
export async function scanPdsRepos(
  pdsUrl: string,
  onRepo?: (repo: RepoInfo) => void,
): Promise<ScanResult> {
  const counts: StatusCounts = {
    active: 0, deactivated: 0, deleted: 0,
    takendown: 0, suspended: 0, other: 0,
    total: 0, didPlc: 0, didWeb: 0,
  };
  const nonActive: NonActiveRepo[] = [];
  let partial = false;

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
  } catch (err) {
    if (counts.total === 0) throw err;
    partial = true;
    console.warn(`[pds-repos] ${pdsUrl} partial scan at ${counts.total} repos: ${err}`);
  }

  return { counts, nonActive, partial };
}
