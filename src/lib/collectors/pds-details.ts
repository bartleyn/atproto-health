/**
 * Fetches per-PDS details: describeServer and user counts via listRepos.
 */

export interface PdsDetails {
  pdsUrl: string;
  did: string | null;
  availableDomains: string[];
  contact: Record<string, unknown> | null;
  links: Record<string, unknown> | null;
  userCountTotal: number | null;
  userCountActive: number | null;
}

interface DescribeServerResponse {
  did?: string;
  availableUserDomains?: string[];
  inviteCodeRequired?: boolean;
  contact?: Record<string, unknown>;
  links?: Record<string, unknown>;
}

interface ListReposResponse {
  cursor?: string;
  repos: Array<{
    did: string;
    active: boolean;
    status?: string;
  }>;
}

const XRPC_TIMEOUT_MS = 30000;

async function fetchWithTimeout(
  url: string,
  timeoutMs: number
): Promise<Response> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

async function safeJsonParse<T>(res: Response): Promise<T | null> {
  const contentType = res.headers.get("content-type") ?? "";
  if (!contentType.includes("json")) return null;
  try {
    return await res.json();
  } catch {
    return null;
  }
}

async function describeServer(
  pdsUrl: string
): Promise<DescribeServerResponse | null> {
  try {
    const url = `${pdsUrl.replace(/\/$/, "")}/xrpc/com.atproto.server.describeServer`;
    const res = await fetchWithTimeout(url, XRPC_TIMEOUT_MS);
    if (!res.ok) return null;
    return safeJsonParse<DescribeServerResponse>(res);
  } catch {
    return null;
  }
}

async function countUsers(
  pdsUrl: string
): Promise<{ total: number; active: number } | null> {
  const base = `${pdsUrl.replace(/\/$/, "")}/xrpc/com.atproto.sync.listRepos`;
  let cursor: string | undefined;
  let total = 0;
  let active = 0;

  // Paginate through all repos to count users
  // Page limit: 1000 pages (1M users) to avoid runaway loops (could eventually undercount!)
  for (let page = 0; page < 1000; page++) {
    const params = new URLSearchParams({ limit: "1000" });
    if (cursor) params.set("cursor", cursor);

    let res: Response;
    try {
      // Use timeout only on page 0 to quickly skip dead PDSes.
      // Subsequent pages have three minute timeout, to make sure we keep the connection alive after successfully getting the first page
      const timeout = page === 0 ? XRPC_TIMEOUT_MS : 180_000;
      res = await fetchWithTimeout(`${base}?${params}`, timeout);
    } catch (err) {
      if (page === 0) {
        console.warn(`[pds-details] ${pdsUrl} listRepos failed on page 0: ${err}`);
        return null;
      }
      console.warn(`[pds-details] ${pdsUrl} listRepos failed on page ${page}, returning partial count of ${total}: ${err}`);
      return { total, active };
    }

    if (!res.ok) {
      if (page === 0) {
        console.warn(`[pds-details] ${pdsUrl} listRepos HTTP ${res.status} on page 0`);
        return null;
      }
      console.warn(`[pds-details] ${pdsUrl} listRepos HTTP ${res.status} on page ${page}, returning partial count of ${total}`);
      return { total, active };
    }

    const data = await safeJsonParse<ListReposResponse>(res);
    if (!data?.repos) {
      if (page === 0) {
        console.warn(`[pds-details] ${pdsUrl} listRepos bad response on page 0`);
        return null;
      }
      console.warn(`[pds-details] ${pdsUrl} listRepos bad response on page ${page}, returning partial count of ${total}`);
      return { total, active };
    }

    total += data.repos.length;
    active += data.repos.filter((r) => r.active).length;

    if (!data.cursor || data.repos.length === 0) break;
    cursor = data.cursor;
  }

  return { total, active };
}

export async function fetchPdsDetails(
  pdsUrl: string
): Promise<PdsDetails> {
  const [desc, users] = await Promise.all([
    describeServer(pdsUrl),
    countUsers(pdsUrl),
  ]);

  return {
    pdsUrl,
    did: desc?.did ?? null,
    availableDomains: desc?.availableUserDomains ?? [],
    contact: desc?.contact ?? null,
    links: desc?.links ?? null,
    userCountTotal: users?.total ?? null,
    userCountActive: users?.active ?? null,
  };
}

/**
 * Fetch details for multiple PDSes with controlled concurrency.
 */
export async function fetchAllPdsDetails(
  pdsUrls: string[],
  concurrency = 20
): Promise<Map<string, PdsDetails>> {
  const results = new Map<string, PdsDetails>();
  let completed = 0;

  async function worker(urls: string[]) {
    for (const url of urls) {
      const details = await fetchPdsDetails(url);
      results.set(url, details);
      completed++;
      if (completed % 100 === 0 || completed === pdsUrls.length) {
        console.log(
          `[pds-details] ${completed}/${pdsUrls.length} PDSes queried`
        );
      }
    }
  }

  // Split URLs across workers
  const chunks: string[][] = Array.from({ length: concurrency }, () => []);
  pdsUrls.forEach((url, i) => chunks[i % concurrency].push(url));

  await Promise.all(chunks.map(worker));
  return results;
}
