/**
 * Fetches per-PDS details: describeServer and full repo status via listRepos.
 */

import { scanPdsRepos, type RepoInfo, type StatusCounts, type NonActiveRepo } from "./pds-repos";

export type { RepoInfo, StatusCounts, NonActiveRepo };

export interface PdsDetails {
  pdsUrl: string;
  did: string | null;
  availableDomains: string[];
  contact: Record<string, unknown> | null;
  links: Record<string, unknown> | null;
  userCountTotal: number | null;
  userCountActive: number | null;
  statusCounts: StatusCounts | null;
  nonActive: NonActiveRepo[];
  partial: boolean;
}

export interface DescribeServerResponse {
  did?: string;
  availableUserDomains?: string[];
  inviteCodeRequired?: boolean;
  contact?: Record<string, unknown>;
  links?: Record<string, unknown>;
}

const XRPC_TIMEOUT_MS = 30000;

async function fetchWithTimeout(url: string, timeoutMs: number): Promise<Response> {
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

export async function describeServer(pdsUrl: string): Promise<DescribeServerResponse | null> {
  try {
    const url = `${pdsUrl.replace(/\/$/, "")}/xrpc/com.atproto.server.describeServer`;
    const res = await fetchWithTimeout(url, XRPC_TIMEOUT_MS);
    if (!res.ok) return null;
    return safeJsonParse<DescribeServerResponse>(res);
  } catch {
    return null;
  }
}

export async function fetchPdsDetails(
  pdsUrl: string,
  onRepo?: (repo: RepoInfo) => void | Promise<void>,
): Promise<PdsDetails> {
  const [desc, scanResult] = await Promise.allSettled([
    describeServer(pdsUrl),
    scanPdsRepos(pdsUrl, onRepo),
  ]);

  const descData = desc.status === "fulfilled" ? desc.value : null;
  const scan = scanResult.status === "fulfilled" ? scanResult.value : null;

  return {
    pdsUrl,
    did: descData?.did ?? null,
    availableDomains: descData?.availableUserDomains ?? [],
    contact: descData?.contact ?? null,
    links: descData?.links ?? null,
    userCountTotal: scan?.counts.total ?? null,
    userCountActive: scan?.counts.active ?? null,
    statusCounts: scan?.counts ?? null,
    nonActive: scan?.nonActive ?? [],
    partial: scan?.partial ?? false,
  };
}

export interface FetchAllOptions {
  concurrency?: number;
  onRepo?: (pdsUrl: string, repo: RepoInfo) => void | Promise<void>;
  onPdsDone?: (pdsUrl: string, details: PdsDetails) => void;
}

/**
 * Fetch details for multiple PDSes with controlled concurrency.
 * Includes all PDSes (online and offline) — offline ones will return null counts
 * when they don't respond to listRepos.
 */
export async function fetchAllPdsDetails(
  pdsUrls: string[],
  options: FetchAllOptions = {},
): Promise<Map<string, PdsDetails>> {
  const { concurrency = 20, onRepo, onPdsDone } = options;
  const results = new Map<string, PdsDetails>();
  let completed = 0;

  async function worker(urls: string[]) {
    for (const url of urls) {
      const details = await fetchPdsDetails(
        url,
        onRepo ? (repo) => onRepo(url, repo) : undefined,
      );
      results.set(url, details);
      onPdsDone?.(url, details);
      completed++;
      if (completed % 100 === 0 || completed === pdsUrls.length) {
        console.log(`[pds-details] ${completed}/${pdsUrls.length} PDSes queried`);
      }
    }
  }

  const chunks: string[][] = Array.from({ length: concurrency }, () => []);
  pdsUrls.forEach((url, i) => chunks[i % concurrency].push(url));

  await Promise.all(chunks.map(worker));
  return results;
}
