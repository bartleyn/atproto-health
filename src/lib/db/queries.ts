import { getDb } from "./schema";
import {
  getOverviewStats, getCountryDistribution, getReposByCountry,
  getVersionDistribution, getHostingProviders, getCloudflareBreakdown,
  getUserDistribution, getConcentrationStats, getPdsLocations, getPdsLocationsWithProvider,
  getTopPdsByScan,
  type OverviewStats, type CountryCount, type CountryRepoCount, type VersionCount,
  type HostingProviderCount, type UserDistBucket, type ConcentrationStats,
  type CityCluster, type PdsProviderLocation,
} from "./plc-queries";

// Re-export types from plc-queries so existing importers don't break
export type { OverviewStats, CountryCount, CountryRepoCount, VersionCount,
  HostingProviderCount, UserDistBucket, ConcentrationStats,
  CityCluster, PdsProviderLocation };

const CACHE_TTL_MS = 60 * 60 * 1000; // 1 hour
const dashboardCache = new Map<boolean, { data: DashboardData; expires: number }>();

// ── Run info ──────────────────────────────────────────────────────────────────

export interface LatestRunInfo {
  dirRun: { id: number; completedAt: string } | null;
  geoRun: { id: number; completedAt: string } | null;
  usrRun: { id: number; completedAt: string } | null;
}

export function getLatestRunInfo(): LatestRunInfo {
  const db = getDb();
  function latestBySource(pattern: string) {
    const row = db
      .prepare(
        `SELECT id, completed_at as completedAt FROM collection_runs
         WHERE status = 'completed' AND source LIKE ?
         ORDER BY id DESC LIMIT 1`
      )
      .get(pattern) as { id: number; completedAt: string } | undefined;
    return row ?? null;
  }
  return {
    dirRun: latestBySource("%"),
    geoRun: latestBySource("%geo%") ?? latestBySource("%full%"),
    usrRun: latestBySource("%users%") ?? latestBySource("%full%"),
  };
}

// ── GitHub stats ──────────────────────────────────────────────────────────────

export interface GithubTopicStats {
  query: string;
  repoCount: number;
  collectedAt: string;
  topRepos: Array<{
    name: string;
    fullName: string;
    stars: number;
    url: string;
    description: string | null;
  }>;
}

export function getLatestGithubStats(): GithubTopicStats[] {
  const db = getDb();
  const rows = db
    .prepare(
      `SELECT query, repo_count as repoCount, collected_at as collectedAt, top_repos as topRepos
       FROM github_stats g1
       WHERE collected_at = (
         SELECT MAX(collected_at) FROM github_stats g2 WHERE g2.query = g1.query
       )
       ORDER BY query`
    )
    .all() as Array<{ query: string; repoCount: number; collectedAt: string; topRepos: string }>;
  return rows.map((r) => ({ ...r, topRepos: JSON.parse(r.topRepos) }));
}

// ── Firehose samples ──────────────────────────────────────────────────────────

export interface FirehoseSample {
  id: number;
  sampledAt: string;
  sampleCount: number;
  windowDays: number;
  durationMs: number;
  totalEvents: number;
  totalInteractions: number;
  resolvedInteractions: number;
  crossPds: number;
  samePds: number;
  eventsPerSecond: number;
  byType: Record<string, { total: number; crossPds: number; samePds: number }>;
  federation: Record<string, number>;
  topCrossPdsPairs: Array<{ from: string; to: string; count: number }>;
}

export function getLatestFirehoseSample(): FirehoseSample | null {
  const db = getDb();
  const rows = db
    .prepare(
      `SELECT * FROM firehose_samples
       WHERE sampled_at >= datetime('now', '-7 days')
       ORDER BY id ASC`
    )
    .all() as Record<string, unknown>[];

  if (rows.length === 0) return null;

  let totalEvents = 0, totalInteractions = 0, resolvedInteractions = 0;
  let crossPds = 0, samePds = 0, totalDurationMs = 0;
  const byType: Record<string, { total: number; crossPds: number; samePds: number }> = {};
  const federation: Record<string, number> = {};
  const pairCounts = new Map<string, { from: string; to: string; count: number }>();

  for (const row of rows) {
    totalEvents         += row.total_events as number;
    totalInteractions   += row.total_interactions as number;
    resolvedInteractions += row.resolved_interactions as number;
    crossPds            += row.cross_pds as number;
    samePds             += row.same_pds as number;
    totalDurationMs     += row.duration_ms as number;

    const bt = JSON.parse(row.by_type as string) as Record<string, { total: number; crossPds: number; samePds: number }>;
    for (const [type, counts] of Object.entries(bt)) {
      if (!byType[type]) byType[type] = { total: 0, crossPds: 0, samePds: 0 };
      byType[type].total += counts.total;
      byType[type].crossPds += counts.crossPds;
      byType[type].samePds += counts.samePds;
    }
    const fed = JSON.parse(row.federation as string) as Record<string, number>;
    for (const [key, count] of Object.entries(fed)) {
      federation[key] = (federation[key] ?? 0) + count;
    }
    const pairs = JSON.parse(row.top_cross_pds_pairs as string) as Array<{ from: string; to: string; count: number }>;
    for (const pair of pairs) {
      const key = `${pair.from}|${pair.to}`;
      const existing = pairCounts.get(key);
      if (existing) existing.count += pair.count;
      else pairCounts.set(key, { ...pair });
    }
  }

  const topCrossPdsPairs = [...pairCounts.values()].sort((a, b) => b.count - a.count).slice(0, 10);
  const eventsPerSecond  = totalDurationMs > 0 ? Math.round((totalEvents / totalDurationMs) * 1000) : 0;
  const latest = rows[rows.length - 1];

  return {
    id: latest.id as number,
    sampledAt: latest.sampled_at as string,
    sampleCount: rows.length,
    windowDays: 7,
    durationMs: totalDurationMs,
    totalEvents,
    totalInteractions,
    resolvedInteractions,
    crossPds,
    samePds,
    eventsPerSecond,
    byType,
    federation,
    topCrossPdsPairs,
  };
}

// ── Cached dashboard bundle ───────────────────────────────────────────────────

export interface TopPds {
  url: string;
  repoCount: number;
  activeCount: number | null;
  country: string | null;
}

export interface DashboardData {
  runInfo: LatestRunInfo;
  stats: OverviewStats;
  countries: CountryCount[];
  reposByCountry: CountryRepoCount[];
  versions: VersionCount[];
  providers: HostingProviderCount[];
  cdnBreakdown: { behindCdn: number; directHosting: number; unknown: number };
  userDist: UserDistBucket[];
  topPds: TopPds[];
  concentration: ConcentrationStats;
  firehose: FirehoseSample | null;
  locations: CityCluster[];
  providerLocations: PdsProviderLocation[];
  githubStats: GithubTopicStats[];
}

export function getDashboardData(hideBsky = false): DashboardData {
  const cached = dashboardCache.get(hideBsky);
  if (cached && Date.now() < cached.expires) {
    return cached.data;
  }

  const topPdsRaw = getTopPdsByScan(10, hideBsky);

  const data: DashboardData = {
    runInfo: getLatestRunInfo(),
    stats: getOverviewStats(hideBsky),
    countries: getCountryDistribution(hideBsky),
    reposByCountry: getReposByCountry(hideBsky),
    versions: getVersionDistribution(hideBsky),
    providers: getHostingProviders(hideBsky),
    cdnBreakdown: getCloudflareBreakdown(hideBsky),
    userDist: getUserDistribution(hideBsky),
    topPds: topPdsRaw.map(p => ({ url: p.url, repoCount: p.repoCount, activeCount: p.activeCount, country: null })),
    concentration: getConcentrationStats(hideBsky),
    firehose: getLatestFirehoseSample(),
    locations: getPdsLocations(hideBsky),
    providerLocations: getPdsLocationsWithProvider(hideBsky),
    githubStats: getLatestGithubStats(),
  };

  dashboardCache.set(hideBsky, { data, expires: Date.now() + CACHE_TTL_MS });
  return data;
}
