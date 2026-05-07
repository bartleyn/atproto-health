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
  locations: CityCluster[];
  providerLocations: PdsProviderLocation[];
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
    locations: getPdsLocations(hideBsky),
    providerLocations: getPdsLocationsWithProvider(hideBsky),
  };

  dashboardCache.set(hideBsky, { data, expires: Date.now() + CACHE_TTL_MS });
  return data;
}
