import { existsSync, readFileSync } from "fs";
import path from "path";
import { getDbReadonly as getDb } from "./schema";
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

const DISK_CACHE_DIR = path.join(process.cwd(), "cache");
const DISK_CACHE_MAX_AGE_MS = 24 * 60 * 60 * 1000; // 24 hours

function diskCachePath(hideBsky: boolean) {
  return path.join(DISK_CACHE_DIR, hideBsky ? "dashboard-hidebsky.json" : "dashboard.json");
}

function tryLoadDiskCache(hideBsky: boolean): DashboardData | null {
  try {
    const f = diskCachePath(hideBsky);
    if (!existsSync(f)) return null;
    const { data, writtenAt } = JSON.parse(readFileSync(f, "utf8"));
    if (Date.now() - new Date(writtenAt).getTime() > DISK_CACHE_MAX_AGE_MS) return null;
    return data as DashboardData;
  } catch {
    return null;
  }
}

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

export function getDashboardData(hideBsky = false): DashboardData | null {
  // 1. Hot in-memory cache (fastest path)
  const cached = dashboardCache.get(hideBsky);
  if (cached && Date.now() < cached.expires) return cached.data;

  // 2. Disk cache written by `npm run analysis:dashboard-cache` (non-blocking)
  const disk = tryLoadDiskCache(hideBsky);
  if (disk) {
    dashboardCache.set(hideBsky, { data: disk, expires: Date.now() + CACHE_TTL_MS });
    return disk;
  }

  // 3. No cache available — return null so the page can render a loading state
  //    rather than blocking the event loop for ~51 seconds.
  //    Run `npm run analysis:dashboard-cache` to populate the disk cache.
  return null;
}
