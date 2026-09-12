import { existsSync, readFileSync } from "fs";
import path from "path";

const CACHE_TTL_MS = 60 * 60 * 1000; // 1 hour
const DISK_CACHE_PATH = path.join(process.cwd(), "cache", "analytics.json");

export interface WauByCohortRow {
  week_start: string;
  age_bucket: string;
  wau: number;
}

export interface DauMauRow {
  date: string;
  dau: number;
  mau: number;
  dau_mau_pct: number;
}

export interface RetentionCohortRow {
  creation_date: string;
  pds_type: string;
  cohort_size: number;
  d1_n: number;
  d1_pct: number;
  d3_n: number;
  d3_pct: number;
  d7_n: number;
  d7_pct: number;
}

export interface AnalyticsData {
  wauByCohort: WauByCohortRow[];
  dauMauTrend: DauMauRow[];
  dauMauSnapshot: DauMauRow | null;
  retentionByCohort: RetentionCohortRow[];
}

interface AnalyticsCacheFile {
  data: AnalyticsData;
  writtenAt: string;
}

let memCache: { file: AnalyticsCacheFile; expires: number } | null = null;

function tryLoadDiskCache(): AnalyticsCacheFile | null {
  try {
    if (!existsSync(DISK_CACHE_PATH)) return null;
    return JSON.parse(readFileSync(DISK_CACHE_PATH, "utf8")) as AnalyticsCacheFile;
  } catch {
    return null;
  }
}

/**
 * Returns the cached /analytics page data, or null if the cache has never
 * been written (run `npm run analysis:cache`).
 */
export function getAnalyticsData(): AnalyticsCacheFile | null {
  if (memCache && Date.now() < memCache.expires) return memCache.file;

  // Serve the disk cache regardless of age — stale analytics is better than
  // "no data" on a cold restart. Freshness is the refresh job's responsibility.
  const disk = tryLoadDiskCache();
  if (disk) {
    memCache = { file: disk, expires: Date.now() + CACHE_TTL_MS };
    return disk;
  }

  return null;
}
