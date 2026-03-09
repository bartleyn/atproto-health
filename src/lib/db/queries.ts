import { getDb } from "./schema";

export interface LatestRun {
  id: number;
  completedAt: string;
}

export function getLatestRun(): LatestRun | null {
  const db = getDb();
  const row = db
    .prepare(
      `SELECT id, completed_at as completedAt FROM collection_runs
       WHERE status = 'completed' ORDER BY id DESC LIMIT 1`
    )
    .get() as LatestRun | undefined;
  return row ?? null;
}

export interface OverviewStats {
  total: number;
  online: number;
  offline: number;
  openReg: number;
  inviteOnly: number;
  countries: number;
  totalUsers: number;
  activeUsers: number;
}

export function getOverviewStats(runId: number): OverviewStats {
  const db = getDb();
  return db
    .prepare(
      `SELECT
        COUNT(*) as total,
        SUM(is_online) as online,
        SUM(CASE WHEN is_online = 0 THEN 1 ELSE 0 END) as offline,
        SUM(CASE WHEN invite_code_required = 0 THEN 1 ELSE 0 END) as openReg,
        SUM(CASE WHEN invite_code_required = 1 THEN 1 ELSE 0 END) as inviteOnly,
        COUNT(DISTINCT CASE WHEN country_code IS NOT NULL THEN country_code END) as countries,
        COALESCE(SUM(user_count_total), 0) as totalUsers,
        COALESCE(SUM(user_count_active), 0) as activeUsers
      FROM pds_snapshots WHERE run_id = ?`
    )
    .get(runId) as OverviewStats;
}

export interface CountryCount {
  country: string;
  countryCode: string;
  count: number;
}

export function getCountryDistribution(runId: number): CountryCount[] {
  const db = getDb();
  return db
    .prepare(
      `SELECT country, country_code as countryCode, COUNT(*) as count
       FROM pds_snapshots
       WHERE run_id = ? AND country IS NOT NULL
       GROUP BY country_code ORDER BY count DESC`
    )
    .all(runId) as CountryCount[];
}

export interface VersionCount {
  version: string;
  count: number;
}

export function getVersionDistribution(runId: number): VersionCount[] {
  const db = getDb();
  return db
    .prepare(
      `SELECT COALESCE(version, 'unknown') as version, COUNT(*) as count
       FROM pds_snapshots
       WHERE run_id = ?
       GROUP BY version ORDER BY count DESC`
    )
    .all(runId) as VersionCount[];
}

export interface HostingProviderCount {
  provider: string;
  count: number;
}

export function getHostingProviders(runId: number): HostingProviderCount[] {
  const db = getDb();
  // Normalize common provider name variations
  return db
    .prepare(
      `SELECT
        CASE
          WHEN org LIKE '%Cloudflare%' THEN 'Cloudflare'
          WHEN org LIKE '%DigitalOcean%' OR org LIKE '%Digital Ocean%' THEN 'DigitalOcean'
          WHEN org LIKE '%Hetzner%' OR org LIKE '%HETZNER%' THEN 'Hetzner'
          WHEN org LIKE '%OVH%' THEN 'OVH'
          WHEN org LIKE '%AWS%' OR org LIKE '%Amazon%' THEN 'AWS'
          WHEN org LIKE '%Google%' THEN 'Google Cloud'
          WHEN org LIKE '%Microsoft%' OR org LIKE '%Azure%' THEN 'Azure'
          WHEN org LIKE '%Linode%' OR org LIKE '%Akamai%' THEN 'Akamai/Linode'
          WHEN org LIKE '%Vultr%' THEN 'Vultr'
          WHEN org LIKE '%Scaleway%' THEN 'Scaleway'
          WHEN org LIKE '%Oracle%' THEN 'Oracle Cloud'
          WHEN org LIKE '%Contabo%' THEN 'Contabo'
          WHEN org IS NULL OR org = '' THEN 'Unknown'
          ELSE org
        END as provider,
        COUNT(*) as count
       FROM pds_snapshots
       WHERE run_id = ?
       GROUP BY provider ORDER BY count DESC`
    )
    .all(runId) as HostingProviderCount[];
}

export interface UserDistBucket {
  range: string;
  count: number;
  sortKey: number;
}

export function getUserDistribution(runId: number): UserDistBucket[] {
  const db = getDb();
  const rows = db
    .prepare(
      `SELECT user_count_active as users
       FROM pds_snapshots
       WHERE run_id = ? AND user_count_active IS NOT NULL`
    )
    .all(runId) as { users: number }[];

  const buckets: Record<string, { count: number; sortKey: number }> = {
    "0": { count: 0, sortKey: 0 },
    "1": { count: 0, sortKey: 1 },
    "2-5": { count: 0, sortKey: 2 },
    "6-10": { count: 0, sortKey: 3 },
    "11-50": { count: 0, sortKey: 4 },
    "51-100": { count: 0, sortKey: 5 },
    "101-500": { count: 0, sortKey: 6 },
    "501-1K": { count: 0, sortKey: 7 },
    "1K+": { count: 0, sortKey: 8 },
  };

  for (const { users } of rows) {
    if (users === 0) buckets["0"].count++;
    else if (users === 1) buckets["1"].count++;
    else if (users <= 5) buckets["2-5"].count++;
    else if (users <= 10) buckets["6-10"].count++;
    else if (users <= 50) buckets["11-50"].count++;
    else if (users <= 100) buckets["51-100"].count++;
    else if (users <= 500) buckets["101-500"].count++;
    else if (users <= 1000) buckets["501-1K"].count++;
    else buckets["1K+"].count++;
  }

  return Object.entries(buckets).map(([range, { count, sortKey }]) => ({
    range,
    count,
    sortKey,
  }));
}

export interface TopPds {
  url: string;
  userCountActive: number;
  version: string | null;
  country: string | null;
  org: string | null;
}

export function getTopPdsByUsers(runId: number, limit = 25): TopPds[] {
  const db = getDb();
  return db
    .prepare(
      `SELECT
        p.url,
        s.user_count_active as userCountActive,
        s.version,
        s.country,
        s.org
       FROM pds_snapshots s
       JOIN pds_instances p ON p.id = s.pds_id
       WHERE s.run_id = ? AND s.user_count_active IS NOT NULL
       ORDER BY s.user_count_active DESC
       LIMIT ?`
    )
    .all(runId, limit) as TopPds[];
}
