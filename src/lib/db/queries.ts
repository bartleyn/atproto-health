import { getDb } from "./schema";

/**
 * All dashboard queries work against a merged view that pulls the latest
 * non-null value for each field category across all completed runs.
 * This means you can run `collect:geo` and `collect:users` separately
 * and the dashboard composites the freshest data for each PDS.
 */

const CACHE_TTL_MS = 60 * 60 * 1000; // 1 hour
let dashboardCache: { data: DashboardData; expires: number } | null = null;

// Ensures the merged view exists. Called once per request cycle.
function ensureMergedView() {
  const db = getDb();
  db.exec(`
    CREATE TEMPORARY VIEW IF NOT EXISTS pds_latest AS
    SELECT
      p.id as pds_id,
      p.url,

      -- directory data: from the most recent run (always collected)
      dir.version,
      dir.invite_code_required,
      dir.is_online,
      dir.error_at,

      -- user data: latest run that has it
      usr.user_count_total,
      usr.user_count_active,
      usr.did,
      usr.available_domains,
      usr.contact,
      usr.links,

      -- geo data: latest run that has it
      geo.ip_address,
      geo.country,
      geo.country_code,
      geo.region,
      geo.city,
      geo.latitude,
      geo.longitude,
      geo.isp,
      geo.org,
      geo.as_number,
      geo.hosting_provider,

      dir.run_id as dir_run_id,
      usr.run_id as usr_run_id,
      geo.run_id as geo_run_id

    FROM pds_instances p

    -- Latest directory snapshot (from collect:full runs only)
    LEFT JOIN pds_snapshots dir ON dir.id = (
      SELECT s.id FROM pds_snapshots s
      JOIN collection_runs r ON r.id = s.run_id AND r.status = 'completed' AND r.source = 'collect:full'
      WHERE s.pds_id = p.id
      ORDER BY s.run_id DESC LIMIT 1
    )

    -- Latest snapshot with user data
    LEFT JOIN pds_snapshots usr ON usr.id = (
      SELECT s.id FROM pds_snapshots s
      JOIN collection_runs r ON r.id = s.run_id AND r.status = 'completed'
      WHERE s.pds_id = p.id AND s.user_count_total IS NOT NULL
      ORDER BY s.run_id DESC LIMIT 1
    )

    -- Latest snapshot with geo data
    LEFT JOIN pds_snapshots geo ON geo.id = (
      SELECT s.id FROM pds_snapshots s
      JOIN collection_runs r ON r.id = s.run_id AND r.status = 'completed'
      WHERE s.pds_id = p.id AND s.country IS NOT NULL
      ORDER BY s.run_id DESC LIMIT 1
    )

    WHERE dir.id IS NOT NULL
  `);
}

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
    dirRun: latestBySource("%"),  // every run collects directory
    geoRun: latestBySource("%geo%") ?? latestBySource("%full%"),
    usrRun: latestBySource("%users%") ?? latestBySource("%full%"),
  };
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

export function getOverviewStats(): OverviewStats {
  const db = getDb();
  ensureMergedView();
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
      FROM pds_latest`
    )
    .get() as OverviewStats;
}

export interface CountryCount {
  country: string;
  countryCode: string;
  count: number;
}

export function getCountryDistribution(): CountryCount[] {
  const db = getDb();
  ensureMergedView();
  return db
    .prepare(
      `SELECT country, country_code as countryCode, COUNT(*) as count
       FROM pds_latest
       WHERE country IS NOT NULL
       GROUP BY country_code ORDER BY count DESC`
    )
    .all() as CountryCount[];
}

export interface CountryRepoCount {
  country: string;
  countryCode: string;
  repoCount: number;
}

export function getReposByCountry(): CountryRepoCount[] {
  const db = getDb();
  ensureMergedView();
  return db
    .prepare(
      `SELECT country, country_code as countryCode, SUM(user_count_total) as repoCount
       FROM pds_latest
       WHERE country IS NOT NULL AND user_count_total IS NOT NULL
       GROUP BY country_code ORDER BY repoCount DESC`
    )
    .all() as CountryRepoCount[];
}

export interface VersionCount {
  version: string;
  count: number;
}

export function getVersionDistribution(): VersionCount[] {
  const db = getDb();
  ensureMergedView();
  return db
    .prepare(
      `SELECT COALESCE(version, 'unknown') as version, COUNT(*) as count
       FROM pds_latest
       GROUP BY version ORDER BY count DESC`
    )
    .all() as VersionCount[];
}

export interface HostingProviderCount {
  provider: string;
  count: number;
  isCdn: boolean;
}

const PROVIDER_NORMALIZE_SQL = `
  CASE
    WHEN org LIKE '%Cloudflare%' THEN 'Behind Cloudflare (host unknown)'
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
    WHEN org LIKE '%Fastly%' THEN 'Fastly (CDN)'
    WHEN org LIKE '%Fly.io%' OR org LIKE '%Fly IO%' THEN 'Fly.io'
    WHEN org LIKE '%netcup%' OR org LIKE '%NETCUP%' THEN 'netcup'
    WHEN org LIKE '%RackNerd%' THEN 'RackNerd'
    WHEN org LIKE '%IONOS%' OR org LIKE '%Ionos%' THEN 'IONOS'
    WHEN org IS NULL OR org = '' THEN 'Unknown'
    ELSE org
  END
`;

export function getHostingProviders(): HostingProviderCount[] {
  const db = getDb();
  ensureMergedView();
  return db
    .prepare(
      `SELECT
        ${PROVIDER_NORMALIZE_SQL} as provider,
        COUNT(*) as count,
        CASE WHEN org LIKE '%Cloudflare%' OR org LIKE '%Fastly%' THEN 1 ELSE 0 END as isCdn
       FROM pds_latest
       GROUP BY provider ORDER BY count DESC`
    )
    .all() as HostingProviderCount[];
}

export function getCloudflareBreakdown(): {
  behindCdn: number;
  directHosting: number;
  unknown: number;
} {
  const db = getDb();
  ensureMergedView();
  return db
    .prepare(
      `SELECT
        SUM(CASE WHEN org LIKE '%Cloudflare%' OR org LIKE '%Fastly%' THEN 1 ELSE 0 END) as behindCdn,
        SUM(CASE WHEN org IS NOT NULL AND org != '' AND org NOT LIKE '%Cloudflare%' AND org NOT LIKE '%Fastly%' THEN 1 ELSE 0 END) as directHosting,
        SUM(CASE WHEN org IS NULL OR org = '' THEN 1 ELSE 0 END) as unknown
       FROM pds_latest`
    )
    .get() as { behindCdn: number; directHosting: number; unknown: number };
}

export interface UserDistBucket {
  range: string;
  count: number;
  sortKey: number;
}

export function getUserDistribution(): UserDistBucket[] {
  const db = getDb();
  ensureMergedView();
  const rows = db
    .prepare(
      `SELECT user_count_total as users
       FROM pds_latest
       WHERE user_count_total IS NOT NULL`
    )
    .all() as { users: number }[];

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
  repoCount: number;
  version: string | null;
  country: string | null;
  org: string | null;
}

export function getTopPdsByUsers(limit = 10): TopPds[] {
  const db = getDb();
  ensureMergedView();
  return db
    .prepare(
      `SELECT
        CASE
          WHEN url LIKE '%host.bsky.network%' OR url LIKE '%bsky.social%'
          THEN 'https://bsky.social'
          ELSE url
        END as url,
        SUM(user_count_total) as repoCount,
        MAX(version) as version,
        MAX(country) as country,
        MAX(org) as org
       FROM pds_latest
       WHERE user_count_total IS NOT NULL
       GROUP BY
        CASE
          WHEN url LIKE '%host.bsky.network%' OR url LIKE '%bsky.social%'
          THEN 'https://bsky.social'
          ELSE url
        END
       ORDER BY repoCount DESC
       LIMIT ?`
    )
    .all(limit) as TopPds[];
}

// ── Ecosystem queries ──────────────────────────────────────────────────

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
    .all() as Array<{
      query: string;
      repoCount: number;
      collectedAt: string;
      topRepos: string;
    }>;

  return rows.map((r) => ({
    ...r,
    topRepos: JSON.parse(r.topRepos),
  }));
}

// ── Geographic map query ───────────────────────────────────────────────

export interface CityCluster {
  latitude: number;
  longitude: number;
  city: string | null;
  country: string | null;
  pdsCount: number;
}

export function getPdsLocations(): CityCluster[] {
  const db = getDb();
  ensureMergedView();
  return db
    .prepare(
      `SELECT
        AVG(latitude) as latitude,
        AVG(longitude) as longitude,
        city,
        country,
        COUNT(*) as pdsCount
       FROM pds_latest
       WHERE latitude IS NOT NULL AND longitude IS NOT NULL
       GROUP BY city, country
       ORDER BY pdsCount DESC`
    )
    .all() as CityCluster[];
}

// ── Firehose / Federation queries ─────────────────────────────────────

export interface FirehoseSample {
  id: number;
  sampledAt: string;
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
  const row = db
    .prepare(
      `SELECT * FROM firehose_samples ORDER BY id DESC LIMIT 1`
    )
    .get() as Record<string, unknown> | undefined;

  if (!row) return null;

  return {
    id: row.id as number,
    sampledAt: row.sampled_at as string,
    durationMs: row.duration_ms as number,
    totalEvents: row.total_events as number,
    totalInteractions: row.total_interactions as number,
    resolvedInteractions: row.resolved_interactions as number,
    crossPds: row.cross_pds as number,
    samePds: row.same_pds as number,
    eventsPerSecond: row.events_per_second as number,
    byType: JSON.parse(row.by_type as string),
    federation: JSON.parse(row.federation as string),
    topCrossPdsPairs: JSON.parse(row.top_cross_pds_pairs as string),
  };
}

// ── Cached dashboard bundle ────────────────────────────────────────────

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
  firehose: FirehoseSample | null;
  locations: CityCluster[];
  githubStats: GithubTopicStats[];
}

export function getDashboardData(): DashboardData {
  if (dashboardCache && Date.now() < dashboardCache.expires) {
    return dashboardCache.data;
  }

  const data: DashboardData = {
    runInfo: getLatestRunInfo(),
    stats: getOverviewStats(),
    countries: getCountryDistribution(),
    reposByCountry: getReposByCountry(),
    versions: getVersionDistribution(),
    providers: getHostingProviders(),
    cdnBreakdown: getCloudflareBreakdown(),
    userDist: getUserDistribution(),
    topPds: getTopPdsByUsers(),
    firehose: getLatestFirehoseSample(),
    locations: getPdsLocations(),
    githubStats: getLatestGithubStats(),
  };

  dashboardCache = { data, expires: Date.now() + CACHE_TTL_MS };
  return data;
}
