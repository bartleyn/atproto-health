import { getDb } from "./schema";

/**
 * All dashboard queries work against a merged view that pulls the latest
 * non-null value for each field category across all completed runs.
 * This means you can run `collect:geo` and `collect:users` separately
 * and the dashboard composites the freshest data for each PDS.
 */

const CACHE_TTL_MS = 60 * 60 * 1000; // 1 hour
const dashboardCache = new Map<boolean, { data: DashboardData; expires: number }>();

const BSKY_FILTER = `url NOT LIKE '%host.bsky.network%' AND url NOT LIKE '%bsky.social%'`;

// Ensures the merged view exists. Called once per request cycle.
function ensureMergedView() {
  const db = getDb();
  db.exec(`
    CREATE TEMPORARY VIEW IF NOT EXISTS pds_latest AS
    SELECT
      -- Normalize URL: strip trailing slash so duplicate entries (with/without /)
      -- collapse into one row. MAX picks the best non-null value across duplicates.
      RTRIM(p.url, '/') as url,

      -- directory data: from the most recent run (always collected)
      MAX(dir.version) as version,
      MAX(dir.invite_code_required) as invite_code_required,
      MAX(dir.is_online) as is_online,
      MAX(dir.error_at) as error_at,

      -- user data: latest run that has it
      MAX(usr.user_count_total) as user_count_total,
      MAX(usr.user_count_active) as user_count_active,
      MAX(usr.did) as did,
      MAX(usr.available_domains) as available_domains,
      MAX(usr.contact) as contact,
      MAX(usr.links) as links,

      -- geo data: latest run that has it
      MAX(geo.ip_address) as ip_address,
      MAX(geo.country) as country,
      MAX(geo.country_code) as country_code,
      MAX(geo.region) as region,
      MAX(geo.city) as city,
      MAX(geo.latitude) as latitude,
      MAX(geo.longitude) as longitude,
      MAX(geo.isp) as isp,
      MAX(geo.org) as org,
      MAX(geo.as_number) as as_number,
      MAX(geo.hosting_provider) as hosting_provider,

      MAX(dir.run_id) as dir_run_id,
      MAX(usr.run_id) as usr_run_id,
      MAX(geo.run_id) as geo_run_id

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
    GROUP BY RTRIM(p.url, '/')
  `);
  db.exec(`
    CREATE TEMPORARY VIEW IF NOT EXISTS pds_community AS
    SELECT * FROM pds_latest WHERE ${BSKY_FILTER}
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

export function getOverviewStats(hideBsky = false): OverviewStats {
  const db = getDb();
  ensureMergedView();
  const view = hideBsky ? "pds_community" : "pds_latest";
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
      FROM ${view}`
    )
    .get() as OverviewStats;
}

export interface CountryCount {
  country: string;
  countryCode: string;
  count: number;
}

export function getCountryDistribution(hideBsky = false): CountryCount[] {
  const db = getDb();
  ensureMergedView();
  const view = hideBsky ? "pds_community" : "pds_latest";
  return db
    .prepare(
      `SELECT country, country_code as countryCode, COUNT(*) as count
       FROM ${view}
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

export function getReposByCountry(hideBsky = false): CountryRepoCount[] {
  const db = getDb();
  ensureMergedView();
  const view = hideBsky ? "pds_community" : "pds_latest";
  return db
    .prepare(
      `SELECT country, country_code as countryCode, SUM(user_count_total) as repoCount
       FROM ${view}
       WHERE country IS NOT NULL AND user_count_total IS NOT NULL
       GROUP BY country_code ORDER BY repoCount DESC`
    )
    .all() as CountryRepoCount[];
}

export interface VersionCount {
  version: string;
  count: number;
}

export function getVersionDistribution(hideBsky = false): VersionCount[] {
  const db = getDb();
  ensureMergedView();
  const view = hideBsky ? "pds_community" : "pds_latest";
  return db
    .prepare(
      `SELECT COALESCE(version, 'unknown') as version, COUNT(*) as count
       FROM ${view}
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

export function getHostingProviders(hideBsky = false): HostingProviderCount[] {
  const db = getDb();
  ensureMergedView();
  const view = hideBsky ? "pds_community" : "pds_latest";
  return db
    .prepare(
      `SELECT
        ${PROVIDER_NORMALIZE_SQL} as provider,
        COUNT(*) as count,
        CASE WHEN org LIKE '%Cloudflare%' OR org LIKE '%Fastly%' THEN 1 ELSE 0 END as isCdn
       FROM ${view}
       GROUP BY provider ORDER BY count DESC`
    )
    .all() as HostingProviderCount[];
}

export function getCloudflareBreakdown(hideBsky = false): {
  behindCdn: number;
  directHosting: number;
  unknown: number;
} {
  const db = getDb();
  ensureMergedView();
  const view = hideBsky ? "pds_community" : "pds_latest";
  return db
    .prepare(
      `SELECT
        SUM(CASE WHEN org LIKE '%Cloudflare%' OR org LIKE '%Fastly%' THEN 1 ELSE 0 END) as behindCdn,
        SUM(CASE WHEN org IS NOT NULL AND org != '' AND org NOT LIKE '%Cloudflare%' AND org NOT LIKE '%Fastly%' THEN 1 ELSE 0 END) as directHosting,
        SUM(CASE WHEN org IS NULL OR org = '' THEN 1 ELSE 0 END) as unknown
       FROM ${view}`
    )
    .get() as { behindCdn: number; directHosting: number; unknown: number };
}

export interface UserDistBucket {
  range: string;
  count: number;
  sortKey: number;
}

export function getUserDistribution(hideBsky = false): UserDistBucket[] {
  const db = getDb();
  ensureMergedView();
  const view = hideBsky ? "pds_community" : "pds_latest";
  const rows = db
    .prepare(
      `SELECT user_count_total as users
       FROM ${view}
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
  activeCount: number | null;
  version: string | null;
  country: string | null;
  org: string | null;
}

export function getTopPdsByUsers(limit = 10, hideBsky = false): TopPds[] {
  const db = getDb();
  ensureMergedView();
  const view = hideBsky ? "pds_community" : "pds_latest";
  return db
    .prepare(
      `SELECT
        CASE
          WHEN url LIKE '%host.bsky.network%' OR url LIKE '%bsky.social%'
          THEN 'https://bsky.social'
          ELSE url
        END as url,
        SUM(user_count_total) as repoCount,
        SUM(user_count_active) as activeCount,
        MAX(version) as version,
        MAX(country) as country,
        MAX(org) as org
       FROM ${view}
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

export interface ConcentrationStats {
  top1Pct: number;
  top5Pct: number;
  top10Pct: number;
  totalWithData: number;
}

export function getConcentrationStats(hideBsky = false): ConcentrationStats {
  const db = getDb();
  ensureMergedView();
  const view = hideBsky ? "pds_community" : "pds_latest";

  const rows = db
    .prepare(
      `SELECT
        CASE
          WHEN url LIKE '%host.bsky.network%' OR url LIKE '%bsky.social%'
          THEN 'https://bsky.social'
          ELSE url
        END as url,
        SUM(user_count_total) as repos
       FROM ${view}
       WHERE user_count_total IS NOT NULL
       GROUP BY 1
       ORDER BY repos DESC`
    )
    .all() as { url: string; repos: number }[];

  const total = rows.reduce((s, r) => s + r.repos, 0);
  if (total === 0) return { top1Pct: 0, top5Pct: 0, top10Pct: 0, totalWithData: 0 };

  let cum = 0;
  let top1Pct = 0, top5Pct = 0, top10Pct = 0;
  for (let i = 0; i < rows.length; i++) {
    cum += rows[i].repos;
    const pct = (cum / total) * 100;
    if (i === 0) top1Pct = pct;
    if (i === 4) top5Pct = pct;
    if (i === 9) top10Pct = pct;
  }
  // If there are fewer PDSes than the threshold, cumulative % is already 100
  if (rows.length < 5) top5Pct = 100;
  if (rows.length < 10) top10Pct = 100;

  return { top1Pct, top5Pct, top10Pct, totalWithData: rows.length };
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

export interface PdsProviderLocation {
  url: string;
  latitude: number;
  longitude: number;
  city: string | null;
  country: string | null;
  provider: string;
}

export function getPdsLocations(hideBsky = false): CityCluster[] {
  const db = getDb();
  ensureMergedView();
  const view = hideBsky ? "pds_community" : "pds_latest";
  return db
    .prepare(
      `SELECT
        AVG(latitude) as latitude,
        AVG(longitude) as longitude,
        city,
        country,
        COUNT(*) as pdsCount
       FROM ${view}
       WHERE latitude IS NOT NULL AND longitude IS NOT NULL
       GROUP BY city, country
       ORDER BY pdsCount DESC`
    )
    .all() as CityCluster[];
}

export function getPdsLocationsWithProvider(hideBsky = false): PdsProviderLocation[] {
  const db = getDb();
  ensureMergedView();
  const view = hideBsky ? "pds_community" : "pds_latest";
  return db
    .prepare(
      `SELECT
        url,
        latitude,
        longitude,
        city,
        country,
        ${PROVIDER_NORMALIZE_SQL} as provider
       FROM ${view}
       WHERE latitude IS NOT NULL AND longitude IS NOT NULL`
    )
    .all() as PdsProviderLocation[];
}

// ── Firehose / Federation queries ─────────────────────────────────────

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

  // Aggregate counts across all samples in the window
  let totalEvents = 0;
  let totalInteractions = 0;
  let resolvedInteractions = 0;
  let crossPds = 0;
  let samePds = 0;
  let totalDurationMs = 0;
  const byType: Record<string, { total: number; crossPds: number; samePds: number }> = {};
  const federation: Record<string, number> = {};
  const pairCounts = new Map<string, { from: string; to: string; count: number }>();

  for (const row of rows) {
    totalEvents += row.total_events as number;
    totalInteractions += row.total_interactions as number;
    resolvedInteractions += row.resolved_interactions as number;
    crossPds += row.cross_pds as number;
    samePds += row.same_pds as number;
    totalDurationMs += row.duration_ms as number;

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
      if (existing) {
        existing.count += pair.count;
      } else {
        pairCounts.set(key, { ...pair });
      }
    }
  }

  const topCrossPdsPairs = [...pairCounts.values()]
    .sort((a, b) => b.count - a.count)
    .slice(0, 10);

  const eventsPerSecond = totalDurationMs > 0
    ? Math.round((totalEvents / totalDurationMs) * 1000)
    : 0;

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

  const data: DashboardData = {
    runInfo: getLatestRunInfo(),
    stats: getOverviewStats(hideBsky),
    countries: getCountryDistribution(hideBsky),
    reposByCountry: getReposByCountry(hideBsky),
    versions: getVersionDistribution(hideBsky),
    providers: getHostingProviders(hideBsky),
    cdnBreakdown: getCloudflareBreakdown(hideBsky),
    userDist: getUserDistribution(hideBsky),
    topPds: getTopPdsByUsers(10, hideBsky),
    concentration: getConcentrationStats(hideBsky),
    firehose: getLatestFirehoseSample(),
    locations: getPdsLocations(hideBsky),
    providerLocations: getPdsLocationsWithProvider(hideBsky),
    githubStats: getLatestGithubStats(),
  };

  dashboardCache.set(hideBsky, { data, expires: Date.now() + CACHE_TTL_MS });
  return data;
}
