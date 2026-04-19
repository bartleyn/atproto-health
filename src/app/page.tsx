import { getDashboardData, getLatestFirehoseSample, type ConcentrationStats } from "@/lib/db/queries";
import { SimpleBarChart, DonutChart, InfraSection } from "@/components/charts";
import type { GithubTopicStats } from "@/lib/db/queries";
import { getPdsLangSummary, getTopLangs, getLastScanTime } from "@/lib/db/plc-queries";
import type { PdsLangLocation } from "@/components/world-map";

export const dynamic = "force-dynamic";

export default async function Home({
  searchParams,
}: {
  searchParams: Promise<{ hideBsky?: string }>;
}) {
  const params = await searchParams;
  const hideBsky = params.hideBsky === "1";

  const {
    runInfo,
    stats,
    countries,
    reposByCountry,
    versions,
    providers,
    cdnBreakdown,
    userDist,
    topPds,
    concentration,
    firehose,
    locations,
    providerLocations,
    githubStats,
  } = getDashboardData(hideBsky);

  // Join pds_lang_summary with providerLocations geo data by URL.
  // Normalize URLs by stripping trailing slashes — main DB stores them WITH trailing slash
  // (e.g. "https://blacksky.app/") while plc_did_pds stores them WITHOUT.
  // pds_lang_summary is populated by aggregate:plc; returns [] if not yet run.
  const normalizeUrl = (u: string) => u.replace(/\/+$/, "");
  const isBskyUrl = (u: string) => /bsky\.network|bsky\.social/.test(u);
  const geoByUrl = new Map(providerLocations.map(p => [normalizeUrl(p.url), { city: p.city, country: p.country }]));
  // bsky.network providerLocations — used to geo-locate the virtual 'bsky.network' lang row
  const bskyProviderLocs = providerLocations.filter(p => isBskyUrl(p.url));

  const lastScanTime = getLastScanTime();
  const langRows = getPdsLangSummary();
  const langLocations: PdsLangLocation[] = langRows.flatMap(row => {
    if (row.pds_url === "bsky.network") {
      // Distribute bsky.network across all bsky cluster locations for map highlighting.
      // dids=1 per server so the cluster key accumulates per-city PDS count (same as provider mode).
      return bskyProviderLocs
        .filter(p => p.city !== null)
        .map(p => ({ url: p.url, city: p.city, country: p.country, lang: row.lang, dids: 1 }));
    }
    const geo = geoByUrl.get(normalizeUrl(row.pds_url));
    if (!geo) return [];
    return [{ url: row.pds_url, city: geo.city, country: geo.country, lang: row.lang, dids: row.dids }];
  });
  const topLangs = getTopLangs(25);

  if (!runInfo.dirRun) {
    return (
      <main className="max-w-6xl mx-auto px-6 py-12">
        <h1 className="text-3xl font-bold mb-2">ATProto Health</h1>
        <p className="text-gray-400 mb-10">
          AT Protocol ecosystem health dashboard
        </p>
        <div className="rounded-lg border border-gray-800 p-8 text-center text-gray-500">
          <p>No data collected yet.</p>
          <p className="mt-2 text-sm">
            Run{" "}
            <code className="bg-gray-800 px-2 py-0.5 rounded text-gray-300">
              npm run collect
            </code>{" "}
            to fetch PDS data.
          </p>
        </div>
      </main>
    );
  }

  const plotUserDist = true;

  return (
    <main className="max-w-6xl mx-auto px-6 py-12">
      <div className="flex items-baseline justify-between mb-10">
        <div>
          <h1 className="text-3xl font-bold">ATProto Health</h1>
          <p className="text-gray-400 mt-1">
            AT Protocol PDS ecosystem — distribution, infrastructure, and activity
          </p>
          <div className="mt-3">
            <a
              href={hideBsky ? "/" : "/?hideBsky=1"}
              className={`inline-flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-full border transition-colors ${
                hideBsky
                  ? "bg-blue-900/40 border-blue-700 text-blue-300 hover:bg-blue-900/60"
                  : "bg-gray-800 border-gray-700 text-gray-400 hover:bg-gray-700"
              }`}
            >
              <span className={`w-1.5 h-1.5 rounded-full ${hideBsky ? "bg-blue-400" : "bg-gray-600"}`} />
              {hideBsky ? "Hiding Bluesky PBC infrastructure" : "Showing all PDSes"}
            </a>
          </div>
        </div>
        <div className="text-right text-xs text-gray-500 space-y-0.5">
          <div className="flex justify-end gap-3 mb-2">
            <a
              href="https://github.com/bartleyn/atproto-health"
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-1.5 text-gray-500 hover:text-gray-300 transition-colors"
              aria-label="View source on GitHub"
            >
              <svg viewBox="0 0 16 16" className="w-4 h-4 fill-current" aria-hidden="true">
                <path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z" />
              </svg>
              <span className="text-xs">bartleyn/atproto-health</span>
            </a>
          </div>
          {runInfo.dirRun && (
            <p>
              Directory:{" "}
              {new Date(runInfo.dirRun.completedAt + "Z").toLocaleString("en-US", { timeZone: "America/Los_Angeles", timeZoneName: "short" })}
            </p>
          )}
          {runInfo.geoRun && (
            <p>
              Geo:{" "}
              {new Date(runInfo.geoRun.completedAt + "Z").toLocaleString("en-US", { timeZone: "America/Los_Angeles", timeZoneName: "short" })}
            </p>
          )}
          {runInfo.usrRun && (
            <p>
              Users:{" "}
              {new Date(runInfo.usrRun.completedAt + "Z").toLocaleString("en-US", { timeZone: "America/Los_Angeles", timeZoneName: "short" })}
            </p>
          )}
          {lastScanTime && (
            <p>
              Scan:{" "}
              {new Date(lastScanTime + "Z").toLocaleString("en-US", { timeZone: "America/Los_Angeles", timeZoneName: "short" })}
            </p>
          )}
        </div>
      </div>

      {/* Overview stats */}
      <p className="text-xs text-gray-500 mb-3">
        PDS directory sourced from{" "}
        <a href="https://github.com/mary-ext/atproto-scraping" target="_blank" rel="noopener noreferrer" className="underline hover:text-gray-300">
          mary-ext/atproto-scraping
        </a>
        . Online/offline reflects that scraper&apos;s last health check. Open Reg indicates no invite code required to register.
      </p>
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3 mb-4">
        <StatCard label="Total PDSes" value={stats.total} />
        <StatCard label="Online" value={stats.online} accent="green" />
        <StatCard label="Offline" value={stats.offline} accent="red" />
        <StatCard label="Open Reg" value={stats.openReg} accent="blue" />
        <StatCard label="Invite Only" value={stats.inviteOnly} />
        <StatCard label="Countries" value={stats.countries} accent="purple" />
      </div>

      {/* Concentration summary */}
      {concentration.totalWithData > 0 && (
        <ConcentrationSection concentration={concentration} totalRepos={stats.totalUsers} activeRepos={stats.activeUsers} />
      )}

      {/* Infrastructure map + provider donut (linked) */}
      {locations.length > 0 && (
        <div className="mb-12">
          <InfraSection
            providers={providers}
            cdnBreakdown={cdnBreakdown}
            locations={locations}
            providerLocations={providerLocations}
            langLocations={langLocations}
            topLangs={topLangs}
          />
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mb-12">
        {/* Geographic distribution */}
        <ChartCard title="PDS by Country" subtitle={`${countries.length} countries`}>
          <SimpleBarChart
            data={countries.slice(0, 15).map((c) => ({
              name: c.countryCode,
              value: c.count,
            }))}
            color="#3b82f6"
          />
        </ChartCard>

        {/* Version distribution */}
        <ChartCard
          title="PDS Versions"
          subtitle={`${versions.length} distinct versions`}
        >
          <SimpleBarChart
            data={versions.slice(0, 12).map((v) => ({
              name: v.version.length > 12 ? v.version.slice(0, 12) + "..." : v.version,
              value: v.count,
            }))}
            color="#8b5cf6"
          />
        </ChartCard>

        {/* User distribution */}
        {plotUserDist ? (
          <ChartCard
            title="Users per PDS"
            subtitle="Distribution of total accounts"
          >
            <SimpleBarChart
              data={userDist
                .sort((a, b) => a.sortKey - b.sortKey)
                .map((b) => ({ name: b.range, value: b.count }))}
              color="#06b6d4"
              layout="horizontal"
              xLabel="Total accounts"
              yLabel="PDSes"
            />
          </ChartCard>
        ) : (
          <ChartCard
            title="Users per PDS"
            subtitle="Run npm run collect:users to populate"
          >
            <div className="flex items-center justify-center h-64 text-gray-600 text-sm">
              No user data yet
            </div>
          </ChartCard>
        )}

        {/* Repos by country */}
        <ChartCard title="Repos by Country" subtitle="Share of total repos in each PDS geolocated to that country · may undercount total repos · log scale">
          <SimpleBarChart
            data={reposByCountry.slice(0, 15).map((c) => ({
              name: c.countryCode,
              value: c.repoCount,
            }))}
            color="#06b6d4"
            logScale
          />
        </ChartCard>
      </div>

      {/* Top PDSes by users */}
      {topPds.length > 0 && (
        <div className="mb-12">
          <h2 className="text-lg font-semibold mb-1">Largest PDSes</h2>
          <p className="text-sm text-gray-500 mb-4">
            Ranked by total repos · Bluesky shards aggregated by *.host.bsky.network pattern
          </p>
          <div className="overflow-x-auto rounded-lg border border-gray-800">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-800 text-gray-400 text-left">
                  <th className="px-4 py-3 font-medium">#</th>
                  <th className="px-4 py-3 font-medium">PDS</th>
                  <th className="px-4 py-3 font-medium text-right">Total</th>
                  <th className="px-4 py-3 font-medium">Country</th>
                </tr>
              </thead>
              <tbody>
                {topPds.map((pds, i) => {
                  return (
                    <tr
                      key={pds.url}
                      className="border-b border-gray-800/50 hover:bg-gray-900/50"
                    >
                      <td className="px-4 py-2.5 text-gray-500 tabular-nums">
                        {i + 1}
                      </td>
                      <td className="px-4 py-2.5 font-mono text-xs">
                        {pds.url.replace(/^https?:\/\//, "").replace(/\/$/, "")}
                      </td>
                      <td className="px-4 py-2.5 text-right tabular-nums">
                        {pds.repoCount.toLocaleString()}
                      </td>
                      <td className="px-4 py-2.5 text-gray-400">
                        {pds.country ?? "—"}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Federation health */}
      {firehose && <FederationSection sample={firehose} />}

      {/* GitHub ecosystem stats */}
      {githubStats.length > 0 && (
        <>
          <h2 className="text-xl font-semibold mb-6 text-gray-400">Extra</h2>
          <GithubSection stats={githubStats} />
        </>
      )}

      {/* Country breakdown table */}
      <div className="mb-12">
        <h2 className="text-lg font-semibold mb-1">All Countries</h2>
        <p className="text-sm text-gray-500 mb-4">
          PDS instances by country
        </p>
        <div className="overflow-x-auto rounded-lg border border-gray-800">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-800 text-gray-400 text-left">
                <th className="px-4 py-3 font-medium">Country</th>
                <th className="px-4 py-3 font-medium text-right">PDSes</th>
                <th className="px-4 py-3 font-medium text-right">% of Total</th>
              </tr>
            </thead>
            <tbody>
              {countries.map((c) => (
                <tr
                  key={c.countryCode}
                  className="border-b border-gray-800/50 hover:bg-gray-900/50"
                >
                  <td className="px-4 py-2">
                    {c.country}{" "}
                    <span className="text-gray-500">{c.countryCode}</span>
                  </td>
                  <td className="px-4 py-2 text-right tabular-nums">
                    {c.count}
                  </td>
                  <td className="px-4 py-2 text-right tabular-nums text-gray-400">
                    {((c.count / stats.total) * 100).toFixed(1)}%
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </main>
  );
}

function ConcentrationSection({
  concentration,
  totalRepos,
  activeRepos,
}: {
  concentration: ConcentrationStats;
  totalRepos: number;
  activeRepos: number;
}) {
  const activeRate = totalRepos > 0 ? Math.round((activeRepos / totalRepos) * 100) : 0;
  return (
    <div className="mb-12">
      <p className="text-xs text-gray-500 mb-3">
        Repo counts from <code className="bg-gray-800 px-1 rounded">listRepos</code> pagination across {concentration.totalWithData.toLocaleString()} PDSes with data.
        Active = repos marked active by their PDS.
        Concentration = cumulative share of repos held by the top N PDSes (Bluesky shards counted as one).
      </p>
      <div className="grid grid-cols-2 gap-3">
        <StatCard label="Total Repos" value={totalRepos} />
        <StatCard label="Active Rate" value={activeRate} suffix="%" accent="cyan" />
      </div>
    </div>
  );
}

function FederationSection({ sample }: { sample: NonNullable<ReturnType<typeof getLatestFirehoseSample>> }) {
  const fed = sample.federation;
  const trueFed = (fed["bsky-to-third"] ?? 0) + (fed["third-to-bsky"] ?? 0) + (fed["third-to-third"] ?? 0);
  const trueFedRate = sample.resolvedInteractions > 0
    ? ((trueFed / sample.resolvedInteractions) * 100).toFixed(1)
    : "0";
  const rawCrossRate = sample.resolvedInteractions > 0
    ? ((sample.crossPds / sample.resolvedInteractions) * 100).toFixed(1)
    : "0";

  const fedData = [
    { name: "Bluesky internal", value: fed["bsky-internal"] ?? 0 },
    { name: "Bluesky → Independent", value: fed["bsky-to-third"] ?? 0 },
    { name: "Independent → Bluesky", value: fed["third-to-bsky"] ?? 0 },
    { name: "Independent → Independent", value: fed["third-to-third"] ?? 0 },
    { name: "Same PDS", value: fed["same-pds"] ?? 0 },
  ];

  const byTypeTotalData = Object.entries(sample.byType).map(([type, stats]) => ({
    name: type,
    value: stats.total,
  }));


  return (
    <div className="mb-12">
      <h2 className="text-lg font-semibold mb-1">Cross-PDS Interactions</h2>
      <p className="text-sm text-gray-500 mb-4">
        Measures how often interactions (likes, replies, reposts, follows) cross PDS boundaries.
        Aggregated from {sample.sampleCount} firehose sample{sample.sampleCount !== 1 ? "s" : ""} over the last {sample.windowDays} days
        ({sample.totalEvents.toLocaleString()} total events · {sample.eventsPerSecond} evt/s avg · {Math.round(sample.durationMs / 1000 / 60)} min total sampled)
        {" \u00b7 "}
        last sampled {new Date(sample.sampledAt + "Z").toLocaleString("en-US", { timeZone: "America/Los_Angeles", timeZoneName: "short" })}
      </p>
      <p className="text-xs text-gray-600 mb-4">
        Includes Fediverse bridge traffic (e.g. Bridgy Fed) as independent traffic.
      </p>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-6">
        <StatCard label="Cross-PDS Rate (excl. Bluesky internal)" value={Number(trueFedRate)} suffix="%" accent="cyan" />
        <StatCard label="Cross-PDS Rate (all)" value={Number(rawCrossRate)} suffix="%" />
        <StatCard label="Interactions Sampled" value={sample.resolvedInteractions} />
        <StatCard label="Cross-PDS (independent PDS involved)" value={trueFed} accent="green" />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        <ChartCard
          title="Interaction Flow"
          subtitle="Where interactions cross boundaries"
        >
          <DonutChart data={fedData} />
        </ChartCard>

        <ChartCard
          title="Interaction Distribution by Type"
          subtitle="Total resolved interactions by type"
        >
          <SimpleBarChart data={byTypeTotalData} color="#6366f1" layout="horizontal" />
        </ChartCard>

      </div>
    </div>
  );
}

function StatCard({
  label,
  value,
  suffix,
  accent,
}: {
  label: string;
  value: number;
  suffix?: string;
  accent?: "green" | "red" | "blue" | "purple" | "cyan";
}) {
  const accentMap: Record<string, string> = {
    green: "text-green-400",
    red: "text-red-400",
    blue: "text-blue-400",
    purple: "text-purple-400",
    cyan: "text-cyan-400",
  };
  const accentColor = accent ? accentMap[accent] ?? "text-gray-100" : "text-gray-100";

  return (
    <div className="rounded-lg border border-gray-800 bg-gray-900 p-4">
      <div className={`text-2xl font-semibold tabular-nums ${accentColor}`}>
        {value == null || isNaN(value) ? "—" : value.toLocaleString()}{suffix && <span className="text-lg">{suffix}</span>}
      </div>
      <div className="text-xs text-gray-400 mt-1">{label}</div>
    </div>
  );
}

function GithubSection({ stats }: { stats: GithubTopicStats[] }) {
  const collectedAt = stats[0]?.collectedAt;

  // Deduplicate top repos across all queries by fullName, keep highest star count
  const repoMap = new Map<string, GithubTopicStats["topRepos"][number]>();
  for (const s of stats) {
    for (const repo of s.topRepos) {
      const existing = repoMap.get(repo.fullName);
      if (!existing || repo.stars > existing.stars) {
        repoMap.set(repo.fullName, repo);
      }
    }
  }
  const topRepos = [...repoMap.values()].sort((a, b) => b.stars - a.stars).slice(0, 15);

  return (
    <div className="mb-12">
      <div className="flex items-baseline justify-between mb-1">
        <h2 className="text-lg font-semibold">GitHub Ecosystem</h2>
        {collectedAt && (
          <span className="text-xs text-gray-500">
            {new Date(collectedAt + "Z").toLocaleString("en-US", { timeZone: "America/Los_Angeles", timeZoneName: "short" })}
          </span>
        )}
      </div>
      <p className="text-sm text-gray-500 mb-4">
        Public repositories tagged with ATProto-related topics
      </p>

      <div className="grid grid-cols-3 gap-3 mb-6">
        {stats.map((s) => (
          <div key={s.query} className="rounded-lg border border-gray-800 bg-gray-900 p-4">
            <div className="text-2xl font-semibold tabular-nums text-gray-100">
              {s.repoCount.toLocaleString()}
            </div>
            <div className="text-xs text-gray-400 mt-1 font-mono">{s.query}</div>
          </div>
        ))}
      </div>

      <div className="overflow-x-auto rounded-lg border border-gray-800">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-800 text-gray-400 text-left">
              <th className="px-4 py-3 font-medium">#</th>
              <th className="px-4 py-3 font-medium">Repository</th>
              <th className="px-4 py-3 font-medium">Description</th>
              <th className="px-4 py-3 font-medium text-right">Stars</th>
            </tr>
          </thead>
          <tbody>
            {topRepos.map((repo, i) => (
              <tr key={repo.fullName} className="border-b border-gray-800/50 hover:bg-gray-900/50">
                <td className="px-4 py-2.5 text-gray-500 tabular-nums">{i + 1}</td>
                <td className="px-4 py-2.5 font-mono text-xs">
                  <a
                    href={repo.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-blue-400 hover:text-blue-300"
                  >
                    {repo.fullName}
                  </a>
                </td>
                <td className="px-4 py-2.5 text-gray-400 text-xs max-w-sm truncate">
                  {repo.description ?? "—"}
                </td>
                <td className="px-4 py-2.5 text-right tabular-nums">
                  ★ {repo.stars.toLocaleString()}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function ChartCard({
  title,
  subtitle,
  children,
}: {
  title: string;
  subtitle?: string;
  children: React.ReactNode;
}) {
  return (
    <div className="rounded-lg border border-gray-800 bg-gray-900 p-5">
      <h2 className="text-base font-semibold mb-0.5">{title}</h2>
      {subtitle && (
        <p className="text-xs text-gray-500 mb-4">{subtitle}</p>
      )}
      {children}
    </div>
  );
}
