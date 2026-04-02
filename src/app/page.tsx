import { getDashboardData } from "@/lib/db/queries";
import { SimpleBarChart, DonutChart } from "@/components/charts";
import { WorldMap } from "@/components/world-map";
import type { GithubTopicStats } from "@/lib/db/queries";

export const dynamic = "force-dynamic";

export default function Home() {
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
    firehose,
    locations,
    githubStats,
  } = getDashboardData();

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
        </div>
        <div className="text-right text-xs text-gray-500 space-y-0.5">
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
      <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-3 mb-12">
        <StatCard label="Total PDSes" value={stats.total} />
        <StatCard label="Online" value={stats.online} accent="green" />
        <StatCard label="Offline" value={stats.offline} accent="red" />
        <StatCard label="Open Reg" value={stats.openReg} accent="blue" />
        <StatCard label="Invite Only" value={stats.inviteOnly} />
        <StatCard label="Countries" value={stats.countries} accent="purple" />
      </div>

      {/* Geographic map */}
      {locations.length > 0 && (
        <div className="rounded-lg border border-gray-800 bg-gray-900 p-5 mb-12">
          <h2 className="text-base font-semibold mb-0.5">PDS Geographic Distribution</h2>
          <p className="text-xs text-gray-500 mb-3">
            {locations.length.toLocaleString()} cities · dot size scales with PDS count
          </p>
          <WorldMap locations={locations} />
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

        {/* Hosting providers */}
        <ChartCard
          title="Infrastructure Providers"
          subtitle={`Derived from IP org via WHOIS/ASN \u00b7 ${cdnBreakdown.behindCdn} behind CDN \u00b7 ${cdnBreakdown.directHosting} direct \u00b7 ${cdnBreakdown.unknown} unknown`}
        >
          <DonutChart
            data={providers
              .filter((p) => !p.isCdn)
              .slice(0, 11)
              .map((p) => ({
                name: p.provider,
                value: p.count,
              }))}
          />
          <p className="text-xs text-gray-500 mt-2">
            {cdnBreakdown.behindCdn} PDSes behind Cloudflare/CDN (origin
            host unknown) are excluded above.
          </p>
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
        <ChartCard title="Repos by Country" subtitle="Share of total repos in each PDS geolocated to that country · may undercount total repos">
          <SimpleBarChart
            data={reposByCountry.slice(0, 15).map((c) => ({
              name: c.countryCode,
              value: c.repoCount,
            }))}
            color="#06b6d4"
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
                  <th className="px-4 py-3 font-medium">Country</th>
                </tr>
              </thead>
              <tbody>
                {topPds.map((pds, i) => (
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
                    <td className="px-4 py-2.5 text-gray-400">
                      {pds.country ?? "—"}
                    </td>
                  </tr>
                ))}
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
    { name: "Bluesky → 3rd party", value: fed["bsky-to-third"] ?? 0 },
    { name: "3rd party → Bluesky", value: fed["third-to-bsky"] ?? 0 },
    { name: "3rd party → 3rd party", value: fed["third-to-third"] ?? 0 },
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
        Sampled from the AT Protocol firehose over a {(sample.durationMs / 1000).toFixed(0)}s window
        ({sample.totalEvents.toLocaleString()} total firehose events incl. posts · {sample.eventsPerSecond} evt/s)
        {" \u00b7 "}
        {new Date(sample.sampledAt + "Z").toLocaleString("en-US", { timeZone: "America/Los_Angeles", timeZoneName: "short" })}
      </p>
      <p className="text-xs text-gray-600 mb-4">
        Includes Fediverse bridge traffic (e.g. Bridgy Fed) as third-party.
      </p>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-6">
        <StatCard label="Cross-PDS Rate (excl. Bluesky internal)" value={Number(trueFedRate)} suffix="%" accent="cyan" />
        <StatCard label="Cross-PDS Rate (all)" value={Number(rawCrossRate)} suffix="%" />
        <StatCard label="Interactions Sampled" value={sample.resolvedInteractions} />
        <StatCard label="Cross-PDS (3rd-party involved)" value={trueFed} accent="green" />
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
