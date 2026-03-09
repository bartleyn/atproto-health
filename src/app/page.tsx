import {
  getLatestRunInfo,
  getOverviewStats,
  getCountryDistribution,
  getVersionDistribution,
  getHostingProviders,
  getCloudflareBreakdown,
  getUserDistribution,
  getTopPdsByUsers,
  getLatestFirehoseSample,
  getPdsLocations,
} from "@/lib/db/queries";
import { SimpleBarChart, DonutChart } from "@/components/charts";
import { WorldMap } from "@/components/world-map";

export const dynamic = "force-dynamic";

export default function Home() {
  const runInfo = getLatestRunInfo();

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

  const stats = getOverviewStats();
  const countries = getCountryDistribution();
  const versions = getVersionDistribution();
  const providers = getHostingProviders();
  const cdnBreakdown = getCloudflareBreakdown();
  const userDist = getUserDistribution();
  const topPds = getTopPdsByUsers();
  const firehose = getLatestFirehoseSample();
  const locations = getPdsLocations();

  const hasUserData = stats.activeUsers > 0;

  return (
    <main className="max-w-6xl mx-auto px-6 py-12">
      <div className="flex items-baseline justify-between mb-10">
        <div>
          <h1 className="text-3xl font-bold">ATProto Health</h1>
          <p className="text-gray-400 mt-1">
            AT Protocol ecosystem health dashboard
          </p>
        </div>
        <div className="text-right text-xs text-gray-500 space-y-0.5">
          {runInfo.dirRun && (
            <p>
              Directory:{" "}
              {new Date(runInfo.dirRun.completedAt + "Z").toLocaleString()}
            </p>
          )}
          {runInfo.geoRun && (
            <p>
              Geo:{" "}
              {new Date(runInfo.geoRun.completedAt + "Z").toLocaleString()}
            </p>
          )}
          {runInfo.usrRun && (
            <p>
              Users:{" "}
              {new Date(runInfo.usrRun.completedAt + "Z").toLocaleString()}
            </p>
          )}
        </div>
      </div>

      {/* Overview stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-3 mb-12">
        <StatCard label="Total PDSes" value={stats.total} />
        <StatCard label="Online" value={stats.online} accent="green" />
        <StatCard label="Offline" value={stats.offline} accent="red" />
        <StatCard label="Open Reg" value={stats.openReg} accent="blue" />
        <StatCard label="Invite Only" value={stats.inviteOnly} />
        <StatCard label="Countries" value={stats.countries} accent="purple" />
        {hasUserData && (
          <StatCard
            label="Active Users"
            value={stats.activeUsers}
            accent="cyan"
          />
        )}
      </div>

      {/* Geographic map */}
      {locations.length > 0 && (
        <div className="rounded-lg border border-gray-800 bg-gray-900 p-5 mb-12">
          <h2 className="text-base font-semibold mb-0.5">PDS Geographic Distribution</h2>
          <p className="text-xs text-gray-500 mb-3">
            {locations.length.toLocaleString()} located instances · dot size scales with user count
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
          subtitle={`${cdnBreakdown.behindCdn} behind CDN \u00b7 ${cdnBreakdown.directHosting} direct \u00b7 ${cdnBreakdown.unknown} unknown`}
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
        {hasUserData ? (
          <ChartCard
            title="Users per PDS"
            subtitle="Distribution of active users"
          >
            <SimpleBarChart
              data={userDist
                .sort((a, b) => a.sortKey - b.sortKey)
                .map((b) => ({ name: b.range, value: b.count }))}
              color="#06b6d4"
              layout="horizontal"
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
      </div>

      {/* Federation health */}
      {firehose && <FederationSection sample={firehose} />}

      {/* Top PDSes by users */}
      {hasUserData && topPds.length > 0 && (
        <div className="mb-12">
          <h2 className="text-lg font-semibold mb-1">Largest PDSes by Users</h2>
          <p className="text-sm text-gray-500 mb-4">
            Third-party PDSes (excludes bsky.social)
          </p>
          <div className="overflow-x-auto rounded-lg border border-gray-800">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-800 text-gray-400 text-left">
                  <th className="px-4 py-3 font-medium">#</th>
                  <th className="px-4 py-3 font-medium">PDS</th>
                  <th className="px-4 py-3 font-medium text-right">
                    Active Users
                  </th>
                  <th className="px-4 py-3 font-medium">Version</th>
                  <th className="px-4 py-3 font-medium">Country</th>
                  <th className="px-4 py-3 font-medium">Host</th>
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
                    <td className="px-4 py-2.5 text-right tabular-nums">
                      {pds.userCountActive.toLocaleString()}
                    </td>
                    <td className="px-4 py-2.5 text-gray-400">
                      {pds.version ?? "—"}
                    </td>
                    <td className="px-4 py-2.5 text-gray-400">
                      {pds.country ?? "—"}
                    </td>
                    <td className="px-4 py-2.5 text-gray-400 text-xs">
                      {pds.org ?? "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
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

  const byTypeData = Object.entries(sample.byType).map(([type, stats]) => {
    const resolved = stats.crossPds + stats.samePds;
    return {
      name: type,
      value: resolved > 0 ? Math.round((stats.crossPds / resolved) * 100) : 0,
    };
  });

  return (
    <div className="mb-12">
      <h2 className="text-lg font-semibold mb-1">Federation Health</h2>
      <p className="text-sm text-gray-500 mb-4">
        Sampled {sample.totalEvents.toLocaleString()} firehose events over{" "}
        {(sample.durationMs / 1000).toFixed(0)}s ({sample.eventsPerSecond} evt/s)
        {" \u00b7 "}
        {new Date(sample.sampledAt + "Z").toLocaleString()}
      </p>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-6">
        <StatCard label="True Federation Rate" value={Number(trueFedRate)} suffix="%" accent="cyan" />
        <StatCard label="Raw Cross-PDS Rate" value={Number(rawCrossRate)} suffix="%" />
        <StatCard label="Interactions Sampled" value={sample.resolvedInteractions} />
        <StatCard label="True Federated" value={trueFed} accent="green" />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        <ChartCard
          title="Interaction Flow"
          subtitle="Where interactions cross boundaries"
        >
          <DonutChart data={fedData} />
        </ChartCard>

        <ChartCard
          title="Cross-PDS Rate by Type"
          subtitle="% of resolved interactions that cross PDS boundaries"
        >
          <SimpleBarChart data={byTypeData} color="#10b981" layout="horizontal" />
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
        {value.toLocaleString()}{suffix && <span className="text-lg">{suffix}</span>}
      </div>
      <div className="text-xs text-gray-400 mt-1">{label}</div>
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
