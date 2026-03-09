import {
  getLatestRun,
  getOverviewStats,
  getCountryDistribution,
  getVersionDistribution,
  getHostingProviders,
  getUserDistribution,
  getTopPdsByUsers,
} from "@/lib/db/queries";
import { SimpleBarChart, DonutChart } from "@/components/charts";

export const dynamic = "force-dynamic";

export default function Home() {
  const run = getLatestRun();

  if (!run) {
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

  const stats = getOverviewStats(run.id);
  const countries = getCountryDistribution(run.id);
  const versions = getVersionDistribution(run.id);
  const providers = getHostingProviders(run.id);
  const userDist = getUserDistribution(run.id);
  const topPds = getTopPdsByUsers(run.id);

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
        <p className="text-sm text-gray-500">
          Last collected:{" "}
          {new Date(run.completedAt + "Z").toLocaleString()}
        </p>
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
        <ChartCard title="Hosting Providers" subtitle="Normalized">
          <DonutChart
            data={providers.slice(0, 12).map((p) => ({
              name: p.provider,
              value: p.count,
            }))}
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

function StatCard({
  label,
  value,
  accent,
}: {
  label: string;
  value: number;
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
        {value.toLocaleString()}
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
