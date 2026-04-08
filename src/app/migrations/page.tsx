export const dynamic = "force-dynamic";

import Link from "next/link";
import {
  getCreationTimeseriesWeekly,
  getMigrationFlows,
  getMigrationWeeklyBreakdown,
  getEcosystemStats,
} from "@/lib/db/plc-queries";
import { StackedAreaChart, MigrationChartsSection } from "@/components/charts";

function StatCard({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="bg-gray-900 rounded-lg p-4 border border-gray-800">
      <p className="text-xs text-gray-500 uppercase tracking-wide">{label}</p>
      <p className="text-2xl font-bold text-white mt-1">{value}</p>
      {sub && <p className="text-xs text-gray-500 mt-1">{sub}</p>}
    </div>
  );
}

export default async function MigrationsPage({
  searchParams,
}: {
  searchParams: Promise<{ trump?: string; bsky?: string }>;
}) {
  const { trump, bsky } = await searchParams;
  const includeTrump = trump === "1";
  const hideBsky = bsky === "0";

  const creations = getCreationTimeseriesWeekly(includeTrump, hideBsky);
  const flows = getMigrationFlows();
  const weeklyMigrations = getMigrationWeeklyBreakdown();
  const stats = getEcosystemStats(hideBsky);

  const fmt = (n: number) => n.toLocaleString();
  const allPeriods = creations.map(r => r.period).filter((v, i, a) => a.indexOf(v) === i).sort();

  return (
    <main className="min-h-screen bg-gray-950 text-gray-100 p-8">
      <div className="max-w-6xl mx-auto space-y-12">

        {/* Header */}
        <div>
          <h1 className="text-3xl font-bold text-white">PDS Migrations</h1>
          <p className="text-gray-400 mt-2">
            DID creations and migrations across the AT Protocol ecosystem.
          </p>
        </div>

        {/* Summary stats */}
        <section>
          <h2 className="text-xl font-semibold text-gray-200 mb-4">Ecosystem Summary</h2>
          <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
            <StatCard
              label="Total DIDs (approx.)"
              value={fmt(stats.total_dids_ex_trump)}
              sub="Counted from PLC directory, non-spam PDSes only"
            />
            <StatCard
              label="Total Migrations"
              value={fmt(stats.total_migrations)}
              sub="PDS-to-PDS transfers recorded, not internal bsky.network migrations"
            />
            <StatCard
              label="Independent PDSes"
              value={fmt(stats.independent_pds_count)}
              sub="Excluding bsky.network shards"
            />
          </div>
        </section>

        

        {/* Migration Flows Sankey + weekly bar */}
        <section>
          <h2 className="text-xl font-semibold text-gray-200 mb-1">
            Migration Flows
          </h2>
          <p className="text-xs text-gray-500 mb-4">
            All-time account migrations between PDSes. Excludes bsky.network destinations.
            Top 10 sources and destinations shown. Hover nodes and links for details.
            Click a destination node to highlight its weekly trend below.
          </p>
          <MigrationChartsSection sankeyData={flows} weeklyData={weeklyMigrations} />
        </section>

        {/* Account Creations */}
        <section>
          <div className="flex items-center justify-between mb-1">
            <h2 className="text-xl font-semibold text-gray-200">
              DID Creations by PDS
            </h2>
            <div className="flex gap-2">
              <Link
                href={`/migrations?trump=${includeTrump ? "0" : "1"}&bsky=${hideBsky ? "0" : "1"}`}
                className="text-xs px-3 py-1 rounded border border-gray-700 text-gray-400 hover:text-gray-200 hover:border-gray-500 transition-colors"
              >
                {includeTrump ? "Hide pds.trump.com" : "Show pds.trump.com"}
              </Link>
              <Link
                href={`/migrations?trump=${includeTrump ? "1" : "0"}&bsky=${hideBsky ? "1" : "0"}`}
                className="text-xs px-3 py-1 rounded border border-gray-700 text-gray-400 hover:text-gray-200 hover:border-gray-500 transition-colors"
              >
                {hideBsky ? "Show bsky.network" : "Hide bsky.network"}
              </Link>
            </div>
          </div>
          <p className="text-xs text-gray-500 mb-4">
            Normalized to 100% — hover for actual counts. Top 10 PDSes by total volume.
            {hideBsky && <span className="text-blue-500 ml-2">bsky.network hidden.</span>}
            {includeTrump && <span className="text-yellow-600 ml-2">pds.trump.com included (~20.6M DIDs).</span>}
          </p>
          {creations.length === 0 ? (
            <p className="text-gray-500">
              No data yet — run{" "}
              <code className="text-gray-300">npm run collect:plc</code> then{" "}
              <code className="text-gray-300">npm run aggregate:plc</code>.
            </p>
          ) : (
            <StackedAreaChart data={creations} allPeriods={allPeriods} />
          )}
        </section>

      </div>
    </main>
  );
}
