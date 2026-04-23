export const dynamic = "force-dynamic";

import {
  getMigrationFlows,
  getMigrationWeeklyBreakdown,
  getMigrationTrajectories,
  getEcosystemStats,
  getPlcDataTimestamp,
  getScannedPdsCount,
} from "@/lib/db/plc-queries";
import { getOverviewStats } from "@/lib/db/queries";
import { MigrationChartsSection, MultiStepSankeyChart } from "@/components/charts";

function StatCard({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="bg-gray-900 rounded-lg p-4 border border-gray-800">
      <p className="text-xs text-gray-500 uppercase tracking-wide">{label}</p>
      <p className="text-2xl font-bold text-white mt-1">{value}</p>
      {sub && <p className="text-xs text-gray-500 mt-1">{sub}</p>}
    </div>
  );
}

export default async function MigrationsPage() {
  const flows = getMigrationFlows();
  const weeklyMigrations = getMigrationWeeklyBreakdown();
  const trajectories = getMigrationTrajectories();
  const stats = getEcosystemStats();
  const scanStats = getOverviewStats();
  const scannedPdsCount = getScannedPdsCount();
  const timestamp = getPlcDataTimestamp();

  const fmt = (n: number) => n.toLocaleString();

  return (
    <main className="min-h-screen bg-gray-950 text-gray-100 p-8">
      <div className="max-w-6xl mx-auto space-y-12">

        {/* Header */}
        <div>
          <h1 className="text-3xl font-bold text-white">PDS Migrations</h1>
          <p className="text-gray-400 mt-2">
            did:plc account creations and migrations recorded in plc.directory
          </p>
          {timestamp && (
            <p className="text-xs text-gray-600 mt-1">
              PLC data collected through{" "}
              {new Date(timestamp.collected_through).toLocaleString("en-US", {
                month: "short", day: "numeric", year: "numeric",
                hour: "numeric", minute: "2-digit", timeZoneName: "short",
              })}
            </p>
          )}
        </div>

        {/* Summary stats */}
        <section>
          <h2 className="text-xl font-semibold text-gray-200 mb-4">Ecosystem Summary</h2>
          {/* Row 1: the headline — concentration and scale */}
          <div className="grid grid-cols-2 gap-4 mb-4">
            <StatCard
              label="On bsky.network"
              value={`${stats.bsky_concentration_pct}%`}
              sub="of accounts are on Bluesky-operated infrastructure"
            />
            <StatCard
              label="Total repos"
              value={fmt(scanStats.totalUsers)}
              sub={`Across ${scanStats.total.toLocaleString()} scanned PDSes · from listRepos`}
            />
          </div>
          {/* Row 2: migration details */}
          <div className="grid grid-cols-3 gap-4">
            <StatCard
              label="Accounts that migrated"
              value={fmt(stats.unique_migrating_dids)}
              sub="Unique DIDs with at least one voluntary migration"
            />
            <StatCard
              label="Total migration events"
              value={fmt(stats.total_migrations)}
              sub="PDS-to-PDS transfers, excluding internal bsky.network resharding"
            />
            <StatCard
              label="Scanned PDSes"
              value={fmt(scanStats.total)}
              sub="PDSes that responded to the repo scanner"
            />
          </div>
        </section>


        {/* Migration Flows Sankey + weekly bar */}
        <section>
          <h2 className="text-xl font-semibold text-gray-200 mb-1">
            Where do users end up?
          </h2>
          <p className="text-xs text-gray-500 mb-4">
            Where accounts ultimately landed — collapsed origin → current PDS. Excludes bsky.network destinations.
            Hover for details. Click any node to highlight its trajectories forward and backward.
          </p>
          <MigrationChartsSection sankeyData={flows} weeklyData={weeklyMigrations} />
        </section>

        {/* Migration Trajectories */}
        <section>
          <h2 className="text-xl font-semibold text-gray-200 mb-1">
            Migration Flows
          </h2>
          <p className="text-xs text-gray-500 mb-4">
            Actual per-hop migration steps — each column is one PDS hop. bsky.network shards collapsed.
            Click a node to highlight all paths through it in both directions.
          </p>
          <MultiStepSankeyChart data={trajectories} height={500} />
        </section>

      </div>
    </main>
  );
}
