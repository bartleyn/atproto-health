export const dynamic = "force-dynamic";

import {
  getMigrationFlows,
  getMigrationWeeklyBreakdown,
  getMigrationTrajectories,
  getMigrationJourneyStats,
  getEcosystemStats,
  getPlcDataTimestamp,
} from "@/lib/db/plc-queries";
import { getOverviewStats } from "@/lib/db/plc-queries";
import { MigrationChartsSection, MultiStepSankeyChart, SimpleBarChart } from "@/components/charts";
import { CollapsibleSection } from "@/components/collapsible-section";

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
  const journeyStats = getMigrationJourneyStats();
  const stats = getEcosystemStats();
  const scanStats = getOverviewStats();
  const timestamp = getPlcDataTimestamp();

  const fmt = (n: number) => n.toLocaleString();

  return (
    <main className="min-h-screen bg-gray-950 text-gray-100 p-8">
      <div className="max-w-6xl mx-auto space-y-12">

        {/* Header */}
        <div>
          <h1 className="text-3xl font-bold text-white">PDS Migrations</h1>
          <p className="text-gray-400 mt-2">
            did:plc account creations and migrations recorded in plc.directory · did:web accounts are not tracked here
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
        <CollapsibleSection title="Ecosystem Summary" storageKey="migrations-summary">
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
              sub={`Across ${scanStats.total.toLocaleString()} tracked PDSes · from listRepos`}
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
              sub="Independent PDS transfers only · excludes bsky.social and internal bsky.network resharding"
            />
            <StatCard
              label="PDSes tracked"
              value={fmt(scanStats.total)}
              sub="From directory scans, PLC discovery, and did:web"
            />
          </div>
        </CollapsibleSection>

        {/* Migration Flows Sankey + weekly bar */}
        <CollapsibleSection
          title="Where do users end up?"
          subtitle="Where did:plc accounts ultimately landed — collapsed origin → current PDS. Hover for details. Click any node to highlight its trajectories forward and backward."
          storageKey="migrations-sankey"
        >
          <MigrationChartsSection sankeyData={flows} weeklyData={weeklyMigrations} />
        </CollapsibleSection>

        {/* Migration Trajectories */}
        <CollapsibleSection
          title="Migration Flows"
          subtitle="Actual per-hop did:plc migration steps — each column is one PDS hop. bsky.network shards collapsed. Click a node to highlight all paths through it in both directions."
          storageKey="migrations-multistep"
        >
          <MultiStepSankeyChart data={trajectories} height={650} />
        </CollapsibleSection>

        {/* Journey length distribution */}
        {journeyStats.totalMigrants > 0 && (
          <CollapsibleSection
            title="Migration Journey Length"
            subtitle={`How many times did each account migrate? Excludes bsky.network internal resharding — only voluntary migrations between distinct operators counted. Max recorded: ${journeyStats.maxMigrations} migrations by a single account.`}
            storageKey="migrations-journey"
          >
            <div className="grid grid-cols-2 gap-4 mb-6">
              <StatCard
                label="Migrated more than once"
                value={`${journeyStats.pctMultiple.toFixed(1)}%`}
                sub="Of real migrants"
              />
              <StatCard
                label="Most migrations by one account"
                value={journeyStats.maxMigrations.toLocaleString()}
                sub="Single account record"
              />
            </div>
            <div className="rounded-lg border border-gray-800 bg-gray-900 p-5">
              <h3 className="text-sm font-medium text-gray-300 mb-1">Migrations per account</h3>
              <p className="text-xs text-gray-600 mb-4">Accounts by number of voluntary migration events</p>
              <SimpleBarChart
                data={journeyStats.buckets.map(b => ({ name: b.label, value: b.users }))}
                color="#8b5cf6"
                layout="horizontal"
                xLabel="Migration count"
                yLabel="Accounts"
                logScale
              />
            </div>
          </CollapsibleSection>
        )}

      </div>
    </main>
  );
}
