export const dynamic = "force-dynamic";

import Link from "next/link";
import {
  getCreationTimeseriesWeekly,
  getActiveCreationTimeseriesWeekly,
  getMigrationFlows,
  getMigrationWeeklyBreakdown,
  getEcosystemStats,
  getPlcDataTimestamp,
} from "@/lib/db/plc-queries";
import { CreationChartsSection, MigrationChartsSection } from "@/components/charts";

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
  const activeCreations = getActiveCreationTimeseriesWeekly(hideBsky);
  const flows = getMigrationFlows();
  const weeklyMigrations = getMigrationWeeklyBreakdown();
  const stats = getEcosystemStats(hideBsky);
  const timestamp = getPlcDataTimestamp();

  const fmt = (n: number) => n.toLocaleString();
  const allPeriods = [...new Set([...creations, ...activeCreations].map(r => r.period))].sort();

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
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <StatCard
              label="Total did:plc accounts (approx.)"
              value={fmt(stats.total_dids_ex_trump)}
              sub="Scanned PDSes only, excludes pds.trump.com"
            />
            <StatCard
              label="did:plc accounts that migrated"
              value={fmt(stats.unique_migrating_dids)}
              sub="Unique DIDs with at least one voluntary migration (excludes bsky.social resharding)"
            />
            <StatCard
              label="Total Migrations"
              value={fmt(stats.total_migrations)}
              sub="PDS-to-PDS transfers, excluding internal bsky.network resharding"
            />
            <StatCard
              label="Scanned PDSes"
              value={fmt(stats.independent_pds_count)}
              sub="PDSes that responded to the repo scanner"
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
              Weekly did:plc Creations by PDS
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
            <CreationChartsSection plcData={creations} repoData={activeCreations} allPeriods={allPeriods} />
          )}
        </section>

      </div>
    </main>
  );
}
