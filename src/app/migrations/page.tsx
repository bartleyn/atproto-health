export const dynamic = "force-dynamic";

import Link from "next/link";
import {
  getCreationTimeseriesWeekly,
  getMigrationTimeseriesWeekly,
  getLatestPdsStatusSnapshot,
  getEcosystemStats,
} from "@/lib/db/plc-queries";
import { StackedAreaChart, PdsStatusChart } from "@/components/charts";

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
  searchParams: Promise<{ trump?: string }>;
}) {
  const { trump } = await searchParams;
  const includeTrump = trump === "1";

  const creations = getCreationTimeseriesWeekly(includeTrump);
  const migrations = getMigrationTimeseriesWeekly();
  const statusSnapshot = getLatestPdsStatusSnapshot();
  const stats = getEcosystemStats();

  const fmt = (n: number) => n.toLocaleString();
  const snapshotDate = statusSnapshot[0]?.snapshot_date ?? null;

  // Shared x-axis: union of all periods from both charts, sorted
  const allPeriods = [...new Set([
    ...creations.map(r => r.period),
    ...migrations.map(r => r.period),
  ])].sort();

  // PDSes seen in the scanner (active/reachable), not the PLC historical count
  const scannedPdsCount = statusSnapshot.length;

  return (
    <main className="min-h-screen bg-gray-950 text-gray-100 p-8">
      <div className="max-w-6xl mx-auto space-y-12">

        {/* Header */}
        <div>
          <h1 className="text-3xl font-bold text-white">PDS Migrations</h1>
          <p className="text-gray-400 mt-2">
            Account creations, inbound migrations, and active account health per PDS.
          </p>
        </div>

        {/* Summary stats */}
        <section>
          <h2 className="text-xl font-semibold text-gray-200 mb-4">Ecosystem Summary</h2>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <StatCard
              label="Total DIDs"
              value={fmt(stats.total_dids_ex_trump)}
              sub={`+${fmt(stats.total_dids - stats.total_dids_ex_trump)} pds.trump.com`}
            />
            <StatCard
              label="Total Migrations"
              value={fmt(stats.total_migrations)}
              sub="PDS-to-PDS transfers recorded"
            />
            <StatCard
              label="PDSes Scanned"
              value={scannedPdsCount > 0 ? fmt(scannedPdsCount) : fmt(stats.independent_pds_count)}
              sub={scannedPdsCount > 0 ? `As of ${snapshotDate}` : "All-time from PLC (no scan yet)"}
            />
            <StatCard
              label="On Independent PDS"
              value={`${stats.independent_pds_account_pct}%`}
              sub="Of non-trump accounts"
            />
          </div>
        </section>

        {/* Account Creations */}
        <section>
          <div className="flex items-center justify-between mb-1">
            <h2 className="text-xl font-semibold text-gray-200">
              Account Creations by PDS
            </h2>
            <Link
              href={includeTrump ? "/migrations" : "/migrations?trump=1"}
              className="text-xs px-3 py-1 rounded border border-gray-700 text-gray-400 hover:text-gray-200 hover:border-gray-500 transition-colors"
            >
              {includeTrump ? "Hide pds.trump.com" : "Show pds.trump.com"}
            </Link>
          </div>
          <p className="text-xs text-gray-500 mb-4">
            Normalized to 100% — hover for actual counts. Top 10 PDSes by total volume.
            {includeTrump && (
              <span className="text-yellow-600 ml-2">
                pds.trump.com included (~20.6M DIDs).
              </span>
            )}
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

        {/* Inbound Migrations */}
        <section>
          <h2 className="text-xl font-semibold text-gray-200 mb-1">
            Inbound Migrations by PDS
          </h2>
          <p className="text-xs text-gray-500 mb-4">
            Excludes bsky.network destinations. Normalized to 100% — hover for actual counts.
          </p>
          {migrations.length === 0 ? (
            <p className="text-gray-500">No data yet.</p>
          ) : (
            <StackedAreaChart data={migrations} allPeriods={allPeriods} />
          )}
        </section>

        {/* Active Account Health */}
        <section>
          <h2 className="text-xl font-semibold text-gray-200 mb-1">
            Account Health by PDS
          </h2>
          <p className="text-xs text-gray-500 mb-4">
            {snapshotDate
              ? `Latest scan: ${snapshotDate}. Shows active, deactivated, takendown, and suspended counts per PDS.`
              : "No scan data yet — run npm run scan:pds-status."}
          </p>
          {statusSnapshot.length === 0 ? (
            <p className="text-gray-500">
              No snapshot data yet — run{" "}
              <code className="text-gray-300">npm run scan:pds-status</code>.
            </p>
          ) : (() => {
            const totals = statusSnapshot.reduce(
              (acc, r) => ({
                active:      acc.active      + r.active,
                deactivated: acc.deactivated + r.deactivated,
                takendown:   acc.takendown   + r.takendown,
                suspended:   acc.suspended   + r.suspended,
                deleted:     acc.deleted     + r.deleted,
                other:       acc.other       + r.other,
                total:       acc.total       + r.total_scanned,
              }),
              { active: 0, deactivated: 0, takendown: 0, suspended: 0, deleted: 0, other: 0, total: 0 }
            );
            const pct = (n: number) => totals.total > 0 ? ` (${((n / totals.total) * 100).toFixed(1)}%)` : "";
            return (
              <>
                <div className="grid grid-cols-3 md:grid-cols-6 gap-3 mb-6">
                  <StatCard label="Active"      value={fmt(totals.active)}      sub={pct(totals.active)} />
                  <StatCard label="Deactivated" value={fmt(totals.deactivated)} sub={pct(totals.deactivated)} />
                  <StatCard label="Takendown"   value={fmt(totals.takendown)}   sub={pct(totals.takendown)} />
                  <StatCard label="Suspended"   value={fmt(totals.suspended)}   sub={pct(totals.suspended)} />
                  <StatCard label="Deleted"     value={fmt(totals.deleted)}     sub={pct(totals.deleted)} />
                  <StatCard label="Total Scanned" value={fmt(totals.total)}     sub={`across ${fmt(scannedPdsCount)} PDSes`} />
                </div>
                <PdsStatusChart data={statusSnapshot} topN={25} />
              </>
            );
          })()}
        </section>

      </div>
    </main>
  );
}
