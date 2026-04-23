export const dynamic = "force-dynamic";

import { getPdsAgeData, getAccountCohortCounts, getPlcDataTimestamp } from "@/lib/db/plc-queries";
import { PdsAgeChart } from "@/components/charts";
import { SimpleBarChart } from "@/components/charts";

function StatCard({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="bg-gray-900 rounded-lg p-4 border border-gray-800">
      <p className="text-xs text-gray-500 uppercase tracking-wide">{label}</p>
      <p className="text-2xl font-bold text-white mt-1">{value}</p>
      {sub && <p className="text-xs text-gray-500 mt-1">{sub}</p>}
    </div>
  );
}

function ChartCard({ title, subtitle, children }: { title: string; subtitle?: string; children: React.ReactNode }) {
  return (
    <div className="rounded-lg border border-gray-800 bg-gray-900 p-5">
      <h2 className="text-base font-semibold mb-0.5">{title}</h2>
      {subtitle && <p className="text-xs text-gray-500 mb-4">{subtitle}</p>}
      {children}
    </div>
  );
}

export default async function LongevityPage() {
  const pdsAgeDataAll = getPdsAgeData(1);   // all PDSes with ≥ 1 repo — for stat cards
  const pdsAgeData    = getPdsAgeData(5);   // ≥ 5 repos — for chart
  const cohortData    = getAccountCohortCounts();
  const timestamp     = getPlcDataTimestamp();

  const totalAccounts = cohortData.reduce((s, r) => s + r.count, 0);
  const cohortBuckets = cohortData.map(r => ({ name: r.cohort, value: r.count }));

  // Summary stats — use unfiltered set so count/oldest/newest reflect all real PDSes
  const indieOnly = pdsAgeDataAll.filter((r) => r.pds_url !== "bsky.network");
  const oldestIndie = indieOnly[0];
  const newestIndie = indieOnly[indieOnly.length - 1];
  const medianIdx = Math.floor(indieOnly.length / 2);
  const medianFirstWeek = indieOnly[medianIdx]?.first_week ?? "—";

  return (
    <main className="min-h-screen bg-gray-950 text-gray-100 p-8">
      <div className="max-w-6xl mx-auto space-y-12">

        <div>
          <h1 className="text-3xl font-bold text-white">PDS &amp; Account Longevity</h1>
          <p className="text-gray-400 mt-2">When PDSes launched and how old the accounts are</p>
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
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <StatCard
              label="Independent PDSes tracked"
              value={indieOnly.length.toLocaleString()}
              sub="With at least 1 repo, excluding bsky.network"
            />
            <StatCard
              label="Oldest independent PDS"
              value={oldestIndie?.first_week ?? "—"}
              sub={oldestIndie?.pds_url.replace(/^https?:\/\//, "") ?? ""}
            />
            <StatCard
              label="Median PDS launch"
              value={medianFirstWeek}
              sub="Half of indie PDSes launched after this"
            />
            <StatCard
              label="Newest indie PDS"
              value={newestIndie?.first_week ?? "—"}
              sub={newestIndie?.pds_url.replace(/^https?:\/\//, "") ?? ""}
            />
          </div>
        </section>

        {/* PDS age scatter */}
        <section>
          <h2 className="text-xl font-semibold text-gray-200 mb-1">When Did PDSes Launch?</h2>
          <p className="text-xs text-gray-500 mb-4">
            Each point is a PDS. X = first repo-backed account (proxy for launch). Y = total repos (log scale).
            Colored by launch era. Excludes pds.trump.com and junk PDSes.
          </p>
          <ChartCard title="PDS Age vs. Size">
            <PdsAgeChart data={pdsAgeData} />
          </ChartCard>
        </section>

        {/* Account age histogram */}
        <section>
          <h2 className="text-xl font-semibold text-gray-200 mb-1">When Were Accounts Created?</h2>
          <p className="text-xs text-gray-500 mb-4">
            {totalAccounts.toLocaleString()} repo-backed accounts grouped by creation era (excludes pds.trump.com and spam PDSes).
          </p>
          <ChartCard
            title="Account Age Distribution"
            subtitle="Accounts grouped by creation era"
          >
            <SimpleBarChart
              data={cohortBuckets}
              color="#8b5cf6"
              layout="horizontal"
              xLabel="Creation cohort"
              yLabel="Accounts"
            />
          </ChartCard>
        </section>

      </div>
    </main>
  );
}
