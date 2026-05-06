export const dynamic = "force-dynamic";

import Link from "next/link";
import { getPdsAgeData, getAccountCohortCounts, getActiveCreationTimeseriesWeekly, getPlcDataTimestamp } from "@/lib/db/plc-queries";
import { PdsAgeChart, SimpleBarChart, CreationChartsSection } from "@/components/charts";
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

function ChartCard({ title, subtitle, children }: { title: string; subtitle?: string; children: React.ReactNode }) {
  return (
    <div className="rounded-lg border border-gray-800 bg-gray-900 p-5">
      <h2 className="text-base font-semibold mb-0.5">{title}</h2>
      {subtitle && <p className="text-xs text-gray-500 mb-4">{subtitle}</p>}
      {children}
    </div>
  );
}

export default async function LongevityPage({
  searchParams,
}: {
  searchParams: Promise<{ bsky?: string }>;
}) {
  const { bsky } = await searchParams;
  const hideBsky = bsky === "0";

  const pdsAgeDataAll  = getPdsAgeData(1);
  const pdsAgeData     = pdsAgeDataAll.filter(r => r.total_accounts >= 5);
  const cohortData     = getAccountCohortCounts();
  const activeCreations = getActiveCreationTimeseriesWeekly(hideBsky);
  const timestamp      = getPlcDataTimestamp();

  const totalAccounts = cohortData.reduce((s, r) => s + r.count, 0);
  const cohortBuckets = cohortData.map(r => ({ name: r.cohort, value: r.count }));

  // Summary stats — use unfiltered set so count/oldest/newest reflect all real PDSes
  const indieOnly = pdsAgeDataAll;//.filter((r) => r.pds_url !== "bsky.network");
  const oldestIndie = indieOnly[0];
  const newestIndie = indieOnly[indieOnly.length - 1];
  const medianIdx = Math.floor(indieOnly.length / 2);
  const medianFirstWeek = indieOnly[medianIdx]?.first_week ?? "—";

  return (
    <main className="min-h-screen bg-gray-950 text-gray-100 p-8">
      <div className="max-w-6xl mx-auto space-y-12">

        <div>
          <h1 className="text-3xl font-bold text-white">PDS &amp; Account Age</h1>
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

        {/* Summary stats — no toggle, always visible */}
        <section>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <StatCard
              label="PDSes with active repos"
              value={indieOnly.length.toLocaleString()}
              sub="PDSes with at least one active repo and at least one native did:plc account creation — excludes migration-only and did:web-only PDSes"
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

        <CollapsibleSection
          title="When Did PDSes Launch?"
          subtitle="Each point is a PDS with 5+ active repos. X = first repo-backed account (proxy for launch). Y = total repos (log scale). Colored by launch era. Excludes pds.trump.com, junk PDSes, and migration-only PDSes (no native did:plc creations)."
          storageKey="longevity-age-scatter"
        >
          <ChartCard title="PDS Age vs. Size">
            <PdsAgeChart data={pdsAgeData} />
          </ChartCard>
        </CollapsibleSection>

        <CollapsibleSection
          title="When Were Accounts Created?"
          subtitle={`${totalAccounts.toLocaleString()} repo-backed accounts grouped by creation era (excludes pds.trump.com and spam PDSes).`}
          storageKey="longevity-account-age"
        >
          <ChartCard title="Account Age Distribution" subtitle="Accounts grouped by creation era">
            <SimpleBarChart
              data={cohortBuckets}
              color="#8b5cf6"
              layout="horizontal"
              xLabel="Creation cohort"
              yLabel="Accounts"
            />
          </ChartCard>
        </CollapsibleSection>

        <CollapsibleSection
          title="Weekly Account Creations per PDS"
          subtitle={`did:plc repo-backed accounts only (did:web excluded). Normalized to 100% — hover for actual counts. Top 10 PDSes by total volume.${hideBsky ? " bsky.network hidden." : ""}`}
          storageKey="longevity-creations"
        >
          <div className="flex justify-end mb-3">
            <Link
              href={`/longevity?bsky=${hideBsky ? "1" : "0"}`}
              className="text-xs px-3 py-1 rounded border border-gray-700 text-gray-400 hover:text-gray-200 hover:border-gray-500 transition-colors"
            >
              {hideBsky ? "Show bsky.network" : "Hide bsky.network"}
            </Link>
          </div>
          {activeCreations.length === 0 ? (
            <p className="text-gray-500">
              No data yet — run{" "}
              <code className="text-gray-300">npm run collect:plc</code> then{" "}
              <code className="text-gray-300">npm run aggregate:plc</code>.
            </p>
          ) : (
            <CreationChartsSection repoData={activeCreations} />
          )}
        </CollapsibleSection>

        

      </div>
    </main>
  );
}
