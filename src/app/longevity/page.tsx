export const dynamic = "force-dynamic";

import { getPdsAgeData, getAccountAgeHistogram, getPlcDataTimestamp } from "@/lib/db/plc-queries";
import { PdsAgeChart } from "@/components/charts";
import { SimpleBarChart } from "@/components/charts";

// Age-bucket thresholds in days from today
const AGE_BUCKETS: { label: string; maxDays: number }[] = [
  { label: "< 3 months",  maxDays: 90  },
  { label: "3–6 months",  maxDays: 180 },
  { label: "6–12 months", maxDays: 365 },
  { label: "1–2 years",   maxDays: 730 },
  { label: "2–3 years",   maxDays: 1095 },
  { label: "3–4 years",   maxDays: 1460 },
  { label: "4+ years",    maxDays: Infinity },
];

function computeAgeBuckets(rows: { month: string; count: number }[]) {
  const now = Date.now();
  const buckets = AGE_BUCKETS.map((b) => ({ name: b.label, value: 0 }));
  for (const row of rows) {
    const ageDays = (now - new Date(row.month + "-01").getTime()) / 86_400_000;
    const idx = AGE_BUCKETS.findIndex((b) => ageDays <= b.maxDays);
    if (idx >= 0) buckets[idx].value += row.count;
  }
  return buckets;
}

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
  const pdsAgeData    = getPdsAgeData();
  const monthlyData   = getAccountAgeHistogram();
  const timestamp     = getPlcDataTimestamp();

  const ageBuckets    = computeAgeBuckets(monthlyData);
  const totalAccounts = ageBuckets.reduce((s, b) => s + b.value, 0);

  // Summary stats
  const indieOnly = pdsAgeData.filter((r) => r.pds_url !== "bsky.network");
  const oldestIndie = indieOnly[0];
  const newestIndie = indieOnly[indieOnly.length - 1];
  const medianIdx = Math.floor(indieOnly.length / 2);
  const medianFirstMonth = indieOnly[medianIdx]?.first_month ?? "—";

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
              sub="With ≥ 10 accounts, excluding bsky.network"
            />
            <StatCard
              label="Oldest independent PDS"
              value={oldestIndie?.first_month ?? "—"}
              sub={oldestIndie?.pds_url.replace(/^https?:\/\//, "") ?? ""}
            />
            <StatCard
              label="Median PDS launch"
              value={medianFirstMonth}
              sub="Half of indie PDSes launched after this"
            />
            <StatCard
              label="Newest indie PDS"
              value={newestIndie?.first_month ?? "—"}
              sub={newestIndie?.pds_url.replace(/^https?:\/\//, "") ?? ""}
            />
          </div>
        </section>

        {/* PDS age scatter */}
        <section>
          <h2 className="text-xl font-semibold text-gray-200 mb-1">When Did PDSes Launch?</h2>
          <p className="text-xs text-gray-500 mb-4">
            Each point is a PDS. X = first account creation date (proxy for launch). Y = total accounts (log scale).
            Colored by launch era. Excludes pds.trump.com and PDSes with &lt; 10 accounts.
          </p>
          <ChartCard title="PDS Age vs. Size">
            <PdsAgeChart data={pdsAgeData} />
          </ChartCard>
        </section>

        {/* Account age histogram */}
        <section>
          <h2 className="text-xl font-semibold text-gray-200 mb-1">How Old Are Accounts?</h2>
          <p className="text-xs text-gray-500 mb-4">
            Age distribution of all {totalAccounts.toLocaleString()} accounts in plc_account_creations (excludes pds.trump.com).
            Age computed relative to today.
          </p>
          <ChartCard
            title="Account Age Distribution"
            subtitle="Buckets by time since account creation"
          >
            <SimpleBarChart
              data={ageBuckets}
              color="#8b5cf6"
              layout="horizontal"
              xLabel="Account age"
              yLabel="Accounts"
            />
          </ChartCard>
        </section>

        {/* PDS age table */}
        <section>
          <h2 className="text-xl font-semibold text-gray-200 mb-1">Independent PDSes by Age</h2>
          <p className="text-xs text-gray-500 mb-4">
            Sorted oldest to newest. First account date is a proxy for PDS launch.
          </p>
          <div className="overflow-x-auto rounded-lg border border-gray-800">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-800 text-gray-400 text-left">
                  <th className="px-4 py-3 font-medium">#</th>
                  <th className="px-4 py-3 font-medium">PDS</th>
                  <th className="px-4 py-3 font-medium">First account</th>
                  <th className="px-4 py-3 font-medium text-right">Total accounts</th>
                </tr>
              </thead>
              <tbody>
                {indieOnly.slice(0, 50).map((row, i) => (
                  <tr key={row.pds_url} className="border-b border-gray-800/50 hover:bg-gray-900/50">
                    <td className="px-4 py-2.5 text-gray-500 tabular-nums">{i + 1}</td>
                    <td className="px-4 py-2.5 font-mono text-xs">
                      {row.pds_url.replace(/^https?:\/\//, "")}
                    </td>
                    <td className="px-4 py-2.5 text-gray-400">{row.first_month}</td>
                    <td className="px-4 py-2.5 text-right tabular-nums">{row.total_accounts.toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>

      </div>
    </main>
  );
}
