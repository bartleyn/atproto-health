export const dynamic = "force-dynamic";

import { getAnalyticsData } from "@/lib/db/analytics-queries";
import { WauByCohortChart, DauMauChart, RetentionChart } from "@/components/analytics-charts";
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

export default async function AnalyticsPage() {
  const cache = getAnalyticsData();

  if (!cache) {
    return (
      <main className="min-h-screen bg-gray-950 text-gray-100 p-8">
        <div className="max-w-6xl mx-auto">
          <h1 className="text-3xl font-bold text-white mb-4">Analytics</h1>
          <div className="rounded-lg border border-gray-800 p-8 text-center text-gray-500">
            <p>Analytics cache is cold.</p>
            <p className="mt-2 text-sm">
              Run{" "}
              <code className="bg-gray-800 px-2 py-0.5 rounded text-gray-300">npm run analysis:marts</code>{" "}
              then{" "}
              <code className="bg-gray-800 px-2 py-0.5 rounded text-gray-300">npm run analysis:cache</code>{" "}
              to populate it, then reload.
            </p>
          </div>
        </div>
      </main>
    );
  }

  const { data, writtenAt } = cache;
  const { wauByCohort, dauMauTrend, dauMauSnapshot, retentionByCohort } = data;

  const latestRetentionTotal = [...retentionByCohort]
    .reverse()
    .find(r => r.pds_type === "~ total");

  return (
    <main className="min-h-screen bg-gray-950 text-gray-100 p-8">
      <div className="max-w-6xl mx-auto space-y-12">

        <div>
          <h1 className="text-3xl font-bold text-white">Analytics</h1>
          <p className="text-gray-400 mt-2">Network-wide activity, engagement, and retention — not tied to any one PDS</p>
          <p className="text-xs text-gray-600 mt-1">
            Marts last refreshed{" "}
            {new Date(writtenAt).toLocaleString("en-US", {
              month: "short", day: "numeric", year: "numeric",
              hour: "numeric", minute: "2-digit", timeZoneName: "short",
            })}
          </p>
        </div>

        <section>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <StatCard
              label="Daily Active"
              value={dauMauSnapshot ? dauMauSnapshot.dau.toLocaleString() : "—"}
              sub={dauMauSnapshot ? `as of ${dauMauSnapshot.date}` : undefined}
            />
            <StatCard
              label="Monthly Active"
              value={dauMauSnapshot ? dauMauSnapshot.mau.toLocaleString() : "—"}
              sub="trailing 28 days"
            />
            <StatCard
              label="DAU / MAU"
              value={dauMauSnapshot ? `${dauMauSnapshot.dau_mau_pct}%` : "—"}
              sub="stickiness"
            />
            <StatCard
              label="D7 Retention"
              value={latestRetentionTotal ? `${latestRetentionTotal.d7_pct}%` : "—"}
              sub={latestRetentionTotal ? `cohort created ${latestRetentionTotal.creation_date}` : undefined}
            />
          </div>
        </section>

        <CollapsibleSection
          title="Weekly Active Users by Account Age"
          subtitle="Distinct active DIDs per week, broken down by when the account was created. Excludes spam/impersonation/suspended accounts."
          storageKey="analytics-wau-cohort"
        >
          <ChartCard title="WAU by Cohort">
            {wauByCohort.length === 0 ? (
              <p className="text-gray-500">No data yet.</p>
            ) : (
              <WauByCohortChart data={wauByCohort} />
            )}
          </ChartCard>
        </CollapsibleSection>

        <CollapsibleSection
          title="DAU / MAU Trend"
          subtitle="Daily active users, trailing 28-day monthly active users, and the stickiness ratio between them."
          storageKey="analytics-dau-mau"
        >
          <ChartCard title="DAU / MAU">
            {dauMauTrend.length === 0 ? (
              <p className="text-gray-500">No data yet.</p>
            ) : (
              <DauMauChart data={dauMauTrend} />
            )}
          </ChartCard>
        </CollapsibleSection>

        <CollapsibleSection
          title="New Account Retention"
          subtitle="Of accounts created on a given day, what % were active 1/3/7 days later. Network-wide total across bsky-hosted and indie PDSes."
          storageKey="analytics-retention"
        >
          <ChartCard title="D1 / D3 / D7 Retention">
            {retentionByCohort.length === 0 ? (
              <p className="text-gray-500">No data yet.</p>
            ) : (
              <RetentionChart data={retentionByCohort} />
            )}
          </ChartCard>
        </CollapsibleSection>

      </div>
    </main>
  );
}
