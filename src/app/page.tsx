import { getDb } from "@/lib/db/schema";

interface SnapshotSummary {
  total: number;
  online: number;
  open_reg: number;
  countries: number;
  total_users: number;
  collected_at: string | null;
}

function getSummary(): SnapshotSummary | null {
  try {
    const db = getDb();

    const latestRun = db
      .prepare(
        `SELECT id, completed_at FROM collection_runs
         WHERE status = 'completed' ORDER BY id DESC LIMIT 1`
      )
      .get() as { id: number; completed_at: string } | undefined;

    if (!latestRun) return null;

    const stats = db
      .prepare(
        `SELECT
          COUNT(*) as total,
          SUM(is_online) as online,
          SUM(CASE WHEN invite_code_required = 0 THEN 1 ELSE 0 END) as open_reg,
          COUNT(DISTINCT country_code) as countries,
          COALESCE(SUM(user_count_active), 0) as total_users
        FROM pds_snapshots WHERE run_id = ?`
      )
      .get(latestRun.id) as SnapshotSummary;

    return { ...stats, collected_at: latestRun.completed_at };
  } catch {
    return null;
  }
}

export default function Home() {
  const summary = getSummary();

  return (
    <main className="max-w-5xl mx-auto px-6 py-12">
      <h1 className="text-3xl font-bold mb-2">ATProto Health</h1>
      <p className="text-gray-400 mb-10">
        AT Protocol ecosystem health dashboard
      </p>

      {!summary ? (
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
      ) : (
        <>
          <p className="text-sm text-gray-500 mb-6">
            Last collected:{" "}
            {summary.collected_at
              ? new Date(summary.collected_at + "Z").toLocaleString()
              : "unknown"}
          </p>

          <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-12">
            <StatCard label="PDS Instances" value={summary.total} />
            <StatCard label="Online" value={summary.online} />
            <StatCard label="Open Registration" value={summary.open_reg} />
            <StatCard label="Countries" value={summary.countries} />
            <StatCard label="Users (3rd party)" value={summary.total_users} />
          </div>
        </>
      )}
    </main>
  );
}

function StatCard({ label, value }: { label: string; value: number }) {
  return (
    <div className="rounded-lg border border-gray-800 bg-gray-900 p-4">
      <div className="text-2xl font-semibold tabular-nums">
        {value.toLocaleString()}
      </div>
      <div className="text-sm text-gray-400 mt-1">{label}</div>
    </div>
  );
}
