export const dynamic = "force-dynamic";

import { getCreationTimeseries, getMigrationTimeseries } from "@/lib/db/plc-queries";
import { StackedAreaChart } from "@/components/charts";

export default function MigrationsPage() {
  const creations = getCreationTimeseries();
  const migrations = getMigrationTimeseries();

  return (
    <main className="min-h-screen bg-gray-950 text-gray-100 p-8">
      <div className="max-w-6xl mx-auto space-y-12">
        <div>
          <h1 className="text-3xl font-bold text-white">PDS Migrations</h1>
          <p className="text-gray-400 mt-2">
            Account creations and inbound migrations per PDS, by month.
          </p>
        </div>

        <section>
          <h2 className="text-xl font-semibold text-gray-200 mb-4">
            Account Creations by PDS
          </h2>
          {creations.length === 0 ? (
            <p className="text-gray-500">No data yet — run <code className="text-gray-300">npm run collect:plc</code> then <code className="text-gray-300">npm run aggregate:plc</code>.</p>
          ) : (
            <StackedAreaChart data={creations} />
          )}
        </section>

        <section>
          <h2 className="text-xl font-semibold text-gray-200 mb-4">
            Inbound Migrations by PDS
          </h2>
          {migrations.length === 0 ? (
            <p className="text-gray-500">No data yet.</p>
          ) : (
            <StackedAreaChart data={migrations} />
          )}
        </section>
      </div>
    </main>
  );
}
