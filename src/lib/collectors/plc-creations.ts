/**
 * Backfill pass over the PLC directory export to record account creation events.
 *
 * A creation op is identified by operation.prev === null (first op for a DID).
 * This pass skips plc_did_pds lookups entirely — it only cares about create ops.
 *
 * Uses a separate cursor (plc_creations_cursor) so it runs independently
 * from the migration collector.
 */

import sql from "../db/pg";

const EXPORT_URL = "https://plc.directory/export";
const BATCH_SIZE = 1000;
const COMMIT_EVERY = 5_000;
const COURTESY_DELAY_MS = 150;

interface PlcOp {
  did: string;
  nullified: boolean;
  createdAt: string;
  operation: {
    type: string;
    prev?: string | null;
    service?: string;
    services?: {
      atproto_pds?: { endpoint?: string };
    };
  };
}

function isCreationOp(op: PlcOp): boolean {
  // Legacy format: type === 'create'
  if (op.operation.type === "create") return true;
  // Modern format: prev is explicitly null (first op in chain)
  // Modern format: prev is null (first op in chain)
  if (op.operation.prev === null) return true;
  return false;
}

function extractPds(op: PlcOp): string | null {
  if (op.operation.type === "create") {
    return op.operation.service ?? null;
  }
  return op.operation.services?.atproto_pds?.endpoint ?? null;
}

async function fetchBatch(after: string): Promise<PlcOp[]> {
  const params = new URLSearchParams({ count: String(BATCH_SIZE) });
  if (after) params.set("after", after);

  const res = await fetch(`${EXPORT_URL}?${params}`);
  if (!res.ok) throw new Error(`PLC export HTTP ${res.status}`);

  const text = await res.text();
  return text
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line) as PlcOp);
}

export async function backfillPlcCreations(): Promise<{
  opsScanned: number;
  creationsFound: number;
}> {
  const cursorRows = await sql<{ after: string }[]>`SELECT after FROM plc.plc_creations_cursor WHERE id = 1`;
  let cursor = cursorRows[0]?.after ?? "";

  let totalOps = 0;
  let totalCreations = 0;
  let pendingCreations: Array<{ did: string; pds_url: string; created_at: string }> = [];

  while (true) {
    let ops: PlcOp[];
    try {
      ops = await fetchBatch(cursor);
    } catch (err) {
      console.error(`[plc-creations] fetch failed at cursor ${cursor}: ${err}`);
      await new Promise((r) => setTimeout(r, 5000));
      continue;
    }

    if (ops.length === 0) break;

    for (const op of ops) {
      if (op.nullified) continue;
      if (!isCreationOp(op)) continue;

      const pds = extractPds(op);
      if (!pds) continue;

      pendingCreations.push({
        did: op.did,
        pds_url: pds.replace(/\/$/, "").toLowerCase().replace(/^http:\/\//, "https://"),
        created_at: op.createdAt,
      });
      totalCreations++;
    }

    cursor = ops[ops.length - 1].createdAt;
    totalOps += ops.length;

    if (pendingCreations.length >= COMMIT_EVERY) {
      await sql.begin(async sql => {
        await sql`
          INSERT INTO plc.plc_account_creations ${sql(pendingCreations, "did", "pds_url", "created_at")}
          ON CONFLICT DO NOTHING
        `;
        await sql`
          INSERT INTO plc.plc_creations_cursor (id, after, updated_at) VALUES (1, ${cursor}, NOW())
          ON CONFLICT (id) DO UPDATE SET after = EXCLUDED.after, updated_at = NOW()
        `;
      });
      pendingCreations = [];
      console.log(
        `[plc-creations] ${totalOps.toLocaleString()} ops scanned · ${totalCreations.toLocaleString()} creations · cursor ${cursor}`
      );
    }

    if (ops.length < BATCH_SIZE) break;

    await new Promise((r) => setTimeout(r, COURTESY_DELAY_MS));
  }

  if (pendingCreations.length > 0) {
    await sql.begin(async sql => {
      await sql`
        INSERT INTO plc.plc_account_creations ${sql(pendingCreations, "did", "pds_url", "created_at")}
        ON CONFLICT DO NOTHING
      `;
      await sql`
        INSERT INTO plc.plc_creations_cursor (id, after, updated_at) VALUES (1, ${cursor}, NOW())
        ON CONFLICT (id) DO UPDATE SET after = EXCLUDED.after, updated_at = NOW()
      `;
    });
  }

  return { opsScanned: totalOps, creationsFound: totalCreations };
}
