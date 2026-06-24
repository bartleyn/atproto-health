/**
 * Streams the PLC directory export and records PDS migration events.
 *
 * A migration is detected when a DID's atproto_pds endpoint changes
 * between consecutive PLC operations.
 *
 * Run once for historical backfill (takes hours), then incrementally
 * via cron to stay current.
 */

import sql from "../db/pg";

const EXPORT_URL = "https://plc.directory/export";
const BATCH_SIZE = 1000;
const COMMIT_EVERY = 2_000;
const COURTESY_DELAY_MS = 150;

interface PlcOp {
  did: string;
  cid: string;
  nullified: boolean;
  createdAt: string;
  operation: {
    type: string;
    // legacy create
    service?: string;
    // modern plc_operation
    services?: {
      atproto_pds?: {
        endpoint?: string;
      };
    };
  };
}

function extractPds(op: PlcOp): string | null {
  if (op.operation.type === "create") {
    return op.operation.service ?? null;
  }
  return op.operation.services?.atproto_pds?.endpoint ?? null;
}

function normalizeUrl(url: string): string {
  return url.replace(/\/$/, "").toLowerCase().replace(/^http:\/\//, "https://");
}

async function fetchBatch(after: string): Promise<PlcOp[]> {
  const params = new URLSearchParams({ count: String(BATCH_SIZE) });
  if (after) params.set("after", after);

  const res = await fetch(`${EXPORT_URL}?${params}`);
  if (!res.ok) {
    throw new Error(`PLC export HTTP ${res.status}`);
  }

  const text = await res.text();
  return text
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line) as PlcOp);
}

type PendingOp = {
  did: string;
  pdsUrl: string;
  updatedAt: string;
  migration: { fromPds: string; toPds: string } | null;
  isNew: boolean;
};

async function commitBatch(ops: PendingOp[], newCursor: string): Promise<void> {
  const creations = ops
    .filter(op => op.isNew)
    .map(op => ({ did: op.did, pds_url: op.pdsUrl, created_at: op.updatedAt }));

  const migrations = ops
    .filter(op => op.migration)
    .map(op => ({ did: op.did, from_pds: op.migration!.fromPds, to_pds: op.migration!.toPds, migrated_at: op.updatedAt }));

  // Deduplicate did_pds rows by DID, keeping last (most recent) value.
  const didPdsMap = new Map<string, { did: string; pds_url: string; updated_at: string }>();
  for (const op of ops) didPdsMap.set(op.did, { did: op.did, pds_url: op.pdsUrl, updated_at: op.updatedAt });
  const didPdsRows = [...didPdsMap.values()];

  await sql.begin(async sql => {
    if (creations.length > 0) {
      await sql`
        INSERT INTO plc.plc_account_creations ${sql(creations, "did", "pds_url", "created_at")}
        ON CONFLICT DO NOTHING
      `;
    }
    if (migrations.length > 0) {
      await sql`INSERT INTO plc.plc_migrations ${sql(migrations, "did", "from_pds", "to_pds", "migrated_at")}`;
    }
    await sql`
      INSERT INTO plc.plc_did_pds ${sql(didPdsRows, "did", "pds_url", "updated_at")}
      ON CONFLICT (did) DO UPDATE SET pds_url = EXCLUDED.pds_url, updated_at = EXCLUDED.updated_at
    `;
    await sql`
      INSERT INTO plc.plc_cursor (id, after, updated_at) VALUES (1, ${newCursor}, NOW())
      ON CONFLICT (id) DO UPDATE SET after = EXCLUDED.after, updated_at = NOW()
    `;
  });
}

export async function collectPlcMigrations(): Promise<{
  opsProcessed: number;
  migrationsFound: number;
  creationsFound: number;
}> {
  const cursorRows = await sql<{ after: string }[]>`SELECT after FROM plc.plc_cursor WHERE id = 1`;
  let cursor = cursorRows[0]?.after ?? "";

  // In-memory DID→PDS map to avoid per-row DB lookups in the inner loop.
  // Populated via batch SELECT before processing each fetched page.
  const knownPds = new Map<string, string>();

  let totalOps = 0;
  let totalMigrations = 0;
  let totalCreations = 0;
  let pendingBatch: PendingOp[] = [];

  while (true) {
    let ops: PlcOp[];
    try {
      ops = await fetchBatch(cursor);
    } catch (err) {
      console.error(`[plc] fetch failed at cursor ${cursor}: ${err}`);
      await new Promise((r) => setTimeout(r, 5000));
      continue;
    }

    if (ops.length === 0) break;

    // Batch-load any DIDs we haven't seen yet.
    const unknownDids = ops.map(o => o.did).filter(d => !knownPds.has(d));
    if (unknownDids.length > 0) {
      const rows = await sql<{ did: string; pds_url: string }[]>`
        SELECT did, pds_url FROM plc.plc_did_pds WHERE did = ANY(${unknownDids})
      `;
      for (const r of rows) knownPds.set(r.did, r.pds_url);
    }

    for (const op of ops) {
      if (op.nullified) continue;

      const newPds = extractPds(op);
      if (!newPds) continue;

      const normalizedNew = normalizeUrl(newPds);
      const existingPds = knownPds.get(op.did);

      let migration: { fromPds: string; toPds: string } | null = null;
      const isNew = existingPds === undefined;
      if (isNew) totalCreations++;
      if (existingPds !== undefined && normalizeUrl(existingPds) !== normalizedNew) {
        migration = { fromPds: existingPds, toPds: normalizedNew };
        totalMigrations++;
      }

      // Update the map immediately so subsequent ops for the same DID see the new value.
      knownPds.set(op.did, normalizedNew);

      pendingBatch.push({ did: op.did, pdsUrl: normalizedNew, updatedAt: op.createdAt, migration, isNew });
    }

    cursor = ops[ops.length - 1].createdAt;
    totalOps += ops.length;

    if (pendingBatch.length >= COMMIT_EVERY) {
      await commitBatch(pendingBatch, cursor);
      pendingBatch = [];
      console.log(
        `[plc] ${totalOps.toLocaleString()} ops · ${totalCreations.toLocaleString()} creations · ${totalMigrations.toLocaleString()} migrations · cursor ${cursor}`
      );
    }

    if (ops.length < BATCH_SIZE) break; // caught up

    await new Promise((r) => setTimeout(r, COURTESY_DELAY_MS));
  }

  // Commit any remaining ops.
  if (pendingBatch.length > 0) {
    await commitBatch(pendingBatch, cursor);
  }

  return { opsProcessed: totalOps, migrationsFound: totalMigrations, creationsFound: totalCreations };
}
