/**
 * Streams the PLC directory export and records PDS migration events.
 *
 * A migration is detected when a DID's atproto_pds endpoint changes
 * between consecutive PLC operations.
 *
 * Run once for historical backfill (takes hours), then incrementally
 * via cron to stay current.
 */

import { getPlcDb } from "../db/plc-schema";

const EXPORT_URL = "https://plc.directory/export";
const BATCH_SIZE = 1000;
const COMMIT_EVERY = 2_000; // ops per transaction — keep WAL file small
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
  return url.replace(/\/$/, "").toLowerCase();
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

export async function collectPlcMigrations(): Promise<{
  opsProcessed: number;
  migrationsFound: number;
  creationsFound: number;
}> {
  const db = getPlcDb();

  const cursorRow = db
    .prepare(`SELECT after FROM plc_cursor WHERE id = 1`)
    .get() as { after: string } | undefined;
  let cursor = cursorRow?.after ?? "";

  const upsertDidPds = db.prepare(`
    INSERT INTO plc_did_pds (did, pds_url, updated_at)
    VALUES (?, ?, ?)
    ON CONFLICT(did) DO UPDATE SET pds_url = excluded.pds_url, updated_at = excluded.updated_at
  `);

  const insertCreation = db.prepare(`
    INSERT OR IGNORE INTO plc_account_creations (did, pds_url, created_at)
    VALUES (?, ?, ?)
  `);

  const insertMigration = db.prepare(`
    INSERT INTO plc_migrations (did, from_pds, to_pds, migrated_at)
    VALUES (?, ?, ?, ?)
  `);

  const upsertCursor = db.prepare(`
    INSERT INTO plc_cursor (id, after, updated_at)
    VALUES (1, ?, datetime('now'))
    ON CONFLICT(id) DO UPDATE SET after = excluded.after, updated_at = excluded.updated_at
  `);

  const commitBatch = db.transaction(
    (
      ops: Array<{
        did: string;
        pdsUrl: string;
        updatedAt: string;
        migration: { fromPds: string; toPds: string } | null;
        isNew: boolean;
      }>,
      newCursor: string
    ) => {
      for (const op of ops) {
        if (op.isNew) {
          insertCreation.run(op.did, op.pdsUrl, op.updatedAt);
        }
        if (op.migration) {
          insertMigration.run(
            op.did,
            op.migration.fromPds,
            op.migration.toPds,
            op.updatedAt
          );
        }
        upsertDidPds.run(op.did, op.pdsUrl, op.updatedAt);
      }
      upsertCursor.run(newCursor);
    }
  );

  let totalOps = 0;
  let totalMigrations = 0;
  let totalCreations = 0;
  let pendingBatch: Parameters<typeof commitBatch>[0] = [];

  while (true) {
    let ops: PlcOp[];
    try {
      ops = await fetchBatch(cursor);
    } catch (err) {
      console.error(`[plc] fetch failed at cursor ${cursor}: ${err}`);
      // Retry after a longer delay
      await new Promise((r) => setTimeout(r, 5000));
      continue;
    }

    if (ops.length === 0) break;

    for (const op of ops) {
      if (op.nullified) continue;

      const newPds = extractPds(op);
      if (!newPds) continue;

      const normalizedNew = normalizeUrl(newPds);

      const existing = db
        .prepare(`SELECT pds_url FROM plc_did_pds WHERE did = ?`)
        .get(op.did) as { pds_url: string } | undefined;

      let migration: { fromPds: string; toPds: string } | null = null;
      const isNew = !existing;
      if (isNew) totalCreations++;
      if (existing && normalizeUrl(existing.pds_url) !== normalizedNew) {
        migration = { fromPds: existing.pds_url, toPds: newPds };
        totalMigrations++;
      }

      pendingBatch.push({
        did: op.did,
        pdsUrl: newPds,
        updatedAt: op.createdAt,
        migration,
        isNew,
      });
    }

    cursor = ops[ops.length - 1].createdAt;
    totalOps += ops.length;

    if (pendingBatch.length >= COMMIT_EVERY) {
      commitBatch(pendingBatch, cursor);
      pendingBatch = [];
      console.log(
        `[plc] ${totalOps.toLocaleString()} ops · ${totalCreations.toLocaleString()} creations · ${totalMigrations.toLocaleString()} migrations · cursor ${cursor}`
      );
    }

    if (ops.length < BATCH_SIZE) break; // caught up

    await new Promise((r) => setTimeout(r, COURTESY_DELAY_MS));
  }

  // Commit any remaining
  if (pendingBatch.length > 0) {
    commitBatch(pendingBatch, cursor);
  }

  return { opsProcessed: totalOps, migrationsFound: totalMigrations, creationsFound: totalCreations };
}
