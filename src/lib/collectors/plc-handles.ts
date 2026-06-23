/**
 * Backfill + incremental pass over the PLC directory export to record each DID's
 * current handle (from operation.alsoKnownAs).
 *
 * Every non-nullified op can carry a handle, and handles change over time, so we
 * process ALL ops (not just creations) and keep the latest value per DID — ops are
 * delivered in chronological order, so the last write wins.
 *
 * Only DIDs whose repos we actually have (plc.did_in_repo) are recorded, which excludes
 * accounts on fake/unreachable PDSes (e.g. pds.trump.com) that we never scanned.
 *
 * Uses its own cursor (plc_handles_cursor) so it runs independently of the migration
 * and creation collectors. Run once for historical backfill (hours), then incrementally
 * via cron to stay current.
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
    // legacy create op: bare handle string
    handle?: string;
    // modern plc_operation: ["at://alice.bsky.social", ...]
    alsoKnownAs?: string[];
  };
}

// atproto handles are domain names, spec-capped at 253 chars. Anything longer is invalid
// (spam/garbage) and would also blow the btree index size limit — truncate defensively.
const MAX_HANDLE_LEN = 253;

function normHandle(raw: string): string | null {
  const h = raw.trim().toLowerCase();
  if (!h) return null;
  return h.length > MAX_HANDLE_LEN ? h.slice(0, MAX_HANDLE_LEN) : h;
}

function extractHandle(op: PlcOp): string | null {
  // Modern format: first at:// alias is the primary handle.
  const aka = op.operation.alsoKnownAs;
  if (Array.isArray(aka)) {
    for (const uri of aka) {
      if (typeof uri === "string" && uri.startsWith("at://")) {
        const h = normHandle(uri.slice("at://".length));
        if (h) return h;
      }
    }
  }
  // Legacy create op: bare handle field.
  if (typeof op.operation.handle === "string") {
    const h = normHandle(op.operation.handle);
    if (h) return h;
  }
  return null;
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

async function commitBatch(
  rows: Map<string, { did: string; handle: string; updated_at: string }>,
  newCursor: string
): Promise<void> {
  const handleRows = [...rows.values()];
  await sql.begin(async sql => {
    if (handleRows.length > 0) {
      await sql`
        INSERT INTO plc.plc_did_handle ${sql(handleRows, "did", "handle", "updated_at")}
        ON CONFLICT (did) DO UPDATE
          SET handle = EXCLUDED.handle, updated_at = EXCLUDED.updated_at
      `;
    }
    await sql`
      INSERT INTO plc.plc_handles_cursor (id, after, updated_at) VALUES (1, ${newCursor}, NOW())
      ON CONFLICT (id) DO UPDATE SET after = EXCLUDED.after, updated_at = NOW()
    `;
  });
}

export async function backfillPlcHandles(): Promise<{
  opsScanned: number;
  handlesFound: number;
}> {
  const cursorRows = await sql<{ after: string }[]>`SELECT after FROM plc.plc_handles_cursor WHERE id = 1`;
  let cursor = cursorRows[0]?.after ?? "";

  let totalOps = 0;
  let totalHandles = 0;
  // Dedup within the pending window by DID, keeping the most recent handle.
  let pending = new Map<string, { did: string; handle: string; updated_at: string }>();

  while (true) {
    let ops: PlcOp[];
    try {
      ops = await fetchBatch(cursor);
    } catch (err) {
      console.error(`[plc-handles] fetch failed at cursor ${cursor}: ${err}`);
      await new Promise((r) => setTimeout(r, 5000));
      continue;
    }

    if (ops.length === 0) break;

    // Collect this page's handle candidates, then keep only DIDs whose repos we actually
    // have (plc.did_in_repo). This excludes accounts on fake/unreachable PDSes
    // (e.g. pds.trump.com) that we never scanned — one indexed lookup per page.
    const cands = ops
      .filter(op => !op.nullified)
      .map(op => ({ did: op.did, handle: extractHandle(op), updated_at: op.createdAt }))
      .filter((c): c is { did: string; handle: string; updated_at: string } => c.handle !== null);

    if (cands.length > 0) {
      const dids = [...new Set(cands.map(c => c.did))];
      const inRepo = await sql<{ did: string }[]>`
        SELECT did FROM plc.did_in_repo WHERE did = ANY(${dids})
      `;
      const present = new Set(inRepo.map(r => r.did));
      for (const c of cands) {
        if (!present.has(c.did)) continue;
        pending.set(c.did, c);
        totalHandles++;
      }
    }

    cursor = ops[ops.length - 1].createdAt;
    totalOps += ops.length;

    if (pending.size >= COMMIT_EVERY) {
      await commitBatch(pending, cursor);
      pending = new Map();
      console.log(
        `[plc-handles] ${totalOps.toLocaleString()} ops scanned · ${totalHandles.toLocaleString()} handles · cursor ${cursor}`
      );
    }

    if (ops.length < BATCH_SIZE) break;

    await new Promise((r) => setTimeout(r, COURTESY_DELAY_MS));
  }

  if (pending.size > 0) {
    await commitBatch(pending, cursor);
  }

  return { opsScanned: totalOps, handlesFound: totalHandles };
}
