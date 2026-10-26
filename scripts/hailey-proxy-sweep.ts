/**
 * Per-DID label sweep for @labeler.hailey.at via the bsky appview proxy.
 *
 * hailey's Ozone (ozone.hailey.at) is network-firewalled from us, and the appview
 * proxy refuses `uriPatterns=*` ("wildcards not supported"), so the normal
 * skywatch-labels collector can't reach it. The only working path is per-DID
 * queryLabels through the authenticated proxy, ≤20 uriPatterns/request (hailey's
 * Ozone silently returns nothing for 25+).
 *
 * Auth: BSKY_IDENTIFIER / BSKY_APP_PASSWORD (app password) in .env — minted into a
 * session JWT and re-minted on expiry.
 *
 * Resumable: a cursor table stores the last processed DID (stable ORDER BY did);
 * re-running continues. Idempotent upsert on (did, label).
 *
 * Usage:
 *   npx tsx --env-file .env scripts/hailey-proxy-sweep.ts [SINCE]   # SINCE default 2026-03-18
 *   npx tsx --env-file .env scripts/hailey-proxy-sweep.ts --reset    # clear cursor (re-sweep)
 */

import sql from "../src/lib/db/pg";

const HAILEY = "did:plc:saslbwamakedc4h6c5bmshvz";
const PROXY_HEADER = `${HAILEY}#atproto_labeler`;
const APPVIEW = "https://bsky.social";
const PATTERN_BATCH = 20;       // hailey Ozone cap; 25+ returns nothing
const CONCURRENCY = 8;
const MIN_INTERVAL_MS = 115;    // ~8.7 req/s, under the proxy's 3000/300s (=10/s) budget
const TABLE = "plc.hailey_labels";
const CURSOR_TABLE = "plc.hailey_sweep_cursor";

const args = process.argv.slice(2);
const SINCE = args.find(a => !a.startsWith("--")) ?? "2026-03-18";

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

interface RawLabel { src: string; uri: string; val: string; cts: string; neg?: boolean }

// ── auth ──────────────────────────────────────────────────────────────────────
let jwt = "";
async function createSession(): Promise<void> {
  const identifier = process.env.BSKY_IDENTIFIER, password = process.env.BSKY_APP_PASSWORD;
  if (!identifier || !password) throw new Error("BSKY_IDENTIFIER / BSKY_APP_PASSWORD not set in .env");
  const res = await fetch(`${APPVIEW}/xrpc/com.atproto.server.createSession`, {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ identifier, password }),
  });
  if (!res.ok) throw new Error(`createSession failed: HTTP ${res.status}`);
  jwt = ((await res.json()) as { accessJwt: string }).accessJwt;
}

// ── rate limiter (shared across workers) ───────────────────────────────────────
let nextSlot = 0;
async function rateGate(): Promise<void> {
  const now = Date.now();
  const slot = Math.max(now, nextSlot);
  nextSlot = slot + MIN_INTERVAL_MS;
  const wait = slot - now;
  if (wait > 0) await sleep(wait);
}

async function queryLabels(dids: string[]): Promise<RawLabel[]> {
  const params = new URLSearchParams({ sources: HAILEY, limit: "250" });
  for (const d of dids) params.append("uriPatterns", d);
  const url = `${APPVIEW}/xrpc/com.atproto.label.queryLabels?${params}`;
  for (let attempt = 1; attempt <= 5; attempt++) {
    await rateGate();
    try {
      const res = await fetch(url, {
        headers: { Authorization: `Bearer ${jwt}`, "atproto-proxy": PROXY_HEADER },
        signal: AbortSignal.timeout(30_000),
      });
      if (res.status === 401) { await createSession(); continue; }
      if (res.status === 429) { await sleep(2000 * attempt); continue; }
      if (!res.ok) {
        const body = await res.json().catch(() => ({})) as { error?: string };
        if (body.error === "ExpiredToken") { await createSession(); continue; }
        await sleep(1000 * attempt); continue;
      }
      return ((await res.json()) as { labels?: RawLabel[] }).labels ?? [];
    } catch {
      await sleep(1000 * attempt);
    }
  }
  return [];
}

async function upsert(labels: RawLabel[]): Promise<number> {
  const rows = labels.filter(l =>
    l.uri.startsWith("did:") && l.src === HAILEY && !l.neg && !l.val.startsWith("!")
  ).map(l => ({ did: l.uri, label: l.val, labeled_at: l.cts }));
  if (rows.length === 0) return 0;
  const values = rows.map((_, i) => `($${i * 3 + 1}, $${i * 3 + 2}, $${i * 3 + 3})`).join(", ");
  const params = rows.flatMap(r => [r.did, r.label, r.labeled_at]);
  await sql.unsafe(
    `INSERT INTO ${TABLE} (did, label, labeled_at) VALUES ${values}
     ON CONFLICT (did, label) DO UPDATE SET labeled_at = EXCLUDED.labeled_at`,
    params
  );
  return rows.length;
}

async function main() {
  await sql.unsafe(`
    CREATE TABLE IF NOT EXISTS ${TABLE} (
      did text NOT NULL, label text NOT NULL, labeled_at timestamptz,
      PRIMARY KEY (did, label)
    );
    CREATE TABLE IF NOT EXISTS ${CURSOR_TABLE} (
      id int PRIMARY KEY DEFAULT 1, last_did text, updated_at timestamptz
    );
  `);
  if (args.includes("--reset")) {
    await sql.unsafe(`DELETE FROM ${CURSOR_TABLE}`);
    console.log("Reset cursor.");
  }

  const [curRow] = await sql.unsafe<{ last_did: string }[]>(`SELECT last_did FROM ${CURSOR_TABLE} WHERE id = 1`);
  const lastDid = curRow?.last_did ?? "";
  console.log(`\n=== hailey proxy sweep ===`);
  console.log(`Candidates: activated accounts created >= ${SINCE}`);
  console.log(lastDid ? `Resuming after did: ${lastDid}` : `Starting from beginning`);

  const candidates = await sql.unsafe<{ did: string }[]>(
    `SELECT r.did FROM plc.did_in_repo r
     JOIN plc.plc_account_creations p ON p.did = r.did
     WHERE p.created_at >= $1 AND r.did > $2
     ORDER BY r.did`,
    [SINCE, lastDid]
  );
  console.log(`To process: ${candidates.length.toLocaleString()} DIDs (${Math.ceil(candidates.length / PATTERN_BATCH).toLocaleString()} requests)`);
  if (candidates.length === 0) { await sql.end(); return; }

  await createSession();
  console.log(`Authenticated.\n`);

  const batches: string[][] = [];
  for (let i = 0; i < candidates.length; i += PATTERN_BATCH) {
    batches.push(candidates.slice(i, i + PATTERN_BATCH).map(r => r.did));
  }

  const start = Date.now();
  let done = 0, inserted = 0;
  let nextIdx = 0;
  async function worker() {
    while (nextIdx < batches.length) {
      const idx = nextIdx++;
      try {
        // Two statements, not `inserted += await …`: compound assignment reads
        // `inserted` BEFORE the await, so concurrent workers would lose updates.
        const got = await upsert(await queryLabels(batches[idx]));
        inserted += got;
      } catch (err) {
        console.error(`  batch ${idx} failed: ${String(err)}`);
      }
      done++;
      if (done % 500 === 0) {
        const pct = ((done / batches.length) * 100).toFixed(1);
        const rate = (done / ((Date.now() - start) / 1000)).toFixed(1);
        // checkpoint cursor at the last DID of the most recently dispatched batch
        const ckpt = batches[Math.min(nextIdx, batches.length) - 1].at(-1)!;
        await sql.unsafe(
          `INSERT INTO ${CURSOR_TABLE} (id, last_did, updated_at) VALUES (1, $1, NOW())
           ON CONFLICT (id) DO UPDATE SET last_did = EXCLUDED.last_did, updated_at = NOW()`,
          [ckpt]
        );
        console.log(`  ${done.toLocaleString()}/${batches.length.toLocaleString()} (${pct}%) ` +
          `labels=${inserted.toLocaleString()} ${rate} req/s`);
      }
    }
  }
  await Promise.all(Array.from({ length: CONCURRENCY }, worker));

  // final cursor = last candidate overall
  await sql.unsafe(
    `INSERT INTO ${CURSOR_TABLE} (id, last_did, updated_at) VALUES (1, $1, NOW())
     ON CONFLICT (id) DO UPDATE SET last_did = EXCLUDED.last_did, updated_at = NOW()`,
    [candidates.at(-1)!.did]
  );

  console.log(`\n=== Done in ${((Date.now() - start) / 1000 / 60).toFixed(1)} min ===`);
  console.log(`  Requests: ${batches.length.toLocaleString()}`);
  console.log(`  Label rows upserted: ${inserted.toLocaleString()}`);
  const dist = await sql.unsafe<{ label: string; n: number }[]>(
    `SELECT label, COUNT(*)::int AS n FROM ${TABLE} GROUP BY label ORDER BY n DESC`
  );
  console.log(`\nLabel distribution:`);
  for (const r of dist) console.log(`  ${r.label.padEnd(22)} ${r.n.toLocaleString()}`);
  await sql.end();
}

main().catch(err => { console.error(err); process.exit(1); });
