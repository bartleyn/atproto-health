/**
 * One-time data migration: atproto-health.db (SQLite) → health schema (Postgres)
 *
 * Safe to re-run — all inserts use ON CONFLICT DO NOTHING.
 * Run from the project root:
 *   npx tsx --env-file .env migrations/migrate-health.ts
 */

import Database from "better-sqlite3";
import postgres from "postgres";
import path from "path";

const BATCH_SIZE = 2_000;

const SQLITE_DIR = path.resolve(process.cwd(), "../atproto-health");
const sqliteDb = new Database(path.join(SQLITE_DIR, "atproto-health.db"), { readonly: true });
const sql = postgres(process.env.DATABASE_URL!);

function parseJsonOrNull(val: string | null): unknown {
  if (val === null || val === undefined) return null;
  try { return JSON.parse(val); } catch { return null; }
}

async function migrateTable(name: string, fn: () => Promise<number>) {
  const before = (await sql`SELECT COUNT(*)::int AS n FROM health.${sql(name)}`)[0].n;
  const inserted = await fn();
  const after = (await sql`SELECT COUNT(*)::int AS n FROM health.${sql(name)}`)[0].n;
  console.log(`  ${name}: ${inserted} inserted, ${after} total (was ${before})`);
}

async function main() {
  console.log("\n=== health schema migration: SQLite → Postgres ===\n");

  // ── collection_runs ──────────────────────────────────────────────────────────
  await migrateTable("collection_runs", async () => {
    const rows = sqliteDb.prepare(`SELECT * FROM collection_runs ORDER BY id`).all() as any[];
    if (!rows.length) return 0;
    const mapped = rows.map(r => ({
      id: r.id,
      started_at: r.started_at ?? null,
      completed_at: r.completed_at ?? null,
      source: r.source,
      status: r.status,
      metadata: r.metadata ?? null,
    }));
    const result = await sql`
      INSERT INTO health.collection_runs ${sql(mapped)}
      ON CONFLICT (id) DO NOTHING
    `;
    return result.count;
  });

  // ── pds_instances ────────────────────────────────────────────────────────────
  await migrateTable("pds_instances", async () => {
    const rows = sqliteDb.prepare(`SELECT * FROM pds_instances ORDER BY id`).all() as any[];
    if (!rows.length) return 0;
    const mapped = rows.map(r => ({
      id: r.id,
      url: r.url,
      first_seen_at: r.first_seen_at ?? null,
    }));
    const result = await sql`
      INSERT INTO health.pds_instances ${sql(mapped)}
      ON CONFLICT (id) DO NOTHING
    `;
    return result.count;
  });

  // ── pds_snapshots (batched — 263k rows) ──────────────────────────────────────
  await migrateTable("pds_snapshots", async () => {
    const rows = sqliteDb.prepare(`SELECT * FROM pds_snapshots ORDER BY id`).all() as any[];
    let inserted = 0;
    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const batch = rows.slice(i, i + BATCH_SIZE).map(r => ({
        id: r.id,
        pds_id: r.pds_id,
        collected_at: r.collected_at ?? null,
        run_id: r.run_id ?? null,
        version: r.version ?? null,
        invite_code_required: r.invite_code_required ?? null,
        is_online: r.is_online ?? null,
        error_at: r.error_at ?? null,
        did: r.did ?? null,
        available_domains: parseJsonOrNull(r.available_domains),
        contact: parseJsonOrNull(r.contact),
        links: parseJsonOrNull(r.links),
        user_count_total: r.user_count_total ?? null,
        user_count_active: r.user_count_active ?? null,
        ip_address: r.ip_address ?? null,
        country: r.country ?? null,
        country_code: r.country_code ?? null,
        region: r.region ?? null,
        city: r.city ?? null,
        latitude: r.latitude ?? null,
        longitude: r.longitude ?? null,
        isp: r.isp ?? null,
        org: r.org ?? null,
        as_number: r.as_number ?? null,
        hosting_provider: r.hosting_provider ?? null,
      }));
      const result = await sql`
        INSERT INTO health.pds_snapshots ${sql(batch)}
        ON CONFLICT (id) DO NOTHING
      `;
      inserted += result.count;
      process.stdout.write(`\r  pds_snapshots: ${i + batch.length}/${rows.length}...`);
    }
    process.stdout.write("\n");
    return inserted;
  });

  // ── github_stats ─────────────────────────────────────────────────────────────
  await migrateTable("github_stats", async () => {
    const rows = sqliteDb.prepare(`SELECT * FROM github_stats ORDER BY id`).all() as any[];
    if (!rows.length) return 0;
    const mapped = rows.map(r => ({
      id: r.id,
      collected_at: r.collected_at ?? null,
      query: r.query,
      repo_count: r.repo_count,
      top_repos: parseJsonOrNull(r.top_repos),
    }));
    const result = await sql`
      INSERT INTO health.github_stats ${sql(mapped)}
      ON CONFLICT (id) DO NOTHING
    `;
    return result.count;
  });

  // ── pds_manual_geo ───────────────────────────────────────────────────────────
  await migrateTable("pds_manual_geo", async () => {
    const rows = sqliteDb.prepare(`SELECT * FROM pds_manual_geo`).all() as any[];
    if (!rows.length) return 0;
    const mapped = rows.map(r => ({
      url: r.url,
      city: r.city ?? null,
      country: r.country ?? null,
      latitude: r.latitude,
      longitude: r.longitude,
      org: r.org ?? null,
      note: r.note ?? null,
    }));
    const result = await sql`
      INSERT INTO health.pds_manual_geo ${sql(mapped)}
      ON CONFLICT (url) DO NOTHING
    `;
    return result.count;
  });

  // ── firehose_samples ─────────────────────────────────────────────────────────
  await migrateTable("firehose_samples", async () => {
    const rows = sqliteDb.prepare(`SELECT * FROM firehose_samples ORDER BY id`).all() as any[];
    if (!rows.length) return 0;
    const mapped = rows.map(r => ({
      id: r.id,
      sampled_at: r.sampled_at ?? null,
      duration_ms: r.duration_ms,
      total_events: r.total_events,
      total_interactions: r.total_interactions,
      resolved_interactions: r.resolved_interactions,
      cross_pds: r.cross_pds,
      same_pds: r.same_pds,
      events_per_second: r.events_per_second,
      by_type: parseJsonOrNull(r.by_type),
      federation: parseJsonOrNull(r.federation),
      top_cross_pds_pairs: parseJsonOrNull(r.top_cross_pds_pairs),
    }));
    const result = await sql`
      INSERT INTO health.firehose_samples ${sql(mapped)}
      ON CONFLICT (id) DO NOTHING
    `;
    return result.count;
  });

  // ── Sync sequences so future inserts don't collide with migrated IDs ─────────
  console.log("\nSyncing sequences...");
  await sql`SELECT setval('health.collection_runs_id_seq', (SELECT MAX(id) FROM health.collection_runs))`;
  await sql`SELECT setval('health.pds_instances_id_seq',   (SELECT MAX(id) FROM health.pds_instances))`;
  await sql`SELECT setval('health.pds_snapshots_id_seq',   (SELECT MAX(id) FROM health.pds_snapshots))`;
  await sql`SELECT setval('health.github_stats_id_seq',    (SELECT MAX(id) FROM health.github_stats))`;
  await sql`SELECT setval('health.firehose_samples_id_seq',(SELECT MAX(id) FROM health.firehose_samples))`;
  console.log("  Done.\n");

  await sql.end();
  sqliteDb.close();
  console.log("=== Migration complete ===\n");
}

main().catch(err => { console.error(err); process.exit(1); });
