/**
 * One-time data migration: plc-migrations.db (SQLite) → plc schema (Postgres)
 *
 * Safe to re-run — all inserts use ON CONFLICT DO NOTHING.
 * Run from the project root:
 *   npx tsx --env-file .env migrations/migrate-plc.ts
 *
 * The three large tables use CSV → COPY rather than batched INSERT:
 *
 *   plc_did_pds           ~95M rows  → COPY
 *   plc_account_creations ~95M rows  → COPY
 *   did_in_repo           ~44M rows  → COPY
 *
 * Everything else uses batched INSERT (10K rows/batch).
 *
 * COPY workflow (per large table):
 *   1. Dump SQLite table to a temp CSV via sqlite3 CLI
 *   2. Stream that CSV into Postgres with COPY FROM STDIN
 *   3. Delete the temp CSV
 *
 * The CSV dump is the bottleneck — expect 10-30 min per 95M-row table on
 * a USB-attached drive.  If you need to re-run, pass --skip-copy to skip
 * already-populated large tables and only re-run the small ones.
 */

import Database from "better-sqlite3";
import postgres from "postgres";
import { execSync, spawn } from "child_process";
import { createReadStream, unlinkSync, existsSync } from "fs";
import { createInterface } from "readline";
import path from "path";
import os from "os";

// ── Config ──────────────────────────────────────────────────────────────────

const SQLITE_PATH = "/Volumes/miniext/atproto-dbs/plc-migrations.db";
const CSV_DIR     = "/Volumes/miniext/pg_migration";
const BATCH_SIZE  = 10_000;

const args = process.argv.slice(2);
const skipCopy = args.includes("--skip-copy");

const db  = new Database(SQLITE_PATH, { readonly: true });
const sql = postgres(process.env.DATABASE_URL!);

// ── Helpers ─────────────────────────────────────────────────────────────────

function progress(label: string, done: number, total: number) {
  const pct = total > 0 ? ((done / total) * 100).toFixed(1) : "?";
  process.stdout.write(`\r  ${label}: ${done.toLocaleString()} / ${total.toLocaleString()} (${pct}%)`);
}

async function countPg(table: string): Promise<number> {
  return (await sql`SELECT COUNT(*)::int AS n FROM plc.${sql(table)}`)[0].n;
}

// ── Small table helper ───────────────────────────────────────────────────────

async function migrateSmall(
  sqliteTable: string,
  pgTable: string,
  mapper: (row: any) => Record<string, unknown>,
) {
  console.log(`\n── ${pgTable} ──`);
  const rows = (db.prepare(`SELECT * FROM ${sqliteTable}`).all() as any[]).map(mapper);
  if (!rows.length) { console.log("  empty, skipping"); return; }
  const res = await sql`
    INSERT INTO plc.${sql(pgTable)} ${sql(rows)}
    ON CONFLICT DO NOTHING
  `;
  const after = await countPg(pgTable);
  console.log(`  inserted: ${res.count} | total: ${after.toLocaleString()}`);
}

// ── Batched stream helper (mid-size tables) ──────────────────────────────────

async function migrateStreamed(
  sqliteTable: string,
  pgTable: string,
  mapper: (row: any) => Record<string, unknown>,
  batchSize: number = BATCH_SIZE,
) {
  console.log(`\n── ${pgTable} ──`);
  const total = (db.prepare(`SELECT COUNT(*) AS n FROM ${sqliteTable}`).get() as { n: number }).n;
  const before = await countPg(pgTable);
  console.log(`  SQLite: ${total.toLocaleString()} | Postgres before: ${before.toLocaleString()}`);

  const stmt = db.prepare(`SELECT * FROM ${sqliteTable}`);
  let inserted = 0;
  let batch: Record<string, unknown>[] = [];

  for (const row of stmt.iterate() as Iterable<any>) {
    batch.push(mapper(row));
    if (batch.length >= batchSize) {
      const res = await sql`INSERT INTO plc.${sql(pgTable)} ${sql(batch)} ON CONFLICT DO NOTHING`;
      inserted += res.count;
      batch = [];
      progress(pgTable, inserted + before, total);
    }
  }
  if (batch.length) {
    const res = await sql`INSERT INTO plc.${sql(pgTable)} ${sql(batch)} ON CONFLICT DO NOTHING`;
    inserted += res.count;
  }

  process.stdout.write("\n");
  const after = await countPg(pgTable);
  console.log(`  inserted: ${inserted.toLocaleString()} | total now: ${after.toLocaleString()}`);
}

// ── COPY helper (large tables) ───────────────────────────────────────────────
//
// Dumps the SQLite table to CSV, then streams it into Postgres via COPY FROM STDIN.
// The CSV is written to CSV_DIR and deleted afterwards.

async function migrateCopy(opts: {
  sqliteTable: string;
  pgTable: string;
  // Column names in order — must match the SELECT and the Postgres table
  columns: string[];
  // The SELECT query to dump (allows renaming / casting in sqlite3)
  selectSql: string;
}) {
  const { sqliteTable, pgTable, columns, selectSql } = opts;
  console.log(`\n── ${pgTable} (COPY) ──`);

  const before = await countPg(pgTable);
  if (skipCopy && before > 0) {
    console.log(`  --skip-copy: ${before.toLocaleString()} rows already present, skipping`);
    return;
  }

  execSync(`mkdir -p "${CSV_DIR}"`);
  const csvPath = path.join(CSV_DIR, `${pgTable}.csv`);

  // 1. Dump to CSV using the sqlite3 CLI (fastest path — avoids Node overhead)
  console.log(`  Dumping to ${csvPath}...`);
  const dumpStart = Date.now();
  execSync(
    `sqlite3 "${SQLITE_PATH}" -cmd ".headers on" -cmd ".mode csv" -cmd ".output ${csvPath}" "${selectSql}" -cmd ".quit"`,
    { stdio: "inherit" },
  );
  const dumpSecs = ((Date.now() - dumpStart) / 1000).toFixed(1);
  console.log(`  Dump complete in ${dumpSecs}s`);

  // 2. Stream CSV into Postgres via COPY FROM STDIN
  console.log(`  Loading into Postgres via COPY...`);
  const copyStart = Date.now();

  // Use a raw connection from the pool for COPY
  await sql.begin(async (tx) => {
    // Disable indices during load for speed, then re-enable
    await tx`SET session_replication_role = replica`;

    const copyStream = await tx`
      COPY plc.${sql(pgTable)} (${sql(columns)})
      FROM STDIN
      WITH (FORMAT csv, HEADER true)
    `.writable();

    await new Promise<void>((resolve, reject) => {
      const fileStream = createReadStream(csvPath);
      fileStream.on("error", reject);
      copyStream.on("error", reject);
      copyStream.on("finish", resolve);
      fileStream.pipe(copyStream);
    });

    await tx`SET session_replication_role = DEFAULT`;
  });

  const copySecs = ((Date.now() - copyStart) / 1000).toFixed(1);
  const after = await countPg(pgTable);
  console.log(`  COPY complete in ${copySecs}s | total now: ${after.toLocaleString()}`);

  // 3. Clean up CSV
  if (existsSync(csvPath)) unlinkSync(csvPath);
  console.log(`  Cleaned up ${csvPath}`);
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log("=== plc schema migration: SQLite → Postgres ===");
  console.log(`Source: ${SQLITE_PATH}`);
  if (skipCopy) console.log("  --skip-copy: large COPY tables will be skipped if already populated");
  console.log();

  // ── Cursors (single-row) ───────────────────────────────────────────────────

  await migrateSmall("plc_cursor", "plc_cursor", r => ({
    id: r.id, after: r.after, updated_at: r.updated_at,
  }));

  await migrateSmall("plc_creations_cursor", "plc_creations_cursor", r => ({
    id: r.id, after: r.after, updated_at: r.updated_at,
  }));

  await migrateSmall("plc_aggregation_cursor", "plc_aggregation_cursor", r => ({
    id: r.id, creations_cursor: r.creations_cursor, migrations_cursor: r.migrations_cursor, updated_at: r.updated_at,
  }));

  await migrateSmall("plc_aggregation_weekly_cursor", "plc_aggregation_weekly_cursor", r => ({
    id: r.id, creations_cursor: r.creations_cursor, migrations_cursor: r.migrations_cursor, updated_at: r.updated_at,
  }));

  await migrateSmall("active_creation_cursor", "active_creation_cursor", r => ({
    id: r.id, last_scanned_at: r.last_scanned_at, updated_at: r.updated_at,
  }));

  await migrateSmall("plc_heavy_recompute_cursor", "plc_heavy_recompute_cursor", r => ({
    id: r.id, migrations_cursor: r.migrations_cursor, did_scanned_cursor: r.did_scanned_cursor, updated_at: r.updated_at,
  }));

  await migrateSmall("skywatch_labels_cursor", "skywatch_labels_cursor", r => ({
    id: r.id, cursor: r.cursor, updated_at: r.updated_at,
  }));

  await migrateSmall("bsky_mod_labels_cursor", "bsky_mod_labels_cursor", r => ({
    id: r.id, cursor: r.cursor, updated_at: r.updated_at,
  }));

  // ── Small aggregate / lookup tables ────────────────────────────────────────

  await migrateSmall("plc_stats_cache", "plc_stats_cache", r => ({
    id: r.id,
    total_dids: r.total_dids,
    bsky_concentration_pct: r.bsky_concentration_pct,
    unique_migrating_dids: r.unique_migrating_dids,
    updated_at: r.updated_at,
  }));

  await migrateSmall("plc_trajectory_edges", "plc_trajectory_edges", r => ({
    source: r.source, target: r.target, value: r.value,
  }));

  await migrateSmall("plc_migration_hops", "plc_migration_hops", r => ({
    source: r.source, target: r.target, value: r.value,
  }));

  await migrateSmall("did_web_pds", "did_web_pds", r => ({
    did: r.did,
    pds_url: r.pds_url ?? null,
    first_seen: r.first_seen,
    last_seen: r.last_seen,
    resolved_at: r.resolved_at ?? null,
  }));

  await migrateSmall("pds_manual_geo", "pds_manual_geo", r => ({
    url: r.url,
    city: r.city ?? null,
    country: r.country ?? null,
    latitude: r.latitude,
    longitude: r.longitude,
    org: r.org ?? null,
    note: r.note ?? null,
  }));

  // ── Medium aggregate tables (batched stream) ────────────────────────────────

  await migrateStreamed("plc_creation_monthly", "plc_creation_monthly", r => ({
    pds_url: r.pds_url, month: r.month, count: r.count,
  }));

  await migrateStreamed("plc_creation_weekly", "plc_creation_weekly", r => ({
    pds_url: r.pds_url, week: r.week, count: r.count,
  }));

  await migrateStreamed("plc_migration_monthly", "plc_migration_monthly", r => ({
    from_pds: r.from_pds, to_pds: r.to_pds, month: r.month, count: r.count,
  }));

  await migrateStreamed("plc_migration_weekly", "plc_migration_weekly", r => ({
    from_pds: r.from_pds, to_pds: r.to_pds, week: r.week, count: r.count,
  }));

  await migrateStreamed("active_creation_weekly", "active_creation_weekly", r => ({
    pds_url: r.pds_url, week: r.week, count: r.count,
  }));

  await migrateStreamed("pds_lang_summary", "pds_lang_summary", r => ({
    pds_url: r.pds_url, lang: r.lang, dids: r.dids, post_count: r.post_count,
  }));

  await migrateStreamed("skywatch_labels", "skywatch_labels", r => ({
    did: r.did, label: r.label, labeled_at: r.labeled_at,
  }));

  await migrateStreamed("bsky_mod_labels", "bsky_mod_labels", r => ({
    did: r.did, label: r.label, labeled_at: r.labeled_at,
  }));

  // 29 columns × batch = params; stay under 65,534 limit → max ~2,200 rows/batch
  await migrateStreamed("pds_repo_status_snapshots", "pds_repo_status_snapshots", r => ({
    pds_url: r.pds_url,
    snapshot_date: r.snapshot_date,
    active: r.active,
    deactivated: r.deactivated,
    deleted: r.deleted,
    takendown: r.takendown,
    suspended: r.suspended,
    other: r.other,
    total_scanned: r.total_scanned,
    is_sampled: r.is_sampled,
    did_plc_count: r.did_plc_count,
    did_web_count: r.did_web_count,
    is_partial: r.is_partial,
    scanned_at: r.scanned_at ?? null,
    in_directory: r.in_directory ?? 0,
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
    version: r.version ?? null,
    invite_code_required: r.invite_code_required ?? null,
    is_online: r.is_online ?? null,
  }), 2_000);

  await migrateStreamed("did_repo_status", "did_repo_status", r => ({
    did: r.did, status: r.status, pds_url: r.pds_url, scanned_at: r.scanned_at,
  }));

  await migrateStreamed("plc_migrations", "plc_migrations", r => ({
    did: r.did, from_pds: r.from_pds, to_pds: r.to_pds, migrated_at: r.migrated_at,
  }));

  // ── Large tables: CSV → COPY ───────────────────────────────────────────────

  await migrateCopy({
    sqliteTable: "plc_did_pds",
    pgTable:     "plc_did_pds",
    columns:     ["did", "pds_url", "updated_at"],
    selectSql:   "SELECT did, pds_url, updated_at FROM plc_did_pds",
  });

  await migrateCopy({
    sqliteTable: "plc_account_creations",
    pgTable:     "plc_account_creations",
    columns:     ["did", "pds_url", "created_at"],
    selectSql:   "SELECT did, pds_url, created_at FROM plc_account_creations",
  });

  await migrateCopy({
    sqliteTable: "did_in_repo",
    pgTable:     "did_in_repo",
    columns:     ["did", "pds_url", "scanned_at", "first_scanned_at"],
    selectSql:   "SELECT did, pds_url, scanned_at, first_scanned_at FROM did_in_repo",
  });

  // ── Sync sequences ─────────────────────────────────────────────────────────

  console.log("\nSyncing sequences...");
  await sql`SELECT setval('plc.plc_migrations_id_seq', COALESCE((SELECT MAX(id) FROM plc.plc_migrations), 1))`;
  await sql`SELECT setval('plc.pds_repo_status_snapshots_id_seq', COALESCE((SELECT MAX(id) FROM plc.pds_repo_status_snapshots), 1))`;
  console.log("  Done.");

  console.log("\n=== Migration complete ===\n");
  await sql.end();
  db.close();
}

main().catch(err => { console.error(err); process.exit(1); });
