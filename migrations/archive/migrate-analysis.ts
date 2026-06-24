/**
 * One-time data migration: analysis.db (SQLite) → analysis schema (Postgres)
 *
 * Safe to re-run — all inserts use ON CONFLICT DO NOTHING.
 * Run from the project root:
 *   npx tsx --env-file .env migrations/migrate-analysis.ts
 *
 * NOTE: analysis.db is a derived/materialized cache built by analysis:build.
 * Once the plc schema is fully migrated to Postgres you can skip this and
 * run a Postgres-native rebuild instead (INSERT ... SELECT from plc.*).
 *
 * Row counts (approx):
 *   excluded_dids  ~3.4M
 *   cohort_base    ~40M
 */

import Database from "better-sqlite3";
import postgres from "postgres";
import path from "path";

const BATCH_SIZE = 10_000;

const SQLITE_DIR = path.resolve(process.cwd(), "../atproto-health");
const db = new Database(path.join(SQLITE_DIR, "analysis.db"), { readonly: true });
const sql = postgres(process.env.DATABASE_URL!);

function progress(label: string, done: number, total: number) {
  const pct = total > 0 ? ((done / total) * 100).toFixed(1) : "?";
  process.stdout.write(`\r  ${label}: ${done.toLocaleString()} / ${total.toLocaleString()} (${pct}%)`);
}

async function migrateExcludedDids() {
  console.log("\n── excluded_dids ──────────────────────────────────────────────────");
  const total = (db.prepare("SELECT COUNT(*) AS n FROM excluded_dids").get() as { n: number }).n;
  const before = (await sql`SELECT COUNT(*)::int AS n FROM analysis.excluded_dids`)[0].n;
  console.log(`  SQLite rows: ${total.toLocaleString()} | Postgres before: ${before.toLocaleString()}`);

  const stmt = db.prepare("SELECT did FROM excluded_dids ORDER BY did");
  let inserted = 0;
  let batch: { did: string }[] = [];

  for (const row of stmt.iterate() as Iterable<{ did: string }>) {
    batch.push({ did: row.did });
    if (batch.length >= BATCH_SIZE) {
      const res = await sql`
        INSERT INTO analysis.excluded_dids ${sql(batch)}
        ON CONFLICT (did) DO NOTHING
      `;
      inserted += res.count;
      batch = [];
      progress("excluded_dids", inserted + before, total);
    }
  }
  if (batch.length) {
    const res = await sql`
      INSERT INTO analysis.excluded_dids ${sql(batch)}
      ON CONFLICT (did) DO NOTHING
    `;
    inserted += res.count;
  }

  process.stdout.write("\n");
  const after = (await sql`SELECT COUNT(*)::int AS n FROM analysis.excluded_dids`)[0].n;
  console.log(`  inserted: ${inserted.toLocaleString()} | total now: ${after.toLocaleString()}`);
}

async function migrateCohortBase() {
  console.log("\n── cohort_base ────────────────────────────────────────────────────");
  const total = (db.prepare("SELECT COUNT(*) AS n FROM cohort_base").get() as { n: number }).n;
  const before = (await sql`SELECT COUNT(*)::int AS n FROM analysis.cohort_base`)[0].n;
  console.log(`  SQLite rows: ${total.toLocaleString()} | Postgres before: ${before.toLocaleString()}`);

  const stmt = db.prepare(
    "SELECT did, created_at, pds_url, pds_type FROM cohort_base ORDER BY did"
  );
  let inserted = 0;
  let batch: { did: string; created_at: string; pds_url: string; pds_type: string }[] = [];

  for (const row of stmt.iterate() as Iterable<{ did: string; created_at: string; pds_url: string; pds_type: string }>) {
    batch.push({
      did:        row.did,
      created_at: row.created_at,
      pds_url:    row.pds_url,
      pds_type:   row.pds_type,
    });
    if (batch.length >= BATCH_SIZE) {
      const res = await sql`
        INSERT INTO analysis.cohort_base ${sql(batch)}
        ON CONFLICT (did) DO NOTHING
      `;
      inserted += res.count;
      batch = [];
      progress("cohort_base", inserted + before, total);
    }
  }
  if (batch.length) {
    const res = await sql`
      INSERT INTO analysis.cohort_base ${sql(batch)}
      ON CONFLICT (did) DO NOTHING
    `;
    inserted += res.count;
  }

  process.stdout.write("\n");
  const after = (await sql`SELECT COUNT(*)::int AS n FROM analysis.cohort_base`)[0].n;
  console.log(`  inserted: ${inserted.toLocaleString()} | total now: ${after.toLocaleString()}`);
}

async function main() {
  console.log("=== analysis schema migration: SQLite → Postgres ===");
  console.log(`Source: ${path.join(SQLITE_DIR, "analysis.db")}`);

  await migrateExcludedDids();
  await migrateCohortBase();

  console.log("\n=== Done ===\n");
  await sql.end();
  db.close();
}

main().catch(err => { console.error(err); process.exit(1); });
