/**
 * One-time setup: move atproto_health DB to miniext tablespace.
 * Run from project root:
 *   npx tsx --env-file .env migrations/setup-tablespace.ts
 */
import postgres from "postgres";
import { readFileSync } from "fs";
import path from "path";

async function main() {
  const admin = postgres("postgresql://nathanbartley@localhost/postgres", { max: 1 });

  // Terminate lingering connections so DROP DATABASE succeeds
  await admin`
    SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity
    WHERE datname = 'atproto_health' AND pid <> pg_backend_pid()
  `;
  console.log("Terminated existing connections to atproto_health");

  await admin.unsafe("DROP DATABASE IF EXISTS atproto_health");
  console.log("Dropped atproto_health");

  await admin.unsafe("DROP TABLESPACE IF EXISTS miniext");
  console.log("Dropped existing miniext tablespace (if any)");

  await admin.unsafe("CREATE TABLESPACE miniext LOCATION '/Volumes/miniext/postgresql'");
  console.log("Created miniext tablespace → /Volumes/miniext/postgresql");

  await admin.unsafe("CREATE DATABASE atproto_health ENCODING 'UTF8' TABLESPACE miniext");
  console.log("Created atproto_health on miniext");

  await admin.end();

  // Apply all 4 schema migration files
  const db = postgres("postgresql://nathanbartley@localhost/atproto_health", { max: 1 });
  const migrationsDir = path.join(process.cwd(), "migrations");

  console.log("\nApplying schemas...");
  for (const file of [
    "001_health_schema.sql",
    "002_plc_schema.sql",
    "003_activity_schema.sql",
    "004_analysis_schema.sql",
  ]) {
    const ddl = readFileSync(path.join(migrationsDir, file), "utf8");
    await db.unsafe(ddl);
    console.log(`  applied ${file}`);
  }

  const schemas = await db`
    SELECT schemaname, count(*)::int AS tables
    FROM pg_tables
    WHERE schemaname IN ('health','plc','activity','analysis')
    GROUP BY schemaname ORDER BY schemaname
  `;
  console.log("\nSchemas created:");
  console.table(schemas);

  await db.end();
  console.log("\nDone — atproto_health is on /Volumes/miniext/postgresql");
}

main().catch(err => { console.error(err); process.exit(1); });
