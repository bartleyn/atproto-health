/**
 * One-time data migration: jetstream-activity.db (SQLite) → activity schema (Postgres)
 *
 * Safe to re-run — all inserts use ON CONFLICT DO NOTHING.
 * Run from the project root:
 *   npx tsx --env-file .env migrations/migrate-activity.ts
 *
 * Row counts (approx):
 *   did_activity_daily         ~83M   ← streamed in batches
 *   did_langs                  ~2.8M
 *   collection_activity        ~149K
 *   score_dlq                  ~89K
 *   delete_events_daily        ~3.4K
 *   feed_generators            ~6.8K
 *   feed_generator_likes_daily ~9.9K
 *   lang_stats                 small
 *   pds_activity_summary       small
 *   jetstream_cursor           1 row
 */

import Database from "better-sqlite3";
import postgres from "postgres";
const BATCH_SIZE = 10_000;

const SQLITE_PATH = "/Volumes/miniext/atproto-dbs/jetstream-activity.db";
const db = new Database(SQLITE_PATH, { readonly: true });
const sql = postgres(process.env.DATABASE_URL!);

function progress(label: string, done: number, total: number) {
  const pct = total > 0 ? ((done / total) * 100).toFixed(1) : "?";
  process.stdout.write(`\r  ${label}: ${done.toLocaleString()} / ${total.toLocaleString()} (${pct}%)`);
}

async function migrateSmall(
  sqliteTable: string,
  pgTable: string,
  mapper: (row: any) => Record<string, unknown>,
  conflictKey?: string,
) {
  console.log(`\n── ${pgTable} ──`);
  const rows = (db.prepare(`SELECT * FROM ${sqliteTable}`).all() as any[]).map(mapper);
  if (!rows.length) { console.log("  empty, skipping"); return; }
  const res = await sql`
    INSERT INTO activity.${sql(pgTable)} ${sql(rows)}
    ON CONFLICT DO NOTHING
  `;
  const after = (await sql`SELECT COUNT(*)::int AS n FROM activity.${sql(pgTable)}`)[0].n;
  console.log(`  inserted: ${res.count} | total: ${after.toLocaleString()}`);
}

async function migrateStreamed(
  sqliteTable: string,
  pgTable: string,
  mapper: (row: any) => Record<string, unknown>,
) {
  console.log(`\n── ${pgTable} ──`);
  const total = (db.prepare(`SELECT COUNT(*) AS n FROM ${sqliteTable}`).get() as { n: number }).n;
  const before = (await sql`SELECT COUNT(*)::int AS n FROM activity.${sql(pgTable)}`)[0].n;
  console.log(`  SQLite: ${total.toLocaleString()} | Postgres before: ${before.toLocaleString()}`);

  const stmt = db.prepare(`SELECT * FROM ${sqliteTable}`);
  let inserted = 0;
  let batch: Record<string, unknown>[] = [];

  for (const row of stmt.iterate() as Iterable<any>) {
    batch.push(mapper(row));
    if (batch.length >= BATCH_SIZE) {
      const res = await sql`
        INSERT INTO activity.${sql(pgTable)} ${sql(batch)}
        ON CONFLICT DO NOTHING
      `;
      inserted += res.count;
      batch = [];
      progress(pgTable, inserted + before, total);
    }
  }
  if (batch.length) {
    const res = await sql`
      INSERT INTO activity.${sql(pgTable)} ${sql(batch)}
      ON CONFLICT DO NOTHING
    `;
    inserted += res.count;
  }

  process.stdout.write("\n");
  const after = (await sql`SELECT COUNT(*)::int AS n FROM activity.${sql(pgTable)}`)[0].n;
  console.log(`  inserted: ${inserted.toLocaleString()} | total now: ${after.toLocaleString()}`);
}

async function main() {
  console.log("=== activity schema migration: SQLite → Postgres ===");
  console.log(`Source: ${SQLITE_PATH}\n`);

  // ── Small tables ────────────────────────────────────────────────────────────

  await migrateSmall("jetstream_cursor", "jetstream_cursor", r => ({
    id: r.id,
    cursor: BigInt(r.cursor),
    updated_at: r.updated_at,
  }));

  await migrateSmall("delete_events_daily", "delete_events_daily", r => ({
    date: r.date,
    event_type: r.event_type,
    count: r.count,
  }));

  await migrateSmall("starterpack_joins_daily", "starterpack_joins_daily", r => ({
    starterpack_uri: r.starterpack_uri,
    date: r.date,
    count: r.count,
  }));

  await migrateSmall("lang_stats", "lang_stats", r => ({
    date: r.date,
    total_posts: r.total_posts,
    tagged_posts: r.tagged_posts,
    updated_at: r.updated_at,
  }));

  await migrateSmall("pds_activity_summary", "pds_activity_summary", r => ({
    pds_url: r.pds_url,
    window_days: r.window_days,
    active_dids: r.active_dids,
    poster_dids: r.poster_dids,
    liker_dids: r.liker_dids,
    reposter_dids: r.reposter_dids,
    follower_dids: r.follower_dids,
    updated_at: r.updated_at,
  }));

  await migrateSmall("feed_generators", "feed_generators", r => ({
    uri: r.uri,
    creator_did: r.creator_did,
    display_name: r.display_name ?? null,
    description: r.description ?? null,
    first_seen: r.first_seen,
    deleted_at: r.deleted_at ?? null,
  }));

  await migrateSmall("feed_generator_likes_daily", "feed_generator_likes_daily", r => ({
    feed_uri: r.feed_uri,
    date: r.date,
    likes: r.likes,
  }));

  // ── Medium tables ───────────────────────────────────────────────────────────

  await migrateStreamed("collection_activity", "collection_activity", r => ({
    collection: r.collection,
    did: r.did,
    date: r.date,
    event_count: r.event_count,
  }));

  await migrateStreamed("score_dlq", "score_dlq", r => ({
    id: r.id,
    posts_json: r.posts_json,
    failed_at: r.failed_at,
    attempts: r.attempts,
    last_error: r.last_error ?? null,
    next_retry_at: r.next_retry_at,
  }));

  await migrateStreamed("did_langs", "did_langs", r => ({
    did: r.did,
    lang: r.lang,
    post_count: r.post_count,
    last_seen: r.last_seen,
  }));

  // ── Large table: did_activity_daily (~83M rows) ─────────────────────────────
  await migrateStreamed("did_activity_daily", "did_activity_daily", r => ({
    did: r.did,
    date: r.date,
    activity_types: r.activity_types,
  }));

  // Sync score_dlq sequence so future inserts don't collide with migrated IDs
  console.log("\nSyncing sequences...");
  await sql`SELECT setval('activity.score_dlq_id_seq', (SELECT MAX(id) FROM activity.score_dlq))`;
  console.log("  Done.");

  console.log("\n=== Migration complete ===\n");
  await sql.end();
  db.close();
}

main().catch(err => { console.error(err); process.exit(1); });
