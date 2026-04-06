/**
 * Builds monthly aggregation tables from raw PLC data.
 *
 * Reads from plc_account_creations and plc_migrations, writes to
 * plc_creation_monthly and plc_migration_monthly. Incremental: only
 * processes months not yet aggregated. The current (partial) month is
 * always skipped and re-aggregated on each run.
 */

import { getPlcDb } from "../db/plc-schema";

function lastCompleteMonth(): string {
  const d = new Date();
  d.setUTCDate(1);
  d.setUTCMonth(d.getUTCMonth() - 1);
  return d.toISOString().slice(0, 7); // 'YYYY-MM'
}

function currentMonth(): string {
  return new Date().toISOString().slice(0, 7);
}

export function aggregatePlc(): { creationMonths: number; migrationMonths: number } {
  const db = getPlcDb();

  const cursor = db
    .prepare(`SELECT creations_through, migrations_through FROM plc_aggregation_cursor WHERE id = 1`)
    .get() as { creations_through: string; migrations_through: string } | undefined;

  const lastComplete = lastCompleteMonth();
  const current = currentMonth();

  // ── Creations ──────────────────────────────────────────────────────────

  // Start from month after last aggregated, or the earliest month in the data
  let creationsFrom: string;
  if (cursor?.creations_through) {
    // Advance one month past the last completed month
    const [y, m] = cursor.creations_through.split("-").map(Number);
    const next = new Date(Date.UTC(y, m, 1)); // m is already 0-indexed after +1
    creationsFrom = next.toISOString().slice(0, 7);
  } else {
    const row = db
      .prepare(`SELECT strftime('%Y-%m', MIN(created_at)) as m FROM plc_account_creations`)
      .get() as { m: string } | undefined;
    creationsFrom = row?.m ?? lastComplete;
  }

  // Always re-aggregate the current (partial) month
  db.prepare(`DELETE FROM plc_creation_monthly WHERE month = ?`).run(current);

  const insertCreations = db.prepare(`
    INSERT OR REPLACE INTO plc_creation_monthly (month, pds_url, count)
    VALUES (?, ?, ?)
  `);

  const creationRows = db
    .prepare(
      `SELECT strftime('%Y-%m', created_at) as month, pds_url, COUNT(*) as count
       FROM plc_account_creations
       WHERE strftime('%Y-%m', created_at) >= ?
       GROUP BY month, pds_url
       ORDER BY month`
    )
    .all(creationsFrom) as Array<{ month: string; pds_url: string; count: number }>;

  let creationMonths = 0;
  const insertCreationsBatch = db.transaction(() => {
    const seen = new Set<string>();
    for (const row of creationRows) {
      insertCreations.run(row.month, row.pds_url, row.count);
      seen.add(row.month);
    }
    creationMonths = seen.size;
  });
  insertCreationsBatch();

  // ── Migrations ─────────────────────────────────────────────────────────

  let migrationsFrom: string;
  if (cursor?.migrations_through) {
    const [y, m] = cursor.migrations_through.split("-").map(Number);
    const next = new Date(Date.UTC(y, m, 1));
    migrationsFrom = next.toISOString().slice(0, 7);
  } else {
    const row = db
      .prepare(`SELECT strftime('%Y-%m', MIN(migrated_at)) as m FROM plc_migrations`)
      .get() as { m: string } | undefined;
    migrationsFrom = row?.m ?? lastComplete;
  }

  db.prepare(`DELETE FROM plc_migration_monthly WHERE month = ?`).run(current);

  const insertMigrations = db.prepare(`
    INSERT OR REPLACE INTO plc_migration_monthly (month, from_pds, to_pds, count)
    VALUES (?, ?, ?, ?)
  `);

  const migrationRows = db
    .prepare(
      `SELECT strftime('%Y-%m', migrated_at) as month, from_pds, to_pds, COUNT(*) as count
       FROM plc_migrations
       WHERE strftime('%Y-%m', migrated_at) >= ?
       GROUP BY month, from_pds, to_pds
       ORDER BY month`
    )
    .all(migrationsFrom) as Array<{ month: string; from_pds: string; to_pds: string; count: number }>;

  let migrationMonths = 0;
  const insertMigrationsBatch = db.transaction(() => {
    const seen = new Set<string>();
    for (const row of migrationRows) {
      insertMigrations.run(row.month, row.from_pds, row.to_pds, row.count);
      seen.add(row.month);
    }
    migrationMonths = seen.size;
  });
  insertMigrationsBatch();

  // ── Update cursor ──────────────────────────────────────────────────────

  db.prepare(`
    INSERT INTO plc_aggregation_cursor (id, creations_through, migrations_through, updated_at)
    VALUES (1, ?, ?, datetime('now'))
    ON CONFLICT(id) DO UPDATE SET
      creations_through = excluded.creations_through,
      migrations_through = excluded.migrations_through,
      updated_at = excluded.updated_at
  `).run(lastComplete, lastComplete);

  return { creationMonths, migrationMonths };
}
