import { getPlcDb } from "../db/plc-schema";

// SQLite expression: Monday of the ISO week containing a datetime column.
// strftime('%w') returns 0=Sunday…6=Saturday; (dow+6)%7 gives days since Monday.
const WEEK_EXPR = `date(created_at, '-' || ((strftime('%w', created_at) + 6) % 7) || ' days')`;
const MIGRATED_WEEK_EXPR = `date(migrated_at, '-' || ((strftime('%w', migrated_at) + 6) % 7) || ' days')`;

export function aggregatePlc() {
  const db = getPlcDb();

  // ── Monthly ────────────────────────────────────────────────────────────────
  const monthlyCursor = db
    .prepare(`SELECT creations_cursor, migrations_cursor FROM plc_aggregation_cursor WHERE id = 1`)
    .get() as { creations_cursor: string; migrations_cursor: string } | undefined;

  const monthlyCreationsCursor  = monthlyCursor?.creations_cursor  ?? "2020-01-01T00:00:00Z";
  const monthlyMigrationsCursor = monthlyCursor?.migrations_cursor ?? "2020-01-01T00:00:00Z";

  db.prepare(`
    INSERT INTO plc_creation_monthly (pds_url, month, count)
    SELECT pds_url, substr(created_at, 1, 7) AS month, COUNT(*) AS count
    FROM plc_account_creations
    WHERE created_at > ?
    GROUP BY pds_url, month
    ON CONFLICT (pds_url, month) DO UPDATE SET count = count + excluded.count
  `).run(monthlyCreationsCursor);

  db.prepare(`
    INSERT INTO plc_migration_monthly (from_pds, to_pds, month, count)
    SELECT from_pds, to_pds, substr(migrated_at, 1, 7) AS month, COUNT(*) AS count
    FROM plc_migrations
    WHERE migrated_at > ?
    GROUP BY from_pds, to_pds, month
    ON CONFLICT (from_pds, to_pds, month) DO UPDATE SET count = count + excluded.count
  `).run(monthlyMigrationsCursor);

  // ── Weekly ─────────────────────────────────────────────────────────────────
  const weeklyCursor = db
    .prepare(`SELECT creations_cursor, migrations_cursor FROM plc_aggregation_weekly_cursor WHERE id = 1`)
    .get() as { creations_cursor: string; migrations_cursor: string } | undefined;

  const weeklyCreationsCursor  = weeklyCursor?.creations_cursor  ?? "2020-01-01T00:00:00Z";
  const weeklyMigrationsCursor = weeklyCursor?.migrations_cursor ?? "2020-01-01T00:00:00Z";

  db.prepare(`
    INSERT INTO plc_creation_weekly (pds_url, week, count)
    SELECT pds_url, ${WEEK_EXPR} AS week, COUNT(*) AS count
    FROM plc_account_creations
    WHERE created_at > ?
    GROUP BY pds_url, week
    ON CONFLICT (pds_url, week) DO UPDATE SET count = count + excluded.count
  `).run(weeklyCreationsCursor);

  db.prepare(`
    INSERT INTO plc_migration_weekly (from_pds, to_pds, week, count)
    SELECT from_pds, to_pds, ${MIGRATED_WEEK_EXPR} AS week, COUNT(*) AS count
    FROM plc_migrations
    WHERE migrated_at > ?
    GROUP BY from_pds, to_pds, week
    ON CONFLICT (from_pds, to_pds, week) DO UPDATE SET count = count + excluded.count
  `).run(weeklyMigrationsCursor);

  // ── Update cursors ─────────────────────────────────────────────────────────
  const now = new Date().toISOString();

  db.prepare(`
    INSERT INTO plc_aggregation_cursor (id, creations_cursor, migrations_cursor, updated_at)
    VALUES (1, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      creations_cursor = excluded.creations_cursor,
      migrations_cursor = excluded.migrations_cursor,
      updated_at = excluded.updated_at
  `).run(now, now, now);

  db.prepare(`
    INSERT INTO plc_aggregation_weekly_cursor (id, creations_cursor, migrations_cursor, updated_at)
    VALUES (1, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      creations_cursor = excluded.creations_cursor,
      migrations_cursor = excluded.migrations_cursor,
      updated_at = excluded.updated_at
  `).run(now, now, now);

  console.log(`Aggregated PLC data (monthly + weekly) up to ${now}`);
}
