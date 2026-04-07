import { getPlcDb} from "../db/plc-schema";

export function aggregatePlc() {
    const db = getPlcDb();

    // get cursor
    const cursor = db.prepare(`SELECT creations_cursor, migrations_cursor FROM plc_aggregation_cursor WHERE id = 1`).get() as
        | { creations_cursor: string; migrations_cursor: string }
        | undefined;

    const creationsCursor = cursor?.creations_cursor ?? "2020-01-01T00:00:00Z";
    const migrationsCursor = cursor?.migrations_cursor ?? "2020-01-01T00:00:00Z";

    // aggregate account creations into plc_creation_monthly
    db.prepare(`
        INSERT INTO plc_creation_monthly (pds_url, month, count)
        SELECT pds_url, substr(created_at, 1, 7) AS month, COUNT(*) AS count
        FROM plc_account_creations
        WHERE created_at > ?
        GROUP BY pds_url, month
        ON CONFLICT (pds_url, month) DO UPDATE SET count = count + excluded.count
        `).run(creationsCursor);


    // aggregate new migrations since cursor into plc_migration_monthly

    db.prepare(`
        INSERT INTO plc_migration_monthly (from_pds, to_pds, month, count)
        SELECT from_pds, to_pds, substr(migrated_at, 1, 7) AS month, COUNT(*) AS count
        FROM plc_migrations
        WHERE migrated_at > ?
        GROUP BY from_pds, to_pds, month
        ON CONFLICT (from_pds, to_pds, month) DO UPDATE SET count = count + excluded.count
        `).run(migrationsCursor);

    // update cursors
    const now = new Date().toISOString();
    db.prepare(`
        INSERT INTO plc_aggregation_cursor (id, creations_cursor, migrations_cursor, updated_at)
        VALUES (1, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET creations_cursor = excluded.creations_cursor, migrations_cursor = excluded.migrations_cursor, updated_at = excluded.updated_at
    `).run(now, now, now);

    console.log(`Aggregated PLC data up to ${now}`);
}