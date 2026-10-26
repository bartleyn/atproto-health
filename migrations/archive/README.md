# Archived: one-time SQLite → PostgreSQL data migration

These scripts performed the original one-time migration from the legacy SQLite
databases (`plc-migrations.db`, `jetstream-activity.db`, `atproto-health.db`) into
the consolidated PostgreSQL `atproto_health` database. The migration is complete and
PostgreSQL is the single source of truth.

They are kept here **for reference only** and are no longer part of the build or
typecheck path (the `migrations/` directory is excluded in `tsconfig.json`). They
import `better-sqlite3`, which has since been removed from the project's
dependencies — so they will not run as-is. To re-run a migration you would need to
reinstall `better-sqlite3` and have the source `.db` files on disk.

- `migrate-plc.ts` — plc-migrations.db → `plc` schema (CSV→COPY for the ~95M/44M-row tables)
- `migrate-activity.ts` — jetstream-activity.db → `activity` schema
- `migrate-analysis.ts` — analysis tables
- `migrate-health.ts` — health tables
