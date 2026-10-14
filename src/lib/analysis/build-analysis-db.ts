/**
 * Materialises cross-schema join results into the `analysis` schema so the
 * analysis scripts (activity-crosstabs, retention-cohorts) don't recompute a
 * multi-table plc join on every invocation.
 *
 * Run after each plc data update:
 *   npm run analysis:build
 *
 * Tables written (schema `analysis`):
 *   excluded_dids  — spam/suspended DIDs (replaces the temp table each script creates)
 *   cohort_base    — pre-joined activated accounts with pds_type precomputed
 *                    (replaces the repeated plc_account_creations × did_in_repo × filters CTE)
 *
 */

import sql from "../db/pg";

const args = process.argv.slice(2);

if (args.includes("--help") || args.includes("-h")) {
  console.log(`
usage: npm run analysis:build -- [flags]

flags:
  --cohort-end DATE     only include accounts created before DATE (YYYY-MM-DD)
                        useful for reproducible snapshots or debugging a specific window
                        e.g. --cohort-end 2026-06-01
  --help                show this message

notes:
  - always run after the plc tables are updated to keep cohort_base current
  - cohort_base excludes spam/impersonation/takendown/deactivated accounts (excluded_dids)
  - without --cohort-end, cohort_base covers all accounts up to now

examples:
  npm run analysis:build
  npm run analysis:build -- --cohort-end 2026-06-01
`);
  process.exit(0);
}

const cohortEndIdx = args.indexOf("--cohort-end");
const cohortEnd = cohortEndIdx >= 0 ? args[cohortEndIdx + 1] : null;

const cohortEndFilter = cohortEnd ? `AND p.created_at < '${cohortEnd}'` : "";

async function main() {
  console.log("Building analysis schema in Postgres...");
  if (cohortEnd) console.log(`  cohort-end: ${cohortEnd}`);

  await sql.unsafe(`CREATE SCHEMA IF NOT EXISTS analysis`);

  // ── excluded_dids ───────────────────────────────────────────────────────────
  console.log("Building excluded_dids...");
  await sql.unsafe(`
    DROP TABLE IF EXISTS analysis.excluded_dids_new;
    CREATE TABLE analysis.excluded_dids_new AS
      SELECT did FROM plc.did_repo_status
      UNION
      SELECT did FROM plc.skywatch_labels
        WHERE label IN ('spam', 'impersonation', 'follow-farming',
                        'inauthentic-fundraising', 'platform-manipulation',
                        'engagement-abuse', 'amplifier')
      UNION
      SELECT did FROM plc.bsky_mod_labels
        WHERE label IN ('spam', 'impersonation');
    ALTER TABLE analysis.excluded_dids_new ADD PRIMARY KEY (did);
  `);
  const [{ n: excludedCount }] = await sql.unsafe(
    `SELECT COUNT(*)::int AS n FROM analysis.excluded_dids_new`
  ) as { n: number }[];
  console.log(`  excluded_dids: ${excludedCount.toLocaleString()} rows`);

  // ── cohort_base ─────────────────────────────────────────────────────────────
  // Activated accounts: created (plc_account_creations) and present in a repo
  // (did_in_repo), with spam/suspended DIDs anti-joined out.
  console.log("Building cohort_base...");
  await sql.unsafe(`
    DROP TABLE IF EXISTS analysis.cohort_base_new;
    CREATE TABLE analysis.cohort_base_new AS
      SELECT
        p.did,
        p.created_at,
        r.pds_url,
        CASE
          WHEN r.pds_url LIKE '%bsky.network%'
            OR r.pds_url LIKE '%bsky.social%' THEN 'bsky'
          ELSE 'indie'
        END AS pds_type
      FROM plc.plc_account_creations p
      JOIN plc.did_in_repo r ON r.did = p.did
      LEFT JOIN analysis.excluded_dids_new ex ON ex.did = p.did
      WHERE ex.did IS NULL ${cohortEndFilter};
    ALTER TABLE analysis.cohort_base_new ADD PRIMARY KEY (did);
    CREATE INDEX idx_cohort_base_created_at_new ON analysis.cohort_base_new(created_at);
    CREATE INDEX idx_cohort_base_pds_type_new   ON analysis.cohort_base_new(pds_type);
  `);
  const [{ n: cohortCount }] = await sql.unsafe(
    `SELECT COUNT(*)::int AS n FROM analysis.cohort_base_new`
  ) as { n: number }[];
  console.log(`  cohort_base: ${cohortCount.toLocaleString()} rows`);

  // ── Atomic swap ───────────────────────────────────────────────────────────────
  console.log("Swapping new tables into place...");
  await sql.begin(async (tx) => {
    await tx.unsafe(`
      DROP TABLE IF EXISTS analysis.excluded_dids;
      DROP TABLE IF EXISTS analysis.cohort_base;
      ALTER TABLE analysis.excluded_dids_new RENAME TO excluded_dids;
      ALTER TABLE analysis.cohort_base_new   RENAME TO cohort_base;
      ALTER INDEX analysis.excluded_dids_new_pkey       RENAME TO excluded_dids_pkey;
      ALTER INDEX analysis.cohort_base_new_pkey         RENAME TO cohort_base_pkey;
      ALTER INDEX analysis.idx_cohort_base_created_at_new RENAME TO idx_cohort_base_created_at;
      ALTER INDEX analysis.idx_cohort_base_pds_type_new   RENAME TO idx_cohort_base_pds_type;
    `);
  });

  console.log("Running ANALYZE...");
  await sql.unsafe(`ANALYZE analysis.excluded_dids`);
  await sql.unsafe(`ANALYZE analysis.cohort_base`);

  console.log("\nDone. analysis schema rebuilt in Postgres.\n");
}

main()
  .catch(err => { console.error(err); process.exitCode = 1; })
  .finally(() => sql.end());
