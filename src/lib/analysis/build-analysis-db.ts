/**
 * Materialises cross-DB join results into analysis.db so the analysis scripts
 * don't recompute a 4-table plc cross-DB join on every invocation.
 *
 * Run this after each plc-migrations.db update:
 *   npx tsx src/lib/analysis/build-analysis-db.ts
 *
 * Tables written:
 *   excluded_dids  — spam/suspended DIDs (replaces the temp table each script creates)
 *   cohort_base    — pre-joined activated accounts with pds_type precomputed
 *                    (replaces the repeated plc_account_creations × did_in_repo × filters CTE)
 */

import Database from "better-sqlite3";
import path from "path";

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
  - always run after plc-migrations.db is updated to keep cohort_base current
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

const ANALYSIS_DB_PATH = path.join(process.cwd(), "analysis.db");
const PLC_DB_PATH      = path.join(process.cwd(), "plc-migrations.db");

console.log("Opening analysis.db...");
if (cohortEnd) console.log(`  cohort-end: ${cohortEnd}`);

const db = new Database(ANALYSIS_DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("synchronous = NORMAL");
db.pragma("temp_store = MEMORY");
db.pragma("cache_size = -131072");  // 128 MB

db.exec(`ATTACH DATABASE '${PLC_DB_PATH}' AS plc`);

console.log("Building excluded_dids...");
db.exec(`
  DROP TABLE IF EXISTS excluded_dids;
  CREATE TABLE excluded_dids AS
    SELECT did FROM plc.did_repo_status
    UNION
    SELECT did FROM plc.skywatch_labels WHERE label IN ('spam', 'impersonation');
  CREATE INDEX idx_excluded_dids_did ON excluded_dids(did);
`);
const { excluded_count } = db.prepare(
  `SELECT COUNT(*) AS excluded_count FROM excluded_dids`
).get() as { excluded_count: number };
console.log(`  excluded_dids: ${excluded_count.toLocaleString()} rows`);

// Build cohort_base in two steps to avoid a slow cross-DB three-way join.
// Step 1: join only within plc (single attached-DB, indices work).
// Step 2: delete excluded DIDs locally (analysis.db join, fast with local index).
const cohortEndFilter = cohortEnd ? `AND p.created_at < '${cohortEnd}'` : "";

console.log("Building cohort_base (step 1/2: plc join)...");
db.exec(`
  DROP TABLE IF EXISTS cohort_base;
  CREATE TABLE cohort_base AS
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
    INNER JOIN plc.did_in_repo r ON r.did = p.did
    WHERE 1=1 ${cohortEndFilter};
  CREATE INDEX idx_cohort_base_did ON cohort_base(did);
`);
console.log("Building cohort_base (step 2/2: removing excluded DIDs)...");
db.exec(`
  DELETE FROM cohort_base WHERE did IN (SELECT did FROM excluded_dids);
  CREATE INDEX idx_cohort_base_created_at ON cohort_base(created_at);
`);
const { cohort_count } = db.prepare(
  `SELECT COUNT(*) AS cohort_count FROM cohort_base`
).get() as { cohort_count: number };
console.log(`  cohort_base: ${cohort_count.toLocaleString()} rows`);

console.log("Running ANALYZE...");
db.exec(`ANALYZE`);

db.close();
console.log(`\nDone. analysis.db written to ${ANALYSIS_DB_PATH}\n`);
