/**
 * Pre-computes the /analytics page data and writes it to cache/analytics.json.
 * Run after `npm run analysis:marts` so the marts it reads are current.
 *
 * Usage: npm run analysis:cache
 */

import { mkdirSync, writeFileSync } from "fs";
import path from "path";
import sql from "../db/pg";

const CACHE_DIR = path.join(process.cwd(), "cache");
mkdirSync(CACHE_DIR, { recursive: true });

async function main() {
  console.time("compute analytics cache");

  const [wauByCohort, dauMauTrend, retentionByCohort] = await Promise.all([
    sql`
      SELECT week_start::text AS week_start, age_bucket, wau
      FROM analysis.wau_by_cohort
      ORDER BY week_start, age_bucket
    `,
    sql`
      SELECT date, dau, mau, dau_mau_pct
      FROM analysis.dau_mau_daily
      ORDER BY date
    `,
    sql`
      SELECT creation_date::text AS creation_date, pds_type, cohort_size,
             d1_n, d1_pct, d3_n, d3_pct, d7_n, d7_pct
      FROM analysis.retention_cohorts
      ORDER BY creation_date, pds_type
    `,
  ]);

  const dauMauSnapshot = dauMauTrend.length > 0 ? dauMauTrend[dauMauTrend.length - 1] : null;

  console.timeEnd("compute analytics cache");

  const data = { wauByCohort, dauMauTrend, dauMauSnapshot, retentionByCohort };

  const file = path.join(CACHE_DIR, "analytics.json");
  writeFileSync(file, JSON.stringify({ data, writtenAt: new Date().toISOString() }, null, 0));
  console.log(`Written: ${file}`);

  await sql.end();
}

main().catch(e => { console.error(e); process.exit(1); });
