/**
 * D1/D3/D7 retention for new accounts created since the start of activity collection.
 *
 * Cohort = DIDs in both plc_account_creations and did_in_repo (confirmed activated repo),
 * excluding spam/impersonation (skywatch_labels) and suspended/deleted (did_repo_status).
 * These exclusions are already baked into analysis.cohort_base (built by build-analysis-db.ts).
 * Raw PLC creations are ~10x larger due to DIDs allocated but never activated.
 *
 * Usage:
 *   npx tsx --env-file .env src/lib/analysis/retention-cohorts.ts
 *   npx tsx --env-file .env src/lib/analysis/retention-cohorts.ts --start 2026-04-11  # default
 *
 * For each daily creation cohort, reports how many new accounts were active
 * exactly N days later (D1 = day after creation, D3, D7). Broken down by
 * bsky-hosted vs indie PDS.
 *
 * Note: D7 for the most recent complete cohort day may be partial if the
 * activity collector hasn't finished that day yet.
 */

import path from "path";
import fs from "fs";
import sql from "../db/pg";

const args = process.argv.slice(2);
const startIdx = args.indexOf("--start");
const START_DATE = startIdx >= 0 ? args[startIdx + 1] : "2026-04-11";

const RUN_DATE = new Date().toISOString().slice(0, 10);
const OUT_DIR = path.join(process.cwd(), "analysis-output", RUN_DATE);
fs.mkdirSync(OUT_DIR, { recursive: true });

function printTable(title: string, rows: Record<string, unknown>[]) {
  console.log(`\n=== ${title} ===`);
  if (rows.length === 0) { console.log("No data"); return; }
  console.table(rows);
}

function writeCsv(filename: string, rows: Record<string, unknown>[]) {
  if (rows.length === 0) return;
  const headers = Object.keys(rows[0]);
  const lines = [
    headers.join(","),
    ...rows.map(r =>
      headers.map(h => {
        const v = r[h];
        const s = v === null || v === undefined ? "" : String(v);
        return s.includes(",") || s.includes('"') || s.includes("\n")
          ? `"${s.replace(/"/g, '""')}"` : s;
      }).join(",")
    ),
  ];
  const outPath = path.join(OUT_DIR, filename);
  fs.writeFileSync(outPath, lines.join("\n") + "\n");
  console.log(`  → wrote ${outPath}`);
}

function report(title: string, filename: string, rows: Record<string, unknown>[]) {
  printTable(title, rows);
  writeCsv(filename, rows);
}

async function main() {
  // Earliest and latest dates with activity data — used to bound the cohort window.
  // did_activity_daily.date is a text 'YYYY-MM-DD' column, so MIN/MAX sort lexically
  // (identical to date order for ISO strings).
  const [{ min_date, max_date }] = await sql<{ min_date: string; max_date: string }[]>`
    SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM activity.did_activity_daily
  `;

  // D7 requires data 7 days out, so the last valid cohort day is max_date - 7.
  const lastValidCohortDay = new Date(max_date);
  lastValidCohortDay.setDate(lastValidCohortDay.getDate() - 7);
  const END_DATE = lastValidCohortDay.toISOString().slice(0, 10);

  console.log(`\nRetention cohorts — created ${START_DATE} → ${END_DATE}`);
  console.log(`Activity data: ${min_date} → ${max_date}`);
  if (max_date > END_DATE) {
    const partialD7Day = new Date(max_date);
    partialD7Day.setDate(partialD7Day.getDate() - 6);
    console.log(`  Note: D7 for ${partialD7Day.toISOString().slice(0, 10)} cohort falls on ${max_date} (may be partial)`);
  }

  // ── 1. D1/D3/D7 retention by cohort day × PDS type ───────────────────────────
  // SQLite date()/'+N day' arithmetic ported to Postgres date math: created_at::date
  // gives a date, (date + N) stays a date, and ::text compares against the text
  // activity.did_activity_daily.date column. Both UNION branches cast to identical
  // types (::int / ::float8) — Postgres UNION requires matching column types.

  const retentionByDay = await sql<Record<string, unknown>[]>`
    WITH cohorts AS (
      SELECT
        c.created_at::date AS creation_date,
        c.did,
        c.pds_type
      FROM analysis.cohort_base c
      WHERE c.created_at::date BETWEEN ${START_DATE}::date AND ${END_DATE}::date
    ),
    joined AS (
      SELECT
        c.creation_date,
        c.did,
        c.pds_type,
        MAX(CASE WHEN a.date = (c.creation_date + 1)::text THEN 1 ELSE 0 END) AS d1,
        MAX(CASE WHEN a.date = (c.creation_date + 3)::text THEN 1 ELSE 0 END) AS d3,
        MAX(CASE WHEN a.date = (c.creation_date + 7)::text THEN 1 ELSE 0 END) AS d7
      FROM cohorts c
      LEFT JOIN activity.did_activity_daily a
        ON a.did = c.did
        AND a.date IN (
          (c.creation_date + 1)::text,
          (c.creation_date + 3)::text,
          (c.creation_date + 7)::text
        )
      GROUP BY c.creation_date, c.did, c.pds_type
    )
    SELECT
      creation_date::text                              AS creation_date,
      pds_type,
      COUNT(*)::int                                    AS cohort_size,
      SUM(d1)::int                                     AS d1_n,
      ROUND(100.0 * SUM(d1) / COUNT(*), 2)::float8     AS d1_pct,
      SUM(d3)::int                                     AS d3_n,
      ROUND(100.0 * SUM(d3) / COUNT(*), 2)::float8     AS d3_pct,
      SUM(d7)::int                                     AS d7_n,
      ROUND(100.0 * SUM(d7) / COUNT(*), 2)::float8     AS d7_pct
    FROM joined
    GROUP BY creation_date, pds_type
    UNION ALL
    SELECT
      creation_date::text,
      '~ total',
      COUNT(*)::int,
      SUM(d1)::int, ROUND(100.0 * SUM(d1) / COUNT(*), 2)::float8,
      SUM(d3)::int, ROUND(100.0 * SUM(d3) / COUNT(*), 2)::float8,
      SUM(d7)::int, ROUND(100.0 * SUM(d7) / COUNT(*), 2)::float8
    FROM joined
    GROUP BY creation_date
    ORDER BY creation_date, pds_type
  `;

  report("D1/D3/D7 Retention by Cohort Day × PDS Type", "retention_by_day.csv", retentionByDay);

  // ── 2. Aggregate retention across the full cohort window ──────────────────────

  const retentionAggregate = await sql<Record<string, unknown>[]>`
    WITH cohorts AS (
      SELECT
        c.created_at::date AS creation_date,
        c.did,
        c.pds_type
      FROM analysis.cohort_base c
      WHERE c.created_at::date BETWEEN ${START_DATE}::date AND ${END_DATE}::date
    ),
    joined AS (
      SELECT
        c.did,
        c.pds_type,
        MAX(CASE WHEN a.date = (c.creation_date + 1)::text THEN 1 ELSE 0 END) AS d1,
        MAX(CASE WHEN a.date = (c.creation_date + 3)::text THEN 1 ELSE 0 END) AS d3,
        MAX(CASE WHEN a.date = (c.creation_date + 7)::text THEN 1 ELSE 0 END) AS d7
      FROM cohorts c
      LEFT JOIN activity.did_activity_daily a
        ON a.did = c.did
        AND a.date IN (
          (c.creation_date + 1)::text,
          (c.creation_date + 3)::text,
          (c.creation_date + 7)::text
        )
      GROUP BY c.creation_date, c.did, c.pds_type
    )
    SELECT
      pds_type,
      COUNT(*)::int                                    AS cohort_size,
      SUM(d1)::int                                     AS d1_n,
      ROUND(100.0 * SUM(d1) / COUNT(*), 2)::float8     AS d1_pct,
      SUM(d3)::int                                     AS d3_n,
      ROUND(100.0 * SUM(d3) / COUNT(*), 2)::float8     AS d3_pct,
      SUM(d7)::int                                     AS d7_n,
      ROUND(100.0 * SUM(d7) / COUNT(*), 2)::float8     AS d7_pct
    FROM joined
    GROUP BY pds_type
    UNION ALL
    SELECT
      '~ total',
      COUNT(*)::int,
      SUM(d1)::int, ROUND(100.0 * SUM(d1) / COUNT(*), 2)::float8,
      SUM(d3)::int, ROUND(100.0 * SUM(d3) / COUNT(*), 2)::float8,
      SUM(d7)::int, ROUND(100.0 * SUM(d7) / COUNT(*), 2)::float8
    FROM joined
    ORDER BY pds_type
  `;

  report("Aggregate Retention (full window) × PDS Type", "retention_aggregate.csv", retentionAggregate);

  // ── 3. D7 retention by PDS (top indie PDSes only) ────────────────────────────

  const retentionByPds = await sql<Record<string, unknown>[]>`
    WITH cohorts AS (
      SELECT
        c.created_at::date AS creation_date,
        c.did,
        c.pds_url
      FROM analysis.cohort_base c
      WHERE c.created_at::date BETWEEN ${START_DATE}::date AND ${END_DATE}::date
        AND c.pds_type = 'indie'
    ),
    joined AS (
      SELECT
        c.pds_url,
        c.did,
        MAX(CASE WHEN a.date = (c.creation_date + 1)::text THEN 1 ELSE 0 END) AS d1,
        MAX(CASE WHEN a.date = (c.creation_date + 3)::text THEN 1 ELSE 0 END) AS d3,
        MAX(CASE WHEN a.date = (c.creation_date + 7)::text THEN 1 ELSE 0 END) AS d7
      FROM cohorts c
      LEFT JOIN activity.did_activity_daily a
        ON a.did = c.did
        AND a.date IN (
          (c.creation_date + 1)::text,
          (c.creation_date + 3)::text,
          (c.creation_date + 7)::text
        )
      GROUP BY c.pds_url, c.creation_date, c.did
    )
    SELECT
      pds_url,
      COUNT(*)::int                                    AS cohort_size,
      SUM(d1)::int                                     AS d1_n,
      ROUND(100.0 * SUM(d1) / COUNT(*), 2)::float8     AS d1_pct,
      SUM(d3)::int                                     AS d3_n,
      ROUND(100.0 * SUM(d3) / COUNT(*), 2)::float8     AS d3_pct,
      SUM(d7)::int                                     AS d7_n,
      ROUND(100.0 * SUM(d7) / COUNT(*), 2)::float8     AS d7_pct
    FROM joined
    GROUP BY pds_url
    HAVING COUNT(*) >= 10
    ORDER BY cohort_size DESC
    LIMIT 50
  `;

  report("D1/D3/D7 Retention by PDS (indie, ≥10 new accounts, top 50)", "retention_by_pds.csv", retentionByPds);

  console.log(`\nDone. CSVs written to ${OUT_DIR}/\n`);
  await sql.end();
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
