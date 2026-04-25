/**
 * D1/D3/D7 retention for new accounts created since the start of activity collection.
 *
 * Cohort = DIDs that appear in both plc_account_creations (for creation date)
 * AND did_in_repo (confirmed to have an actual repo). Raw PLC creations are
 * ~10x larger due to DIDs that were allocated but never activated.
 *
 * Usage:
 *   npx tsx src/lib/analysis/retention-cohorts.ts
 *   npx tsx src/lib/analysis/retention-cohorts.ts --start 2026-04-11  # default
 *
 * For each daily creation cohort, reports how many new accounts were active
 * exactly N days later (D1 = day after creation, D3, D7). Broken down by
 * bsky-hosted vs indie PDS.
 *
 * Note: D7 for the most recent complete cohort day may be partial if the
 * activity collector hasn't finished that day yet.
 */

import Database from "better-sqlite3";
import path from "path";
import fs from "fs";

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

// ── DB setup ──────────────────────────────────────────────────────────────────

const activityDb = new Database(path.join(process.cwd(), "jetstream-activity.db"), { readonly: true });
const plcDb = new Database(path.join(process.cwd(), "plc-migrations.db"), { readonly: true });
activityDb.exec(`ATTACH DATABASE '${plcDb.name}' as plc`);

// Earliest and latest dates with activity data — used to bound cohort window
const { min_date, max_date } = activityDb.prepare(
  `SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM did_activity_daily`
).get() as { min_date: string; max_date: string };

// D7 requires data 7 days out, so last valid cohort day is max_date - 7
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

const retentionByDay = activityDb.prepare(`
  WITH cohorts AS (
    SELECT
      date(p.created_at)                                          AS creation_date,
      r.did,
      CASE
        WHEN r.pds_url LIKE '%bsky.network%'
          OR r.pds_url LIKE '%bsky.social%'  THEN 'bsky'
        ELSE                                      'indie'
      END                                                         AS pds_type
    FROM plc.plc_account_creations p
    INNER JOIN plc.did_in_repo r ON r.did = p.did
    WHERE date(p.created_at) BETWEEN ? AND ?
  ),
  joined AS (
    SELECT
      c.creation_date,
      c.did,
      c.pds_type,
      MAX(CASE WHEN a.date = date(c.creation_date, '+1 day')  THEN 1 ELSE 0 END) AS d1,
      MAX(CASE WHEN a.date = date(c.creation_date, '+3 days') THEN 1 ELSE 0 END) AS d3,
      MAX(CASE WHEN a.date = date(c.creation_date, '+7 days') THEN 1 ELSE 0 END) AS d7
    FROM cohorts c
    LEFT JOIN did_activity_daily a
      ON a.did = c.did
      AND a.date IN (
        date(c.creation_date, '+1 day'),
        date(c.creation_date, '+3 days'),
        date(c.creation_date, '+7 days')
      )
    GROUP BY c.creation_date, c.did, c.pds_type
  )
  SELECT
    creation_date,
    pds_type,
    COUNT(*)                                         AS cohort_size,
    SUM(d1)                                          AS d1_n,
    ROUND(100.0 * SUM(d1) / COUNT(*), 2)             AS d1_pct,
    SUM(d3)                                          AS d3_n,
    ROUND(100.0 * SUM(d3) / COUNT(*), 2)             AS d3_pct,
    SUM(d7)                                          AS d7_n,
    ROUND(100.0 * SUM(d7) / COUNT(*), 2)             AS d7_pct
  FROM joined
  GROUP BY creation_date, pds_type
  UNION ALL
  SELECT
    creation_date,
    '~ total',
    COUNT(*),
    SUM(d1),  ROUND(100.0 * SUM(d1) / COUNT(*), 2),
    SUM(d3),  ROUND(100.0 * SUM(d3) / COUNT(*), 2),
    SUM(d7),  ROUND(100.0 * SUM(d7) / COUNT(*), 2)
  FROM joined
  GROUP BY creation_date
  ORDER BY creation_date, pds_type
`).all(START_DATE, END_DATE) as Record<string, unknown>[];

report("D1/D3/D7 Retention by Cohort Day × PDS Type", "retention_by_day.csv", retentionByDay);

// ── 2. Aggregate retention across the full cohort window ──────────────────────

const retentionAggregate = activityDb.prepare(`
  WITH cohorts AS (
    SELECT
      date(p.created_at)                                          AS creation_date,
      r.did,
      CASE
        WHEN r.pds_url LIKE '%bsky.network%'
          OR r.pds_url LIKE '%bsky.social%'  THEN 'bsky'
        ELSE                                      'indie'
      END                                                         AS pds_type
    FROM plc.plc_account_creations p
    INNER JOIN plc.did_in_repo r ON r.did = p.did
    WHERE date(p.created_at) BETWEEN ? AND ?
  ),
  joined AS (
    SELECT
      c.did,
      c.pds_type,
      MAX(CASE WHEN a.date = date(c.creation_date, '+1 day')  THEN 1 ELSE 0 END) AS d1,
      MAX(CASE WHEN a.date = date(c.creation_date, '+3 days') THEN 1 ELSE 0 END) AS d3,
      MAX(CASE WHEN a.date = date(c.creation_date, '+7 days') THEN 1 ELSE 0 END) AS d7
    FROM cohorts c
    LEFT JOIN did_activity_daily a
      ON a.did = c.did
      AND a.date IN (
        date(c.creation_date, '+1 day'),
        date(c.creation_date, '+3 days'),
        date(c.creation_date, '+7 days')
      )
    GROUP BY c.creation_date, c.did, c.pds_type
  )
  SELECT
    pds_type,
    COUNT(*)                                         AS cohort_size,
    SUM(d1)                                          AS d1_n,
    ROUND(100.0 * SUM(d1) / COUNT(*), 2)             AS d1_pct,
    SUM(d3)                                          AS d3_n,
    ROUND(100.0 * SUM(d3) / COUNT(*), 2)             AS d3_pct,
    SUM(d7)                                          AS d7_n,
    ROUND(100.0 * SUM(d7) / COUNT(*), 2)             AS d7_pct
  FROM joined
  GROUP BY pds_type
  UNION ALL
  SELECT
    '~ total',
    COUNT(*),
    SUM(d1),  ROUND(100.0 * SUM(d1) / COUNT(*), 2),
    SUM(d3),  ROUND(100.0 * SUM(d3) / COUNT(*), 2),
    SUM(d7),  ROUND(100.0 * SUM(d7) / COUNT(*), 2)
  FROM joined
  ORDER BY pds_type
`).all(START_DATE, END_DATE) as Record<string, unknown>[];

report("Aggregate Retention (full window) × PDS Type", "retention_aggregate.csv", retentionAggregate);

// ── 3. D7 retention by PDS (top indie PDSes only) ────────────────────────────

const retentionByPds = activityDb.prepare(`
  WITH cohorts AS (
    SELECT
      date(p.created_at)  AS creation_date,
      r.did,
      r.pds_url
    FROM plc.plc_account_creations p
    INNER JOIN plc.did_in_repo r ON r.did = p.did
    WHERE date(p.created_at) BETWEEN ? AND ?
      AND r.pds_url NOT LIKE '%bsky.network%'
      AND r.pds_url NOT LIKE '%bsky.social%'
  ),
  joined AS (
    SELECT
      c.pds_url,
      c.did,
      MAX(CASE WHEN a.date = date(c.creation_date, '+7 days') THEN 1 ELSE 0 END) AS d7
    FROM cohorts c
    LEFT JOIN did_activity_daily a
      ON a.did = c.did
      AND a.date = date(c.creation_date, '+7 days')
    GROUP BY c.pds_url, c.creation_date, c.did
  )
  SELECT
    pds_url,
    COUNT(*)                                         AS cohort_size,
    SUM(d7)                                          AS d7_n,
    ROUND(100.0 * SUM(d7) / COUNT(*), 2)             AS d7_pct
  FROM joined
  GROUP BY pds_url
  HAVING cohort_size >= 10
  ORDER BY cohort_size DESC
  LIMIT 50
`).all(START_DATE, END_DATE) as Record<string, unknown>[];

report("D7 Retention by PDS (indie, ≥10 new accounts, top 50)", "retention_by_pds.csv", retentionByPds);

console.log(`\nDone. CSVs written to ${OUT_DIR}/\n`);
