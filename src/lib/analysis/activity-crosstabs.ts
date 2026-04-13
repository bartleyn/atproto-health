import Database from "better-sqlite3";
import path from "path";

const args = process.argv.slice(2);
const daysBack = args[0] ? parseInt(args[0], 10) : 3;

function printTable(title: string, rows: Record<string, unknown>[]) {
    console.log(`\n=== ${title} ===`);
    if (rows.length === 0) {
        console.log("No data");
        return;
    }
    console.table(rows);
}

const activityDb = new Database(path.join(process.cwd(), "jetstream-activity.db"), { readonly: true });
const plcDb = new Database(path.join(process.cwd(), "plc-migrations.db"), { readonly: true });

activityDb.exec(`ATTACH DATABASE '${plcDb.name}' as plc`);

const result = activityDb.prepare(`
  WITH active AS (
    SELECT DISTINCT did, activity_types
    FROM did_activity_daily
    WHERE date >= date('now', '-' || ? || ' days')
  ),
  aged AS (
    SELECT a.activity_types, p.created_at
    FROM active a
    JOIN plc.plc_account_creations p ON a.did = p.did
  ),
  buckets AS (
    SELECT
      CASE
        WHEN created_at >= date('now', '-7 days') THEN '0. last 7 days'
        WHEN created_at < '2023-01-01'            THEN '1. pre-2023'
        WHEN created_at < '2024-01-01'            THEN '2. 2023'
        WHEN created_at < '2024-11-01'            THEN '3. 2024 pre-Nov'
        WHEN created_at < '2025-01-01'            THEN '4. 2024 Nov-Dec (exodus)'
        WHEN created_at < '2025-07-01'            THEN '5. 2025 H1'
        WHEN created_at < '2026-01-01'            THEN '6. 2025 H2'
        ELSE                                           '7. 2026'
      END AS age_bucket,
      activity_types
    FROM aged
  )
  SELECT age_bucket,
    COUNT(*) AS total,
    SUM(CASE WHEN activity_types & 1 THEN 1 ELSE 0 END) AS posted,
    SUM(CASE WHEN activity_types & 2 THEN 1 ELSE 0 END) AS liked,
    SUM(CASE WHEN activity_types & 4 THEN 1 ELSE 0 END) AS reposted,
    SUM(CASE WHEN activity_types & 8 THEN 1 ELSE 0 END) AS followed
  FROM buckets GROUP BY age_bucket
  UNION ALL
  SELECT '~ TOTAL',
    COUNT(*),
    SUM(CASE WHEN activity_types & 1 THEN 1 ELSE 0 END),
    SUM(CASE WHEN activity_types & 2 THEN 1 ELSE 0 END),
    SUM(CASE WHEN activity_types & 4 THEN 1 ELSE 0 END),
    SUM(CASE WHEN activity_types & 8 THEN 1 ELSE 0 END)
  FROM buckets
  ORDER BY age_bucket
`).all(daysBack);

printTable("Activity by Age Bucket", result);

const labelResult = activityDb.prepare(`
  WITH active AS (
    SELECT DISTINCT did, activity_types
    FROM did_activity_daily
    WHERE date >= date('now', '-' || ? || ' days')
  ),
  labeled AS (
    SELECT sl.label, a.activity_types
    FROM active a
    JOIN plc.skywatch_labels sl ON a.did = sl.did
  )
  SELECT label,
    COUNT(*) AS total,
    SUM(CASE WHEN activity_types & 1 THEN 1 ELSE 0 END) AS posted,
    SUM(CASE WHEN activity_types & 2 THEN 1 ELSE 0 END) AS liked,
    SUM(CASE WHEN activity_types & 4 THEN 1 ELSE 0 END) AS reposted,
    SUM(CASE WHEN activity_types & 8 THEN 1 ELSE 0 END) AS followed
  FROM labeled GROUP BY label
  UNION ALL
  SELECT '~ TOTAL',
    COUNT(*),
    SUM(CASE WHEN activity_types & 1 THEN 1 ELSE 0 END),
    SUM(CASE WHEN activity_types & 2 THEN 1 ELSE 0 END),
    SUM(CASE WHEN activity_types & 4 THEN 1 ELSE 0 END),
    SUM(CASE WHEN activity_types & 8 THEN 1 ELSE 0 END)
  FROM labeled
  ORDER BY total DESC
`).all(daysBack);

printTable("Activity by Skywatch Label", labelResult);


  const stickiness = activityDb.prepare(`                                                                                             
    WITH daily_counts AS (                                                                                                            
      SELECT date, COUNT(DISTINCT did) AS daily_uniques                                                                               
      FROM did_activity_daily                                                                                                         
      WHERE date >= date('now', '-' || ? || ' days')
      GROUP BY date                                                                                                                   
    ),            
    total_unique AS (
      SELECT COUNT(DISTINCT did) AS total_uniques
      FROM did_activity_daily                                                                                                         
      WHERE date >= date('now', '-' || ? || ' days')
    )                                                                                                                                 
    SELECT        
      ROUND(AVG(daily_uniques), 0) AS avg_daily_uniques,
      total_uniques,
      ROUND(1.0 * AVG(daily_uniques) / total_uniques, 3) AS ratio                                                                     
    FROM daily_counts, total_unique
  `).get(daysBack, daysBack);                                                                                                         
                                                                                                                                      
  printTable("Stickiness (avg daily uniques / unique users in window)", [stickiness as Record<string, unknown>]);
                                                                                                                    