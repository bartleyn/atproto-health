/**
 * CLI runner for data collection.
 *
 * Usage:
 *   npm run collect          # Full collection (directory + geo + users)
 *   npm run collect:geo      # Directory + geo only (faster)
 *   npm run collect:users    # Directory + user counts only
 */

import { getDb } from "../db/schema";
import { fetchPdsDirectory } from "./pds-directory";
import { geolocatePdses } from "./geo-ip";
import { fetchAllPdsDetails } from "./pds-details";

const args = new Set(process.argv.slice(2));
const geoOnly = args.has("--geo");
const usersOnly = args.has("--users");
const fullRun = !geoOnly && !usersOnly;

async function main() {
  const db = getDb();
  const mode = geoOnly ? "geo" : usersOnly ? "users" : "full";
  console.log(`\n=== ATProto Health Collection (${mode}) ===\n`);

  // Start a collection run
  const run = db
    .prepare(
      `INSERT INTO collection_runs (source, status) VALUES (?, 'running') RETURNING id`
    )
    .get(`collect:${mode}`) as { id: number };

  try {
    // 1. Always fetch the PDS directory first
    const directory = await fetchPdsDirectory();

    // Upsert PDS instances
    const upsertPds = db.prepare(`
      INSERT INTO pds_instances (url) VALUES (?)
      ON CONFLICT(url) DO NOTHING
    `);
    const getPdsId = db.prepare(
      `SELECT id FROM pds_instances WHERE url = ?`
    );

    const insertTransaction = db.transaction(() => {
      for (const entry of directory) {
        upsertPds.run(entry.url);
      }
    });
    insertTransaction();

    // 2. Collect enrichment data based on mode
    const onlineUrls = directory
      .filter((e) => e.isOnline)
      .map((e) => e.url);

    const allUrls = directory.map((e) => e.url);

    const geoResults =
      fullRun || geoOnly ? await geolocatePdses(allUrls) : null;

    const detailResults =
      fullRun || usersOnly ? await fetchAllPdsDetails(onlineUrls) : null;

    // 3. Write snapshots
    const insertSnapshot = db.prepare(`
      INSERT INTO pds_snapshots (
        pds_id, run_id,
        version, invite_code_required, is_online, error_at,
        did, available_domains, contact, links,
        user_count_total, user_count_active,
        ip_address, country, country_code, region, city,
        latitude, longitude, isp, org, as_number, hosting_provider
      ) VALUES (
        ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?,
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?
      )
    `);

    const snapshotTransaction = db.transaction(() => {
      for (const entry of directory) {
        const pdsRow = getPdsId.get(entry.url) as { id: number };
        const geo = geoResults?.get(entry.url);
        const detail = detailResults?.get(entry.url);

        insertSnapshot.run(
          pdsRow.id,
          run.id,
          entry.version,
          entry.inviteCodeRequired ? 1 : 0,
          entry.isOnline ? 1 : 0,
          entry.errorAt ? new Date(entry.errorAt).toISOString() : null,
          detail?.did ?? null,
          detail?.availableDomains
            ? JSON.stringify(detail.availableDomains)
            : null,
          detail?.contact ? JSON.stringify(detail.contact) : null,
          detail?.links ? JSON.stringify(detail.links) : null,
          detail?.userCountTotal ?? null,
          detail?.userCountActive ?? null,
          geo?.ip ?? null,
          geo?.country ?? null,
          geo?.countryCode ?? null,
          geo?.region ?? null,
          geo?.city ?? null,
          geo?.lat ?? null,
          geo?.lon ?? null,
          geo?.isp ?? null,
          geo?.org ?? null,
          geo?.asNumber ?? null,
          geo?.org ?? null // hosting_provider = org for now
        );
      }
    });
    snapshotTransaction();

    // Mark run as complete
    db.prepare(
      `UPDATE collection_runs SET completed_at = datetime('now'), status = 'completed' WHERE id = ?`
    ).run(run.id);

    // Summary
    const totalUsers = detailResults
      ? [...detailResults.values()].reduce(
          (sum, d) => sum + (d.userCountActive ?? 0),
          0
        )
      : null;

    console.log(`\n=== Collection Complete ===`);
    console.log(`  PDS instances: ${directory.length}`);
    console.log(
      `  Online: ${directory.filter((e) => e.isOnline).length}`
    );
    if (geoResults) {
      const withGeo = [...geoResults.values()].filter(
        (g) => g.country
      ).length;
      console.log(`  Geolocated: ${withGeo}`);
    }
    if (totalUsers !== null) {
      console.log(`  Total active users (third-party): ${totalUsers}`);
    }
    console.log(`  Run ID: ${run.id}\n`);
  } catch (err) {
    db.prepare(
      `UPDATE collection_runs SET completed_at = datetime('now'), status = 'failed', metadata = ? WHERE id = ?`
    ).run(String(err), run.id);
    throw err;
  }
}

main().catch((err) => {
  console.error("Collection failed:", err);
  process.exit(1);
});
