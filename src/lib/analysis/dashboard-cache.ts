/**
 * Pre-computes dashboard data and writes it to cache/dashboard.json.
 * Run this after each collection cycle so the web server never blocks
 * on expensive DB queries.
 *
 * Usage: npm run analysis:dashboard-cache
 */

import { mkdirSync, writeFileSync } from "fs";
import path from "path";
import sql from "../db/pg";
import {
  getOverviewStats, getCountryDistribution, getReposByCountry,
  getVersionDistribution, getHostingProviders, getCloudflareBreakdown,
  getUserDistribution, getConcentrationStats, getPdsLocations,
  getPdsLocationsWithProvider, getTopPdsByScan,
} from "../db/plc-queries";
import { getLatestRunInfo } from "../db/queries";
import { computeAndSaveCollectionPdsData } from "../db/activity-queries";

const CACHE_DIR = path.join(process.cwd(), "cache");
mkdirSync(CACHE_DIR, { recursive: true });

async function main() {
  for (const hideBsky of [false, true]) {
    console.time(`compute hideBsky=${hideBsky}`);
    const [
      runInfo, topPdsRaw, stats, countries, reposByCountry,
      versions, providers, cdnBreakdown, userDist, concentration,
      locations, providerLocations,
    ] = await Promise.all([
      getLatestRunInfo(),
      getTopPdsByScan(10, hideBsky),
      getOverviewStats(hideBsky),
      getCountryDistribution(hideBsky),
      getReposByCountry(hideBsky),
      getVersionDistribution(hideBsky),
      getHostingProviders(hideBsky),
      getCloudflareBreakdown(hideBsky),
      getUserDistribution(hideBsky),
      getConcentrationStats(hideBsky),
      getPdsLocations(hideBsky),
      getPdsLocationsWithProvider(hideBsky),
    ]);
    console.timeEnd(`compute hideBsky=${hideBsky}`);

    const data = {
      runInfo,
      stats,
      countries,
      reposByCountry,
      versions,
      providers,
      cdnBreakdown,
      userDist,
      topPds: topPdsRaw.map(p => ({ url: p.url, repoCount: p.repoCount, activeCount: p.activeCount, country: null })),
      concentration,
      locations,
      providerLocations,
    };

    const file = path.join(CACHE_DIR, hideBsky ? "dashboard-hidebsky.json" : "dashboard.json");
    writeFileSync(file, JSON.stringify({ data, writtenAt: new Date().toISOString() }, null, 0));
    console.log(`Written: ${file}`);

    console.time(`collection-pds hideBsky=${hideBsky}`);
    await computeAndSaveCollectionPdsData(hideBsky);
    console.timeEnd(`collection-pds hideBsky=${hideBsky}`);
    console.log(`Written: cache/collection-pds${hideBsky ? "-hidebsky" : ""}.json`);
  }

  await sql.end();
}

main().catch(e => { console.error(e); process.exit(1); });
