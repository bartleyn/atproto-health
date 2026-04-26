/**
 * Upserts hand-curated geo entries for PDSes that appear in Jetstream/PLC data
 * but are not covered by the collector scan (not in pds_instances).
 *
 * Re-runnable: uses INSERT OR REPLACE so updates are safe to apply.
 *
 * Usage:
 *   npx tsx src/lib/collectors/seed-manual-geo.ts
 */

import { getDb } from "../db/schema";

// NOTE: atproto.brid.gy is intentionally omitted — the collector already scans
// https://atproto.brid.gy/ (trailing slash) and has real geo from ip-api. The
// page.tsx normalizeUrl() strips trailing slashes before lookup, so it matches.

// NOTE: Only add PDSes here that are genuinely absent from the collector scan
// (i.e., not in pds_instances). Always verify first with:
//   sqlite3 atproto-health.db "SELECT url FROM pds_instances WHERE url LIKE '%hostname%';"
// If they're in pds_instances with a trailing slash, normalizeUrl() in page.tsx
// handles the mismatch and the collector geo is used automatically.

const entries = [
  {
    url: "https://berlin-user.eurosky.social",
    city: "Berlin",
    country: "DE",
    latitude: 52.52,
    longitude: 13.405,
    org: "EuroSky Social",
    note: "EuroSky Social Berlin shard — not in collector scan",
  },
];

const db = getDb();
const upsert = db.prepare(`
  INSERT OR REPLACE INTO pds_manual_geo (url, city, country, latitude, longitude, org, note)
  VALUES (?, ?, ?, ?, ?, ?, ?)
`);

let count = 0;
const tx = db.transaction(() => {
  for (const e of entries) {
    upsert.run(e.url, e.city, e.country, e.latitude, e.longitude, e.org, e.note);
    count++;
  }
});
tx();

console.log(`Seeded ${count} manual geo entries.`);
const rows = db.prepare(`SELECT url, city, country FROM pds_manual_geo ORDER BY url`).all();
for (const r of rows as { url: string; city: string; country: string }[]) {
  console.log(`  ${r.url.replace("https://", "").padEnd(40)} ${r.city}, ${r.country}`);
}
