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

const entries = [
  {
    url: "https://atproto.brid.gy",
    city: "San Francisco",
    country: "US",
    latitude: 37.7749,
    longitude: -122.4194,
    org: "Bridgy Fed",
    note: "Fediverse bridge (Mastodon ↔ ATProto); operated by Ryan Barrett",
  },
  {
    url: "https://berlin-user.eurosky.social",
    city: "Berlin",
    country: "DE",
    latitude: 52.52,
    longitude: 13.405,
    org: "EuroSky Social",
    note: "EuroSky Social Berlin shard",
  },
  {
    url: "https://northsky.social",
    city: "Stockholm",
    country: "SE",
    latitude: 59.3293,
    longitude: 18.0686,
    org: "Self-hosted",
    note: "Nordic Bluesky-compatible PDS",
  },
  {
    url: "https://at.app.wafrn.net",
    city: "Madrid",
    country: "ES",
    latitude: 40.4168,
    longitude: -3.7038,
    org: "WAFRN",
    note: "WAFRN Fediverse app ATProto bridge",
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
