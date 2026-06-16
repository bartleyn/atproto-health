/**
 * Upserts hand-curated geo entries for PDSes that appear in Jetstream/PLC data
 * but are not covered by the collector scan (not in pds_instances).
 *
 * Re-runnable: uses INSERT ... ON CONFLICT DO UPDATE so updates are safe to apply.
 *
 * Usage:
 *   npx tsx src/lib/collectors/seed-manual-geo.ts
 */

import sql from "../db/pg";

// NOTE: atproto.brid.gy is intentionally omitted — the collector already scans
// https://atproto.brid.gy/ (trailing slash) and has real geo from ip-api. The
// page.tsx normalizeUrl() strips trailing slashes before lookup, so it matches.

// NOTE: Only add PDSes here that are genuinely absent from the collector scan
// (i.e., not in pds_instances). Always verify first with:
//   psql -c "SELECT url FROM health.pds_instances WHERE url LIKE '%hostname%';"
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

async function main() {
  for (const e of entries) {
    await sql`
      INSERT INTO plc.pds_manual_geo (url, city, country, latitude, longitude, org, note)
      VALUES (${e.url}, ${e.city}, ${e.country}, ${e.latitude}, ${e.longitude}, ${e.org}, ${e.note})
      ON CONFLICT (url) DO UPDATE SET
        city = EXCLUDED.city, country = EXCLUDED.country,
        latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude,
        org = EXCLUDED.org, note = EXCLUDED.note
    `;
  }
  console.log(`Seeded ${entries.length} manual geo entries.`);

  const rows = await sql<{ url: string; city: string; country: string }[]>`
    SELECT url, city, country FROM plc.pds_manual_geo ORDER BY url
  `;
  for (const r of rows) {
    console.log(`  ${r.url.replace("https://", "").padEnd(40)} ${r.city}, ${r.country}`);
  }

  await sql.end();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
