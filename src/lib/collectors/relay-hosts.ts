/**
 * Relay host collector.
 *
 * Reads the full host list the AT Protocol relay advertises via
 * com.atproto.sync.listHosts and upserts it into health.relay_hosts.
 *
 * This is a different population from health.pds_instances: that table is the
 * union of everything we've *ever* discovered (mary-ext's directory scrape, PLC
 * log, did:web resolution), while this is specifically "what the relay federates
 * with right now". A PDS can be in one and not the other — the relay lists hosts
 * that never appear in the directory, and we track PDSes the relay has never
 * seen. Keeping them separate is the point; do not merge them.
 *
 * Run: npm run collect:relay
 */
import sql from "../db/pg";

const RELAY = process.env.RELAY_URL ?? "https://bsky.network";
const PAGE_LIMIT = 1000;
// listHosts pages at 1000; ~6K hosts today. The cap is a runaway guard against a
// relay returning a non-advancing cursor, not an expected limit.
const MAX_PAGES = 100;

interface RelayHost {
  hostname: string;
  seq?: number;
  accountCount?: number;
  status?: string;
}

async function fetchAllHosts(): Promise<RelayHost[]> {
  const hosts: RelayHost[] = [];
  let cursor: string | undefined;
  let pages = 0;

  do {
    const url = new URL(`${RELAY}/xrpc/com.atproto.sync.listHosts`);
    url.searchParams.set("limit", String(PAGE_LIMIT));
    if (cursor) url.searchParams.set("cursor", cursor);

    const res = await fetch(url, { signal: AbortSignal.timeout(30_000) });
    if (!res.ok) throw new Error(`listHosts ${res.status} ${res.statusText}`);

    const body = (await res.json()) as { hosts?: RelayHost[]; cursor?: string };
    const batch = body.hosts ?? [];
    hosts.push(...batch);

    const nextCursor = body.cursor;
    // A repeated cursor means the relay isn't advancing — stop rather than spin.
    if (nextCursor && nextCursor === cursor) break;
    cursor = nextCursor;
    pages++;
    console.log(`  page ${pages}: +${batch.length} (${hosts.length} total)`);
  } while (cursor && pages < MAX_PAGES);

  if (pages >= MAX_PAGES) console.warn(`  ! hit MAX_PAGES=${MAX_PAGES}; list may be truncated`);
  return hosts;
}

async function main() {
  console.log(`=== Relay Host Collector ===`);
  console.log(`Relay: ${RELAY}`);

  const hosts = await fetchAllHosts();
  if (hosts.length === 0) {
    console.error("No hosts returned — leaving existing rows untouched.");
    await sql.end();
    process.exit(1);
  }

  // Dedupe on hostname: ON CONFLICT cannot fire twice for the same key inside a
  // single INSERT, so a duplicate in one page would abort the statement.
  const byHost = new Map<string, RelayHost>();
  for (const h of hosts) if (h.hostname) byHost.set(h.hostname, h);

  const rows = [...byHost.values()].map(h => ({
    hostname: h.hostname,
    account_count: h.accountCount ?? null,
    status: h.status ?? null,
    seq: h.seq ?? null,
  }));

  const CHUNK = 1000; // 4 cols/row — well under Postgres's 65,534-parameter limit
  let written = 0;
  for (let i = 0; i < rows.length; i += CHUNK) {
    const chunk = rows.slice(i, i + CHUNK);
    await sql`
      INSERT INTO health.relay_hosts ${sql(chunk, "hostname", "account_count", "status", "seq")}
      ON CONFLICT (hostname) DO UPDATE SET
        account_count = EXCLUDED.account_count,
        status        = EXCLUDED.status,
        seq           = EXCLUDED.seq,
        last_seen_at  = NOW()
    `;
    written += chunk.length;
  }

  console.log(`\n=== Done ===`);
  console.log(`  Hosts fetched: ${hosts.length.toLocaleString()} (${rows.length.toLocaleString()} unique)`);
  console.log(`  Rows upserted: ${written.toLocaleString()}`);

  const dist = await sql<{ status: string | null; count: number }[]>`
    SELECT status, COUNT(*)::int AS count FROM health.relay_hosts GROUP BY status ORDER BY count DESC
  `;
  console.log(`\nStatus distribution:`);
  for (const r of dist) console.log(`  ${(r.status ?? "(null)").padEnd(12)} ${r.count.toLocaleString()}`);

  await sql.end();
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
