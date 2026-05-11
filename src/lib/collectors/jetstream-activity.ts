/**
 * Long-running Jetstream collector that tracks:
 *   1. Daily account activity (did_activity_daily) — one row per (did, date), bitmask of collections used
 *   2. Delete events (delete_events_daily) — daily counts by event type
 *   3. Starter pack joins (starterpack_joins_daily)
 *   4. Per-DID language usage (did_langs)
 *   5. Non-bsky collection activity (collection_activity) — event counts per (collection, DID)
 *      for any collection outside app.bsky.* and chat.bsky.*
 *
 * Subscribes to a curated wantedCollections list: all bitmask collections + known non-bsky appviews.
 * Unfiltered subscription (no wantedCollections) overwhelms Node's event loop at ~30-50K events/sec.
 * Add new non-bsky collections to THIRD_PARTY_COLLECTIONS as new appviews are discovered.
 *
 * Safety flags:
 *   --no-collection-tracking          disable collection_activity writes entirely
 *   --max-collection-rows <N>         pause collection_activity writes once table exceeds N rows
 *                                     (default: 5_000_000; re-checked each flush)
 *
 * Usage:
 *   npm run collect:activity
 *   npm run collect:activity -- --retention-days 90
 *   npm run collect:activity -- --no-collection-tracking
 *   npm run collect:activity -- --max-collection-rows 1000000
 */

import WebSocket from "ws";
import { getActivityDb } from "../db/activity-schema";
import { getPlcDb } from "../db/plc-schema";

// Bitmask assignments for activity_types column.
// Bits 13+ are synthetic — derived from record content, not the collection name alone.
const ACTIVITY_BITS: Record<string, number> = {
  "app.bsky.feed.post":           1 << 0,   //    1
  "app.bsky.feed.like":           1 << 1,   //    2
  "app.bsky.feed.repost":         1 << 2,   //    4
  "app.bsky.graph.follow":        1 << 3,   //    8
  "app.bsky.graph.block":         1 << 4,   //   16
  "app.bsky.graph.listitem":      1 << 5,   //   32
  "app.bsky.graph.listblock":     1 << 6,   //   64
  "app.bsky.graph.list":          1 << 7,   //  128
  "app.bsky.feed.threadgate":     1 << 8,   //  256
  "app.bsky.feed.generator":      1 << 9,   //  512
  "app.bsky.graph.starterpack":   1 << 10,  // 1024
  "app.bsky.actor.profile":       1 << 11,  // 2048
  "chat.bsky.actor.declaration":  1 << 12,  // 4096
};
const BIT_REPLIED = 1 << 13;  //  8192 — posted a reply (post.reply is set)
const BIT_QUOTED  = 1 << 14;  // 16384 — posted a quote (post.embed is app.bsky.embed.record or recordWithMedia)

// Third-party/appview namespaces to subscribe to via prefix matching (namespace.*).
// All matching events are tracked in collection_activity (keyed by exact collection name).
// Add new namespaces here as appviews grow; source new ones from delete_events_daily.
const THIRD_PARTY_PREFIXES = [
  "chat.bsky.*",                      // Bluesky DMs (non-declaration events)
  "com.whtwnd.*",                     // WhiteWind blog
  "fyi.unravel.*",                    // Frontpage link aggregator
  "sh.tangled.*",                     // Tangled git forge
  "xyz.opnshelf.*",                   // OpenShelf media tracking
  "xyz.statusphere.*",                // Statusphere status updates
  "social.kibun.*",                   // Kibun status
  "social.grain.*",                   // Grain photography
  "social.craftsky.*",                // CraftSky
  "social.popfeed.*",                 // Popfeed
  "org.titlegraph.*",                 // Titlegraph
  "place.stream.*",                   // place.stream
  "site.standard.*",                  // site.standard
  "site.mochott.*",                   // Mochott
  "cx.vmx.*",                         // vmx
  "games.gamesgamesgamesgames.*",     // Games
  "fm.teal.*",                        // Teal music
  "jp.5leaf.*",                       // 5leaf
  "blue.moji.*",                      // Moji custom emoji
  "blue.flashes.*",                   // Flashes
  "community.lexicon.*",              // community lexicon (calendar, etc.)
  "net.shino3.*",                     // Trailcast
  "net.anisota.*",                    // Anisota
  "land.atlink.*",                    // Atlink
  "blog.pckt.*",                      // Pckt blog
  "bio.lexicons.*",                   // bio.lexicons
  "app.tomarigi.*",                   // Tomarigi
  "id.sifa.*",                        // Sifa
  "io.zzstoatzz.*",                   // zzstoatzz
  "app.rocksky.*",                    // Rocksky music scrobbling
  "uk.skyblur.*",                     // Skyblur social
  "network.cosmik.*",                 // Cosmik social
  "net.wafrn.*",                      // WAFRN fediverse bridge
  "garden.goals.*",                   // Goal tracker
  "at.margin.*",                      // Margin notes/collections
  "buzz.bookhive.*",                  // BookHive book tracking
  "dev.radl.*",                       // Radl goals/planning
  "space.roomy.*",                    // Roomy spaces
  "pub.leaflet.*",                    // Leaflet documents
  "com.puzzmo.*",                     // Puzzmo game streaks
  "blue.trilinesat.*",                // Trilinesat
  "app.offprint.*",                   // Offprint
  "store.lexicon.*",                  // ATStore
  "actor.rpg.*",                      // RPG Actor
];

const WANTED_COLLECTIONS = [
  ...Object.keys(ACTIVITY_BITS),
  ...THIRD_PARTY_PREFIXES,
];
const COLLECTION_PARAMS = WANTED_COLLECTIONS.map(c => `wantedCollections=${encodeURIComponent(c)}`).join("&");

// All four official Jetstream relays — cursor is compatible across all of them.
// On disconnect we round-robin to the next one so a single relay outage doesn't stall us.
const JETSTREAM_RELAYS = [
  "wss://jetstream1.us-east.bsky.network/subscribe",
  "wss://jetstream2.us-east.bsky.network/subscribe",
  "wss://jetstream1.us-west.bsky.network/subscribe",
  "wss://jetstream2.us-west.bsky.network/subscribe",
];

let relayIdx = 0;

const FLUSH_INTERVAL_MS  = 60 * 1000;
const RECONNECT_DELAY_MS = 5_000;

const args = process.argv.slice(2);
const retentionIdx = args.indexOf("--retention-days");
const RETENTION_DAYS = retentionIdx >= 0 ? parseInt(args[retentionIdx + 1], 10) : 90;
const backfillIdx = args.indexOf("--backfill-hours");
const BACKFILL_HOURS = backfillIdx >= 0 ? parseInt(args[backfillIdx + 1], 10) : null;
const NO_COLLECTION_TRACKING = args.includes("--no-collection-tracking");
const maxCollectionRowsIdx = args.indexOf("--max-collection-rows");
const MAX_COLLECTION_ROWS = maxCollectionRowsIdx >= 0 ? parseInt(args[maxCollectionRowsIdx + 1], 10) : 5_000_000;

// Activity buffer: "did|date" → bitmask of activity_types seen
const activityBuffer = new Map<string, number>();

// Delete buffer: "date|event_type" → count
const deleteBuffer = new Map<string, number>();

// Starter pack join buffer: "starterpack_uri|date" → count
const starterpackBuffer = new Map<string, number>();

// Lang buffer: "did|lang" → post count (posts in that language by that DID this flush window)
const langBuffer = new Map<string, number>();
// Lang stats buffer: date → { total posts, tagged posts } for this flush window
const langStatsBuffer = new Map<string, { total: number; tagged: number }>();

// Collection buffer: "collection|did|date" → event count (non-bsky/non-chat-bsky creates only)
const collectionBuffer = new Map<string, number>();
let collectionTrackingPaused = NO_COLLECTION_TRACKING;

// Feed generator buffer: uri → { creatorDid, displayName, description, firstSeen }
const feedGenBuffer = new Map<string, { creatorDid: string; displayName: string | null; description: string | null; firstSeen: string }>();
// Feed generator deletes: set of URIs that received a delete event this flush window
const feedGenDeleteBuffer = new Set<string>();
// Feed like buffer: "feed_uri|date" → count (only likes targeting a feed generator)
const feedLikeBuffer = new Map<string, number>();

// did:web PDS discovery: DIDs seen in the stream that need did document resolution
const didWebSeen = new Set<string>();
// Cache of DIDs we've already successfully resolved so we don't re-hit them every flush
const didWebResolved = new Set<string>();

let lastCursor = 0;
let totalActivityFlushed = 0;
let totalDeletesFlushed = 0;
let totalStarterpackFlushed = 0;
let totalLangDIDsFlushed = 0;
let totalCollectionsFlushed = 0;
let totalFeedGensFlushed = 0;
let totalFeedLikesFlushed = 0;
let totalEvents = 0;

// Resolve a did:web DID document and return the #atproto_pds service endpoint.
// did:web:example.com → https://example.com/.well-known/did.json
// did:web:example.com:path:seg → https://example.com/path/seg/did.json
async function resolveDidWebPds(did: string): Promise<string | null> {
  try {
    const encoded = did.slice("did:web:".length);
    const parts = encoded.split(":");
    const host = decodeURIComponent(parts[0]);
    const docPath = parts.length > 1
      ? parts.slice(1).map(decodeURIComponent).join("/") + "/did.json"
      : ".well-known/did.json";
    const url = `https://${host}/${docPath}`;
    const res = await fetch(url, { signal: AbortSignal.timeout(5000) });
    if (!res.ok) return null;
    const doc = await res.json() as { service?: { id: string; serviceEndpoint: string }[] };
    return doc?.service?.find(s => s.id === "#atproto_pds")?.serviceEndpoint ?? null;
  } catch {
    return null;
  }
}

// Resolve any newly-seen did:web DIDs and write discovered PDS URLs to plcDb.
// Runs async alongside each flush; does not block the main event loop.
async function resolveAndStoreDidWebs() {
  if (didWebSeen.size === 0) return;

  const toResolve = [...didWebSeen].filter(d => !didWebResolved.has(d));
  didWebSeen.clear();
  if (toResolve.length === 0) return;

  const plcDb = getPlcDb();

  // Check which ones we've already stored with a successful resolution
  const alreadyStored = new Set(
    (plcDb.prepare(`SELECT did FROM did_web_pds WHERE pds_url IS NOT NULL`).all() as { did: string }[])
      .map(r => r.did)
  );
  const fresh = toResolve.filter(d => !alreadyStored.has(d));
  // Warm the resolved cache
  for (const d of alreadyStored) didWebResolved.add(d);
  if (fresh.length === 0) return;

  const upsert = plcDb.prepare(`
    INSERT INTO did_web_pds (did, pds_url, first_seen, last_seen, resolved_at)
    VALUES (?, ?, datetime('now'), datetime('now'), datetime('now'))
    ON CONFLICT (did) DO UPDATE SET
      pds_url     = COALESCE(excluded.pds_url, pds_url),
      last_seen   = excluded.last_seen,
      resolved_at = CASE WHEN excluded.pds_url IS NOT NULL THEN excluded.resolved_at ELSE resolved_at END
  `);

  const CONCURRENCY = 5;
  let discovered = 0;
  for (let i = 0; i < fresh.length; i += CONCURRENCY) {
    const batch = fresh.slice(i, i + CONCURRENCY);
    const results = await Promise.all(batch.map(async did => ({ did, pdsUrl: await resolveDidWebPds(did) })));
    plcDb.transaction(() => {
      for (const { did, pdsUrl } of results) {
        upsert.run(did, pdsUrl);
        if (pdsUrl) { didWebResolved.add(did); discovered++; }
      }
    })();
  }

  if (discovered > 0) {
    console.log(`[activity] Discovered ${discovered} new did:web PDS${discovered > 1 ? "es" : ""} (${fresh.length} DIDs resolved)`);
  }
}

// Snapshot all in-memory buffers and clear them. Fast and synchronous — call this
// at the top of any flush so new incoming events accumulate in fresh buffers while
// we write the snapshot to SQLite.
function snapshotBuffers() {
  // Check collection_activity row count and pause if over the limit (fast COUNT query)
  if (!collectionTrackingPaused && collectionBuffer.size > 0) {
    const db = getActivityDb();
    const { n } = db.prepare(`SELECT COUNT(*) AS n FROM collection_activity`).get() as { n: number };
    if (n >= MAX_COLLECTION_ROWS) {
      console.warn(`[activity] collection_activity has ${n.toLocaleString()} rows (limit ${MAX_COLLECTION_ROWS.toLocaleString()}). Pausing collection tracking.`);
      collectionTrackingPaused = true;
      collectionBuffer.clear();
    }
  }

  const snapshot = {
    activityRows:    [...activityBuffer.entries()],
    deleteRows:      [...deleteBuffer.entries()],
    starterpackRows: [...starterpackBuffer.entries()],
    langRows:        [...langBuffer.entries()],
    langStatsRows:   [...langStatsBuffer.entries()],
    collectionRows:  collectionTrackingPaused ? [] : [...collectionBuffer.entries()],
    feedGenRows:     [...feedGenBuffer.entries()],
    feedGenDeletes:  [...feedGenDeleteBuffer],
    feedLikeRows:    [...feedLikeBuffer.entries()],
    cursorToSave:    lastCursor,
  };
  activityBuffer.clear();
  deleteBuffer.clear();
  starterpackBuffer.clear();
  langBuffer.clear();
  langStatsBuffer.clear();
  collectionBuffer.clear();
  feedGenBuffer.clear();
  feedGenDeleteBuffer.clear();
  feedLikeBuffer.clear();
  return snapshot;
}

function logFlush(snapshot: ReturnType<typeof snapshotBuffers>) {
  const { activityRows, deleteRows, starterpackRows, langRows, langStatsRows, collectionRows, feedGenRows, feedLikeRows } = snapshot;
  const postsThisFlush  = langStatsRows.reduce((s, [, v]) => s + v.total, 0);
  const taggedThisFlush = langStatsRows.reduce((s, [, v]) => s + v.tagged, 0);
  totalActivityFlushed    += activityRows.length;
  totalDeletesFlushed     += deleteRows.reduce((s, [, c]) => s + c, 0);
  totalStarterpackFlushed += starterpackRows.reduce((s, [, c]) => s + c, 0);
  totalLangDIDsFlushed    += langRows.length;
  totalCollectionsFlushed += collectionRows.length;
  totalFeedGensFlushed    += feedGenRows.length;
  totalFeedLikesFlushed   += feedLikeRows.reduce((s, [, c]) => s + c, 0);
  const tagPct = postsThisFlush > 0 ? ((taggedThisFlush / postsThisFlush) * 100).toFixed(1) : "—";
  const collectionNote = collectionTrackingPaused ? " [collection tracking PAUSED]" : ` collections=${collectionRows.length.toLocaleString()}`;
  console.log(
    `[activity] Flushed activity=${activityRows.length.toLocaleString()} deletes=${deleteRows.length} types starterpack=${starterpackRows.length} uris lang=${langRows.length.toLocaleString()} did×lang (${tagPct}% tagged)${collectionNote} feeds=${feedGenRows.length} feed_likes=${feedLikeRows.reduce((s, [, c]) => s + c, 0)} | ` +
    `totals: activity=${totalActivityFlushed.toLocaleString()} deletes=${totalDeletesFlushed.toLocaleString()} starterpack=${totalStarterpackFlushed.toLocaleString()} lang_dids=${totalLangDIDsFlushed.toLocaleString()} collections=${totalCollectionsFlushed.toLocaleString()} feed_gens=${totalFeedGensFlushed.toLocaleString()} feed_likes=${totalFeedLikesFlushed.toLocaleString()} | ` +
    `events=${totalEvents.toLocaleString()} | cursor=${snapshot.cursorToSave}`
  );
}

function prepareStatements(db: ReturnType<typeof getActivityDb>) {
  return {
    upsertActivity: db.prepare(`
      INSERT INTO did_activity_daily (did, date, activity_types) VALUES (?, ?, ?)
      ON CONFLICT (did, date) DO UPDATE SET activity_types = activity_types | excluded.activity_types
    `),
    upsertDelete: db.prepare(`
      INSERT INTO delete_events_daily (date, event_type, count) VALUES (?, ?, ?)
      ON CONFLICT (date, event_type) DO UPDATE SET count = count + excluded.count
    `),
    upsertStarterpack: db.prepare(`
      INSERT INTO starterpack_joins_daily (starterpack_uri, date, count) VALUES (?, ?, ?)
      ON CONFLICT (starterpack_uri, date) DO UPDATE SET count = count + excluded.count
    `),
    upsertLang: db.prepare(`
      INSERT INTO did_langs (did, lang, post_count, last_seen) VALUES (?, ?, ?, datetime('now'))
      ON CONFLICT (did, lang) DO UPDATE SET
        post_count = post_count + excluded.post_count,
        last_seen  = excluded.last_seen
    `),
    upsertLangStats: db.prepare(`
      INSERT INTO lang_stats (date, total_posts, tagged_posts, updated_at) VALUES (?, ?, ?, datetime('now'))
      ON CONFLICT (date) DO UPDATE SET
        total_posts  = total_posts  + excluded.total_posts,
        tagged_posts = tagged_posts + excluded.tagged_posts,
        updated_at   = excluded.updated_at
    `),
    upsertCollection: db.prepare(`
      INSERT INTO collection_activity (collection, did, date, event_count)
      VALUES (?, ?, ?, ?)
      ON CONFLICT (collection, did, date) DO UPDATE SET event_count = event_count + excluded.event_count
    `),
    saveCursor: db.prepare(`
      INSERT INTO jetstream_cursor (id, cursor, updated_at)
      VALUES (1, ?, datetime('now'))
      ON CONFLICT (id) DO UPDATE SET cursor = excluded.cursor, updated_at = excluded.updated_at
    `),
    upsertFeedGen: db.prepare(`
      INSERT INTO feed_generators (uri, creator_did, display_name, description, first_seen)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT (uri) DO UPDATE SET
        display_name = coalesce(excluded.display_name, display_name),
        description  = coalesce(excluded.description,  description),
        deleted_at   = NULL
    `),
    markFeedGenDeleted: db.prepare(`
      UPDATE feed_generators SET deleted_at = datetime('now') WHERE uri = ?
    `),
    upsertFeedLike: db.prepare(`
      INSERT INTO feed_generator_likes_daily (feed_uri, date, likes)
      VALUES (?, ?, ?)
      ON CONFLICT (feed_uri, date) DO UPDATE SET likes = likes + excluded.likes
    `),
  };
}

// Async flush: snapshots buffers immediately (so new events accumulate while we write),
// then writes to SQLite in chunks of ACTIVITY_CHUNK_SIZE, yielding to the event loop
// between chunks. This prevents the TCP receive buffer from filling up during large
// flushes on a grown did_activity_daily table.
const ACTIVITY_CHUNK_SIZE = 2_000;

async function flush() {
  if (activityBuffer.size === 0 && deleteBuffer.size === 0 && starterpackBuffer.size === 0 && langBuffer.size === 0 && collectionBuffer.size === 0 && feedGenBuffer.size === 0 && feedGenDeleteBuffer.size === 0 && feedLikeBuffer.size === 0) return;

  const snapshot = snapshotBuffers();
  const { activityRows, deleteRows, starterpackRows, langRows, langStatsRows, collectionRows, feedGenRows, feedGenDeletes, feedLikeRows, cursorToSave } = snapshot;

  const db = getActivityDb();
  const stmts = prepareStatements(db);

  // Write activity in chunks, yielding between each so the event loop stays responsive.
  for (let i = 0; i < activityRows.length; i += ACTIVITY_CHUNK_SIZE) {
    db.transaction(() => {
      for (const [entry, bits] of activityRows.slice(i, i + ACTIVITY_CHUNK_SIZE)) {
        const pipe = entry.indexOf("|");
        stmts.upsertActivity.run(entry.slice(0, pipe), entry.slice(pipe + 1), bits);
      }
    })();
    if (i + ACTIVITY_CHUNK_SIZE < activityRows.length) {
      await new Promise<void>(resolve => setImmediate(resolve));
    }
  }

  // Write lang rows in chunks too — also grows large.
  for (let i = 0; i < langRows.length; i += ACTIVITY_CHUNK_SIZE) {
    db.transaction(() => {
      for (const [key, count] of langRows.slice(i, i + ACTIVITY_CHUNK_SIZE)) {
        const pipe = key.indexOf("|");
        stmts.upsertLang.run(key.slice(0, pipe), key.slice(pipe + 1), count);
      }
    })();
    if (i + ACTIVITY_CHUNK_SIZE < langRows.length) {
      await new Promise<void>(resolve => setImmediate(resolve));
    }
  }

  // All remaining tables are small — write them in one transaction with the cursor save.
  db.transaction(() => {
    for (const [key, count] of deleteRows) {
      const pipe = key.indexOf("|");
      stmts.upsertDelete.run(key.slice(0, pipe), key.slice(pipe + 1), count);
    }
    for (const [key, count] of starterpackRows) {
      const pipe = key.indexOf("|");
      stmts.upsertStarterpack.run(key.slice(0, pipe), key.slice(pipe + 1), count);
    }
    for (const [key, count] of collectionRows) {
      const [collection, did, date] = key.split("|");
      stmts.upsertCollection.run(collection, did, date, count);
    }
    for (const [uri, meta] of feedGenRows) {
      stmts.upsertFeedGen.run(uri, meta.creatorDid, meta.displayName, meta.description, meta.firstSeen);
    }
    for (const uri of feedGenDeletes) {
      stmts.markFeedGenDeleted.run(uri);
    }
    for (const [key, count] of feedLikeRows) {
      const pipe = key.lastIndexOf("|");
      stmts.upsertFeedLike.run(key.slice(0, pipe), key.slice(pipe + 1), count);
    }
    for (const [date, { total, tagged }] of langStatsRows) {
      if (total > 0) stmts.upsertLangStats.run(date, total, tagged);
    }
    if (cursorToSave > 0) stmts.saveCursor.run(cursorToSave);
  })();

  // Truncate the WAL after every flush — auto-checkpoint only copies frames (PASSIVE mode)
  // and never shrinks the WAL file. TRUNCATE zeroes it while we're the sole connection.
  db.pragma("wal_checkpoint(TRUNCATE)");

  logFlush(snapshot);
}

// Synchronous flush for shutdown — can't await in signal handlers.
function flushSync() {
  if (activityBuffer.size === 0 && deleteBuffer.size === 0 && starterpackBuffer.size === 0 && langBuffer.size === 0 && collectionBuffer.size === 0 && feedGenBuffer.size === 0 && feedGenDeleteBuffer.size === 0 && feedLikeBuffer.size === 0) return;

  const snapshot = snapshotBuffers();
  const { activityRows, deleteRows, starterpackRows, langRows, langStatsRows, collectionRows, feedGenRows, feedGenDeletes, feedLikeRows, cursorToSave } = snapshot;

  const db = getActivityDb();
  const stmts = prepareStatements(db);

  db.transaction(() => {
    for (const [entry, bits] of activityRows) {
      const pipe = entry.indexOf("|");
      stmts.upsertActivity.run(entry.slice(0, pipe), entry.slice(pipe + 1), bits);
    }
    for (const [key, count] of deleteRows) {
      const pipe = key.indexOf("|");
      stmts.upsertDelete.run(key.slice(0, pipe), key.slice(pipe + 1), count);
    }
    for (const [key, count] of starterpackRows) {
      const pipe = key.indexOf("|");
      stmts.upsertStarterpack.run(key.slice(0, pipe), key.slice(pipe + 1), count);
    }
    for (const [key, count] of langRows) {
      const pipe = key.indexOf("|");
      stmts.upsertLang.run(key.slice(0, pipe), key.slice(pipe + 1), count);
    }
    for (const [key, count] of collectionRows) {
      const [collection, did, date] = key.split("|");
      stmts.upsertCollection.run(collection, did, date, count);
    }
    for (const [uri, meta] of feedGenRows) {
      stmts.upsertFeedGen.run(uri, meta.creatorDid, meta.displayName, meta.description, meta.firstSeen);
    }
    for (const uri of feedGenDeletes) {
      stmts.markFeedGenDeleted.run(uri);
    }
    for (const [key, count] of feedLikeRows) {
      const pipe = key.lastIndexOf("|");
      stmts.upsertFeedLike.run(key.slice(0, pipe), key.slice(pipe + 1), count);
    }
    for (const [date, { total, tagged }] of langStatsRows) {
      if (total > 0) stmts.upsertLangStats.run(date, total, tagged);
    }
    if (cursorToSave > 0) stmts.saveCursor.run(cursorToSave);
  })();

  db.pragma("wal_checkpoint(TRUNCATE)");
  logFlush(snapshot);
}

function pruneOldRows() {
  const db = getActivityDb();
  const cutoff = new Date(Date.now() - RETENTION_DAYS * 24 * 60 * 60 * 1000)
    .toISOString().slice(0, 10);
  const a = db.prepare(`DELETE FROM did_activity_daily WHERE date < ?`).run(cutoff);
  const d = db.prepare(`DELETE FROM delete_events_daily WHERE date < ?`).run(cutoff);
  db.prepare(`DELETE FROM lang_stats WHERE date < ?`).run(cutoff);
  db.prepare(`DELETE FROM collection_activity WHERE date < ?`).run(cutoff);
  if (a.changes > 0 || d.changes > 0) {
    console.log(`[activity] Pruned ${a.changes.toLocaleString()} activity + ${d.changes.toLocaleString()} delete rows older than ${cutoff}`);
  }
}

function recordDelete(key: string) {
  deleteBuffer.set(key, (deleteBuffer.get(key) ?? 0) + 1);
}

// Stall detection: if no message is received within this window the connection
// is considered hung (TCP half-open). We close it ourselves to trigger reconnect.
const STALL_TIMEOUT_MS = 120_000;
// Client-side ping keeps the server from closing idle connections.
const PING_INTERVAL_MS = 30_000;

function connect() {
  const db = getActivityDb();
  const cursorRow = db.prepare(`SELECT cursor FROM jetstream_cursor WHERE id = 1`).get() as { cursor: number } | undefined;

  let cursor = cursorRow?.cursor;
  if (BACKFILL_HOURS !== null) {
    cursor = (Date.now() - BACKFILL_HOURS * 60 * 60 * 1000) * 1000;
    console.log(`[activity] Backfill mode: overriding cursor to ${BACKFILL_HOURS}h ago (${new Date(cursor / 1000).toISOString()})`);
  }

  const relay = JETSTREAM_RELAYS[relayIdx % JETSTREAM_RELAYS.length];
  const url = `${relay}?${COLLECTION_PARAMS}${cursor ? `&cursor=${cursor}` : ""}`;
  console.log(`[activity] Connecting to ${relay.replace("wss://", "")}${cursor ? ` from cursor ${cursor}` : " live"}...`);

  const ws = new WebSocket(url);

  let stallTimer: ReturnType<typeof setTimeout> | null = null;
  let pingTimer: ReturnType<typeof setInterval> | null = null;

  function resetStallTimer() {
    if (stallTimer) clearTimeout(stallTimer);
    stallTimer = setTimeout(() => {
      console.warn(`[activity] No messages in ${STALL_TIMEOUT_MS / 1000}s — connection stalled. Reconnecting...`);
      ws.terminate();
    }, STALL_TIMEOUT_MS);
  }

  function cleanup() {
    if (stallTimer) { clearTimeout(stallTimer); stallTimer = null; }
    if (pingTimer)  { clearInterval(pingTimer);  pingTimer  = null; }
  }

  ws.on("open", () => {
    const collectionStatus = NO_COLLECTION_TRACKING
      ? "DISABLED (--no-collection-tracking)"
      : `enabled, pauses at ${MAX_COLLECTION_ROWS.toLocaleString()} rows`;
    console.log(`[activity] Connected. Retention: ${RETENTION_DAYS}d. Flushing every ${FLUSH_INTERVAL_MS / 1000}s. Collection tracking: ${collectionStatus}`);
    resetStallTimer();
    pingTimer = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) ws.ping();
    }, PING_INTERVAL_MS);
  });

  ws.on("message", (data: Buffer) => {
    resetStallTimer();
    try {
      const evt = JSON.parse(data.toString());
      const date = new Date(evt.time_us / 1000).toISOString().slice(0, 10);
      lastCursor = evt.time_us;
      totalEvents++;

      // did:web PDS discovery: capture every did:web DID seen in the stream
      if (typeof evt.did === "string" && evt.did.startsWith("did:web:") && !didWebResolved.has(evt.did)) {
        didWebSeen.add(evt.did);
      }

      if (evt.kind === "commit") {
        if (evt.commit.operation === "create") {
          // Activity: track which collection types were seen per (did, date)
          const collection = evt.commit.collection;
          const key = `${evt.did}|${date}`;
          const bit = ACTIVITY_BITS[collection] ?? 0;
          if (bit) activityBuffer.set(key, (activityBuffer.get(key) ?? 0) | bit);

          // Collection activity: track anything not already covered by the bitmask
          if (!collectionTrackingPaused && !(collection in ACTIVITY_BITS)) {
            const ckey = `${collection}|${evt.did}|${date}`;
            collectionBuffer.set(ckey, (collectionBuffer.get(ckey) ?? 0) + 1);
          }

          // Reply / quote sub-type bits
          if (collection === "app.bsky.feed.post") {
            const record = evt.commit.record;
            if (record?.reply) {
              activityBuffer.set(key, (activityBuffer.get(key) ?? 0) | BIT_REPLIED);
            }
            const embedType = record?.embed?.$type;
            if (embedType === "app.bsky.embed.record" || embedType === "app.bsky.embed.recordWithMedia") {
              activityBuffer.set(key, (activityBuffer.get(key) ?? 0) | BIT_QUOTED);
            }
          }

          // Lang tracking: extract langs[] from post records
          if (collection === "app.bsky.feed.post") {
            const ls = langStatsBuffer.get(date) ?? { total: 0, tagged: 0 };
            ls.total++;
            const langs = evt.commit.record?.langs;
            if (Array.isArray(langs) && langs.length > 0) {
              ls.tagged++;
              for (const lang of langs as string[]) {
                if (typeof lang !== "string" || lang.length > 20) continue;
                const key = `${evt.did}|${lang}`;
                langBuffer.set(key, (langBuffer.get(key) ?? 0) + 1);
              }
            }
            langStatsBuffer.set(date, ls);
          }

          // Starter pack joins: profile creates that reference a joinedViaStarterPack
          if (collection === "app.bsky.actor.profile") {
            const uri = evt.commit.record?.joinedViaStarterPack?.uri;
            if (typeof uri === "string" && uri.startsWith("at://")) {
              const spKey = `${uri}|${date}`;
              starterpackBuffer.set(spKey, (starterpackBuffer.get(spKey) ?? 0) + 1);
            }
          }

          // Feed generator creates: capture URI + metadata from the record
          if (collection === "app.bsky.feed.generator") {
            const uri = `at://${evt.did}/app.bsky.feed.generator/${evt.commit.rkey}`;
            feedGenBuffer.set(uri, {
              creatorDid:  evt.did,
              displayName: evt.commit.record?.displayName ?? null,
              description: evt.commit.record?.description ?? null,
              firstSeen:   date,
            });
          }

          // Feed likes: only count likes whose subject is a feed generator
          if (collection === "app.bsky.feed.like") {
            const subjectUri = evt.commit.record?.subject?.uri;
            if (typeof subjectUri === "string" && subjectUri.includes("/app.bsky.feed.generator/")) {
              const lkey = `${subjectUri}|${date}`;
              feedLikeBuffer.set(lkey, (feedLikeBuffer.get(lkey) ?? 0) + 1);
            }
          }
        } else if (evt.commit.operation === "delete") {
          // Record-level delete by collection
          recordDelete(`${date}|record:${evt.commit.collection}`);

          // Feed generator deletes: mark the URI so we can tombstone the row
          if (evt.commit.collection === "app.bsky.feed.generator") {
            feedGenDeleteBuffer.add(`at://${evt.did}/app.bsky.feed.generator/${evt.commit.rkey}`);
          }
        }
      } else if (evt.kind === "account") {
        const status = evt.account?.active === false
          ? (evt.account.status ?? "deleted")
          : "reactivated";
        recordDelete(`${date}|account:${status}`);
      } else if (evt.kind === "tombstone") {
        recordDelete(`${date}|tombstone`);
      }
    } catch {
      // skip malformed
    }
  });

  ws.on("pong", () => {
    // Server acknowledged our ping — connection is alive
    resetStallTimer();
  });

  ws.on("error", (err: Error) => {
    console.error(`[activity] WebSocket error: ${err.message}`);
  });

  ws.on("close", (code, reason) => {
    cleanup();
    flushSync();
    relayIdx++;
    const next = JETSTREAM_RELAYS[relayIdx % JETSTREAM_RELAYS.length];
    const why = code !== 1000 ? ` (code ${code}${reason?.length ? `: ${reason}` : ""})` : "";
    console.log(`[activity] Disconnected${why}. Trying ${next.replace("wss://", "")} in ${RECONNECT_DELAY_MS / 1000}s...`);
    setTimeout(connect, RECONNECT_DELAY_MS);
  });
}

async function main() {
  console.log(`\n=== Jetstream Activity Collector ===`);

  if (BACKFILL_HOURS !== null) {
    const db = getActivityDb();
    const backfillDate = new Date(Date.now() - BACKFILL_HOURS * 60 * 60 * 1000).toISOString().slice(0, 10);
    console.log(`[activity] Clearing additive daily tables from ${backfillDate} to prevent double-counting on backfill...`);
    db.prepare(`DELETE FROM delete_events_daily WHERE date >= ?`).run(backfillDate);
    db.prepare(`DELETE FROM starterpack_joins_daily WHERE date >= ?`).run(backfillDate);
    db.prepare(`DELETE FROM feed_generator_likes_daily WHERE date >= ?`).run(backfillDate);
    db.prepare(`DELETE FROM lang_stats WHERE date >= ?`).run(backfillDate);
    // collection_activity uses (collection, did, date) PK — replaying events only inflates
    // event_count, not unique DID counts. No delete needed; backfill upserts safely.
  }

  setInterval(() => {
    flush()
      .then(() => pruneOldRows())
      .then(() => resolveAndStoreDidWebs())
      .catch(err => console.error("[activity] flush error:", err));
  }, FLUSH_INTERVAL_MS);

  for (const sig of ["SIGINT", "SIGTERM"]) {
    process.on(sig, () => {
      console.log(`\n[activity] ${sig} received, flushing...`);
      flushSync();
      process.exit(0);
    });
  }

  connect();
}

main();
