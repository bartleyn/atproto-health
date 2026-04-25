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

const FLUSH_INTERVAL_MS  = 5 * 60 * 1000;
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
let langPostsSeen = 0;    // total posts in this flush window
let langTaggedSeen = 0;   // posts with at least one lang tag in this flush window

// Collection buffer: "collection|did" → event count (non-bsky/non-chat-bsky creates only)
const collectionBuffer = new Map<string, number>();
let collectionTrackingPaused = NO_COLLECTION_TRACKING;

let lastCursor = 0;
let totalActivityFlushed = 0;
let totalDeletesFlushed = 0;
let totalStarterpackFlushed = 0;
let totalLangDIDsFlushed = 0;
let totalCollectionsFlushed = 0;
let totalEvents = 0;

function flush() {
  if (activityBuffer.size === 0 && deleteBuffer.size === 0 && starterpackBuffer.size === 0 && langBuffer.size === 0 && collectionBuffer.size === 0) return;

  const db = getActivityDb();

  // Check collection_activity row count and pause if over the limit
  if (!collectionTrackingPaused && collectionBuffer.size > 0) {
    const { n } = db.prepare(`SELECT COUNT(*) AS n FROM collection_activity`).get() as { n: number };
    if (n >= MAX_COLLECTION_ROWS) {
      console.warn(`[activity] collection_activity has ${n.toLocaleString()} rows (limit ${MAX_COLLECTION_ROWS.toLocaleString()}). Pausing collection tracking. Restart with --no-collection-tracking to disable permanently.`);
      collectionTrackingPaused = true;
      collectionBuffer.clear();
    }
  }

  const upsertActivity = db.prepare(`
    INSERT INTO did_activity_daily (did, date, activity_types) VALUES (?, ?, ?)
    ON CONFLICT (did, date) DO UPDATE SET activity_types = activity_types | excluded.activity_types
  `);
  const upsertDelete = db.prepare(`
    INSERT INTO delete_events_daily (date, event_type, count) VALUES (?, ?, ?)
    ON CONFLICT (date, event_type) DO UPDATE SET count = count + excluded.count
  `);
  const upsertStarterpack = db.prepare(`
    INSERT INTO starterpack_joins_daily (starterpack_uri, date, count) VALUES (?, ?, ?)
    ON CONFLICT (starterpack_uri, date) DO UPDATE SET count = count + excluded.count
  `);
  const upsertLang = db.prepare(`
    INSERT INTO did_langs (did, lang, post_count, last_seen) VALUES (?, ?, ?, datetime('now'))
    ON CONFLICT (did, lang) DO UPDATE SET
      post_count = post_count + excluded.post_count,
      last_seen  = excluded.last_seen
  `);
  const upsertLangStats = db.prepare(`
    INSERT INTO lang_stats (id, total_posts, tagged_posts, updated_at) VALUES (1, ?, ?, datetime('now'))
    ON CONFLICT (id) DO UPDATE SET
      total_posts  = total_posts  + excluded.total_posts,
      tagged_posts = tagged_posts + excluded.tagged_posts,
      updated_at   = excluded.updated_at
  `);
  const upsertCollection = db.prepare(`
    INSERT INTO collection_activity (collection, did, event_count, last_seen)
    VALUES (?, ?, ?, datetime('now'))
    ON CONFLICT (collection, did) DO UPDATE SET
      event_count = event_count + excluded.event_count,
      last_seen   = excluded.last_seen
  `);
  const saveCursor = db.prepare(`
    INSERT INTO jetstream_cursor (id, cursor, updated_at)
    VALUES (1, ?, datetime('now'))
    ON CONFLICT (id) DO UPDATE SET cursor = excluded.cursor, updated_at = excluded.updated_at
  `);

  const activityRows    = [...activityBuffer.entries()];
  const deleteRows      = [...deleteBuffer.entries()];
  const starterpackRows = [...starterpackBuffer.entries()];
  const langRows        = [...langBuffer.entries()];
  const collectionRows  = collectionTrackingPaused ? [] : [...collectionBuffer.entries()];
  const postsThisFlush  = langPostsSeen;
  const taggedThisFlush = langTaggedSeen;
  activityBuffer.clear();
  deleteBuffer.clear();
  starterpackBuffer.clear();
  langBuffer.clear();
  collectionBuffer.clear();
  langPostsSeen  = 0;
  langTaggedSeen = 0;

  db.transaction(() => {
    for (const [entry, bits] of activityRows) {
      const pipe = entry.indexOf("|");
      upsertActivity.run(entry.slice(0, pipe), entry.slice(pipe + 1), bits);
    }
    for (const [key, count] of deleteRows) {
      const pipe = key.indexOf("|");
      upsertDelete.run(key.slice(0, pipe), key.slice(pipe + 1), count);
    }
    for (const [key, count] of starterpackRows) {
      const pipe = key.indexOf("|");
      upsertStarterpack.run(key.slice(0, pipe), key.slice(pipe + 1), count);
    }
    for (const [key, count] of langRows) {
      const pipe = key.indexOf("|");
      upsertLang.run(key.slice(0, pipe), key.slice(pipe + 1), count);
    }
    for (const [key, count] of collectionRows) {
      const pipe = key.indexOf("|");
      upsertCollection.run(key.slice(0, pipe), key.slice(pipe + 1), count);
    }
    if (postsThisFlush > 0) upsertLangStats.run(postsThisFlush, taggedThisFlush);
    if (lastCursor > 0) saveCursor.run(lastCursor);
  })();

  totalActivityFlushed    += activityRows.length;
  totalDeletesFlushed     += deleteRows.reduce((s, [, c]) => s + c, 0);
  totalStarterpackFlushed += starterpackRows.reduce((s, [, c]) => s + c, 0);
  totalLangDIDsFlushed    += langRows.length;
  totalCollectionsFlushed += collectionRows.length;
  const tagPct = postsThisFlush > 0 ? ((taggedThisFlush / postsThisFlush) * 100).toFixed(1) : "—";
  const collectionNote = collectionTrackingPaused ? " [collection tracking PAUSED]" : ` collections=${collectionRows.length.toLocaleString()}`;
  console.log(
    `[activity] Flushed activity=${activityRows.length.toLocaleString()} deletes=${deleteRows.length} types starterpack=${starterpackRows.length} uris lang=${langRows.length.toLocaleString()} did×lang (${tagPct}% tagged)${collectionNote} | ` +
    `totals: activity=${totalActivityFlushed.toLocaleString()} deletes=${totalDeletesFlushed.toLocaleString()} starterpack=${totalStarterpackFlushed.toLocaleString()} lang_dids=${totalLangDIDsFlushed.toLocaleString()} collections=${totalCollectionsFlushed.toLocaleString()} | ` +
    `events=${totalEvents.toLocaleString()} | cursor=${lastCursor}`
  );
}

function pruneOldRows() {
  const db = getActivityDb();
  const cutoff = new Date(Date.now() - RETENTION_DAYS * 24 * 60 * 60 * 1000)
    .toISOString().slice(0, 10);
  const a = db.prepare(`DELETE FROM did_activity_daily WHERE date < ?`).run(cutoff);
  const d = db.prepare(`DELETE FROM delete_events_daily WHERE date < ?`).run(cutoff);
  if (a.changes > 0 || d.changes > 0) {
    console.log(`[activity] Pruned ${a.changes.toLocaleString()} activity + ${d.changes.toLocaleString()} delete rows older than ${cutoff}`);
  }
}

function recordDelete(key: string) {
  deleteBuffer.set(key, (deleteBuffer.get(key) ?? 0) + 1);
}

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

  ws.on("open", () => {
    const collectionStatus = NO_COLLECTION_TRACKING
      ? "DISABLED (--no-collection-tracking)"
      : `enabled, pauses at ${MAX_COLLECTION_ROWS.toLocaleString()} rows`;
    console.log(`[activity] Connected. Retention: ${RETENTION_DAYS}d. Flushing every ${FLUSH_INTERVAL_MS / 1000}s. Collection tracking: ${collectionStatus}`);
  });

  ws.on("message", (data: Buffer) => {
    try {
      const evt = JSON.parse(data.toString());
      const date = new Date(evt.time_us / 1000).toISOString().slice(0, 10);
      lastCursor = evt.time_us;
      totalEvents++;

      if (evt.kind === "commit") {
        if (evt.commit.operation === "create") {
          // Activity: track which collection types were seen per (did, date)
          const collection = evt.commit.collection;
          const key = `${evt.did}|${date}`;
          const bit = ACTIVITY_BITS[collection] ?? 0;
          if (bit) activityBuffer.set(key, (activityBuffer.get(key) ?? 0) | bit);

          // Collection activity: track anything not already covered by the bitmask
          if (!collectionTrackingPaused && !(collection in ACTIVITY_BITS)) {
            const ckey = `${collection}|${evt.did}`;
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
            langPostsSeen++;
            const langs = evt.commit.record?.langs;
            if (Array.isArray(langs) && langs.length > 0) {
              langTaggedSeen++;
              for (const lang of langs as string[]) {
                if (typeof lang !== "string" || lang.length > 20) continue;
                const key = `${evt.did}|${lang}`;
                langBuffer.set(key, (langBuffer.get(key) ?? 0) + 1);
              }
            }
          }

          // Starter pack joins: profile creates that reference a joinedViaStarterPack
          if (collection === "app.bsky.actor.profile") {
            const uri = evt.commit.record?.joinedViaStarterPack?.uri;
            if (typeof uri === "string" && uri.startsWith("at://")) {
              const spKey = `${uri}|${date}`;
              starterpackBuffer.set(spKey, (starterpackBuffer.get(spKey) ?? 0) + 1);
            }
          }
        } else if (evt.commit.operation === "delete") {
          // Record-level delete by collection
          recordDelete(`${date}|record:${evt.commit.collection}`);
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

  ws.on("error", (err: Error) => {
    console.error(`[activity] WebSocket error: ${err.message}`);
  });

  ws.on("close", () => {
    flush();
    relayIdx++;
    const next = JETSTREAM_RELAYS[relayIdx % JETSTREAM_RELAYS.length];
    console.log(`[activity] Disconnected. Trying ${next.replace("wss://", "")} in ${RECONNECT_DELAY_MS / 1000}s...`);
    setTimeout(connect, RECONNECT_DELAY_MS);
  });
}

async function main() {
  console.log(`\n=== Jetstream Activity Collector ===`);

  setInterval(() => {
    flush();
    pruneOldRows();
  }, FLUSH_INTERVAL_MS);

  for (const sig of ["SIGINT", "SIGTERM"]) {
    process.on(sig, () => {
      console.log(`\n[activity] ${sig} received, flushing...`);
      flush();
      process.exit(0);
    });
  }

  connect();
}

main();
