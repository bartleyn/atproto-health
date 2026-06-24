/**
 * Long-running Jetstream collector that tracks:
 *   1. Daily account activity (did_activity_daily) — one row per (did, date), bitmask of collections used
 *   2. Delete events (delete_events_daily) — daily counts by event type
 *   2b. Per-DID post deletes (post_deletes_daily) — app.bsky.feed.post deletes per (did, date)
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
 *   --no-scoring                      disable post scoring + GCS upload entirely
 *   --min-toxicity <float>            only upload posts where toxicity >= value (default: 0)
 *   --scorer-interval <seconds>       how often to flush scored posts to GCS (default: 300)
 *
 * Env vars for post scorer:
 *   TOXIC_API_URL      Fly.io API base URL (default: https://toxic-cicd.fly.dev)
 *   GCS_SCORED_BUCKET  GCS bucket (default: bsky-labeled-posts)
 *   GCS_SCORED_PREFIX  Object prefix (default: posts/)
 *
 * Usage:
 *   npm run collect:activity
 *   npm run collect:activity -- --retention-days 90
 *   npm run collect:activity -- --no-collection-tracking
 *   npm run collect:activity -- --max-collection-rows 1000000
 *   npm run collect:activity -- --no-scoring
 *   npm run collect:activity -- --min-toxicity 0.5
 */

import WebSocket from "ws";
import sql from "../db/pg";
import { bufferPost, flushScorer, scorerShutdown } from "./post-scorer";

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
  "store.tz2at.*",                    // tz2at
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
// During backfill, events arrive faster than the timer fires. Flush when the three
// largest buffers exceed this combined entry count to keep heap pressure bounded.
const BUFFER_FLUSH_THRESHOLD = 200_000;

const args = process.argv.slice(2);
const retentionIdx = args.indexOf("--retention-days");
const RETENTION_DAYS = retentionIdx >= 0 ? parseInt(args[retentionIdx + 1], 10) : 90;
const backfillIdx = args.indexOf("--backfill-hours");
const BACKFILL_HOURS = backfillIdx >= 0 ? parseInt(args[backfillIdx + 1], 10) : null;
const NO_COLLECTION_TRACKING = args.includes("--no-collection-tracking");
const maxCollectionRowsIdx = args.indexOf("--max-collection-rows");
const MAX_COLLECTION_ROWS = maxCollectionRowsIdx >= 0 ? parseInt(args[maxCollectionRowsIdx + 1], 10) : 5_000_000;
const scorerIntervalIdx = args.indexOf("--scorer-interval");
const SCORER_FLUSH_INTERVAL_MS = (scorerIntervalIdx >= 0 ? parseInt(args[scorerIntervalIdx + 1], 10) : 300) * 1000;

// Activity buffer: "did|date" → bitmask of activity_types seen
const activityBuffer = new Map<string, number>();

// Delete buffer: "date|event_type" → count
const deleteBuffer = new Map<string, number>();

// Per-DID post-delete buffer: "did|date" → count (app.bsky.feed.post deletes only).
// delete_events_daily only keeps aggregate-by-collection counts; this attributes
// post deletes to the account doing them so we can see who deletes posts.
const postDeleteBuffer = new Map<string, number>();

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

// Guard so a single pressure flush is in-flight at a time
let pressureFlushPending = false;

// did:web PDS discovery: DIDs seen in the stream that need did document resolution
const didWebSeen = new Set<string>();
// Cache of DIDs we've already successfully resolved so we don't re-hit them every flush
const didWebResolved = new Set<string>();

let lastCursor = 0;
let totalActivityFlushed = 0;
let totalDeletesFlushed = 0;
let totalPostDeletesFlushed = 0;
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

// Resolve any newly-seen did:web DIDs and write discovered PDS URLs to plc.did_web_pds.
// Runs async alongside each flush; does not block the main event loop.
async function resolveAndStoreDidWebs() {
  if (didWebSeen.size === 0) return;

  const toResolve = [...didWebSeen].filter(d => !didWebResolved.has(d));
  didWebSeen.clear();
  if (toResolve.length === 0) return;

  const stored = await sql<{ did: string }[]>`SELECT did FROM plc.did_web_pds WHERE pds_url IS NOT NULL`;
  const alreadyStored = new Set(stored.map(r => r.did));
  const fresh = toResolve.filter(d => !alreadyStored.has(d));
  for (const d of alreadyStored) didWebResolved.add(d);
  if (fresh.length === 0) return;

  const CONCURRENCY = 5;
  let discovered = 0;
  for (let i = 0; i < fresh.length; i += CONCURRENCY) {
    const batch = fresh.slice(i, i + CONCURRENCY);
    const results = await Promise.all(batch.map(async did => ({ did, pds_url: await resolveDidWebPds(did) })));
    await sql`
      INSERT INTO plc.did_web_pds ${sql(results.map(r => ({
        did: r.did, pds_url: r.pds_url,
        first_seen: new Date(), last_seen: new Date(), resolved_at: new Date(),
      })))}
      ON CONFLICT (did) DO UPDATE SET
        pds_url     = COALESCE(EXCLUDED.pds_url, plc.did_web_pds.pds_url),
        last_seen   = EXCLUDED.last_seen,
        resolved_at = CASE WHEN EXCLUDED.pds_url IS NOT NULL THEN EXCLUDED.resolved_at ELSE plc.did_web_pds.resolved_at END
    `;
    for (const { did, pds_url } of results) {
      if (pds_url) { didWebResolved.add(did); discovered++; }
    }
  }

  if (discovered > 0) {
    console.log(`[activity] Discovered ${discovered} new did:web PDS${discovered > 1 ? "es" : ""} (${fresh.length} DIDs resolved)`);
  }
}

// Snapshot all in-memory buffers and clear them. Synchronous — call this at the top
// of flush() so new incoming events accumulate in fresh buffers while we write to PG.
// Collection row-count check is done async before this call in flush().
function snapshotBuffers() {
  const snapshot = {
    activityRows:    [...activityBuffer.entries()],
    deleteRows:      [...deleteBuffer.entries()],
    postDeleteRows:  [...postDeleteBuffer.entries()],
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
  postDeleteBuffer.clear();
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
  const { activityRows, deleteRows, postDeleteRows, starterpackRows, langRows, langStatsRows, collectionRows, feedGenRows, feedLikeRows } = snapshot;
  const postsThisFlush  = langStatsRows.reduce((s, [, v]) => s + v.total, 0);
  const taggedThisFlush = langStatsRows.reduce((s, [, v]) => s + v.tagged, 0);
  totalActivityFlushed    += activityRows.length;
  totalDeletesFlushed     += deleteRows.reduce((s, [, c]) => s + c, 0);
  totalPostDeletesFlushed += postDeleteRows.reduce((s, [, c]) => s + c, 0);
  totalStarterpackFlushed += starterpackRows.reduce((s, [, c]) => s + c, 0);
  totalLangDIDsFlushed    += langRows.length;
  totalCollectionsFlushed += collectionRows.length;
  totalFeedGensFlushed    += feedGenRows.length;
  totalFeedLikesFlushed   += feedLikeRows.reduce((s, [, c]) => s + c, 0);
  const tagPct = postsThisFlush > 0 ? ((taggedThisFlush / postsThisFlush) * 100).toFixed(1) : "—";
  const collectionNote = collectionTrackingPaused ? " [collection tracking PAUSED]" : ` collections=${collectionRows.length.toLocaleString()}`;
  console.log(
    `[activity] Flushed activity=${activityRows.length.toLocaleString()} deletes=${deleteRows.length} types postdel=${postDeleteRows.length.toLocaleString()} dids starterpack=${starterpackRows.length} uris lang=${langRows.length.toLocaleString()} did×lang (${tagPct}% tagged)${collectionNote} feeds=${feedGenRows.length} feed_likes=${feedLikeRows.reduce((s, [, c]) => s + c, 0)} | ` +
    `totals: activity=${totalActivityFlushed.toLocaleString()} deletes=${totalDeletesFlushed.toLocaleString()} postdel=${totalPostDeletesFlushed.toLocaleString()} starterpack=${totalStarterpackFlushed.toLocaleString()} lang_dids=${totalLangDIDsFlushed.toLocaleString()} collections=${totalCollectionsFlushed.toLocaleString()} feed_gens=${totalFeedGensFlushed.toLocaleString()} feed_likes=${totalFeedLikesFlushed.toLocaleString()} | ` +
    `events=${totalEvents.toLocaleString()} | cursor=${snapshot.cursorToSave}`
  );
}

// Async flush: snapshots buffers immediately (so new events accumulate while we write),
// then upserts to Postgres in chunks, yielding between chunks to keep the event loop
// responsive during backfill when events arrive faster than writes can drain.
const ACTIVITY_CHUNK_SIZE = 5_000;

async function flush() {
  if (activityBuffer.size === 0 && deleteBuffer.size === 0 && starterpackBuffer.size === 0 && langBuffer.size === 0 && collectionBuffer.size === 0 && feedGenBuffer.size === 0 && feedGenDeleteBuffer.size === 0 && feedLikeBuffer.size === 0) return;

  // Async collection row-count check before snapshotting buffers.
  if (!collectionTrackingPaused && collectionBuffer.size > 0) {
    const [{ n }] = await sql<{ n: number }[]>`SELECT COUNT(*)::int AS n FROM activity.collection_activity`;
    if (n >= MAX_COLLECTION_ROWS) {
      console.warn(`[activity] collection_activity has ${n.toLocaleString()} rows (limit ${MAX_COLLECTION_ROWS.toLocaleString()}). Pausing collection tracking.`);
      collectionTrackingPaused = true;
      collectionBuffer.clear();
    }
  }

  const snapshot = snapshotBuffers();
  const { activityRows, deleteRows, postDeleteRows, starterpackRows, langRows, langStatsRows, collectionRows, feedGenRows, feedGenDeletes, feedLikeRows, cursorToSave } = snapshot;

  // did_activity_daily — chunked upsert with bitwise OR merge.
  for (let i = 0; i < activityRows.length; i += ACTIVITY_CHUNK_SIZE) {
    const chunk = activityRows.slice(i, i + ACTIVITY_CHUNK_SIZE).map(([entry, bits]) => {
      const pipe = entry.indexOf("|");
      return { did: entry.slice(0, pipe), date: entry.slice(pipe + 1), activity_types: bits };
    });
    await sql`
      INSERT INTO activity.did_activity_daily ${sql(chunk, "did", "date", "activity_types")}
      ON CONFLICT (did, date) DO UPDATE SET
        activity_types = did_activity_daily.activity_types | EXCLUDED.activity_types
    `;
    if (i + ACTIVITY_CHUNK_SIZE < activityRows.length) {
      await new Promise<void>(resolve => setImmediate(resolve));
    }
  }

  // did_langs — chunked upsert accumulating post counts.
  const now = new Date();
  for (let i = 0; i < langRows.length; i += ACTIVITY_CHUNK_SIZE) {
    const chunk = langRows.slice(i, i + ACTIVITY_CHUNK_SIZE).map(([key, count]) => {
      const pipe = key.indexOf("|");
      return { did: key.slice(0, pipe), lang: key.slice(pipe + 1), post_count: count, last_seen: now };
    });
    await sql`
      INSERT INTO activity.did_langs ${sql(chunk, "did", "lang", "post_count", "last_seen")}
      ON CONFLICT (did, lang) DO UPDATE SET
        post_count = did_langs.post_count + EXCLUDED.post_count,
        last_seen  = EXCLUDED.last_seen
    `;
    if (i + ACTIVITY_CHUNK_SIZE < langRows.length) {
      await new Promise<void>(resolve => setImmediate(resolve));
    }
  }

  // post_deletes_daily — one row per (did, date); can be many distinct DIDs per
  // flush, so chunk + yield like did_activity_daily (3 cols, ~21K-row param cap).
  for (let i = 0; i < postDeleteRows.length; i += ACTIVITY_CHUNK_SIZE) {
    const chunk = postDeleteRows.slice(i, i + ACTIVITY_CHUNK_SIZE).map(([entry, count]) => {
      const pipe = entry.indexOf("|");
      return { did: entry.slice(0, pipe), date: entry.slice(pipe + 1), count };
    });
    await sql`
      INSERT INTO activity.post_deletes_daily ${sql(chunk, "did", "date", "count")}
      ON CONFLICT (did, date) DO UPDATE SET count = post_deletes_daily.count + EXCLUDED.count
    `;
    if (i + ACTIVITY_CHUNK_SIZE < postDeleteRows.length) {
      await new Promise<void>(resolve => setImmediate(resolve));
    }
  }

  // collection_activity is unbounded per flush (one row per collection×did×date) and
  // starterpack_joins can grow large too — batch both to stay under Postgres's
  // 65,534-parameter limit (4 / 3 cols per row respectively).
  for (let i = 0; i < collectionRows.length; i += ACTIVITY_CHUNK_SIZE) {
    const chunk = collectionRows.slice(i, i + ACTIVITY_CHUNK_SIZE).map(([key, count]) => {
      const [collection, did, date] = key.split("|");
      return { collection, did, date, event_count: count };
    });
    await sql`
      INSERT INTO activity.collection_activity ${sql(chunk, "collection", "did", "date", "event_count")}
      ON CONFLICT (collection, did, date) DO UPDATE SET
        event_count = collection_activity.event_count + EXCLUDED.event_count
    `;
  }
  for (let i = 0; i < starterpackRows.length; i += ACTIVITY_CHUNK_SIZE) {
    const chunk = starterpackRows.slice(i, i + ACTIVITY_CHUNK_SIZE).map(([key, count]) => {
      const pipe = key.indexOf("|");
      return { starterpack_uri: key.slice(0, pipe), date: key.slice(pipe + 1), count };
    });
    await sql`
      INSERT INTO activity.starterpack_joins_daily ${sql(chunk, "starterpack_uri", "date", "count")}
      ON CONFLICT (starterpack_uri, date) DO UPDATE SET count = starterpack_joins_daily.count + EXCLUDED.count
    `;
  }

  // Small tables (bounded by event-type / distinct-feed / date cardinality) — fire in parallel.
  await Promise.all([
    deleteRows.length > 0 && sql`
      INSERT INTO activity.delete_events_daily ${sql(deleteRows.map(([key, count]) => {
        const pipe = key.indexOf("|");
        return { date: key.slice(0, pipe), event_type: key.slice(pipe + 1), count };
      }), "date", "event_type", "count")}
      ON CONFLICT (date, event_type) DO UPDATE SET count = delete_events_daily.count + EXCLUDED.count
    `,
    feedGenRows.length > 0 && sql`
      INSERT INTO activity.feed_generators ${sql(feedGenRows.map(([uri, meta]) => ({
        uri, creator_did: meta.creatorDid, display_name: meta.displayName,
        description: meta.description, first_seen: meta.firstSeen,
      })), "uri", "creator_did", "display_name", "description", "first_seen")}
      ON CONFLICT (uri) DO UPDATE SET
        display_name = COALESCE(EXCLUDED.display_name, feed_generators.display_name),
        description  = COALESCE(EXCLUDED.description,  feed_generators.description),
        deleted_at   = NULL
    `,
    feedGenDeletes.length > 0 && sql`
      UPDATE activity.feed_generators SET deleted_at = NOW()
      WHERE uri = ANY(${feedGenDeletes})
    `,
    feedLikeRows.length > 0 && sql`
      INSERT INTO activity.feed_generator_likes_daily ${sql(feedLikeRows.map(([key, count]) => {
        const pipe = key.lastIndexOf("|");
        return { feed_uri: key.slice(0, pipe), date: key.slice(pipe + 1), likes: count };
      }), "feed_uri", "date", "likes")}
      ON CONFLICT (feed_uri, date) DO UPDATE SET likes = feed_generator_likes_daily.likes + EXCLUDED.likes
    `,
    langStatsRows.filter(([, v]) => v.total > 0).length > 0 && sql`
      INSERT INTO activity.lang_stats ${sql(langStatsRows.filter(([, v]) => v.total > 0).map(([date, { total, tagged }]) => ({
        date, total_posts: total, tagged_posts: tagged, updated_at: now,
      })), "date", "total_posts", "tagged_posts", "updated_at")}
      ON CONFLICT (date) DO UPDATE SET
        total_posts  = lang_stats.total_posts  + EXCLUDED.total_posts,
        tagged_posts = lang_stats.tagged_posts + EXCLUDED.tagged_posts,
        updated_at   = EXCLUDED.updated_at
    `,
  ].filter(Boolean));

  // Save cursor only after all data writes succeed — ensures we can replay safely on failure.
  if (cursorToSave > 0) {
    await sql`
      INSERT INTO activity.jetstream_cursor (id, cursor, updated_at)
      VALUES (1, ${BigInt(cursorToSave)}, NOW())
      ON CONFLICT (id) DO UPDATE SET cursor = EXCLUDED.cursor, updated_at = EXCLUDED.updated_at
    `;
  }

  logFlush(snapshot);
}

// Serialize all flushes. flush() is fired from four places (timer, pressure-flush,
// reconnect-close, shutdown); nothing stopped two from running at once, and two
// concurrent flush() calls grab different pooled connections and run INSERT ... ON
// CONFLICT on did_activity_daily in different key orders → 40P01 self-deadlock
// (and lost flushes => undercounts on the additive tables). flushNow() runs flushes
// one at a time on a promise chain. Callers that arrive while a flush is already
// queued (not yet started) coalesce onto that queued run, and the returned promise
// resolves only when their covering flush completes — so the pressure path can still
// await an actual drain before it resumes the socket.
let flushChain: Promise<void> = Promise.resolve();
let flushQueued = false;
function flushNow(): Promise<void> {
  if (flushQueued) return flushChain;       // a not-yet-started flush will cover us
  flushQueued = true;
  flushChain = flushChain
    .catch(() => {})                        // a prior failure must not break the chain
    .then(() => { flushQueued = false; return flush(); });
  return flushChain;
}

async function pruneOldRows() {
  const cutoff = new Date(Date.now() - RETENTION_DAYS * 24 * 60 * 60 * 1000)
    .toISOString().slice(0, 10);
  const [a, d] = await Promise.all([
    sql`DELETE FROM activity.did_activity_daily WHERE date < ${cutoff}`,
    sql`DELETE FROM activity.delete_events_daily WHERE date < ${cutoff}`,
    sql`DELETE FROM activity.post_deletes_daily WHERE date < ${cutoff}`,
    sql`DELETE FROM activity.lang_stats WHERE date < ${cutoff}`,
    sql`DELETE FROM activity.collection_activity WHERE date < ${cutoff}`,
  ]);
  if (a.count > 0 || d.count > 0) {
    console.log(`[activity] Pruned ${a.count.toLocaleString()} activity + ${d.count.toLocaleString()} delete rows older than ${cutoff}`);
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

// lastCursor is loaded from PG at startup in main(); on reconnect we pass whatever
// we last observed so the relay resumes from the right position.
function connect() {
  const cursor = lastCursor || undefined;

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

          // Reply / quote sub-type bits + post scoring buffer
          if (collection === "app.bsky.feed.post") {
            const record = evt.commit.record;
            if (record?.reply) {
              activityBuffer.set(key, (activityBuffer.get(key) ?? 0) | BIT_REPLIED);
            }
            const embedType = record?.embed?.$type;
            if (embedType === "app.bsky.embed.record" || embedType === "app.bsky.embed.recordWithMedia") {
              activityBuffer.set(key, (activityBuffer.get(key) ?? 0) | BIT_QUOTED);
            }
            const text = typeof record?.text === "string" ? record.text.trim() : "";
            if (text.length > 0) {
              bufferPost({
                uri:           `at://${evt.did}/app.bsky.feed.post/${evt.commit.rkey}`,
                did:           evt.did,
                rkey:          evt.commit.rkey,
                text,
                langs:         Array.isArray(record?.langs) ? record.langs : [],
                reply_to:      typeof record?.reply?.parent?.uri === "string" ? record.reply.parent.uri : null,
                quote_of:      (embedType === "app.bsky.embed.record" || embedType === "app.bsky.embed.recordWithMedia")
                                 ? (record?.embed?.record?.uri ?? null)
                                 : null,
                created_at_us: evt.time_us,
              });
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

          // Per-DID attribution for post deletes (who deletes posts, how many)
          if (evt.commit.collection === "app.bsky.feed.post") {
            const pkey = `${evt.did}|${date}`;
            postDeleteBuffer.set(pkey, (postDeleteBuffer.get(pkey) ?? 0) + 1);
          }

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
      // Pressure-based flush: during backfill events arrive faster than async writes
      // can drain. Pause the socket so TCP backpressure propagates to the relay and
      // we stop accumulating until the flush completes.
      if (!pressureFlushPending &&
          activityBuffer.size + langBuffer.size + collectionBuffer.size > BUFFER_FLUSH_THRESHOLD) {
        pressureFlushPending = true;
        ws.pause();
        if (stallTimer) { clearTimeout(stallTimer); stallTimer = null; }
        flushNow()
          .catch(err => console.error("[activity] pressure flush error:", err))
          .finally(() => {
            pressureFlushPending = false;
            ws.resume();
            resetStallTimer();
          });
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
    flushNow().catch(err => console.error("[activity] close flush error:", err));
    relayIdx++;
    const next = JETSTREAM_RELAYS[relayIdx % JETSTREAM_RELAYS.length];
    const why = code !== 1000 ? ` (code ${code}${reason?.length ? `: ${reason}` : ""})` : "";
    console.log(`[activity] Disconnected${why}. Trying ${next.replace("wss://", "")} in ${RECONNECT_DELAY_MS / 1000}s...`);
    setTimeout(connect, RECONNECT_DELAY_MS);
  });
}

async function main() {
  console.log(`\n=== Jetstream Activity Collector ===`);

  // Load cursor from PG (or compute backfill start).
  if (BACKFILL_HOURS !== null) {
    const backfillStartMs   = Date.now() - BACKFILL_HOURS * 60 * 60 * 1000;
    lastCursor              = backfillStartMs * 1000;
    const backfillDate      = new Date(backfillStartMs).toISOString().slice(0, 10);
    const midnightMs        = new Date(backfillDate + "T00:00:00.000Z").getTime();
    const isPartialStartDay = backfillStartMs > midnightMs + 1000;
    const deleteFromDate    = isPartialStartDay
      ? new Date(midnightMs + 86_400_000).toISOString().slice(0, 10)
      : backfillDate;
    console.log(`[activity] Backfill mode: cursor set to ${BACKFILL_HOURS}h ago (${new Date(backfillStartMs).toISOString()})`);
    console.log(`[activity] Clearing additive daily tables from ${deleteFromDate} to prevent double-counting...`
      + (isPartialStartDay ? ` (skipping partial start date ${backfillDate})` : ""));
    // collection_activity uses (collection, did, date) PK — replaying only inflates
    // event_count, not unique DID counts. No delete needed; backfill upserts safely.
    await Promise.all([
      sql`DELETE FROM activity.delete_events_daily WHERE date >= ${deleteFromDate}`,
      sql`DELETE FROM activity.post_deletes_daily WHERE date >= ${deleteFromDate}`,
      sql`DELETE FROM activity.starterpack_joins_daily WHERE date >= ${deleteFromDate}`,
      sql`DELETE FROM activity.feed_generator_likes_daily WHERE date >= ${deleteFromDate}`,
      sql`DELETE FROM activity.lang_stats WHERE date >= ${deleteFromDate}`,
    ]);
  } else {
    const rows = await sql<{ cursor: string }[]>`SELECT cursor FROM activity.jetstream_cursor WHERE id = 1`;
    lastCursor = rows[0] ? Number(rows[0].cursor) : 0;
    if (lastCursor) console.log(`[activity] Resuming from cursor ${lastCursor} (${new Date(lastCursor / 1000).toISOString()})`);
  }

  setInterval(() => {
    flushNow()
      .then(() => pruneOldRows())
      .then(() => resolveAndStoreDidWebs())
      .catch(err => console.error("[activity] flush error:", err));
  }, FLUSH_INTERVAL_MS);

  setInterval(() => {
    flushScorer().catch(err => console.error("[scorer] flush error:", err));
  }, SCORER_FLUSH_INTERVAL_MS);

  for (const sig of ["SIGINT", "SIGTERM"]) {
    process.on(sig, () => {
      console.log(`\n[activity] ${sig} received, flushing...`);
      flushNow()
        .then(() => scorerShutdown())
        .then(() => sql.end())
        .then(() => process.exit(0))
        .catch(() => process.exit(1));
    });
  }

  connect();
}

main();
