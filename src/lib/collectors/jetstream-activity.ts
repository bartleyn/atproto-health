/**
 * Long-running Jetstream collector that tracks:
 *   1. Daily account activity (did_activity_daily) — one row per (did, date)
 *   2. Delete events (delete_events_daily) — daily counts by event type:
 *        record:<collection>  — record-level deletes (posts, likes, etc.)
 *        account:<status>     — account status changes (deleted, deactivated, takendown, suspended, reactivated)
 *        tombstone            — permanent account deletions
 *
 * Uses a separate jetstream-activity.db to avoid write-lock contention with
 * other collectors. Flushes every FLUSH_INTERVAL_MS in a single transaction.
 *
 * Resumable: stores Jetstream cursor (Unix microseconds) for replay after restart.
 *
 * Usage:
 *   npm run collect:activity
 *   npm run collect:activity -- --retention-days 90
 */

import WebSocket from "ws";
import { getActivityDb } from "../db/activity-schema";

// Bitmask assignments for activity_types column
const ACTIVITY_BITS: Record<string, number> = {
  "app.bsky.feed.post":          1 << 0,   //    1
  "app.bsky.feed.like":          1 << 1,   //    2
  "app.bsky.feed.repost":        1 << 2,   //    4
  "app.bsky.graph.follow":       1 << 3,   //    8
  "app.bsky.graph.block":        1 << 4,   //   16
  "app.bsky.graph.listitem":     1 << 5,   //   32
  "app.bsky.graph.listblock":    1 << 6,   //   64
  "app.bsky.graph.list":         1 << 7,   //  128
  "app.bsky.feed.threadgate":    1 << 8,   //  256
  "app.bsky.feed.generator":     1 << 9,   //  512
  "app.bsky.graph.starterpack":  1 << 10,  // 1024
};

const COLLECTIONS = [
  "app.bsky.feed.post",
  "app.bsky.feed.like",
  "app.bsky.feed.repost",
  "app.bsky.graph.follow",
  "app.bsky.graph.block",
  "app.bsky.graph.listitem",
  "app.bsky.graph.listblock",
  "app.bsky.graph.list",
  "app.bsky.feed.threadgate",
  "app.bsky.feed.generator",
  "app.bsky.graph.starterpack",
  "app.bsky.actor.profile",
];

const COLLECTION_PARAMS = COLLECTIONS.map(c => `wantedCollections=${c}`).join("&");

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

let lastCursor = 0;
let totalActivityFlushed = 0;
let totalDeletesFlushed = 0;
let totalStarterpackFlushed = 0;
let totalLangDIDsFlushed = 0;
let totalEvents = 0;

function flush() {
  if (activityBuffer.size === 0 && deleteBuffer.size === 0 && starterpackBuffer.size === 0 && langBuffer.size === 0) return;

  const db = getActivityDb();

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
  const saveCursor = db.prepare(`
    INSERT INTO jetstream_cursor (id, cursor, updated_at)
    VALUES (1, ?, datetime('now'))
    ON CONFLICT (id) DO UPDATE SET cursor = excluded.cursor, updated_at = excluded.updated_at
  `);

  const activityRows    = [...activityBuffer.entries()];
  const deleteRows      = [...deleteBuffer.entries()];
  const starterpackRows = [...starterpackBuffer.entries()];
  const langRows        = [...langBuffer.entries()];
  const postsThisFlush  = langPostsSeen;
  const taggedThisFlush = langTaggedSeen;
  activityBuffer.clear();
  deleteBuffer.clear();
  starterpackBuffer.clear();
  langBuffer.clear();
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
    if (postsThisFlush > 0) upsertLangStats.run(postsThisFlush, taggedThisFlush);
    if (lastCursor > 0) saveCursor.run(lastCursor);
  })();

  totalActivityFlushed    += activityRows.length;
  totalDeletesFlushed     += deleteRows.reduce((s, [, c]) => s + c, 0);
  totalStarterpackFlushed += starterpackRows.reduce((s, [, c]) => s + c, 0);
  totalLangDIDsFlushed    += langRows.length;
  const tagPct = postsThisFlush > 0 ? ((taggedThisFlush / postsThisFlush) * 100).toFixed(1) : "—";
  console.log(
    `[activity] Flushed activity=${activityRows.length.toLocaleString()} deletes=${deleteRows.length} types starterpack=${starterpackRows.length} uris lang=${langRows.length.toLocaleString()} did×lang (${tagPct}% tagged) | ` +
    `totals: activity=${totalActivityFlushed.toLocaleString()} deletes=${totalDeletesFlushed.toLocaleString()} starterpack=${totalStarterpackFlushed.toLocaleString()} lang_dids=${totalLangDIDsFlushed.toLocaleString()} | ` +
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
    console.log(`[activity] Connected. Retention: ${RETENTION_DAYS}d. Flushing every ${FLUSH_INTERVAL_MS / 1000}s.`);
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
