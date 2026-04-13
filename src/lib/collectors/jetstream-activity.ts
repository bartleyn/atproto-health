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

const JETSTREAM_URL = "wss://jetstream2.us-east.bsky.network/subscribe"
  + "?wantedCollections=app.bsky.feed.post"
  + "&wantedCollections=app.bsky.feed.like"
  + "&wantedCollections=app.bsky.feed.repost"
  + "&wantedCollections=app.bsky.graph.follow"
  + "&wantedCollections=app.bsky.graph.block"
  + "&wantedCollections=app.bsky.graph.listitem"
  + "&wantedCollections=app.bsky.graph.listblock"
  + "&wantedCollections=app.bsky.graph.list"
  + "&wantedCollections=app.bsky.feed.threadgate"
  + "&wantedCollections=app.bsky.feed.generator"
  + "&wantedCollections=app.bsky.graph.starterpack";

const FLUSH_INTERVAL_MS  = 5 * 60 * 1000;
const RECONNECT_DELAY_MS = 5_000;

const args = process.argv.slice(2);
const retentionIdx = args.indexOf("--retention-days");
const RETENTION_DAYS = retentionIdx >= 0 ? parseInt(args[retentionIdx + 1], 10) : 90;

// Activity buffer: "did|date" → bitmask of activity_types seen
const activityBuffer = new Map<string, number>();

// Delete buffer: "date|event_type" → count
const deleteBuffer = new Map<string, number>();

let lastCursor = 0;
let totalActivityFlushed = 0;
let totalDeletesFlushed = 0;
let totalEvents = 0;

function flush() {
  if (activityBuffer.size === 0 && deleteBuffer.size === 0) return;

  const db = getActivityDb();

  const upsertActivity = db.prepare(`
    INSERT INTO did_activity_daily (did, date, activity_types) VALUES (?, ?, ?)
    ON CONFLICT (did, date) DO UPDATE SET activity_types = activity_types | excluded.activity_types
  `);
  const upsertDelete = db.prepare(`
    INSERT INTO delete_events_daily (date, event_type, count) VALUES (?, ?, ?)
    ON CONFLICT (date, event_type) DO UPDATE SET count = count + excluded.count
  `);
  const saveCursor = db.prepare(`
    INSERT INTO jetstream_cursor (id, cursor, updated_at)
    VALUES (1, ?, datetime('now'))
    ON CONFLICT (id) DO UPDATE SET cursor = excluded.cursor, updated_at = excluded.updated_at
  `);

  const activityRows = [...activityBuffer.entries()];
  const deleteRows   = [...deleteBuffer.entries()];
  activityBuffer.clear();
  deleteBuffer.clear();

  db.transaction(() => {
    for (const [entry, bits] of activityRows) {
      const pipe = entry.indexOf("|");
      upsertActivity.run(entry.slice(0, pipe), entry.slice(pipe + 1), bits);
    }
    for (const [key, count] of deleteRows) {
      const pipe = key.indexOf("|");
      upsertDelete.run(key.slice(0, pipe), key.slice(pipe + 1), count);
    }
    if (lastCursor > 0) saveCursor.run(lastCursor);
  })();

  totalActivityFlushed += activityRows.length;  // unique (did, date) pairs
  totalDeletesFlushed  += deleteRows.reduce((s, [, c]) => s + c, 0);
  console.log(
    `[activity] Flushed activity=${activityRows.length.toLocaleString()} deletes=${deleteRows.length} types | ` +
    `totals: activity=${totalActivityFlushed.toLocaleString()} deletes=${totalDeletesFlushed.toLocaleString()} | ` +
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
  const cursor = cursorRow?.cursor;

  const url = cursor ? `${JETSTREAM_URL}&cursor=${cursor}` : JETSTREAM_URL;
  console.log(`[activity] Connecting${cursor ? ` from cursor ${cursor}` : " live"}...`);

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
          const key = `${evt.did}|${date}`;
          const bit = ACTIVITY_BITS[evt.commit.collection] ?? 0;
          activityBuffer.set(key, (activityBuffer.get(key) ?? 0) | bit);
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
    console.log(`[activity] Disconnected. Reconnecting in ${RECONNECT_DELAY_MS / 1000}s...`);
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
