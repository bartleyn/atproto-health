/**
 * Post scorer: buffers app.bsky.feed.post events from the shared Jetstream connection,
 * batch-scores them via the toxic-cicd Fly.io API, and uploads JSONL to GCS.
 *
 * Called from jetstream-activity.ts:
 *   bufferPost()      — called per post create event in the message handler
 *   flushScorer()     — called from the async flush cycle (scores live buffer + drains DLQ)
 *   scorerShutdown()  — called on SIGINT/SIGTERM to persist unscored posts to DLQ
 *
 * Env vars:
 *   TOXIC_API_URL      Fly.io API base URL (default: https://toxic-cicd.fly.dev)
 *   GCS_SCORED_BUCKET  GCS bucket (default: bsky-labeled-posts)
 *   GCS_SCORED_PREFIX  Object prefix (default: posts/)
 *
 * CLI args:
 *   --min-toxicity <float>   only upload posts where toxicity >= value (default: 0, store all)
 *   --no-scoring             disable scorer entirely (useful when GCS auth is unavailable)
 */

import { Storage } from "@google-cloud/storage";
import { getActivityDb } from "../db/activity-schema";

export type BufferedPost = {
  uri: string;
  did: string;
  rkey: string;
  text: string;
  langs: string[];
  reply_to: string | null;
  quote_of: string | null;
  created_at_us: number;
};

type ScoredPost = BufferedPost & {
  scored_at: string;
  model_version: string;
  toxicity: number;
  hatespeech: number;
  sentiment: number;
  label: number;
};

// Config
const TOXIC_API_URL    = process.env.TOXIC_API_URL    ?? "https://toxic-cicd.fly.dev";
const GCS_SCORED_BUCKET = process.env.GCS_SCORED_BUCKET ?? "bsky-labeled-posts";
const GCS_SCORED_PREFIX = process.env.GCS_SCORED_PREFIX ?? "posts/";

const _args = process.argv.slice(2);
const _minToxIdx = _args.indexOf("--min-toxicity");
const MIN_TOXICITY  = _minToxIdx >= 0 ? parseFloat(_args[_minToxIdx + 1]) : 0;
const SCORER_ENABLED = !_args.includes("--no-scoring");

// DLQ: retry up to 10 times with exponential backoff (5m, 20m, 45m … ~8h cap).
// Drain at most 5 DLQ batches per flush so we don't starve the live path.
const DLQ_MAX_ATTEMPTS = 10;
const DLQ_DRAIN_LIMIT  = 5;

let scorerBuffer: BufferedPost[] = [];
let _storage: Storage | null = null;

function getStorage(): Storage {
  if (!_storage) _storage = new Storage();
  return _storage;
}

export function bufferPost(post: BufferedPost): void {
  if (!SCORER_ENABLED) return;
  scorerBuffer.push(post);
}

// On shutdown: save the live buffer to DLQ synchronously so nothing is lost.
export function scorerShutdown(): void {
  if (!SCORER_ENABLED || scorerBuffer.length === 0) return;
  const posts = scorerBuffer;
  scorerBuffer = [];
  _enqueueDlq(posts, "process shutdown", 0);
  console.log(`[scorer] Saved ${posts.length} unscored posts to DLQ on shutdown`);
}

function _enqueueDlq(posts: BufferedPost[], error: string, attempts: number): void {
  const backoffMs = Math.min(Math.pow(attempts + 1, 2) * 5 * 60_000, 8 * 60 * 60_000);
  const nextRetry = new Date(Date.now() + backoffMs).toISOString();
  getActivityDb().prepare(`
    INSERT INTO score_dlq (posts_json, failed_at, attempts, last_error, next_retry_at)
    VALUES (?, datetime('now'), ?, ?, ?)
  `).run(JSON.stringify(posts), attempts, error, nextRetry);
}

async function _scoreAndUpload(posts: BufferedPost[]): Promise<void> {
  // Score via API
  const res = await fetch(`${TOXIC_API_URL}/score`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ texts: posts.map(p => p.text) }),
    signal: AbortSignal.timeout(30_000),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`API ${res.status}: ${body.slice(0, 200)}`);
  }
  const data = await res.json() as {
    model_version: string;
    results: { label: number; scores: Record<string, number> }[];
  };

  const scoredAt = new Date().toISOString();
  const scored: ScoredPost[] = posts.map((post, i) => ({
    ...post,
    scored_at:     scoredAt,
    model_version: data.model_version,
    toxicity:      data.results[i]?.scores["toxicity"]  ?? 0,
    hatespeech:    data.results[i]?.scores["hatespeech"] ?? 0,
    sentiment:     data.results[i]?.scores["sentiment"]  ?? 0,
    label:         data.results[i]?.label ?? 0,
  }));

  const toUpload = MIN_TOXICITY > 0
    ? scored.filter(p => p.toxicity >= MIN_TOXICITY)
    : scored;

  if (toUpload.length === 0) return;

  // Write Hive-partitioned JSONL: posts/dt=YYYY-MM-DD/hr=HH/batch-{ms}.jsonl
  const now    = new Date();
  const dt     = now.toISOString().slice(0, 10);
  const hr     = String(now.getUTCHours()).padStart(2, "0");
  const blob   = `${GCS_SCORED_PREFIX}dt=${dt}/hr=${hr}/batch-${Date.now()}.jsonl`;
  const jsonl  = toUpload.map(p => JSON.stringify(p)).join("\n");

  await getStorage().bucket(GCS_SCORED_BUCKET).file(blob).save(jsonl, {
    contentType: "application/x-ndjson",
  });
}

async function _drainDlq(): Promise<void> {
  const db  = getActivityDb();
  const now = new Date().toISOString();

  const rows = db.prepare(`
    SELECT id, posts_json, attempts
    FROM score_dlq
    WHERE next_retry_at <= ? AND attempts < ?
    ORDER BY failed_at ASC
    LIMIT ?
  `).all(now, DLQ_MAX_ATTEMPTS, DLQ_DRAIN_LIMIT) as { id: number; posts_json: string; attempts: number }[];

  for (const row of rows) {
    const posts = JSON.parse(row.posts_json) as BufferedPost[];
    try {
      await _scoreAndUpload(posts);
      db.prepare(`DELETE FROM score_dlq WHERE id = ?`).run(row.id);
      console.log(`[scorer] DLQ batch ${row.id} retry succeeded (${posts.length} posts)`);
    } catch (err) {
      const newAttempts = row.attempts + 1;
      if (newAttempts >= DLQ_MAX_ATTEMPTS) {
        db.prepare(`DELETE FROM score_dlq WHERE id = ?`).run(row.id);
        console.warn(`[scorer] DLQ batch ${row.id} exhausted ${DLQ_MAX_ATTEMPTS} retries, dropping ${posts.length} posts`);
      } else {
        const backoffMs  = Math.min(Math.pow(newAttempts, 2) * 5 * 60_000, 8 * 60 * 60_000);
        const nextRetry  = new Date(Date.now() + backoffMs).toISOString();
        const backoffMin = Math.round(backoffMs / 60_000);
        db.prepare(`
          UPDATE score_dlq SET attempts = ?, last_error = ?, next_retry_at = ? WHERE id = ?
        `).run(newAttempts, String(err), nextRetry, row.id);
        console.warn(`[scorer] DLQ batch ${row.id} retry ${newAttempts} failed (next in ${backoffMin}m): ${err}`);
      }
    }
  }
}

export async function flushScorer(): Promise<void> {
  if (!SCORER_ENABLED) return;

  await _drainDlq();

  const posts = scorerBuffer;
  scorerBuffer = [];
  if (posts.length === 0) return;

  try {
    await _scoreAndUpload(posts);
    const filtered = MIN_TOXICITY > 0 ? ` (min-toxicity=${MIN_TOXICITY})` : "";
    console.log(`[scorer] Scored and uploaded ${posts.length} posts${filtered}`);
  } catch (err) {
    _enqueueDlq(posts, String(err), 0);
    console.warn(`[scorer] Score/upload failed, queued ${posts.length} posts to DLQ: ${err}`);
  }
}
