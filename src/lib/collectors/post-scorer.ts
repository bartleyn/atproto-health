/**
 * Post scorer: buffers app.bsky.feed.post events from the shared Jetstream connection,
 * batch-scores them via the toxic-cicd API, and uploads JSONL to GCS.
 *
 * Called from jetstream-activity.ts:
 *   bufferPost()      — called per post create event in the message handler
 *   flushScorer()     — called from the async flush cycle (scores live buffer + drains DLQ)
 *   scorerShutdown()  — called on SIGINT/SIGTERM to persist unscored posts to DLQ
 *
 * Env vars:
 *   TOXIC_API_URL      Scorer API base URL
 *   GCS_SCORED_BUCKET  GCS bucket (default: bsky-labeled-posts)
 *   GCS_SCORED_PREFIX  Object prefix (default: posts/)
 *
 * CLI args:
 *   --min-toxicity <float>   only upload posts where toxicity >= value (default: 0, store all)
 *   --no-scoring             disable scorer entirely (useful when GCS auth is unavailable)
 */

import { Storage } from "@google-cloud/storage";
import sql from "../db/pg";

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
  label: number;
  scores: Record<string, number>;
  details: Record<string, string[]>;
};

// Config
const TOXIC_API_URL    = process.env.TOXIC_API_URL    ?? "http://15.204.11.179:8080";
const GCS_SCORED_BUCKET = process.env.GCS_SCORED_BUCKET ?? "bsky-labeled-posts";
const GCS_SCORED_PREFIX = process.env.GCS_SCORED_PREFIX ?? "posts/";

const _args = process.argv.slice(2);
const _minToxIdx = _args.indexOf("--min-toxicity");
const MIN_TOXICITY  = _minToxIdx >= 0 ? parseFloat(_args[_minToxIdx + 1]) : 0;
const SCORER_ENABLED = !_args.includes("--no-scoring");

// DLQ: retry up to 10 times with exponential backoff (5m, 20m, 45m … ~8h cap).
// Drain at most 50 DLQ batches per flush.
// Large payloads (e.g. from shutdown) are split into SCORE_BATCH_SIZE chunks on enqueue
// so each retry stays well within the API timeout.
const DLQ_MAX_ATTEMPTS = 10;
const DLQ_DRAIN_LIMIT  = 50;
const SCORE_BATCH_SIZE  = 100;
const SCORE_CONCURRENCY = 2;
const DLQ_SCORE_TIMEOUT_MS  = 50_000;
const LIVE_SCORE_TIMEOUT_MS = 30_000;

let scorerBuffer: BufferedPost[] = [];
let _storage: Storage | null = null;
let _flushing = false;

function getStorage(): Storage {
  if (!_storage) _storage = new Storage();
  return _storage;
}

export function bufferPost(post: BufferedPost): void {
  if (!SCORER_ENABLED) return;
  scorerBuffer.push(post);
}

// On shutdown: save the live buffer to DLQ so nothing is lost.
export async function scorerShutdown(): Promise<void> {
  if (!SCORER_ENABLED || scorerBuffer.length === 0) return;
  const posts = scorerBuffer;
  scorerBuffer = [];
  await _enqueueDlq(posts, "process shutdown", 0);
  console.log(`[scorer] Saved ${posts.length} unscored posts to DLQ on shutdown`);
}

async function _enqueueDlq(posts: BufferedPost[], error: string, attempts: number): Promise<void> {
  const backoffMs = Math.min(Math.pow(attempts + 1, 2) * 5 * 60_000, 8 * 60 * 60_000);
  const nextRetry = new Date(Date.now() + backoffMs).toISOString();
  const failedAt = new Date().toISOString();
  const rows = [];
  for (let i = 0; i < posts.length; i += SCORE_BATCH_SIZE) {
    rows.push({
      posts_json: JSON.stringify(posts.slice(i, i + SCORE_BATCH_SIZE)),
      failed_at: failedAt,
      attempts,
      last_error: error,
      next_retry_at: nextRetry,
    });
  }
  if (rows.length > 0) {
    await sql`INSERT INTO activity.score_dlq ${sql(rows, "posts_json", "failed_at", "attempts", "last_error", "next_retry_at")}`;
  }
}

async function _scoreAndUpload(posts: BufferedPost[], timeoutMs = LIVE_SCORE_TIMEOUT_MS): Promise<void> {
  // Score via API
  const t0 = Date.now();
  const res = await fetch(`${TOXIC_API_URL}/score`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ texts: posts.map(p => p.text) }),
    signal: AbortSignal.timeout(timeoutMs),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`API ${res.status}: ${body.slice(0, 200)}`);
  }
  const data = await res.json() as {
    model_version: string;
    results: { label: number; scores: Record<string, number>; details: Record<string, string[]> }[];
  };

  const elapsedMs = Date.now() - t0;
  console.log(`[scorer] API: ${posts.length} posts scored in ${elapsedMs}ms`);

  const scoredAt = new Date().toISOString();
  const scored: ScoredPost[] = posts.map((post, i) => ({
    ...post,
    scored_at:     scoredAt,
    model_version: data.model_version,
    label:         data.results[i]?.label ?? 0,
    scores:        data.results[i]?.scores ?? {},
    details:       data.results[i]?.details ?? {},
  }));

  const toUpload = MIN_TOXICITY > 0
    ? scored.filter(p => (p.scores["toxicity"] ?? 0) >= MIN_TOXICITY)
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
  const now = new Date().toISOString();

  const rows = await sql<{ id: number; posts_json: string; attempts: number }[]>`
    SELECT id, posts_json, attempts
    FROM activity.score_dlq
    WHERE next_retry_at <= ${now} AND attempts < ${DLQ_MAX_ATTEMPTS}
    ORDER BY failed_at ASC
    LIMIT ${DLQ_DRAIN_LIMIT}
  `;

  if (rows.length === 0) return;

  for (const row of rows) {
    // Yield between batches so the event loop stays responsive.
    await new Promise<void>(resolve => setImmediate(resolve));

    const posts = JSON.parse(row.posts_json) as BufferedPost[];

    // Re-chunk oversized rows (e.g. from pre-fix shutdown saves) into smaller rows and drop the original.
    if (posts.length > SCORE_BATCH_SIZE) {
      await sql`DELETE FROM activity.score_dlq WHERE id = ${row.id}`;
      await _enqueueDlq(posts, row.attempts > 0 ? "re-chunked from oversized row" : "initial enqueue", 0);
      console.log(`[scorer] DLQ batch ${row.id} re-chunked ${posts.length} posts into ${Math.ceil(posts.length / SCORE_BATCH_SIZE)} rows`);
      continue;
    }

    try {
      await _scoreAndUpload(posts, DLQ_SCORE_TIMEOUT_MS);
      await sql`DELETE FROM activity.score_dlq WHERE id = ${row.id}`;
      console.log(`[scorer] DLQ batch ${row.id} retry succeeded (${posts.length} posts)`);
    } catch (err) {
      const newAttempts = row.attempts + 1;
      if (newAttempts >= DLQ_MAX_ATTEMPTS) {
        await sql`DELETE FROM activity.score_dlq WHERE id = ${row.id}`;
        console.warn(`[scorer] DLQ batch ${row.id} exhausted ${DLQ_MAX_ATTEMPTS} retries, dropping ${posts.length} posts`);
      } else {
        const backoffMs  = Math.min(Math.pow(newAttempts, 2) * 5 * 60_000, 8 * 60 * 60_000);
        const nextRetry  = new Date(Date.now() + backoffMs).toISOString();
        const backoffMin = Math.round(backoffMs / 60_000);
        await sql`
          UPDATE activity.score_dlq
          SET attempts = ${newAttempts}, last_error = ${String(err)}, next_retry_at = ${nextRetry}
          WHERE id = ${row.id}
        `;
        console.warn(`[scorer] DLQ batch ${row.id} retry ${newAttempts} failed (next in ${backoffMin}m): ${err}`);
      }
    }
  }
}

export async function flushScorer(): Promise<void> {
  if (!SCORER_ENABLED || _flushing) return;
  _flushing = true;
  try {
    await _drainDlq();

    const posts = scorerBuffer;
    scorerBuffer = [];
    if (posts.length === 0) return;

    const chunks: BufferedPost[][] = [];
    for (let i = 0; i < posts.length; i += SCORE_BATCH_SIZE) {
      chunks.push(posts.slice(i, i + SCORE_BATCH_SIZE));
    }

    let succeeded = 0;
    let failed = 0;
    for (let i = 0; i < chunks.length; i += SCORE_CONCURRENCY) {
      const group = chunks.slice(i, i + SCORE_CONCURRENCY);
      const results = await Promise.allSettled(group.map(chunk => _scoreAndUpload(chunk)));
      for (let j = 0; j < results.length; j++) {
        if (results[j].status === "fulfilled") {
          succeeded += group[j].length;
        } else {
          const err = (results[j] as PromiseRejectedResult).reason;
          await _enqueueDlq(group[j], String(err), 0);
          failed += group[j].length;
          console.warn(`[scorer] Chunk failed, queued ${group[j].length} posts to DLQ: ${err}`);
        }
      }
    }

    const filtered = MIN_TOXICITY > 0 ? ` (min-toxicity=${MIN_TOXICITY})` : "";
    const failedNote = failed > 0 ? `, ${failed} to DLQ` : "";
    console.log(`[scorer] Flushed ${succeeded} posts${filtered}${failedNote} (${chunks.length} batches)`);

  } finally {
    _flushing = false;
  }
}
