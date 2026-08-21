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
// Large payloads (e.g. from shutdown) are split into SCORE_BATCH_SIZE chunks on enqueue
const DLQ_MAX_ATTEMPTS = 10;
const DLQ_DRAIN_LIMIT  = 50;
const SCORE_BATCH_SIZE  = 50;
const SCORE_CONCURRENCY = 4;
const DLQ_SCORE_TIMEOUT_MS  = 50_000;
const LIVE_SCORE_TIMEOUT_MS = 30_000;

// Max buffer size to prevent blowup of scoring input:output 
const _maxBufIdx = _args.indexOf("--scorer-max-buffer");
const SCORER_MAX_BUFFER = _maxBufIdx >= 0 ? parseInt(_args[_maxBufIdx + 1], 10) : 100_000;

const METRICS_RETENTION_DAYS = 30;
const METRICS_PRUNE_INTERVAL_MS = 60 * 60_000;

type LatencySample = {
  ts: string;
  source: string;
  posts: number;
  api_ms: number | null;
  upload_ms: number | null;
  total_ms: number;
  ok: boolean;
};

let scorerBuffer: BufferedPost[] = [];
let _droppedSinceFlush = 0;
let _storage: Storage | null = null;
let _flushing = false;
let _latencySamples: LatencySample[] = [];
let _lastPruneAt = 0;

function getStorage(): Storage {
  if (!_storage) _storage = new Storage();
  return _storage;
}

/**
 * Run `worker` over `items` with at most `limit` in flight, starting the next item
 * as soon as any one finishes.
 *
 * This replaces a lock-step loop that awaited a whole group of `limit` before
 * starting the next, so every group cost max(latency) instead of mean(latency).
 * Against the observed scorer latency spread (mean 5,998ms, p95 7,919ms) that
 * barrier wastes 8.9% at concurrency 2 and 16.2% at 4.
 *
 * `worker` must handle its own errors; a rejection here would abandon the rest.
 */
async function _runPool<T>(
  items: T[],
  limit: number,
  worker: (item: T) => Promise<void>,
): Promise<void> {
  let next = 0;
  const runners = Array.from({ length: Math.min(limit, items.length) }, async () => {
    for (;;) {
      const i = next++;
      if (i >= items.length) return;
      // Yield between items so the event loop (and the jetstream socket) stays responsive.
      await new Promise<void>(resolve => setImmediate(resolve));
      await worker(items[i]);
    }
  });
  await Promise.all(runners);
}

export function bufferPost(post: BufferedPost): void {
  if (!SCORER_ENABLED) return;
  if (scorerBuffer.length >= SCORER_MAX_BUFFER) {
    _droppedSinceFlush++;
    return;
  }
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
    const slice = posts.slice(i, i + SCORE_BATCH_SIZE);
    rows.push({
      posts_json: JSON.stringify(slice),
      failed_at: failedAt,
      attempts,
      last_error: error,
      next_retry_at: nextRetry,
      // Recorded at insert so DLQ depth in posts is a SUM of a column rather than
      // a json_array_length over the whole (multi-GB) table.
      post_count: slice.length,
    });
  }
  if (rows.length > 0) {
    await sql`INSERT INTO activity.score_dlq ${sql(rows, "posts_json", "failed_at", "attempts", "last_error", "next_retry_at", "post_count")}`;
  }
}

async function _scoreAndUpload(
  posts: BufferedPost[],
  timeoutMs = LIVE_SCORE_TIMEOUT_MS,
  source = "live",
): Promise<void> {
  // Score via API
  const t0 = Date.now();
  let apiMs: number | null = null;
  let uploadMs: number | null = null;
  let ok = false;
  try {
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

    apiMs = Date.now() - t0;
    console.log(`[scorer] API: ${posts.length} posts scored in ${apiMs}ms`);

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

    if (toUpload.length > 0) {
      // Write Hive-partitioned JSONL: posts/dt=YYYY-MM-DD/hr=HH/batch-{ms}.jsonl
      const now    = new Date();
      const dt     = now.toISOString().slice(0, 10);
      const hr     = String(now.getUTCHours()).padStart(2, "0");
      const blob   = `${GCS_SCORED_PREFIX}dt=${dt}/hr=${hr}/batch-${Date.now()}.jsonl`;
      const jsonl  = toUpload.map(p => JSON.stringify(p)).join("\n");

      const tUpload = Date.now();
      await getStorage().bucket(GCS_SCORED_BUCKET).file(blob).save(jsonl, {
        contentType: "application/x-ndjson",
      });
      uploadMs = Date.now() - tUpload;
    }
    ok = true;
  } finally {
      // store latency sample for monitoring
      _latencySamples.push({
      ts:        new Date(t0).toISOString(),
      source,
      posts:     posts.length,
      api_ms:    apiMs,
      upload_ms: uploadMs,
      total_ms:  Date.now() - t0,
      ok,
    });
  }
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

  // Drained through the same worker pool as live scoring. T
  await _runPool(rows, SCORE_CONCURRENCY, async row => {
    const posts = JSON.parse(row.posts_json) as BufferedPost[];

    // Re-chunk oversized rows (e.g. from pre-fix shutdown saves) into smaller rows and drop the original.
    if (posts.length > SCORE_BATCH_SIZE) {
      await sql`DELETE FROM activity.score_dlq WHERE id = ${row.id}`;
      // Carry the attempt count over. Passing 0 here reset the counter on every
      // re-chunk, so rows could never reach DLQ_MAX_ATTEMPTS and never aged out.
      await _enqueueDlq(posts, row.attempts > 0 ? "re-chunked from oversized row" : "initial enqueue", row.attempts);
      console.log(`[scorer] DLQ batch ${row.id} re-chunked ${posts.length} posts into ${Math.ceil(posts.length / SCORE_BATCH_SIZE)} rows`);
      return;
    }

    try {
      await _scoreAndUpload(posts, DLQ_SCORE_TIMEOUT_MS, "dlq");
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
  });
}

/**
* store metrics in pg 
*/
async function _writeMetrics(row: {
  ts: string; buffered: number; succeeded: number; failed: number;
  dropped: number; batches: number; flush_ms: number;
}): Promise<void> {
  const samples = _latencySamples;
  _latencySamples = [];
  try {
    if (samples.length > 0) {
      await sql`INSERT INTO activity.scorer_latency ${
        sql(samples, "ts", "source", "posts", "api_ms", "upload_ms", "total_ms", "ok")
      }`;
    }

    const [depth] = await sql<{ rows: string; posts: string | null }[]>`
      SELECT count(*)::text AS rows, sum(post_count)::text AS posts
      FROM activity.score_dlq
    `;

    await sql`
      INSERT INTO activity.scorer_flush
        (ts, buffered, succeeded, failed, dropped, batches, flush_ms, dlq_rows, dlq_posts)
      VALUES (${row.ts}, ${row.buffered}, ${row.succeeded}, ${row.failed}, ${row.dropped},
              ${row.batches}, ${row.flush_ms}, ${Number(depth.rows)},
              ${depth.posts === null ? null : Number(depth.posts)})
    `;

    if (Date.now() - _lastPruneAt > METRICS_PRUNE_INTERVAL_MS) {
      _lastPruneAt = Date.now();
      const cutoff = new Date(Date.now() - METRICS_RETENTION_DAYS * 86_400_000).toISOString();
      await sql`DELETE FROM activity.scorer_latency WHERE ts < ${cutoff}`;
      await sql`DELETE FROM activity.scorer_flush   WHERE ts < ${cutoff}`;
    }
  } catch (err) {
    console.warn(`[scorer] Failed to write metrics: ${err}`);
  }
}

export async function flushScorer(): Promise<void> {
  if (!SCORER_ENABLED || _flushing) return;
  _flushing = true;
  const startedAt = Date.now();
  let buffered = 0;
  let succeeded = 0;
  let failed = 0;
  let dropped = 0;
  let batches = 0;
  try {
    await _drainDlq();

    const posts = scorerBuffer;
    scorerBuffer = [];
    dropped = _droppedSinceFlush;
    _droppedSinceFlush = 0;
    buffered = posts.length;
    if (posts.length === 0) return;

    const chunks: BufferedPost[][] = [];
    for (let i = 0; i < posts.length; i += SCORE_BATCH_SIZE) {
      chunks.push(posts.slice(i, i + SCORE_BATCH_SIZE));
    }
    batches = chunks.length;

    await _runPool(chunks, SCORE_CONCURRENCY, async chunk => {
      try {
        await _scoreAndUpload(chunk);
        succeeded += chunk.length;
      } catch (err) {
        await _enqueueDlq(chunk, String(err), 0);
        failed += chunk.length;
        console.warn(`[scorer] Chunk failed, queued ${chunk.length} posts to DLQ: ${err}`);
      }
    });

    const filtered = MIN_TOXICITY > 0 ? ` (min-toxicity=${MIN_TOXICITY})` : "";
    const failedNote = failed > 0 ? `, ${failed} to DLQ` : "";
    const dropNote = dropped > 0 ? `, dropped ${dropped} over cap ${SCORER_MAX_BUFFER}` : "";
    console.log(`[scorer] Flushed ${succeeded} posts${filtered}${failedNote}${dropNote} (${chunks.length} batches)`);

  } finally {
    // catch the DLQ drain speed too
    await _writeMetrics({
      ts: new Date(startedAt).toISOString(),
      buffered, succeeded, failed, dropped, batches,
      flush_ms: Date.now() - startedAt,
    });
    _flushing = false;
  }
}
