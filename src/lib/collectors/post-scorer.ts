/**
 * Post scorer: buffers app.bsky.feed.post events from the shared Jetstream connection,
 * batch-scores them via the toxic-cicd API, and uploads rolled, gzipped JSONL to GCS.
 *
 * Called from jetstream-activity.ts:
 *   bufferPost()      — called per post create event in the message handler
 *   flushScorer()     — called from the async flush cycle (scores live buffer + drains DLQ)
 *   scorerShutdown()  — called on SIGINT/SIGTERM to upload the open roll and persist unscored posts to DLQ
 *
 * Env vars:
 *   TOXIC_API_URL      Scorer API base URL
 *   GCS_SCORED_BUCKET  GCS bucket (default: tox-labeled-data)
 *   GCS_ROLLED_PREFIX  Object prefix for rolled parts (default: scored/). Deliberately not
 *                      GCS_SCORED_PREFIX: .env points that at the legacy posts/ layout, which
 *                      has to stay frozen for the BigQuery backfill.
 *
 * CLI args:
 *   --min-toxicity <float>   only upload posts where toxicity >= value (default: 0, store all)
 *   --no-scoring             disable scorer entirely (useful when GCS auth is unavailable)
 */

import { Storage } from "@google-cloud/storage";
import { promisify } from "util";
import { gzip } from "zlib";
import sql from "../db/pg";

const gzipAsync = promisify(gzip);

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

export type ScoredPost = BufferedPost & {
  scored_at: string;
  model_version: string;
  label: number;
  scores: Record<string, number>;
  details: Record<string, string[]>;
};

// One row as written to GCS, already in toxic_cicd.scored_posts shape so loads need no
// transform. uri is dropped: it is always at://{did}/app.bsky.feed.post/{rkey}, and the
// BigQuery view rebuilds it.
export type StoredPost = {
  did: string;
  rkey: string;
  text: string;
  langs: string[];
  reply_to: string | null;
  quote_of: string | null;
  created_at: string;
  scored_at: string;
  model_version: string;
  label: number;
  scores: Record<string, number>;
  details: Record<string, string[]>;
};

// Config
const TOXIC_API_URL     = process.env.TOXIC_API_URL     ?? "http://15.204.11.179:8080";
const GCS_SCORED_BUCKET = process.env.GCS_SCORED_BUCKET ?? "tox-labeled-data";
const GCS_ROLLED_PREFIX = process.env.GCS_ROLLED_PREFIX ?? "scored/";

const _args = process.argv.slice(2);
const _minToxIdx = _args.indexOf("--min-toxicity");
const MIN_TOXICITY  = _minToxIdx >= 0 ? parseFloat(_args[_minToxIdx + 1]) : 0;
const SCORER_ENABLED = !_args.includes("--no-scoring");

// DLQ: retry up to 10 times with exponential backoff (5m, 20m, 45m … ~8h cap).
// Large payloads (e.g. from shutdown) are split into SCORE_BATCH_SIZE chunks on enqueue
const DLQ_MAX_ATTEMPTS = 10;
// Rows per flush the inline drain will attempt. Set to 0 in case running local DLQ draining
const _dlqLimitIdx = _args.indexOf("--dlq-drain-limit");
const DLQ_DRAIN_LIMIT = _dlqLimitIdx >= 0 ? parseInt(_args[_dlqLimitIdx + 1], 10) : 50;
const SCORE_BATCH_SIZE  = 50;
const SCORE_CONCURRENCY = 4;
const DLQ_SCORE_TIMEOUT_MS  = 50_000;
const LIVE_SCORE_TIMEOUT_MS = 30_000;

// Rolled uploads: one GCS object per ~25k scored posts (~15 MB raw, ~5 MB gz) instead of
// one per 50-post chunk (~64k objects/day). Cut on size or age, whichever comes first;
// a flush runs 80–100 min, so cutting only at flush end would hold ~210k posts in memory
// and send all of them back through the DLQ if that one upload failed.
const UPLOAD_ROLL_POSTS  = 25_000;
const UPLOAD_ROLL_MS     = 10 * 60_000;
const UPLOAD_RETRY_DELAYS_MS = [2_000, 8_000];

// Max buffer size to prevent blowup of scoring input:output
const _maxBufIdx = _args.indexOf("--scorer-max-buffer");
const SCORER_MAX_BUFFER = _maxBufIdx >= 0 ? parseInt(_args[_maxBufIdx + 1], 10) : 250_000;

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

// id is a JS bigint at runtime (pg.ts maps bigint columns to postgres.BigInt).
type DlqRef = { id: bigint; attempts: number };

// Scored rows waiting for the next cut, plus what to fall back to if that upload fails:
// live posts go back to the DLQ unscored, DLQ rows are only deleted once the part is saved.
type Roll = { rows: StoredPost[]; posts: number; live: BufferedPost[]; dlq: DlqRef[]; openedAt: number };
const _newRoll = (): Roll => ({ rows: [], posts: 0, live: [], dlq: [], openedAt: 0 });

let scorerBuffer: BufferedPost[] = [];
let _droppedSinceFlush = 0;
let _storage: Storage | null = null;
let _flushing = false;
let _latencySamples: LatencySample[] = [];
let _lastPruneAt = 0;
let _roll: Roll = _newRoll();
let _partSeq = 0;
let _uploadFailedSinceFlush = 0;

function getStorage(): Storage {
  if (!_storage) _storage = new Storage();
  return _storage;
}

/**
 * Run `worker` over `items` with at most `limit` in flight, starting the next item
 * as soon as any one finishes.
 *
 * `worker` must handle its own errors
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

export function toStoredPost(p: ScoredPost): StoredPost {
  return {
    did:           p.did,
    rkey:          p.rkey,
    text:          p.text,
    langs:         p.langs,
    reply_to:      p.reply_to,
    quote_of:      p.quote_of,
    // created_at_us is Jetstream's event time_us, not client data, so it always converts.
    created_at:    new Date(p.created_at_us / 1000).toISOString(),
    scored_at:     p.scored_at,
    model_version: p.model_version,
    label:         p.label,
    scores:        p.scores,
    details:       p.details,
  };
}

/**
 * Gzip and upload one part to {prefix}dt={scored date}/{name}.jsonl.gz. dt is the scored
 * date because that's the unit the nightly BigQuery load picks up; the table partitions on
 * created_at, so recovered posts still land on their own day. Retries in memory first:
 * the rows are already scored, and the fallback (re-scoring via the DLQ) costs far more.
 *
 * No Content-Encoding: gzip on purpose. With it GCS decompresses on download, so readers
 * get plain JSON from a .gz name; BigQuery detects gzip from the bytes either way.
 */
export async function uploadScoredPart(rows: StoredPost[], name: string): Promise<void> {
  const dt   = new Date().toISOString().slice(0, 10);
  const file = getStorage().bucket(GCS_SCORED_BUCKET).file(`${GCS_ROLLED_PREFIX}dt=${dt}/${name}.jsonl.gz`);
  const body = await gzipAsync(rows.map(r => JSON.stringify(r)).join("\n"));
  for (let attempt = 0; ; attempt++) {
    try {
      await file.save(body, { contentType: "application/gzip", resumable: false });
      return;
    } catch (err) {
      if (attempt >= UPLOAD_RETRY_DELAYS_MS.length) throw err;
      await new Promise(r => setTimeout(r, UPLOAD_RETRY_DELAYS_MS[attempt]));
    }
  }
}

export function bufferPost(post: BufferedPost): void {
  if (!SCORER_ENABLED) return;
  if (scorerBuffer.length >= SCORER_MAX_BUFFER) {
    _droppedSinceFlush++;
    return;
  }
  scorerBuffer.push(post);
}

// On shutdown: upload whatever is already scored, then save the live buffer to DLQ so
// nothing is lost. Chunks still in flight at the API when the process exits are not
// covered (same as before rolled uploads).
export async function scorerShutdown(): Promise<void> {
  if (!SCORER_ENABLED) return;
  await _cutRoll();
  if (scorerBuffer.length === 0) return;
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
      post_count: slice.length,
    });
  }
  if (rows.length > 0) {
    await sql`INSERT INTO activity.score_dlq ${sql(rows, "posts_json", "failed_at", "attempts", "last_error", "next_retry_at", "post_count")}`;
  }
}

// A DLQ row that failed again (scoring or its part's upload): back off, or drop it once
// it has used up its attempts.
async function _deferDlqRow(row: DlqRef, postCount: number | null, err: unknown): Promise<void> {
  const newAttempts = row.attempts + 1;
  if (newAttempts >= DLQ_MAX_ATTEMPTS) {
    await sql`DELETE FROM activity.score_dlq WHERE id = ${row.id}`;
    console.warn(`[scorer] DLQ batch ${row.id} exhausted ${DLQ_MAX_ATTEMPTS} retries, dropping ${postCount ?? "its"} posts`);
    return;
  }
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

async function _score(
  posts: BufferedPost[],
  timeoutMs = LIVE_SCORE_TIMEOUT_MS,
  source = "live",
): Promise<ScoredPost[]> {
  // Score via API
  const t0 = Date.now();
  let apiMs: number | null = null;
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
    ok = true;
    return posts.map((post, i) => ({
      ...post,
      scored_at:     scoredAt,
      model_version: data.model_version,
      label:         data.results[i]?.label ?? 0,
      scores:        data.results[i]?.scores ?? {},
      details:       data.results[i]?.details ?? {},
    }));
  } finally {
      // store latency sample for monitoring (uploads get their own "upload" samples per cut)
      _latencySamples.push({
      ts:        new Date(t0).toISOString(),
      source,
      posts:     posts.length,
      api_ms:    apiMs,
      upload_ms: null,
      total_ms:  Date.now() - t0,
      ok,
    });
  }
}

function _addToRoll(scored: ScoredPost[], live: BufferedPost[], dlq: DlqRef[]): void {
  if (_roll.openedAt === 0) _roll.openedAt = Date.now();
  for (const p of scored) {
    if (MIN_TOXICITY > 0 && (p.scores["toxicity"] ?? 0) < MIN_TOXICITY) continue;
    _roll.rows.push(toStoredPost(p));
  }
  _roll.posts += scored.length;
  for (const p of live) _roll.live.push(p);
  for (const r of dlq) _roll.dlq.push(r);
}

async function _maybeCut(): Promise<void> {
  if (_roll.posts >= UPLOAD_ROLL_POSTS ||
      (_roll.openedAt > 0 && Date.now() - _roll.openedAt >= UPLOAD_ROLL_MS)) {
    await _cutRoll();
  }
}

/**
 * Upload the open roll as one part. Never throws: on failure the live posts go back to the
 * DLQ (to be re-scored) and the roll's DLQ rows get their retry bumped instead of deleted.
 * The roll is swapped out synchronously, so concurrent pool workers can't cut it twice.
 */
async function _cutRoll(): Promise<void> {
  const roll = _roll;
  if (roll.posts === 0) return;
  _roll = _newRoll();

  const t0 = Date.now();
  let ok = false;
  try {
    if (roll.rows.length > 0) await uploadScoredPart(roll.rows, `part-${t0}-${_partSeq++}`);
    ok = true;
  } catch (err) {
    console.warn(`[scorer] Upload of ${roll.posts} scored posts failed after retries; re-queueing: ${err}`);
    _uploadFailedSinceFlush += roll.live.length;
    try {
      await _enqueueDlq(roll.live, `upload failed: ${err}`, 0);
      for (const r of roll.dlq) await _deferDlqRow(r, null, `upload failed: ${err}`);
    } catch (dlqErr) {
      console.error(`[scorer] Could not re-queue ${roll.live.length} posts after failed upload; they are lost: ${dlqErr}`);
    }
  } finally {
    _latencySamples.push({
      ts:        new Date(t0).toISOString(),
      source:    "upload",
      posts:     roll.posts,
      api_ms:    null,
      upload_ms: Date.now() - t0,
      total_ms:  Date.now() - t0,
      ok,
    });
  }

  // Only now is it safe to drop the DLQ rows: their posts are durably in GCS. If this
  // DELETE fails they get scored again later, which the BigQuery dedupe view absorbs.
  if (ok && roll.dlq.length > 0) {
    try {
      // postgres.js can't serialize a bigint[] parameter, so pass strings and cast.
      await sql`DELETE FROM activity.score_dlq WHERE id = ANY(${roll.dlq.map(r => String(r.id))}::bigint[])`;
      console.log(`[scorer] DLQ: ${roll.dlq.length} batches recovered`);
    } catch (err) {
      console.warn(`[scorer] Uploaded ${roll.dlq.length} DLQ batches but could not delete them (will re-score): ${err}`);
    }
  }
}

async function _drainDlq(): Promise<void> {
  if (DLQ_DRAIN_LIMIT <= 0) return;
  const now = new Date().toISOString();

  const rows = await sql<{ id: bigint; posts_json: string; attempts: number }[]>`
    SELECT id, posts_json, attempts
    FROM activity.score_dlq
    WHERE next_retry_at <= ${now} AND attempts < ${DLQ_MAX_ATTEMPTS}
    ORDER BY failed_at ASC
    LIMIT ${DLQ_DRAIN_LIMIT}
  `;

  if (rows.length === 0) return;

  // Drained through the same worker pool as live scoring. A row is scored whole before it
  // joins the roll, so it never straddles two parts and is deleted exactly once (_cutRoll).
  await _runPool(rows, SCORE_CONCURRENCY, async row => {
    const posts = JSON.parse(row.posts_json) as BufferedPost[];
    try {
      const scored: ScoredPost[] = [];
      for (let i = 0; i < posts.length; i += SCORE_BATCH_SIZE) {
        for (const p of await _score(posts.slice(i, i + SCORE_BATCH_SIZE), DLQ_SCORE_TIMEOUT_MS, "dlq")) {
          scored.push(p);
        }
      }
      _addToRoll(scored, [], [{ id: row.id, attempts: row.attempts }]);
      await _maybeCut();
    } catch (err) {
      await _deferDlqRow(row, posts.length, err);
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
  _uploadFailedSinceFlush = 0;
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

    const chunks: BufferedPost[][] = [];
    for (let i = 0; i < posts.length; i += SCORE_BATCH_SIZE) {
      chunks.push(posts.slice(i, i + SCORE_BATCH_SIZE));
    }
    batches = chunks.length;

    await _runPool(chunks, SCORE_CONCURRENCY, async chunk => {
      let scored: ScoredPost[];
      try {
        scored = await _score(chunk);
      } catch (err) {
        await _enqueueDlq(chunk, String(err), 0);
        failed += chunk.length;
        console.warn(`[scorer] Chunk failed, queued ${chunk.length} posts to DLQ: ${err}`);
        return;
      }
      succeeded += chunk.length;
      _addToRoll(scored, chunk, []);
      await _maybeCut();
    });

    // Every flush ends on a cut, so DLQ rows claimed by this flush are settled (uploaded and
    // deleted, or deferred) before the next flush's _drainDlq can select them again.
    await _cutRoll();

    // Scored but lost their upload: they're back in the DLQ, so count them as failed.
    succeeded -= _uploadFailedSinceFlush;
    failed    += _uploadFailedSinceFlush;

    if (posts.length > 0) {
      const filtered = MIN_TOXICITY > 0 ? ` (min-toxicity=${MIN_TOXICITY})` : "";
      const failedNote = failed > 0 ? `, ${failed} to DLQ` : "";
      const dropNote = dropped > 0 ? `, dropped ${dropped} over cap ${SCORER_MAX_BUFFER}` : "";
      console.log(`[scorer] Flushed ${succeeded} posts${filtered}${failedNote}${dropNote} (${chunks.length} batches)`);
    }

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
