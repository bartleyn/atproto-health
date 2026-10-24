/**
 * Standalone DLQ recovery drain.
 *
 * Drains activity.score_dlq against a LOCAL scorer instead of the OVH box, so the
 * backlog is recovered without stealing capacity from live scoring. Measured on
 * this Mac: ~110–180 posts/sec at concurrency 4 vs 36.3 on OVH, and scores match
 * production (label agreement 50/50 and 500/500 in two checks, max delta ≤ 1.1e-05
 * across all four signals) using artifacts v1.1.0 from gs://tox-model-artifacts.
 *
 * Bring up the local scorer first (toxic-cicd repo). The PII signal needs spaCy's
 * en_core_web_lg, which presidio otherwise tries to pip-install at startup:
 *   cd ~/Documents/personal_dev/github/toxic-cicd
 *   GCS_ARTIFACT_BUCKET=tox-model-artifacts MODEL_VERSION=1.1.0 \
 *     MODEL_ARTIFACT_DIR=artifacts/1.1.0 PYTHONPATH=. \
 *     .venv/bin/python scripts/download_artifacts.py
 *   MODEL_ARTIFACT_DIR=artifacts/1.1.0 MODEL_VERSION=1.1.0 PYTHONPATH=. \
 *     .venv/bin/python -m uvicorn api.app:app --host 127.0.0.1 --port 8081
 *
 * Then:
 *   npx tsx --env-file .env scripts/drain-dlq-local.ts [options]
 *
 * Options:
 *   --api-url <url>      scorer base URL           (default http://127.0.0.1:8081)
 *   --concurrency <n>    in-flight rows            (default 2; peak is 4)
 *   --limit <n>          stop after n rows         (default: drain everything)
 *   --dry-run            score and report, write nothing to GCS, delete nothing
 *
 * Writes the same rolled parts as the live scorer (uploadScoredPart): one gzipped
 * object per claim of up to CLAIM_SIZE rows, under scored/dt=<scored date>/.
 * BigQuery partitions on created_at, so months-old recovered posts still land on
 * their own day even though the GCS path carries the recovery date.
 *
 * Safe to stop with Ctrl-C at any point: rows are only deleted after their part is
 * durably in GCS, so the worst case on a kill is re-scoring one claim.
 */

import sql from "../src/lib/db/pg";
import { toStoredPost, uploadScoredPart } from "../src/lib/collectors/post-scorer";
import type { BufferedPost, ScoredPost } from "../src/lib/collectors/post-scorer";

const args = process.argv.slice(2);
const argOf = (name: string, fallback: string): string => {
  const i = args.indexOf(name);
  return i >= 0 && args[i + 1] ? args[i + 1] : fallback;
};

const API_URL     = argOf("--api-url", "http://127.0.0.1:8081");
const CONCURRENCY = parseInt(argOf("--concurrency", "2"), 10);
const LIMIT       = parseInt(argOf("--limit", "0"), 10);
const DRY_RUN     = args.includes("--dry-run");

const BATCH_SIZE = 50;
// One claim becomes one GCS part: 200 rows × ≤50 posts ≈ 10k posts, ~2 MB gz.
const CLAIM_SIZE = 200;
const TIMEOUT_MS = 120_000;

let rowsDone = 0, postsDone = 0, postsFailed = 0;
let stopping = false;
const startedAt = Date.now();

process.on("SIGINT", () => {
  console.log("\n[drain] SIGINT — finishing in-flight batches, then stopping.");
  stopping = true;
});

/**
 * Claim rows by pushing next_retry_at into the future, which makes the collector's
 * own inline drain skip them. SKIP LOCKED lets several drain workers (or a second
 * copy of this script) run without ever handing the same row to two of them.
 */
async function claimRows(n: number): Promise<{ id: bigint; posts_json: string }[]> {
  if (DRY_RUN) {
    // A claim is a write. Keep --dry-run genuinely read-only.
    return sql<{ id: bigint; posts_json: string }[]>`
      SELECT id, posts_json FROM activity.score_dlq
      WHERE next_retry_at <= now() ORDER BY failed_at ASC LIMIT ${n}
    `;
  }
  return sql<{ id: bigint; posts_json: string }[]>`
    UPDATE activity.score_dlq SET next_retry_at = now() + interval '1 day'
    WHERE id IN (
      SELECT id FROM activity.score_dlq
      WHERE next_retry_at <= now()
      ORDER BY failed_at ASC
      LIMIT ${n}
      FOR UPDATE SKIP LOCKED
    )
    RETURNING id, posts_json
  `;
}

async function scoreBatch(posts: BufferedPost[]): Promise<ScoredPost[]> {
  const res = await fetch(`${API_URL}/score`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ texts: posts.map(p => p.text) }),
    signal: AbortSignal.timeout(TIMEOUT_MS),
  });
  if (!res.ok) {
    throw new Error(`API ${res.status}: ${(await res.text().catch(() => "")).slice(0, 200)}`);
  }
  const data = await res.json() as {
    model_version: string;
    results: { label: number; scores: Record<string, number>; details: Record<string, string[]> }[];
  };
  const scoredAt = new Date().toISOString();
  return posts.map((post, i) => ({
    ...post,
    scored_at:     scoredAt,
    model_version: data.model_version,
    label:         data.results[i]?.label ?? 0,
    scores:        data.results[i]?.scores ?? {},
    details:       data.results[i]?.details ?? {},
  }));
}

async function deferRow(id: bigint, err: unknown): Promise<void> {
  // Hand it back so it is retried rather than silently deferred a full day.
  await sql`UPDATE activity.score_dlq SET next_retry_at = now() + interval '10 minutes',
            last_error = ${String(err)} WHERE id = ${id}`;
}

/**
 * Score one row whole. Returns null when the row is parked or deferred, so it stays
 * out of this claim's part.
 */
async function scoreRow(row: { id: bigint; posts_json: string }): Promise<ScoredPost[] | null> {
  let posts: BufferedPost[];
  try {
    posts = JSON.parse(row.posts_json) as BufferedPost[];
  } catch (err) {
    // Poison pill: unparseable rows would otherwise be deferred and retried
    // forever. Park it well beyond the run so it stops consuming attempts.
    await sql`UPDATE activity.score_dlq SET next_retry_at = now() + interval '100 years',
              last_error = ${`unparseable: ${err}`} WHERE id = ${row.id}`;
    console.warn(`[drain] row ${row.id} parked: posts_json will not parse (${err})`);
    return null;
  }

  try {
    const scored: ScoredPost[] = [];
    for (let i = 0; i < posts.length; i += BATCH_SIZE) {
      for (const p of await scoreBatch(posts.slice(i, i + BATCH_SIZE))) scored.push(p);
    }
    return scored;
  } catch (err) {
    postsFailed += posts.length;
    await deferRow(row.id, err);
    console.warn(`[drain] row ${row.id} failed (${posts.length} posts): ${err}`);
    return null;
  }
}

/**
 * Score a claim and write it as ONE part, then delete its rows. The part is named after
 * the claim's id range, so a crash between upload and DELETE re-uploads to the same object
 * when the same rows are claimed again (claims are ordered by failed_at), instead of
 * adding a second copy. Anything that still slips through is absorbed by the BigQuery
 * dedupe view.
 */
async function processClaim(rows: { id: bigint; posts_json: string }[]): Promise<void> {
  const done: { id: bigint; scored: ScoredPost[] }[] = [];
  for (let i = 0; i < rows.length && !stopping; i += CONCURRENCY) {
    const slice = rows.slice(i, i + CONCURRENCY);
    const results = await Promise.all(slice.map(scoreRow));
    results.forEach((scored, j) => { if (scored) done.push({ id: slice[j].id, scored }); });
  }
  // Rows skipped by a SIGINT stay claimed (next_retry_at +1 day); hand them back now.
  const doneIds = new Set(done.map(d => d.id));
  const skipped = rows.filter(r => !doneIds.has(r.id)).map(r => r.id);
  if (stopping && skipped.length > 0 && !DRY_RUN) {
    await sql`UPDATE activity.score_dlq SET next_retry_at = now() WHERE id = ANY(${skipped.map(String)}::bigint[])
              AND next_retry_at > now() + interval '1 hour'`;
  }
  if (done.length === 0) return;

  const posts = done.reduce((n, d) => n + d.scored.length, 0);
  if (!DRY_RUN) {
    const ids = done.map(d => d.id);
    // ids are bigints (pg.ts maps bigint columns to postgres.BigInt): Math.min can't take them.
    const lo = ids.reduce((a, b) => (b < a ? b : a));
    const hi = ids.reduce((a, b) => (b > a ? b : a));
    const stored = done.flatMap(d => d.scored.map(toStoredPost));
    try {
      await uploadScoredPart(stored, `recovered-${lo}-${hi}`);
    } catch (err) {
      postsFailed += posts;
      for (const id of ids) await deferRow(id, `upload failed: ${err}`);
      console.warn(`[drain] upload of ${ids.length} rows (${posts} posts) failed: ${err}`);
      return;
    }
    // postgres.js can't serialize a bigint[] parameter, so pass strings and cast.
    await sql`DELETE FROM activity.score_dlq WHERE id = ANY(${ids.map(String)}::bigint[])`;
  }
  postsDone += posts;
  rowsDone += done.length;
}

function report(): void {
  const secs = (Date.now() - startedAt) / 1000;
  const rate = postsDone / secs;
  console.log(
    `[drain] ${rowsDone.toLocaleString()} rows, ${postsDone.toLocaleString()} posts ` +
    `(${postsFailed.toLocaleString()} failed) | ${rate.toFixed(1)} posts/sec | ${(secs / 60).toFixed(1)} min`,
  );
}

async function main(): Promise<void> {
  const [before] = await sql<{ rows: string; posts: string | null }[]>`
    SELECT count(*)::text AS rows, sum(post_count)::text AS posts FROM activity.score_dlq
  `;
  console.log(`[drain] DLQ holds ${Number(before.rows).toLocaleString()} rows`);
  console.log(`[drain] scorer=${API_URL} concurrency=${CONCURRENCY}${DRY_RUN ? " DRY-RUN" : ""}`);

  const health = await fetch(`${API_URL}/health`, { signal: AbortSignal.timeout(10_000) })
    .then(r => r.json() as Promise<{ model_loaded: boolean }>)
    .catch((e) => { throw new Error(`scorer unreachable at ${API_URL}: ${e}`); });
  if (!health.model_loaded) throw new Error("scorer is up but model_loaded=false");

  let lastReport = Date.now();
  while (!stopping) {
    if (LIMIT && rowsDone >= LIMIT) break;
    // Never claim more than the limit allows; claiming a fixed batch meant --limit 1
    // still processed the whole batch.
    const want = LIMIT ? Math.min(CLAIM_SIZE, LIMIT - rowsDone) : CLAIM_SIZE;
    const rows = await claimRows(want);
    if (rows.length === 0) {
      // An empty claim does NOT mean the DLQ is empty — every remaining row may
      // simply be deferred by a failure. Say which, or a scorer outage looks like
      // a successful completion.
      const [left] = await sql<{ n: string }[]>`SELECT count(*)::text AS n FROM activity.score_dlq`;
      if (Number(left.n) > 0) {
        console.log(`[drain] nothing claimable, but ${Number(left.n).toLocaleString()} rows remain ` +
                    `(deferred by earlier failures) — NOT complete. Re-run once the scorer is healthy.`);
      } else {
        console.log("[drain] DLQ empty — recovery complete.");
      }
      break;
    }

    await processClaim(rows);
    if (Date.now() - lastReport > 30_000) { report(); lastReport = Date.now(); }
    // Dry-run does not claim, so the next pass would return the same rows forever.
    if (DRY_RUN) { console.log("[drain] dry-run: stopping after one pass."); break; }
  }

  report();
  await sql.end();
}

main().catch(async err => {
  console.error(`[drain] fatal: ${err}`);
  report();
  await sql.end();
  process.exit(1);
});
