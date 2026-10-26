/**
 * Prototype BOTH edge sinks against the real firehose, then compare, so we can pick one.
 * Connects to a Jetstream relay (same collections the collector wants), samples for --seconds,
 * runs edgesFromEvent on every commit, and mirrors each flush into ParquetPartsSink AND DuckLakeSink.
 * Reports rows, parquet file count, on-disk bytes, and write overhead for each — plus demonstrates
 * the "changes since last snapshot" delta query on DuckLake. No Postgres, no running-collector touch.
 *
 *   npx tsx src/scripts/prototype-sinks.ts --seconds 120 [--no-likes] [--out <dir>]
 */
import WebSocket from "ws";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { edgesFromEvent, type CommitEvent, type EdgeRow } from "../lib/collectors/edge-capture";
import { ParquetPartsSink, DuckLakeSink } from "../lib/collectors/edge-sinks";

const execFileAsync = promisify(execFile);
const arg = (k: string, d?: string) => { const i = process.argv.indexOf(k); return i >= 0 ? process.argv[i + 1] : d; };
const has = (k: string) => process.argv.includes(k);

const SECONDS = parseInt(arg("--seconds", "120")!, 10);
const CAPTURE_LIKES = !has("--no-likes");
const EDGE_COLLECTIONS = [
  "app.bsky.graph.follow", "app.bsky.graph.block", "app.bsky.feed.repost",
  "app.bsky.feed.like", "app.bsky.feed.post",
];
const RELAY = "wss://jetstream1.us-east.bsky.network/subscribe";

async function main() {
  const root = arg("--out") ?? await mkdtemp(path.join(tmpdir(), "sinkcmp-"));
  const parquet = new ParquetPartsSink(path.join(root, "parquet_parts"), 25_000, 60_000);
  const ducklake = new DuckLakeSink(path.join(root, "ducklake"));
  await ducklake.init();
  console.log(`sampling ${SECONDS}s from ${RELAY.replace("wss://", "")} | captureLikes=${CAPTURE_LIKES}\n out=${root}`);

  const params = EDGE_COLLECTIONS.map(c => `wantedCollections=${encodeURIComponent(c)}`).join("&");
  const ws = new WebSocket(`${RELAY}?${params}`);

  let buf: EdgeRow[] = [];
  let events = 0, edges = 0;
  const relCounts = new Map<string, number>();
  // mirror each ~2s micro-batch into both sinks, like the collector's periodic flush
  const flush = async () => {
    if (buf.length === 0) return;
    const batch = buf; buf = [];
    await Promise.all([parquet.write(batch), ducklake.write(batch)]);
  };
  const timer = setInterval(() => { flush().catch(e => console.error("flush", e)); }, 2000);

  ws.on("message", (data: Buffer) => {
    let evt: any;
    try { evt = JSON.parse(data.toString()); } catch { return; }
    if (evt.kind !== "commit" || !evt.commit) return;
    events++;
    const rows = edgesFromEvent(evt as CommitEvent, { captureLikes: CAPTURE_LIKES });
    for (const r of rows) { relCounts.set(`${r.rel}:${r.op}`, (relCounts.get(`${r.rel}:${r.op}`) ?? 0) + 1); }
    edges += rows.length;
    buf.push(...rows);
  });
  ws.on("error", e => console.error("ws error", e.message));

  await new Promise<void>(res => setTimeout(res, SECONDS * 1000));
  clearInterval(timer);
  ws.close();
  await flush();

  console.log(`\ncaptured: events=${events.toLocaleString()} edges=${edges.toLocaleString()} ` +
    `(${(edges / Math.max(events, 1)).toFixed(2)} edges/event, ${(events / SECONDS).toFixed(0)} events/s)`);
  console.log("by rel:op ->", [...relCounts.entries()].sort().map(([k, v]) => `${k}=${v.toLocaleString()}`).join("  "));

  console.log("\ncompacting both sinks...");
  await Promise.all([parquet.compact(), ducklake.compact()]);

  const [ps, ds] = await Promise.all([parquet.stats(), ducklake.stats()]);
  const fmtMB = (b: number) => (b / 1e6).toFixed(2) + "MB";

  // Current-state (deletes applied) both ways, to prove they agree.
  const q = async (sql: string) => (await execFileAsync("duckdb", ["-noheader", "-list", "-c", sql])).stdout.trim();
  const logGlob = path.join(root, "parquet_parts", "**", "*.parquet");
  const parquetCurrent = await q(
    `SELECT count(*) FROM (SELECT * FROM read_parquet('${logGlob}') WHERE op='c') c ` +
    `WHERE NOT EXISTS (SELECT 1 FROM read_parquet('${logGlob}') d WHERE d.op='d' ` +
    `AND d.src=c.src AND d.rel=c.rel AND d.rkey=c.rkey);`).catch(() => "n/a");
  const ducklakeCurrent = await q(
    `ATTACH 'ducklake:sqlite:${path.join(root, "ducklake", "catalog.sqlite")}' AS lake (DATA_PATH '${path.join(root, "ducklake", "edata")}/');` +
    `SELECT count(*) FROM lake.edges;`).catch(() => "n/a");

  console.log("\n=== SINK COMPARISON (after compaction) ===");
  console.log(`  parquet-parts : logRows=${ps.rows.toLocaleString()}  files=${ps.files}  size=${fmtMB(ps.bytes)}  writeOverhead=${ps.writeMs}ms  currentState(anti-join)=${parquetCurrent}`);
  console.log(`  ducklake      : logRows=${ds.rows.toLocaleString()}  files=${ds.files}  size=${fmtMB(ds.bytes)}  writeOverhead=${ds.writeMs}ms  currentState(table)=${ducklakeCurrent}`);

  // Demonstrate the DuckLake change-feed as the "edges since snapshot N" delta query.
  try {
    const { stdout } = await execFileAsync("duckdb", ["-c",
      `ATTACH 'ducklake:sqlite:${path.join(root, "ducklake", "catalog.sqlite")}' AS lake (DATA_PATH '${path.join(root, "ducklake", "edata")}/');` +
      `SELECT min(snapshot_id) lo, max(snapshot_id) hi, count(*) snapshots FROM lake.snapshots();`]);
    console.log("\nducklake snapshots (each flush = a snapshot; delta = ducklake_table_changes(lake,main,edges,lo,hi)):");
    console.log(stdout.trim());
  } catch (e) { console.error("snapshot query failed", e); }

  if (!arg("--out")) { console.log(`\n(cleaning ${root}; pass --out <dir> to keep)`); await rm(root, { recursive: true, force: true }); }
  process.exit(0);
}

main().catch(e => { console.error(e); process.exit(1); });
