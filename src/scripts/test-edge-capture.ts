/**
 * Standalone check for edge-capture: feeds synthetic Jetstream events through edgesFromEvent, writes
 * two Parquet parts, compacts them, and reads the result back via duckdb to assert row shape/counts.
 * Run: npx tsx src/scripts/test-edge-capture.ts   (no DB / no firehose needed)
 */
import { execFile } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { promisify } from "node:util";
import path from "node:path";
import { edgesFromEvent, writeEdgeParts, compactDay, type CommitEvent } from "../lib/collectors/edge-capture";

const execFileAsync = promisify(execFile);
const US = (iso: string) => new Date(iso).getTime() * 1000;
const DAY = "2026-07-30";
const T = US(`${DAY}T12:00:00Z`);

function commit(did: string, collection: string, rkey: string, record: any, op: "create" | "delete" = "create"): CommitEvent {
  return { did, time_us: T, commit: { operation: op, collection, rkey, record } };
}

let failures = 0;
const check = (name: string, cond: boolean) => { console.log(`${cond ? "ok  " : "FAIL"} ${name}`); if (!cond) failures++; };

async function main() {
  // --- 1. pure extraction ---
  const follow = edgesFromEvent(commit("did:plc:alice", "app.bsky.graph.follow", "r1", { subject: "did:plc:bob" }), { captureLikes: true });
  check("follow -> 1 edge alice->bob", follow.length === 1 && follow[0].dst === "did:plc:bob" && follow[0].rel === "follow" && follow[0].op === "c");

  const block = edgesFromEvent(commit("did:plc:alice", "app.bsky.graph.block", "r2", { subject: "did:plc:carol" }), { captureLikes: true });
  check("block -> 1 edge", block.length === 1 && block[0].rel === "block" && block[0].dst === "did:plc:carol");

  const repost = edgesFromEvent(commit("did:plc:alice", "app.bsky.feed.repost", "r3", { subject: { uri: "at://did:plc:bob/app.bsky.feed.post/p1" } }), { captureLikes: true });
  check("repost -> post uri", repost.length === 1 && repost[0].rel === "repost" && repost[0].dst!.includes("/app.bsky.feed.post/"));

  const like = edgesFromEvent(commit("did:plc:alice", "app.bsky.feed.like", "r4", { subject: { uri: "at://did:plc:bob/app.bsky.feed.post/p2" } }), { captureLikes: true });
  check("like captured when captureLikes=true", like.length === 1 && like[0].rel === "like");
  const likeOff = edgesFromEvent(commit("did:plc:alice", "app.bsky.feed.like", "r4", { subject: { uri: "at://x/app.bsky.feed.post/p2" } }), { captureLikes: false });
  check("like dropped when captureLikes=false", likeOff.length === 0);

  const reply = edgesFromEvent(commit("did:plc:alice", "app.bsky.feed.post", "r5", { reply: { parent: { uri: "at://did:plc:bob/app.bsky.feed.post/parent" } }, text: "hi" }), { captureLikes: true });
  check("post with reply -> reply edge", reply.length === 1 && reply[0].rel === "reply" && reply[0].dst!.endsWith("/parent"));

  const quote = edgesFromEvent(commit("did:plc:alice", "app.bsky.feed.post", "r6", { embed: { $type: "app.bsky.embed.record", record: { uri: "at://did:plc:bob/app.bsky.feed.post/quoted" } } }), { captureLikes: true });
  check("post with quote embed -> quote edge", quote.length === 1 && quote[0].rel === "quote");

  const replyQuote = edgesFromEvent(commit("did:plc:alice", "app.bsky.feed.post", "r7", { reply: { parent: { uri: "at://a/app.bsky.feed.post/x" } }, embed: { $type: "app.bsky.embed.recordWithMedia", record: { uri: "at://b/app.bsky.feed.post/y" } } }), { captureLikes: true });
  check("post reply+quote -> 2 edges", replyQuote.length === 2 && new Set(replyQuote.map(e => e.rel)).size === 2);

  const plainPost = edgesFromEvent(commit("did:plc:alice", "app.bsky.feed.post", "r8", { text: "just a post" }), { captureLikes: true });
  check("plain post -> 0 edges", plainPost.length === 0);

  const profile = edgesFromEvent(commit("did:plc:alice", "app.bsky.actor.profile", "self", { displayName: "A" }), { captureLikes: true });
  check("profile -> 0 edges", profile.length === 0);

  const listitem = edgesFromEvent(commit("did:plc:alice", "app.bsky.graph.listitem", "r10", { subject: "did:plc:bob", list: "at://did:plc:alice/app.bsky.graph.list/L1" }), { captureLikes: true });
  check("listitem -> edge alice->bob with list uri in ctx", listitem.length === 1 && listitem[0].rel === "listitem" && listitem[0].dst === "did:plc:bob" && listitem[0].ctx === "at://did:plc:alice/app.bsky.graph.list/L1");

  const listblock = edgesFromEvent(commit("did:plc:alice", "app.bsky.graph.listblock", "r11", { subject: "at://did:plc:carol/app.bsky.graph.list/L2" }), { captureLikes: true });
  check("listblock -> edge to subscribed list uri", listblock.length === 1 && listblock[0].rel === "listblock" && listblock[0].dst!.includes("/app.bsky.graph.list/"));

  const listCreate = edgesFromEvent(commit("did:plc:alice", "app.bsky.graph.list", "L3", { name: "my list", purpose: "app.bsky.graph.defs#curatelist" }), { captureLikes: true });
  check("list create -> self edge to own list uri", listCreate.length === 1 && listCreate[0].rel === "list" && listCreate[0].dst === "at://did:plc:alice/app.bsky.graph.list/L3");

  const unlistitem = edgesFromEvent(commit("did:plc:alice", "app.bsky.graph.listitem", "r10", undefined, "delete"), { captureLikes: true });
  check("listitem delete -> 1 delete row", unlistitem.length === 1 && unlistitem[0].op === "d" && unlistitem[0].rel === "listitem" && unlistitem[0].dst === null);

  const unfollow = edgesFromEvent(commit("did:plc:alice", "app.bsky.graph.follow", "r1", undefined, "delete"), { captureLikes: true });
  check("unfollow -> 1 delete row, dst null", unfollow.length === 1 && unfollow[0].op === "d" && unfollow[0].dst === null && unfollow[0].rel === "follow");

  const postDelete = edgesFromEvent(commit("did:plc:alice", "app.bsky.feed.post", "r5", undefined, "delete"), { captureLikes: true });
  check("post delete -> reply+quote delete rows", postDelete.length === 2 && postDelete.every(e => e.op === "d" && e.dst === null));

  // --- 2. write two parts + compact + read back ---
  const dir = await mkdtemp(path.join(tmpdir(), "edgecap-"));
  try {
    const batch1 = [...follow, ...block, ...repost, ...like, ...reply, ...quote, ...replyQuote,
                    ...listitem, ...listblock, ...listCreate, ...unlistitem, ...unfollow, ...postDelete];
    const batch2 = edgesFromEvent(commit("did:plc:dave", "app.bsky.graph.follow", "r9", { subject: "did:plc:erin" }), { captureLikes: true });
    const w1 = await writeEdgeParts(batch1, dir, "0001");
    const w2 = await writeEdgeParts(batch2, dir, "0002");
    check("wrote a part per batch", w1 === 1 && w2 === 1);

    // dataset-level append: globbing both parts sees all rows before compaction
    const { stdout: preCount } = await execFileAsync("duckdb", ["-noheader", "-list", "-c",
      `SELECT COUNT(*) FROM read_parquet('${path.join(dir, "dt=" + DAY, "part-*.parquet")}');`]);
    const expected = batch1.length + batch2.length;
    check(`glob over parts sees all ${expected} rows`, parseInt(preCount.trim(), 10) === expected);

    const merged = await compactDay(dir, DAY);
    check("compactDay merged", merged === true);

    // compaction writes c-*.parquet INSIDE dt=<DAY>/ and removes the parts — glob the dir
    const mergedGlob = path.join(dir, "dt=" + DAY, "*.parquet");
    const { stdout: postCount } = await execFileAsync("duckdb", ["-noheader", "-list", "-c",
      `SELECT COUNT(*) FROM read_parquet('${mergedGlob}');`]);
    check(`merged file has ${expected} rows`, parseInt(postCount.trim(), 10) === expected);

    const { stdout: shape } = await execFileAsync("duckdb", ["-noheader", "-list", "-c",
      `SELECT rel, op, COUNT(*) c, COUNT(*) FILTER (WHERE dst IS NULL) null_dst ` +
      `FROM read_parquet('${mergedGlob}') GROUP BY 1,2 ORDER BY 1,2;`]);
    console.log("\n-- merged dataset (rel|op|count|null-dst) --\n" + shape.trim());

    // creates must have non-null dst; deletes must have null dst
    const { stdout: bad } = await execFileAsync("duckdb", ["-noheader", "-list", "-c",
      `SELECT COUNT(*) FROM read_parquet('${mergedGlob}') ` +
      `WHERE (op='c' AND dst IS NULL) OR (op='d' AND dst IS NOT NULL);`]);
    check("no create-with-null-dst or delete-with-dst", parseInt(bad.trim(), 10) === 0);

    // the c-* file must live inside the partition dir (Hive-globbable), not as a top-level file
    const topLevel = path.join(dir, "dt=" + DAY + ".parquet");
    check("no top-level dt=<date>.parquet (stays in partition dir)", !existsSync(topLevel));
  } finally {
    await rm(dir, { recursive: true, force: true });
  }

  console.log(failures === 0 ? "\nALL PASS" : `\n${failures} FAILURE(S)`);
  process.exit(failures === 0 ? 0 : 1);
}

main().catch(e => { console.error(e); process.exit(1); });
