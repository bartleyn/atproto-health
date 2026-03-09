/**
 * Samples the Jetstream firehose for a configurable window and measures
 * cross-PDS interaction rates. For each interaction (like, reply, repost,
 * follow), we check whether the actor and target are on the same PDS.
 *
 * DID → PDS resolution uses our local DB (populated by collect runs)
 * with a fallback to the PLC directory API for unknown DIDs.
 */

import WebSocket from "ws";
import { getDb } from "../db/schema";

const JETSTREAM_URL =
  "wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.like&wantedCollections=app.bsky.feed.post&wantedCollections=app.bsky.graph.follow&wantedCollections=app.bsky.feed.repost";

const PLC_DIRECTORY = "https://plc.directory";

type InteractionType = "like" | "reply" | "repost" | "follow";

interface Interaction {
  type: InteractionType;
  actorDid: string;
  targetDid: string;
}

// Bluesky shards users across mushroom-named servers, so we need to
// distinguish "cross-PDS within Bluesky infra" from true federation.
function isBskyHosted(pds: string): boolean {
  return pds.includes(".host.bsky.network") || pds.includes("bsky.social");
}

type FederationCategory =
  | "bsky-internal"     // both on Bluesky infra (cross-shard, not true federation)
  | "bsky-to-third"     // Bluesky user → third-party PDS (true federation)
  | "third-to-bsky"     // third-party → Bluesky user (true federation)
  | "third-to-third"    // both on third-party PDSes (true federation)
  | "same-pds";         // same exact PDS

interface SampleResult {
  durationMs: number;
  totalEvents: number;
  totalInteractions: number;
  resolvedInteractions: number;
  crossPds: number;
  samePds: number;
  federation: Record<FederationCategory, number>;
  byType: Record<
    InteractionType,
    { total: number; crossPds: number; samePds: number }
  >;
  topCrossPdsPairs: Array<{
    from: string;
    to: string;
    count: number;
  }>;
  eventsPerSecond: number;
}

// ── DID → PDS resolution ──────────────────────────────────────────────

// In-memory cache for the duration of the sample
const didToPds = new Map<string, string | null>();
const PLC_BATCH_SIZE = 50;
const pendingDids = new Set<string>();
let plcLookups = 0;

async function resolveDid(did: string): Promise<string | null> {
  if (didToPds.has(did)) return didToPds.get(did)!;

  // Try PLC directory
  try {
    plcLookups++;
    const res = await fetch(`${PLC_DIRECTORY}/${encodeURIComponent(did)}`, {
      signal: AbortSignal.timeout(3000),
    });
    if (!res.ok) {
      didToPds.set(did, null);
      return null;
    }
    const doc = await res.json();
    const pds =
      doc?.service?.find(
        (s: { id: string; type: string; serviceEndpoint: string }) =>
          s.id === "#atproto_pds"
      )?.serviceEndpoint ?? null;
    didToPds.set(did, pds);
    return pds;
  } catch {
    didToPds.set(did, null);
    return null;
  }
}

// Pre-seed the cache from our DB
function seedCacheFromDb() {
  const db = getDb();
  // Build a map from the firehose.didWebs data in state.json snapshots
  // For now, we'll resolve on the fly from PLC directory
  // But we can use our pds_snapshots to know which PDS URLs exist
  const rows = db
    .prepare(`SELECT DISTINCT url FROM pds_instances`)
    .all() as { url: string }[];
  console.log(
    `[jetstream] Seeded PDS URL list with ${rows.length} known instances`
  );
}

// ── Event parsing ─────────────────────────────────────────────────────

function extractTargetDid(
  collection: string,
  record: Record<string, unknown>
): string | null {
  if (
    collection === "app.bsky.feed.like" ||
    collection === "app.bsky.feed.repost"
  ) {
    const subject = record.subject as { uri?: string } | undefined;
    return extractDidFromUri(subject?.uri);
  }

  if (collection === "app.bsky.feed.post") {
    // Only count replies, not top-level posts
    const reply = record.reply as
      | { parent?: { uri?: string } }
      | undefined;
    if (!reply?.parent?.uri) return null;
    return extractDidFromUri(reply.parent.uri);
  }

  if (collection === "app.bsky.graph.follow") {
    return (record.subject as string) ?? null;
  }

  return null;
}

function extractDidFromUri(uri: string | undefined): string | null {
  if (!uri?.startsWith("at://")) return null;
  const did = uri.slice(5).split("/")[0];
  return did.startsWith("did:") ? did : null;
}

function collectionToType(collection: string): InteractionType | null {
  switch (collection) {
    case "app.bsky.feed.like":
      return "like";
    case "app.bsky.feed.post":
      return "reply";
    case "app.bsky.feed.repost":
      return "repost";
    case "app.bsky.graph.follow":
      return "follow";
    default:
      return null;
  }
}

// ── Main sampler ──────────────────────────────────────────────────────

export async function sampleJetstream(
  durationSec = 60
): Promise<SampleResult> {
  seedCacheFromDb();

  const interactions: Interaction[] = [];
  let totalEvents = 0;

  return new Promise((resolve, reject) => {
    console.log(
      `[jetstream] Connecting and sampling for ${durationSec}s...`
    );

    const ws = new WebSocket(JETSTREAM_URL);
    const startTime = Date.now();

    const timer = setTimeout(() => {
      console.log(`[jetstream] Sample window complete. Closing connection...`);
      ws.close();
    }, durationSec * 1000);

    ws.on("message", (data: Buffer) => {
      totalEvents++;

      try {
        const evt = JSON.parse(data.toString());
        if (evt.kind !== "commit" || evt.commit.operation !== "create") return;

        const type = collectionToType(evt.commit.collection);
        if (!type) return;

        const targetDid = extractTargetDid(
          evt.commit.collection,
          evt.commit.record
        );
        if (!targetDid) return;

        interactions.push({
          type,
          actorDid: evt.did,
          targetDid,
        });
      } catch {
        // Skip malformed events
      }

      if (totalEvents % 10000 === 0) {
        console.log(
          `[jetstream] ${totalEvents} events, ${interactions.length} interactions collected...`
        );
      }
    });

    ws.on("error", (err: Error) => {
      clearTimeout(timer);
      reject(err);
    });

    ws.on("close", async () => {
      clearTimeout(timer);
      const elapsed = Date.now() - startTime;

      console.log(
        `[jetstream] Collected ${interactions.length} interactions from ${totalEvents} events`
      );
      console.log(`[jetstream] Resolving DIDs to PDS instances...`);

      // Collect all unique DIDs
      const allDids = new Set<string>();
      for (const i of interactions) {
        allDids.add(i.actorDid);
        allDids.add(i.targetDid);
      }

      console.log(`[jetstream] ${allDids.size} unique DIDs to resolve`);

      // Resolve in batches with concurrency limit
      const didArray = [...allDids];
      const RESOLVE_CONCURRENCY = 30;
      for (let i = 0; i < didArray.length; i += RESOLVE_CONCURRENCY) {
        const batch = didArray.slice(i, i + RESOLVE_CONCURRENCY);
        await Promise.all(batch.map(resolveDid));

        if ((i + RESOLVE_CONCURRENCY) % 1000 === 0 || i + RESOLVE_CONCURRENCY >= didArray.length) {
          const done = Math.min(i + RESOLVE_CONCURRENCY, didArray.length);
          console.log(`[jetstream] Resolved ${done}/${didArray.length} DIDs (${plcLookups} PLC lookups)`);
        }
      }

      // Analyze interactions
      const byType: SampleResult["byType"] = {
        like: { total: 0, crossPds: 0, samePds: 0 },
        reply: { total: 0, crossPds: 0, samePds: 0 },
        repost: { total: 0, crossPds: 0, samePds: 0 },
        follow: { total: 0, crossPds: 0, samePds: 0 },
      };

      const federation: SampleResult["federation"] = {
        "bsky-internal": 0,
        "bsky-to-third": 0,
        "third-to-bsky": 0,
        "third-to-third": 0,
        "same-pds": 0,
      };

      let crossPds = 0;
      let samePds = 0;
      let resolved = 0;

      const pairCounts = new Map<string, number>();

      for (const interaction of interactions) {
        const actorPds = didToPds.get(interaction.actorDid);
        const targetPds = didToPds.get(interaction.targetDid);

        byType[interaction.type].total++;

        if (!actorPds || !targetPds) continue;
        resolved++;

        const actorBsky = isBskyHosted(actorPds);
        const targetBsky = isBskyHosted(targetPds);

        if (actorPds === targetPds) {
          samePds++;
          byType[interaction.type].samePds++;
          federation["same-pds"]++;
        } else {
          crossPds++;
          byType[interaction.type].crossPds++;

          if (actorBsky && targetBsky) federation["bsky-internal"]++;
          else if (actorBsky && !targetBsky) federation["bsky-to-third"]++;
          else if (!actorBsky && targetBsky) federation["third-to-bsky"]++;
          else federation["third-to-third"]++;

          // Track PDS pairs
          const pair = [actorPds, targetPds].sort().join(" <-> ");
          pairCounts.set(pair, (pairCounts.get(pair) ?? 0) + 1);
        }
      }

      const topPairs = [...pairCounts.entries()]
        .sort((a, b) => b[1] - a[1])
        .slice(0, 15)
        .map(([pair, count]) => {
          const [from, to] = pair.split(" <-> ");
          return { from, to, count };
        });

      resolve({
        durationMs: elapsed,
        totalEvents,
        totalInteractions: interactions.length,
        resolvedInteractions: resolved,
        crossPds,
        samePds,
        federation,
        byType,
        topCrossPdsPairs: topPairs,
        eventsPerSecond: Math.round(totalEvents / (elapsed / 1000)),
      });
    });
  });
}
