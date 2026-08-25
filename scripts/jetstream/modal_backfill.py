"""Run the Jetstream v2 full-network backfill on Modal.

The archive is ~1.89 TB (7,014 segments x ~269 MB, 24.7B events at 76.5 B/event). A
profile-filtered plan still fetches 98.2% of segments whole, because profile records land in
essentially every block -- there is nothing for the planner to skip. Those bytes are transient;
what survives is ~8 GB of gzipped profile rows.

That makes this a bad fit for the Mac (16 GB, home bandwidth, already running Postgres and the
collectors) and a trivial one for Modal: ingress is not billed, so the cost is container time,
roughly $0.20-$4 depending on achieved throughput.

Prereqs:
  1. Cross-compile the extractor (checked in next to this file):
       GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o extract-linux-amd64 .
  2. Create the secret holding your Jetstream key (metered HTTP archive calls need it;
     the live websocket does not):
       modal secret create jetstream-key JETSTREAM_API_KEY=<key>

Usage:
  # bounded rehearsal first -- ~0.02% of the archive, proves the path and the quota behaviour
  modal run modal_backfill.py --before 5000000

  # the cheap bundle: feed generators + starterpacks (~300 GB, no whole-segment fetches)
  modal run modal_backfill.py --collections \
      app.bsky.feed.generator,app.bsky.graph.starterpack,app.bsky.graph.list,app.bsky.graph.listitem

  # profiles for the whole network (~1.85 TB)
  modal run modal_backfill.py --collections app.bsky.actor.profile

  # then pull the shards down
  modal volume get jetstream-backfill /profile ./profile-shards
"""

import pathlib
import subprocess

import modal

HERE = pathlib.Path(__file__).parent
BINARY = HERE / "extract-linux-amd64"

app = modal.App("jetstream-backfill")

volume = modal.Volume.from_name("jetstream-backfill", create_if_missing=True)

image = (
    modal.Image.debian_slim()
    .apt_install("ca-certificates")  # the extractor talks HTTPS to the archive
    .add_local_file(BINARY, "/usr/local/bin/extract", copy=True)
)


@app.function(
    image=image,
    volumes={"/data": volume},
    secrets=[modal.Secret.from_name("jetstream-key")],
    cpu=16.0,
    memory=32768,
    timeout=86_400,  # 24 h: the whole-archive run is hours, and a low quota stretches it
)
def backfill(collections: str = "app.bsky.actor.profile", before: int = 0, shard_rows: int = 2_000_000):
    """Replay the sealed archive, keep matching records, write gzipped NDJSON shards to the volume."""
    # One output dir per collection set so separate runs don't interleave shards.
    tag = collections.split(",")[0].split(".")[-1]
    out = f"/data/{tag}"
    cmd = [
        "/usr/local/bin/extract",
        "-out", out,
        "-collections", collections,
        "-shard-rows", str(shard_rows),
    ]
    if before:
        cmd += ["-before", str(before)]

    print("running:", " ".join(cmd), flush=True)
    proc = subprocess.run(cmd, check=False)

    # Commit whatever landed even on failure: the archive is metered, so partial output is
    # worth keeping rather than re-downloading. Shards are self-contained NDJSON.
    volume.commit()
    if proc.returncode != 0:
        raise RuntimeError(f"extract exited {proc.returncode} (partial shards committed to {out})")

    listing = subprocess.run(["ls", "-la", out], capture_output=True, text=True)
    print(listing.stdout)
    return out


@app.local_entrypoint()
def main(collections: str = "app.bsky.actor.profile", before: int = 0, shard_rows: int = 2_000_000):
    if not BINARY.exists():
        raise SystemExit(
            f"missing {BINARY}\nbuild it first:\n"
            "  cd scripts/jetstream && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o extract-linux-amd64 ."
        )
    out = backfill.remote(collections=collections, before=before, shard_rows=shard_rows)
    print(f"shards written to volume 'jetstream-backfill' at {out}")
    print(f"fetch with: modal volume get jetstream-backfill {out.removeprefix('/data')} ./shards")
