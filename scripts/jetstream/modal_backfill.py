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

  # profiles for the whole network (~1.85 TB), one shot
  modal run modal_backfill.py --collections app.bsky.actor.profile

  # ...or chunked, so a failed/timed-out chunk only costs that window, not the whole archive.
  # Each chunk gets its own output dir (named by its seq window) so shards never collide.
  modal run modal_backfill.py --after 0            --before 8000000000
  modal run modal_backfill.py --after 8000000000   --before 16000000000
  modal run modal_backfill.py --after 16000000000  --before 25000000000

  # then pull the shards down (one dir per chunk under /profile)
  modal volume get jetstream-backfill /profile ./profile-shards

  # if you see "xrpc 429 byte limit exceeded" -- the archive's per-key rate limit -- throttle
  # concurrency down from the auto (NumCPU-derived) default and re-run the affected chunk:
  modal run modal_backfill.py --after 0 --before 8000000000 \
      --download-concurrency 8 --segment-stripes 2
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
def backfill(collections: str = "app.bsky.actor.profile", after: int = 0, before: int = 0,
             shard_rows: int = 2_000_000, download_concurrency: int = 0, segment_stripes: int = 0):
    """Replay the sealed archive, keep matching records, write gzipped NDJSON shards to the volume."""
    # One output dir per collection set, further split by seq window so chunked runs (distinct
    # after/before pairs) never share a directory -- extract's shard numbering restarts at 0
    # per run, so sharing a dir would silently overwrite an earlier chunk's part-00000 etc.
    tag = collections.split(",")[0].split(".")[-1]
    out = f"/data/{tag}"
    if after or before:
        out += f"/{after}-{before or 'end'}"
    cmd = [
        "/usr/local/bin/extract",
        "-out", out,
        "-collections", collections,
        "-shard-rows", str(shard_rows),
    ]
    if after:
        cmd += ["-after", str(after)]
    if before:
        cmd += ["-before", str(before)]
    if download_concurrency:
        cmd += ["-download-concurrency", str(download_concurrency)]
    if segment_stripes:
        cmd += ["-segment-stripes", str(segment_stripes)]

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
def main(collections: str = "app.bsky.actor.profile", after: int = 0, before: int = 0, shard_rows: int = 2_000_000,
          download_concurrency: int = 0, segment_stripes: int = 0):
    if not BINARY.exists():
        raise SystemExit(
            f"missing {BINARY}\nbuild it first:\n"
            "  cd scripts/jetstream && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o extract-linux-amd64 ."
        )
    out = backfill.remote(collections=collections, after=after, before=before, shard_rows=shard_rows,
                           download_concurrency=download_concurrency, segment_stripes=segment_stripes)
    print(f"shards written to volume 'jetstream-backfill' at {out}")
    print(f"fetch with: modal volume get jetstream-backfill {out.removeprefix('/data')} ./shards")
