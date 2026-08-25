// Jetstream v2 full-network backfill extractor.
//
// Replays the sealed archive from the beginning (WithAfterSeq(0) — "the beginning of time",
// per the upstream full-replay example), keeps only the collections we care about, and writes
// gzipped NDJSON shards ready for COPY into Postgres.
//
// Why this runs on Modal and not the Mac: the archive is ~1.89 TB (7,014 segments x ~269 MB,
// 24.7B events at 76.5 B/event compressed) and a profile-filtered plan still fetches 98.2% of
// segments whole — profile records appear in ~every block, so there is nothing to skip. The
// bytes are transient; what lands is ~17 GB of profile rows. Ingress is not billed, so this is
// roughly an hour of container time.
//
// IMPORTANT — what the archive does and does not contain:
// Jetstream seeded its archive on 2026-08-04 by enumerating every repo and emitting a synthetic
// `create` for each record that already existed. So `TimeUS` on those events is the INGESTION
// time, not the original commit time (a 2022 profile shows up as a 2026-08-04 create with a 2022
// createdAt inside the record). That means the replay DOES give current profile state for the
// whole network, but profile VERSION HISTORY before the seed does not exist anywhere. History
// only accrues forward, via the live collector writing to activity.profile_records.
//
// Usage:
//
//	extract -out /data/out -collections app.bsky.actor.profile
//	extract -out /data/out -collections app.bsky.feed.generator,app.bsky.graph.starterpack,app.bsky.graph.list,app.bsky.graph.listitem
//
// Set JETSTREAM_API_KEY (archive HTTP calls are metered; the live websocket is not).
package main

import (
	"compress/gzip"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"path/filepath"
	"strings"
	"time"

	"github.com/bluesky-social/jetstream"
)

type row struct {
	DID          string   `json:"did"`
	Seq          uint64   `json:"seq"`
	TimeUS       int64    `json:"time_us"`
	Collection   string   `json:"collection"`
	Operation    string   `json:"operation"`
	RKey         string   `json:"rkey,omitempty"`
	DisplayName  *string  `json:"display_name,omitempty"`
	Description  *string  `json:"description,omitempty"`
	AvatarCID    *string  `json:"avatar_cid,omitempty"`
	BannerCID    *string  `json:"banner_cid,omitempty"`
	StarterPack  *string  `json:"joined_via_starterpack_uri,omitempty"`
	PinnedPost   *string  `json:"pinned_post_uri,omitempty"`
	SelfLabels   []string `json:"self_labels,omitempty"`
	RecordTime   *string  `json:"record_created_at,omitempty"`
	ContentHash  string   `json:"content_hash,omitempty"`
	RawRecord    string   `json:"raw_record,omitempty"`
}

func main() {
	var host, outDir, collections string
	var shardRows, downloadConc, segStripes int
	var keepRaw bool
	var beforeSeq uint64
	flag.StringVar(&host, "host", "https://jetstream.us-east.bsky.network", "jetstream host")
	flag.StringVar(&outDir, "out", "./out", "output directory for gzipped NDJSON shards")
	flag.StringVar(&collections, "collections", "app.bsky.actor.profile", "comma-separated NSIDs")
	flag.IntVar(&shardRows, "shard-rows", 2_000_000, "rows per output shard")
	// Non-profile collections have no bespoke parser here, so keep their bodies verbatim.
	flag.BoolVar(&keepRaw, "keep-raw", true, "store the raw record JSON for non-profile collections")
	// Smoke-test lever. The archive is metered by downloaded bytes, so never dry-run the
	// unbounded replay: -before 5000000 is ~0.02% of the archive (~380 MB).
	flag.Uint64Var(&beforeSeq, "before", 0, "stop at this seq (0 = full archive, then live tail)")
	// Auto-sizing is NumCPU clamped to [4,32]; each in-flight download holds ~one segment-sized
	// buffer (~269 MB), so concurrency x 270 MB must fit in container memory.
	flag.IntVar(&downloadConc, "download-concurrency", 0, "concurrent segment downloads (0 = auto from NumCPU)")
	flag.IntVar(&segStripes, "segment-stripes", 0, "parallel range requests per segment (0 = default 8)")
	flag.Parse()

	if err := run(context.Background(), host, outDir, collections, shardRows, keepRaw, beforeSeq, downloadConc, segStripes); err != nil {
		slog.Error("extract failed", "err", err)
		os.Exit(1)
	}
}

// bytesPerEvent is the measured archive density: 1.89 TB / 24.7B events. Used only to turn
// observed seq throughput into an approximate wire rate for the progress log.
const bytesPerEvent = 76.5

func run(ctx context.Context, host, outDir, collections string, shardRows int, keepRaw bool,
	beforeSeq uint64, downloadConc, segStripes int) error {
	colls := strings.Split(collections, ",")
	for i := range colls {
		colls[i] = strings.TrimSpace(colls[i])
		// A wildcard like app.bsky.graph.* silently drags in follow and block, which are as
		// ubiquitous as profiles: the plan jumps from ~300 GB to 1.87 TB. Refuse it.
		if strings.Contains(colls[i], "*") {
			return fmt.Errorf("wildcard collection %q would pull ubiquitous collections (follow/block) "+
				"and cost ~1.87 TB; enumerate explicit NSIDs instead", colls[i])
		}
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	opts := []jetstream.Option{
		jetstream.WithAfterSeq(0), // the beginning of the sealed archive
		jetstream.WithCollections(colls),
	}
	if beforeSeq > 0 {
		// WithBeforeSeq requires WithSnapshotOnly, otherwise later live events are dropped silently.
		opts = append(opts, jetstream.WithBeforeSeq(beforeSeq), jetstream.WithSnapshotOnly())
	}
	if downloadConc > 0 {
		opts = append(opts, jetstream.WithDownloadConcurrency(downloadConc))
	}
	if segStripes > 0 {
		opts = append(opts, jetstream.WithSegmentStripes(segStripes))
	}
	slog.Info("replay config", "collections", colls, "before_seq", beforeSeq,
		"download_concurrency", downloadConc, "segment_stripes", segStripes,
		"num_cpu", runtime.NumCPU())
	if k := os.Getenv("JETSTREAM_API_KEY"); k != "" {
		opts = append(opts, jetstream.WithAPIKey(k))
	} else {
		return errors.New("JETSTREAM_API_KEY is required for archive replay (the live tail is unauthenticated, the archive is not)")
	}

	client, err := jetstream.Subscribe(host, opts...)
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	defer client.Close()

	w, err := newShardWriter(outDir, shardRows)
	if err != nil {
		return err
	}
	defer w.Close()

	wanted := map[string]bool{}
	for _, c := range colls {
		wanted[c] = true
	}

	var kept, seen, lastSeq uint64
	start := time.Now()
	lastLog := start

	for batch, err := range client.Events(ctx) {
		if err != nil {
			if errors.Is(err, jetstream.ErrFatal) {
				return fmt.Errorf("fatal: %w", err)
			}
			slog.Warn("non-fatal replay error", "err", err)
			continue
		}
		for _, ev := range batch.Events() {
			seen++
			if ev.Seq > lastSeq {
				lastSeq = ev.Seq
			}
			if ev.Commit == nil {
				continue
			}
			// The server filter is driven by bloom filters and block summaries, so blocks with
			// no matching rows still arrive. Exact filtering is the client's job.
			if !wanted[ev.Commit.Collection] {
				continue
			}
			r := buildRow(ev, keepRaw)
			if err := w.Write(&r); err != nil {
				return err
			}
			kept++
		}
		if time.Since(lastLog) > 15*time.Second {
			lastLog = time.Now()
			logProgress("replaying", seen, kept, lastSeq, start)
		}
	}

	logProgress("done", seen, kept, lastSeq, start)
	return w.Close()
}

// logProgress reports progress against the ARCHIVE POSITION (seq), not the events we happened
// to keep. With a collection filter the client still downloads whole segments, so seq advanced
// per second is the honest measure of how fast we are consuming the archive — and multiplying by
// the measured 76.5 B/event gives the approximate wire rate, which is what tells us whether the
// download concurrency is actually being used.
func logProgress(msg string, seen, kept, lastSeq uint64, start time.Time) {
	el := time.Since(start).Seconds()
	if el <= 0 {
		return
	}
	seqPerSec := float64(lastSeq) / el
	mbPerSec := seqPerSec * bytesPerEvent / 1e6
	const archiveSeq = 24_706_758_487.0
	etaHours := (archiveSeq / seqPerSec) / 3600
	slog.Info(msg,
		"seen", seen, "kept", kept, "last_seq", lastSeq,
		"seq_per_sec", int64(seqPerSec),
		"approx_MB_per_sec", fmt.Sprintf("%.1f", mbPerSec),
		"full_archive_ETA_hours", fmt.Sprintf("%.1f", etaHours),
		"elapsed", time.Since(start).Round(time.Second))
}

func buildRow(ev jetstream.Event, keepRaw bool) row {
	r := row{
		DID:        ev.DID,
		Seq:        ev.Seq,
		TimeUS:     ev.TimeUS,
		Collection: ev.Commit.Collection,
		Operation:  string(ev.Commit.Operation),
	}
	rec := ev.Commit.Record
	if rec == nil {
		return r
	}
	if ev.Commit.Collection == "app.bsky.actor.profile" {
		r.DisplayName = str(rec["displayName"])
		r.Description = str(rec["description"])
		r.AvatarCID = blobCID(rec["avatar"])
		r.BannerCID = blobCID(rec["banner"])
		r.StarterPack = nestedURI(rec["joinedViaStarterPack"])
		r.PinnedPost = nestedURI(rec["pinnedPost"])
		r.RecordTime = str(rec["createdAt"])
		r.SelfLabels = selfLabels(rec["labels"])
		// Same hash basis as the TS collector: identity-bearing fields only, so a pinned-post
		// change does not register as a new profile version.
		r.ContentHash = hashParts(r.DisplayName, r.Description, r.AvatarCID, r.BannerCID,
			r.StarterPack, ptr(strings.Join(r.SelfLabels, ",")))
		return r
	}
	if keepRaw {
		if b, err := json.Marshal(rec); err == nil {
			r.RawRecord = string(b)
		}
	}
	return r
}

// blobCID pulls the content hash out of a lexicon blob. Current form is
// {$type:blob, ref:{$link:...}}; legacy records still carry {cid:...}.
func blobCID(v any) *string {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	if ref, ok := m["ref"].(map[string]any); ok {
		if s, ok := ref["$link"].(string); ok && s != "" {
			return &s
		}
	}
	if s, ok := m["cid"].(string); ok && s != "" {
		return &s
	}
	return nil
}

func nestedURI(v any) *string {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	return str(m["uri"])
}

func selfLabels(v any) []string {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	vals, ok := m["values"].([]any)
	if !ok {
		return nil
	}
	var out []string
	for _, x := range vals {
		if xm, ok := x.(map[string]any); ok {
			if s, ok := xm["val"].(string); ok {
				out = append(out, s)
			}
		}
	}
	return out
}

func str(v any) *string {
	s, ok := v.(string)
	if !ok || strings.TrimSpace(s) == "" {
		return nil
	}
	s = strings.TrimSpace(s)
	return &s
}

func ptr(s string) *string { return &s }

func hashParts(parts ...*string) string {
	h := md5.New()
	for _, p := range parts {
		if p != nil {
			h.Write([]byte(*p))
		}
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// shardWriter rolls gzipped NDJSON files so a failed run leaves usable output behind
// and so Postgres can be loaded in parallel.
type shardWriter struct {
	dir       string
	maxRows   int
	rows      int
	shard     int
	f         *os.File
	gz        *gzip.Writer
	enc       *json.Encoder
}

func newShardWriter(dir string, maxRows int) (*shardWriter, error) {
	w := &shardWriter{dir: dir, maxRows: maxRows}
	return w, w.roll()
}

func (w *shardWriter) roll() error {
	if err := w.closeCurrent(); err != nil {
		return err
	}
	name := filepath.Join(w.dir, fmt.Sprintf("part-%05d.ndjson.gz", w.shard))
	f, err := os.Create(name)
	if err != nil {
		return err
	}
	w.f = f
	w.gz = gzip.NewWriter(f)
	w.enc = json.NewEncoder(w.gz)
	w.rows = 0
	w.shard++
	return nil
}

func (w *shardWriter) Write(r *row) error {
	if err := w.enc.Encode(r); err != nil {
		return err
	}
	w.rows++
	if w.rows >= w.maxRows {
		return w.roll()
	}
	return nil
}

func (w *shardWriter) closeCurrent() error {
	if w.gz != nil {
		if err := w.gz.Close(); err != nil {
			return err
		}
		w.gz = nil
	}
	if w.f != nil {
		if err := w.f.Close(); err != nil {
			return err
		}
		w.f = nil
	}
	return nil
}

func (w *shardWriter) Close() error { return w.closeCurrent() }
