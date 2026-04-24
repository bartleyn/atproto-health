# atproto-health

A Next.js dashboard for monitoring the health of the AT Protocol ecosystem. Tracks PDS (Personal Data Server) instances across the network — geographic distribution, hosting providers, software versions, user counts, federation activity, account age, and migration patterns.

All rendering is server-side. Data lives in a local SQLite database populated by collection scripts you run manually or on a cron.

## Pages

**/** — PDS infrastructure overview. World map of server locations, breakdown by hosting provider, software version distribution, user count concentration, and a Jetstream firehose sample showing federation activity (real cross-party federation vs. Bluesky-internal cross-shard traffic).

**/longevity** — PDS and account age. When each PDS launched, account cohort distribution over time, weekly account creation trends. Useful for understanding how old the indie PDS ecosystem actually is.

**/migrations** — Account migration patterns from plc.directory. How many accounts have migrated, where they went, Sankey flows for origin→destination and multi-hop trajectories.

## Setup

```bash
npm install
npm run dev
```

The SQLite database is created automatically on first run. The dashboard shows a "No data collected yet" message until you run at least one collection.

## Data collection

There are two main data sources: the PDS directory (scraped from GitHub) and the PLC directory (the canonical DID record log).

### PDS infrastructure

```bash
npm run collect          # Full: directory + geo + user counts
npm run collect:geo      # Directory + geo only (~minutes)
npm run collect:users    # Directory + user counts (slow — paginates all repos)
npm run scan:pds-status  # Liveness check on each PDS
```

### Federation activity (firehose sample)

```bash
npm run sample                       # 60s Jetstream sample
npm run sample -- --duration 300     # 5-minute sample
```

### PLC directory (account history)

```bash
npm run collect:plc              # Sync plc.directory operation log
npm run collect:plc:creations    # Extract account creation events
npm run aggregate:plc            # Aggregate per-PDS language/region stats
npm run aggregate:active-plc     # Aggregate weekly active creation timeseries
```

### Supporting collectors

```bash
npm run collect:ecosystem    # Ecosystem-level stats
npm run collect:skywatch     # Skywatch moderation labels
npm run collect:activity     # Jetstream activity metrics
```

## Architecture

Two separate pipelines that share a SQLite database:

**Collector pipeline** — TypeScript scripts under `src/lib/collectors/`, run via `npx tsx`. Each writes snapshot rows to the DB. Designed to run independently on different schedules; the dashboard always shows the freshest available data per field.

**Dashboard pipeline** — Next.js app with server components only (`force-dynamic`, no client-side fetching). All queries go through `src/lib/db/queries.ts` and `src/lib/db/plc-queries.ts`. Charts are Recharts wrappers in `src/components/charts.tsx`; the world map uses react-simple-maps.

**Merged view pattern** — The `pds_latest` view in `queries.ts` composites the freshest directory snapshot, geo snapshot, and user-count snapshot per PDS. Running `collect:geo` and `collect:users` on different schedules still gives you a coherent view.

**Snapshot-based storage** — Collection runs insert new rows, never update. History is preserved; the merged view picks the most recent completed run per field category.

**Federation classification** — The Jetstream sampler distinguishes true cross-party federation from Bluesky-internal cross-shard traffic between `*.host.bsky.network` shards and `bsky.social`. See `isBskyHosted()` in `jetstream-sample.ts`.

**`better-sqlite3` in Next.js** — The package is listed in `serverExternalPackages` in `next.config.ts` so Next.js doesn't bundle it.

## Stack

- Next.js 16 (App Router, Turbopack)
- SQLite via better-sqlite3
- Recharts + react-simple-maps + d3-sankey
- Tailwind CSS v4
- TypeScript + tsx for collection scripts
