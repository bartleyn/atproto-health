/**
 * Two interchangeable on-disk sinks for streamed edges, behind one interface, so we can prototype
 * both against the real firehose and pick one. Both follow the industry-standard streaming pattern
 * (buffer → immutable Parquet → compact); the difference is who manages it:
 *
 * Settled semantics (both sinks deliver both views, differently):
 *   - EVENT LOG: append-only, logs every create (op=c) AND delete (op=d) — for churn/coordination.
 *   - CURRENT STATE: deletes applied (edge exists iff created and not later deleted), consistent
 *     with the RocksDB snapshot which only holds live records.
 *
 *   ParquetPartsSink — we manage it: append-only zstd part files (the EVENT LOG, both op=c/op=d)
 *     into edges/dt=<date>/, compacted daily. CURRENT STATE is materialized at ER time by an
 *     anti-join (creates with no later matching delete on src,rel,rkey). Zero new infra.
 *
 *   DuckLakeSink — DuckLake manages it: create→INSERT, delete→DELETE against one table, so the
 *     table IS the CURRENT STATE (RocksDB-consistent). Its change feed / snapshots preserve the full
 *     EVENT LOG (every insert and delete, with time travel) — both views from one store. Small
 *     inserts inline into the catalog (no small-file problem); flush to Parquet on demand.
 *
 * Both spawn the duckdb CLI rather than linking a native binding into the long-running collector.
 */
import { execFile } from "node:child_process";
import { mkdir, rm, writeFile, stat } from "node:fs/promises";
import { promisify } from "node:util";
import path from "node:path";
import { writeEdgeParts, compactDay, type EdgeRow } from "./edge-capture";

const execFileAsync = promisify(execFile);

export interface SinkStats {
  rows: number;      // rows the sink was asked to persist
  files: number;     // parquet files on disk
  bytes: number;     // total on-disk size of the sink's data dir
  writeMs: number;   // cumulative time spent in write() (the collector-path overhead)
}

export interface EdgeSink {
  readonly name: string;
  write(rows: EdgeRow[]): Promise<void>;
  compact(): Promise<void>;
  stats(): Promise<SinkStats>;
}

async function dirBytesAndFiles(dir: string): Promise<{ bytes: number; files: number }> {
  // du -sk for bytes; find for parquet count. Cheap and portable enough for the prototype.
  try {
    const { stdout: du } = await execFileAsync("du", ["-sk", dir]);
    const kb = parseInt(du.trim().split(/\s+/)[0], 10) || 0;
    const { stdout: fc } = await execFileAsync("bash", ["-c", `find '${dir}' -name '*.parquet' | wc -l`]);
    return { bytes: kb * 1024, files: parseInt(fc.trim(), 10) || 0 };
  } catch {
    return { bytes: 0, files: 0 };
  }
}

/** We-manage-it sink: buffer to a size/time target, write a zstd part per flush, compact by day. */
export class ParquetPartsSink implements EdgeSink {
  readonly name = "parquet-parts";
  private buf: EdgeRow[] = [];
  private lastPartAt = Date.now();
  private partSeq = 0;
  private rows = 0;
  private writeMs = 0;
  private readonly dates = new Set<string>();

  constructor(
    private readonly dir: string,
    private readonly minRows = 25_000,          // bin-pack target: don't emit tiny parts
    private readonly maxIntervalMs = 10 * 60_000,
  ) {}

  async write(rows: EdgeRow[]): Promise<void> {
    const t0 = Date.now();
    this.buf.push(...rows);
    this.rows += rows.length;
    for (const r of rows) this.dates.add(r.dt);
    const due = this.buf.length >= this.minRows || Date.now() - this.lastPartAt >= this.maxIntervalMs;
    if (due && this.buf.length > 0) {
      const batch = this.buf; this.buf = []; this.lastPartAt = Date.now();
      await writeEdgeParts(batch, this.dir, `${Date.now()}-${this.partSeq++}`);
    }
    this.writeMs += Date.now() - t0;
  }

  async compact(): Promise<void> {
    const t0 = Date.now();
    if (this.buf.length > 0) {
      const batch = this.buf; this.buf = [];
      await writeEdgeParts(batch, this.dir, `${Date.now()}-${this.partSeq++}`);
    }
    for (const dt of this.dates) await compactDay(this.dir, dt);
    this.writeMs += Date.now() - t0;
  }

  async stats(): Promise<SinkStats> {
    const { bytes, files } = await dirBytesAndFiles(this.dir);
    return { rows: this.rows, files, bytes, writeMs: this.writeMs };
  }
}

/** DuckLake-managed sink: INSERT per flush (inlined), flush+merge on compact. */
export class DuckLakeSink implements EdgeSink {
  readonly name = "ducklake";
  private rows = 0;
  private writeMs = 0;
  private seq = 0;
  private buf: EdgeRow[] = [];
  private lastAt = Date.now();
  // catalog + data path. For the prototype, a local sqlite catalog; in prod, swap to Postgres.
  constructor(
    private readonly root: string,
    private readonly minRows = 25_000,          // same bin-pack target as ParquetPartsSink
    private readonly maxIntervalMs = 60_000,
    private readonly catalog = path.join(root, "catalog.sqlite"),
    private readonly dataPath = path.join(root, "edata") + path.sep,
  ) {}

  private attach(): string {
    return `INSTALL ducklake; INSTALL sqlite; ATTACH 'ducklake:sqlite:${this.catalog}' AS lake (DATA_PATH '${this.dataPath}'); USE lake;`;
  }

  async init(): Promise<void> {
    await mkdir(this.root, { recursive: true });
    // Current-state table: no op column (creates are rows; deletes are applied as DELETE, and the
    // op history lives in DuckLake's change feed).
    await execFileAsync("duckdb", ["-c",
      `${this.attach()} CREATE TABLE IF NOT EXISTS edges(src VARCHAR, rel VARCHAR, dst VARCHAR, rkey VARCHAR, t BIGINT);`]);
  }

  async write(rows: EdgeRow[]): Promise<void> {
    this.buf.push(...rows);
    const due = this.buf.length >= this.minRows || Date.now() - this.lastAt >= this.maxIntervalMs;
    if (!due) return;
    await this.drain();
  }

  /** Apply the buffered batch: inserts then deletes, in one duckdb invocation. */
  private async drain(): Promise<void> {
    if (this.buf.length === 0) return;
    const rows = this.buf; this.buf = []; this.lastAt = Date.now();
    const t0 = Date.now();
    const creates = rows.filter(r => r.op === "c");
    const deletes = rows.filter(r => r.op === "d");
    const cols = `columns={src:'VARCHAR',rel:'VARCHAR',dst:'VARCHAR',rkey:'VARCHAR',t:'BIGINT'}, format='newline_delimited'`;
    // Inserts first, then apply deletes — so a create+delete within one batch nets to absent
    // (RocksDB-consistent). Deletes carry no dst; match the live edge on (src,rel,rkey).
    const stmts: string[] = [this.attach()];
    let cNd: string | null = null, dNd: string | null = null;
    if (creates.length) {
      cNd = path.join(this.root, `.c-${this.seq++}.ndjson`);
      await writeFile(cNd, creates.map(r => JSON.stringify({ src: r.src, rel: r.rel, dst: r.dst, rkey: r.rkey, t: r.t })).join("\n"));
      stmts.push(`INSERT INTO edges SELECT src,rel,dst,rkey,t FROM read_json('${cNd}', ${cols});`);
    }
    if (deletes.length) {
      dNd = path.join(this.root, `.d-${this.seq++}.ndjson`);
      await writeFile(dNd, deletes.map(r => JSON.stringify({ src: r.src, rel: r.rel, rkey: r.rkey })).join("\n"));
      stmts.push(
        `DELETE FROM edges WHERE EXISTS (SELECT 1 FROM read_json('${dNd}', ` +
        `columns={src:'VARCHAR',rel:'VARCHAR',rkey:'VARCHAR'}, format='newline_delimited') d ` +
        `WHERE d.src=edges.src AND d.rel=edges.rel AND d.rkey=edges.rkey);`);
    }
    try {
      await execFileAsync("duckdb", ["-c", stmts.join(" ")]);
    } finally {
      if (cNd) await rm(cNd, { force: true });
      if (dNd) await rm(dNd, { force: true });
    }
    this.rows += rows.length;
    this.writeMs += Date.now() - t0;
  }

  async compact(): Promise<void> {
    await this.drain();                       // flush any buffered tail before compacting
    const t0 = Date.now();
    await execFileAsync("duckdb", ["-c",
      `${this.attach()} CALL ducklake_flush_inlined_data('lake'); CALL ducklake_merge_adjacent_files('lake');`]);
    this.writeMs += Date.now() - t0;
  }

  async stats(): Promise<SinkStats> {
    const { bytes, files } = await dirBytesAndFiles(this.dataPath);
    return { rows: this.rows, files, bytes, writeMs: this.writeMs };
  }
}
