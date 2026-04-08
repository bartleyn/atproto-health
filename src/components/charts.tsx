"use client";

import { sankey, sankeyLinkHorizontal, sankeyJustify } from "d3-sankey";
import type { SankeyNode, SankeyLink } from "d3-sankey";
import { useState, useRef, useEffect } from "react";
import type { MigrationFlow, WeeklyMigrationRow } from "@/lib/db/plc-queries";

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Cell,
  PieChart,
  Pie,
  Legend,
  AreaChart,
  Area,
} from "recharts";
import type { PdsStatusRow } from "@/lib/db/plc-queries";

const COLORS = [
  "#3b82f6",
  "#8b5cf6",
  "#06b6d4",
  "#10b981",
  "#f59e0b",
  "#ef4444",
  "#ec4899",
  "#6366f1",
  "#14b8a6",
  "#f97316",
  "#84cc16",
  "#a855f7",
];

const tooltipStyle = {
  contentStyle: {
    backgroundColor: "#1f2937",
    border: "1px solid #374151",
    borderRadius: "0.5rem",
    color: "#f3f4f6",
    fontSize: "0.875rem",
  },
  itemStyle: { color: "#f3f4f6" },
};

interface BarChartProps {
  data: { name: string; value: number }[];
  color?: string;
  layout?: "horizontal" | "vertical";
  xLabel?: string;
  yLabel?: string;
}

export function SimpleBarChart({
  data,
  color = "#3b82f6",
  layout = "vertical",
  xLabel,
  yLabel,
}: BarChartProps) {
  if (layout === "horizontal") {
    return (
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={data} margin={{ bottom: xLabel ? 24 : 0 }}>
          <XAxis
            dataKey="name"
            tick={{ fill: "#9ca3af", fontSize: 12 }}
            axisLine={{ stroke: "#374151" }}
            tickLine={false}
            label={xLabel ? { value: xLabel, position: "insideBottom", offset: -16, fill: "#6b7280", fontSize: 11 } : undefined}
          />
          <YAxis
            tick={{ fill: "#9ca3af", fontSize: 12 }}
            axisLine={{ stroke: "#374151" }}
            tickLine={false}
            label={yLabel ? { value: yLabel, angle: -90, position: "insideLeft", offset: 10, fill: "#6b7280", fontSize: 11 } : undefined}
          />
          <Tooltip {...tooltipStyle} />
          <Bar dataKey="value" fill={color} radius={[4, 4, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    );
  }

  return (
    <ResponsiveContainer width="100%" height={Math.max(300, data.length * 28)}>
      <BarChart data={data} layout="vertical" margin={{ left: 10 }}>
        <XAxis
          type="number"
          tick={{ fill: "#9ca3af", fontSize: 12 }}
          axisLine={{ stroke: "#374151" }}
          tickLine={false}
        />
        <YAxis
          type="category"
          dataKey="name"
          tick={{ fill: "#9ca3af", fontSize: 12 }}
          axisLine={{ stroke: "#374151" }}
          tickLine={false}
          width={100}
        />
        <Tooltip {...tooltipStyle} />
        <Bar dataKey="value" fill={color} radius={[0, 4, 4, 0]} />
      </BarChart>
    </ResponsiveContainer>
  );
}

interface StackedAreaChartProps {
  data: { period: string; pds_url: string; count: number }[];
  allPeriods?: string[]; // force x-axis to cover this full range
}

export function StackedAreaChart({ data, allPeriods }: StackedAreaChartProps) {
  // Collect ordered list of PDS keys (Other always last)
  const pdsSet = new Set<string>();
  for (const row of data) {
    if (row.pds_url !== "Other") pdsSet.add(row.pds_url);
  }
  const pdsKeys = [...pdsSet, "Other"].filter(k => data.some(r => r.pds_url === k));

  // Pivot into { period, [pds]: rawCount, [pds + "__pct"]: pctOfTotal }
  const byPeriod = new Map<string, Record<string, number | string>>();
  // Pre-seed all periods so both charts share the same x-axis range
  if (allPeriods) {
    for (const p of allPeriods) byPeriod.set(p, { period: p });
  }
  for (const row of data) {
    if (!byPeriod.has(row.period)) byPeriod.set(row.period, { period: row.period });
    (byPeriod.get(row.period)! as Record<string, number>)[row.pds_url] = row.count;
  }

  const chartData = [...byPeriod.values()]
    .sort((a, b) => (a.period as string).localeCompare(b.period as string))
    .map(row => {
      const total = pdsKeys.reduce((s, k) => s + ((row[k] as number) ?? 0), 0);
      const out: Record<string, number | string> = { period: row.period };
      for (const k of pdsKeys) {
        const raw = (row[k] as number) ?? 0;
        out[k] = raw;                                                   // raw, for tooltip
        out[`${k}__pct`] = total > 0 ? (raw / total) * 100 : 0;        // normalised, for display
      }
      return out;
    });

  // Tooltip: show raw counts even though display is percentage
  const CustomTooltip = ({ active, payload, label }: any) => {
    if (!active || !payload?.length) return null;
    const total = payload.reduce((s: number, p: any) => s + (p.payload[p.dataKey.replace("__pct", "")] ?? 0), 0);
    return (
      <div style={tooltipStyle.contentStyle}>
        <p className="font-medium mb-1" style={{ color: "#e5e7eb" }}>{label}</p>
        {[...payload].reverse().map((p: any) => {
          const rawKey = p.dataKey.replace("__pct", "");
          const raw = p.payload[rawKey] ?? 0;
          const pct = total > 0 ? ((raw / total) * 100).toFixed(1) : "0.0";
          return (
            <p key={rawKey} style={{ color: p.color, fontSize: "0.8rem" }}>
              {rawKey}: {raw.toLocaleString()} ({pct}%)
            </p>
          );
        })}
        <p style={{ color: "#6b7280", fontSize: "0.75rem", marginTop: 4 }}>
          Total: {total.toLocaleString()}
        </p>
      </div>
    );
  };

  return (
    <ResponsiveContainer width="100%" height={400}>
      <AreaChart data={chartData} margin={{ top: 8, right: 16, bottom: 0, left: 16 }}>
        <XAxis
          dataKey="period"
          tick={{ fill: "#9ca3af", fontSize: 11 }}
          axisLine={{ stroke: "#374151" }}
          tickLine={false}
        />
        <YAxis
          tick={{ fill: "#9ca3af", fontSize: 11 }}
          axisLine={{ stroke: "#374151" }}
          tickLine={false}
          width={40}
          tickFormatter={(v) => `${Math.round(v)}%`}
          domain={[0, 100]}
        />
        <Tooltip content={<CustomTooltip />} />
        <Legend wrapperStyle={{ fontSize: "0.75rem", color: "#9ca3af" }}
          formatter={(value: string) => value.replace("__pct", "")} />
        {pdsKeys.map((key, i) => (
          <Area
            key={key}
            type="monotone"
            dataKey={`${key}__pct`}
            name={key}
            stackId="1"
            stroke={COLORS[i % COLORS.length]}
            fill={COLORS[i % COLORS.length]}
            fillOpacity={0.75}
            strokeWidth={1}
          />
        ))}
      </AreaChart>
    </ResponsiveContainer>
  );
}

const STATUS_COLORS: Record<string, string> = {
  active:      "#10b981",
  deactivated: "#f59e0b",
  takendown:   "#ef4444",
  suspended:   "#8b5cf6",
  deleted:     "#6b7280",
  other:       "#374151",
};

const STATUS_KEYS = ["active", "deactivated", "takendown", "suspended", "deleted", "other"] as const;

interface PdsStatusChartProps {
  data: PdsStatusRow[];
  topN?: number;
}

type StatusChartRow = { name: string; total: number } & Record<typeof STATUS_KEYS[number], number>;

export function PdsStatusChart({ data, topN = 20 }: PdsStatusChartProps) {
  const top = data.slice(0, topN);
  const chartData: StatusChartRow[] = top.map(row => ({
    name: row.pds_url.replace(/^https?:\/\//, ""),
    active: row.active,
    deactivated: row.deactivated,
    deleted: row.deleted,
    takendown: row.takendown,
    suspended: row.suspended,
    other: row.other,
    total: row.total_scanned,
  }));

  const CustomTooltip = ({ active, payload, label }: any) => {
    if (!active || !payload?.length) return null;
    const row = chartData.find(r => r.name === label);
    return (
      <div style={tooltipStyle.contentStyle}>
        <p className="font-medium mb-1" style={{ color: "#e5e7eb" }}>{label}</p>
        {STATUS_KEYS.map(k => {
          const val = row?.[k] ?? 0;
          const pct = row?.total ? ((val / row.total) * 100).toFixed(1) : "0.0";
          return val > 0 ? (
            <p key={k} style={{ color: STATUS_COLORS[k], fontSize: "0.8rem" }}>
              {k}: {val.toLocaleString()} ({pct}%)
            </p>
          ) : null;
        })}
        <p style={{ color: "#6b7280", fontSize: "0.75rem", marginTop: 4 }}>
          Total scanned: {row?.total.toLocaleString()}
        </p>
      </div>
    );
  };

  return (
    <ResponsiveContainer width="100%" height={Math.max(300, top.length * 28)}>
      <BarChart data={chartData} layout="vertical" margin={{ left: 10, right: 16 }}>
        <XAxis
          type="number"
          tick={{ fill: "#9ca3af", fontSize: 11 }}
          axisLine={{ stroke: "#374151" }}
          tickLine={false}
          tickFormatter={(v) => v.toLocaleString()}
        />
        <YAxis
          type="category"
          dataKey="name"
          tick={{ fill: "#9ca3af", fontSize: 11 }}
          axisLine={{ stroke: "#374151" }}
          tickLine={false}
          width={160}
        />
        <Tooltip content={<CustomTooltip />} />
        <Legend wrapperStyle={{ fontSize: "0.75rem", color: "#9ca3af" }} />
        {STATUS_KEYS.map(k => (
          <Bar key={k} dataKey={k} stackId="a" fill={STATUS_COLORS[k]} name={k} />
        ))}
      </BarChart>
    </ResponsiveContainer>
  );
}

interface DonutChartProps {
  data: { name: string; value: number }[];
  maxSlices?: number;
}

export function DonutChart({ data, maxSlices = 10 }: DonutChartProps) {
  let chartData = data;
  if (data.length > maxSlices) {
    const top = data.slice(0, maxSlices - 1);
    const other = data.slice(maxSlices - 1).reduce((s, d) => s + d.value, 0);
    chartData = [...top, { name: "Other", value: other }];
  }

  return (
    <ResponsiveContainer width="100%" height={300}>
      <PieChart>
        <Pie
          data={chartData}
          cx="50%"
          cy="50%"
          innerRadius={60}
          outerRadius={100}
          dataKey="value"
          nameKey="name"
          stroke="#111827"
          strokeWidth={2}
        >
          {chartData.map((_, i) => (
            <Cell key={i} fill={COLORS[i % COLORS.length]} />
          ))}
        </Pie>
        <Tooltip {...tooltipStyle} />
        <Legend
          wrapperStyle={{ fontSize: "0.75rem", color: "#9ca3af" }}
        />
      </PieChart>
    </ResponsiveContainer>
  );
}

// ── Sankey Chart ───────────────────────────────────────────────────────────────

type N = { name: string };
type L = Record<string, never>;
type SankeyNodeDatum = SankeyNode<N, L>;
type SankeyLinkDatum = SankeyLink<N, L>;

interface SankeyTooltip {
  x: number;
  y: number;
  content: React.ReactNode;
}

interface SankeyChartProps {
  data: MigrationFlow[];
  height?: number;
  selectedSink?: string | null;
  onSinkClick?: (sink: string | null) => void;
}

export function SankeyChart({ data, height = 400, selectedSink, onSinkClick }: SankeyChartProps) {
  const [tooltip, setTooltip] = useState<SankeyTooltip | null>(null);
  const [width, setWidth] = useState(900);
  const svgRef = useRef<SVGSVGElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const ro = new ResizeObserver(entries => {
      const w = entries[0]?.contentRect.width;
      if (w && w > 0) setWidth(w);
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  if (!data.length) return <p className="text-gray-500">No migration data yet.</p>;

  // Treat sources and targets as distinct nodes to avoid cycles (same PDS can be
  // both a source and destination). Suffix internally; strip for display.
  const SRC_SUFFIX = "\u200b";  // zero-width space
  const srcNames  = [...new Set(data.map(d => d.source + SRC_SUFFIX))];
  const tgtNames  = [...new Set(data.map(d => d.target))];
  const nodeNames = [...srcNames, ...tgtNames];
  const nodeIndex = new Map(nodeNames.map((name, i) => [name, i]));

  const margin = { top: 10, right: 160, bottom: 10, left: 10 };
  const innerW = width - margin.left - margin.right;
  const innerH = height - margin.top - margin.bottom;

  const sankeyLayout = sankey<N, L>()
    .nodeAlign(sankeyJustify)
    .nodeWidth(14)
    .nodePadding(12)
    .extent([[0, 0], [innerW, innerH]]);

  const { nodes, links } = sankeyLayout({
    nodes: nodeNames.map(name => ({ name })),
    links: data.map(d => ({
      source: nodeIndex.get(d.source + SRC_SUFFIX)!,
      target: nodeIndex.get(d.target)!,
      value: d.value,
    })) as any,
  });

  const totalFlow = (nodes as SankeyNodeDatum[]).reduce((s, n) => s + (n.value ?? 0), 0) / 2;

  const linkPath = sankeyLinkHorizontal();

  const handleNodeHover = (e: React.MouseEvent, node: SankeyNodeDatum) => {
    const rect = svgRef.current?.getBoundingClientRect();
    if (!rect) return;
    const inflow  = (node.targetLinks as SankeyLinkDatum[]).reduce((s, l) => s + l.value, 0);
    const outflow = (node.sourceLinks as SankeyLinkDatum[]).reduce((s, l) => s + l.value, 0);
    setTooltip({
      x: e.clientX - rect.left,
      y: e.clientY - rect.top,
      content: (
        <div>
          <p className="font-medium mb-1" style={{ color: "#e5e7eb" }}>{node.name.replace(/\u200b$/, "")}</p>
          {inflow  > 0 && <p style={{ color: "#10b981", fontSize: "0.8rem" }}>Inbound: {inflow.toLocaleString()}</p>}
          {outflow > 0 && <p style={{ color: "#f59e0b", fontSize: "0.8rem" }}>Outbound: {outflow.toLocaleString()}</p>}
        </div>
      ),
    });
  };

  const handleLinkHover = (e: React.MouseEvent, link: SankeyLinkDatum) => {
    const rect = svgRef.current?.getBoundingClientRect();
    if (!rect) return;
    const src = (link.source as SankeyNodeDatum);
    const tgt = (link.target as SankeyNodeDatum);
    const srcTotal = (src.sourceLinks as SankeyLinkDatum[]).reduce((s, l) => s + l.value, 0);
    const pct = srcTotal > 0 ? ((link.value / srcTotal) * 100).toFixed(1) : "0.0";
    setTooltip({
      x: e.clientX - rect.left,
      y: e.clientY - rect.top,
      content: (
        <div>
          <p className="font-medium mb-1" style={{ color: "#e5e7eb" }}>
            {src.name.replace(/\u200b$/, "")} → {tgt.name}
          </p>
          <p style={{ color: "#f3f4f6", fontSize: "0.8rem" }}>
            {link.value.toLocaleString()} migrations ({pct}% of source outflow)
          </p>
        </div>
      ),
    });
  };

  return (
    <div ref={containerRef} className="relative" style={{ width: "100%" }}>
      <svg
        ref={svgRef}
        width={width}
        height={height}
        onMouseLeave={() => setTooltip(null)}
      >
        <g transform={`translate(${margin.left},${margin.top})`}>
          {/* Links */}
          {(links as SankeyLinkDatum[]).map((link, i) => {
            const srcIdx = nodeIndex.get((link.source as SankeyNodeDatum).name) ?? 0;
            const color = COLORS[srcIdx % COLORS.length];
            const tgtName = (link.target as SankeyNodeDatum).name;
            const isHighlighted = !selectedSink || tgtName === selectedSink;
            return (
              <path
                key={i}
                d={linkPath(link as any) ?? ""}
                fill="none"
                stroke={color}
                strokeOpacity={isHighlighted ? 0.5 : 0.1}
                strokeWidth={Math.max(1, link.width ?? 1)}
                onMouseEnter={(e) => handleLinkHover(e, link)}
                onMouseMove={(e) => handleLinkHover(e, link)}
                style={{ cursor: "default" }}
              />
            );
          })}

          {/* Nodes */}
          {(nodes as SankeyNodeDatum[]).map((node, i) => {
            const color = COLORS[i % COLORS.length];
            const x0 = node.x0 ?? 0, x1 = node.x1 ?? 0;
            const y0 = node.y0 ?? 0, y1 = node.y1 ?? 0;
            const labelRight = x1 > innerW / 2;
            const isTarget = !node.name.endsWith(SRC_SUFFIX);
            const isSelected = selectedSink && node.name === selectedSink;
            const isDimmed = selectedSink && isTarget && node.name !== selectedSink;
            return (
              <g
                key={i}
                onMouseEnter={(e) => handleNodeHover(e, node)}
                onMouseMove={(e) => handleNodeHover(e, node)}
                onClick={() => isTarget && onSinkClick?.(isSelected ? null : node.name)}
                style={{ cursor: isTarget ? "pointer" : "default", opacity: isDimmed ? 0.3 : 1 }}
              >
                <rect
                  x={x0} y={y0}
                  width={x1 - x0} height={Math.max(1, y1 - y0)}
                  fill={color}
                  rx={2}
                  stroke={isSelected ? "#fff" : "none"}
                  strokeWidth={isSelected ? 1.5 : 0}
                />
                <text
                  x={labelRight ? x1 + 6 : x0 - 6}
                  y={(y0 + y1) / 2}
                  textAnchor={labelRight ? "start" : "end"}
                  dominantBaseline="middle"
                  fill={isDimmed ? "#6b7280" : "#d1d5db"}
                  fontSize={11}
                >
                  {node.name.replace(/\u200b$/, "").replace(/^https?:\/\//, "")}
                </text>
              </g>
            );
          })}
        </g>
      </svg>

      {/* Tooltip */}
      {tooltip && (
        <div
          style={{
            ...tooltipStyle.contentStyle,
            position: "absolute",
            left: tooltip.x + 12,
            top: tooltip.y - 12,
            pointerEvents: "none",
            whiteSpace: "nowrap",
          }}
        >
          {tooltip.content}
        </div>
      )}

      <p style={{ color: "#6b7280", fontSize: "0.7rem", marginTop: 4 }}>
        Total recorded: {totalFlow.toLocaleString()} migrations · Top 10 sources &amp; destinations shown
        {selectedSink && (
          <> · <span style={{ color: "#d1d5db" }}>
            Click highlighted node to deselect
          </span></>
        )}
      </p>
    </div>
  );
}

// ── Migration weekly bar chart ────────────────────────────────────────────────

interface MigrationWeeklyBarChartProps {
  data: WeeklyMigrationRow[];
  selectedSink: string | null;
}

function MigrationWeeklyBarChart({ data, selectedSink }: MigrationWeeklyBarChartProps) {
  const weeks = [...new Set(data.map(r => r.week))].sort();

  const chartData = weeks.map(week => {
    const rows = data.filter(r => r.week === week);
    const total = rows.reduce((s, r) => s + r.count, 0);
    const selected = selectedSink
      ? rows.filter(r => r.to_pds === selectedSink).reduce((s, r) => s + r.count, 0)
      : 0;
    return { week, total, selected, other: total - selected };
  });

  const CustomTooltip = ({ active, payload, label }: any) => {
    if (!active || !payload?.length) return null;
    const total = payload.find((p: any) => p.dataKey === "total" || p.dataKey === "other" || p.dataKey === "selected");
    const realTotal = chartData.find(d => d.week === label)?.total ?? 0;
    return (
      <div style={{ ...tooltipStyle.contentStyle, whiteSpace: "nowrap" }}>
        <p style={{ color: "#9ca3af", fontSize: "0.75rem", marginBottom: 4 }}>{label}</p>
        <p style={{ color: "#e5e7eb", fontSize: "0.85rem" }}>{realTotal.toLocaleString()} migrations</p>
        {selectedSink && (
          <p style={{ color: COLORS[0], fontSize: "0.8rem" }}>
            {selectedSink.replace(/^https?:\/\//, "")}: {chartData.find(d => d.week === label)?.selected.toLocaleString() ?? 0}
          </p>
        )}
      </div>
    );
  };

  return (
    <ResponsiveContainer width="100%" height={180}>
      <BarChart data={chartData} margin={{ top: 4, right: 16, bottom: 0, left: 16 }} barCategoryGap="10%">
        <XAxis
          dataKey="week"
          tick={{ fill: "#6b7280", fontSize: 10 }}
          tickLine={false}
          axisLine={{ stroke: "#374151" }}
          tickFormatter={(v: string) => v.slice(0, 7)}
          interval="preserveStartEnd"
        />
        <YAxis
          tick={{ fill: "#6b7280", fontSize: 10 }}
          tickLine={false}
          axisLine={false}
          width={36}
        />
        <Tooltip content={<CustomTooltip />} cursor={{ fill: "rgba(255,255,255,0.04)" }} />
        {selectedSink ? (
          <>
            <Bar dataKey="other" stackId="a" fill="#374151" isAnimationActive={false} />
            <Bar dataKey="selected" stackId="a" fill={COLORS[0]} isAnimationActive={false} />
          </>
        ) : (
          <Bar dataKey="total" fill="#4b5563" isAnimationActive={false} />
        )}
      </BarChart>
    </ResponsiveContainer>
  );
}

// ── Migration charts section (Sankey + weekly bar, shared selection state) ───

interface MigrationChartsSectionProps {
  sankeyData: MigrationFlow[];
  weeklyData: WeeklyMigrationRow[];
}

export function MigrationChartsSection({ sankeyData, weeklyData }: MigrationChartsSectionProps) {
  const [selectedSink, setSelectedSink] = useState<string | null>(null);

  return (
    <div className="space-y-6">
      <SankeyChart
        data={sankeyData}
        selectedSink={selectedSink}
        onSinkClick={setSelectedSink}
      />
      <div>
        <p className="text-xs text-gray-500 mb-2">
          Weekly migrations — last 18 months
          {selectedSink && (
            <span className="ml-2 text-blue-400">
              Highlighting: {selectedSink.replace(/^https?:\/\//, "")}
            </span>
          )}
        </p>
        <MigrationWeeklyBarChart data={weeklyData} selectedSink={selectedSink} />
      </div>
    </div>
  );
}
