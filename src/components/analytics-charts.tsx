"use client";

import {
  ResponsiveContainer,
  ComposedChart,
  AreaChart,
  Area,
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
} from "recharts";
import type { WauByCohortRow, DauMauRow, RetentionCohortRow } from "@/lib/db/analytics-queries";

const COLORS = [
  "#3b82f6", "#8b5cf6", "#06b6d4", "#10b981", "#f59e0b",
  "#ef4444", "#ec4899", "#6366f1", "#14b8a6", "#f97316",
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

const axisProps = {
  tick: { fill: "#9ca3af", fontSize: 11 },
  axisLine: { stroke: "#374151" },
  tickLine: false,
};

function fmtCompact(v: number): string {
  if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
  if (v >= 1_000) return `${(v / 1_000).toFixed(0)}K`;
  return `${v}`;
}

// ── WAU by age cohort — stacked area, one series per age_bucket ─────────────

export function WauByCohortChart({ data }: { data: WauByCohortRow[] }) {
  const bucketSet = new Set<string>();
  for (const row of data) bucketSet.add(row.age_bucket);
  const buckets = [...bucketSet].sort();

  const byWeek = new Map<string, Record<string, number | string>>();
  for (const row of data) {
    if (!byWeek.has(row.week_start)) byWeek.set(row.week_start, { week_start: row.week_start });
    (byWeek.get(row.week_start)! as Record<string, number>)[row.age_bucket] = row.wau;
  }
  const chartData = [...byWeek.values()].sort((a, b) => (a.week_start as string).localeCompare(b.week_start as string));

  return (
    <ResponsiveContainer width="100%" height={360}>
      <AreaChart data={chartData} margin={{ top: 8, right: 16, bottom: 0, left: 0 }}>
        <XAxis dataKey="week_start" {...axisProps} interval="preserveStartEnd" />
        <YAxis {...axisProps} width={48} tickFormatter={fmtCompact} />
        <Tooltip {...tooltipStyle} />
        <Legend wrapperStyle={{ fontSize: "0.7rem", color: "#9ca3af" }} />
        {buckets.map((bucket, i) => (
          <Area
            key={bucket}
            type="monotone"
            dataKey={bucket}
            name={bucket}
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

// ── DAU / MAU trend — dual line, DAU/MAU % on a secondary axis ──────────────

export function DauMauChart({ data }: { data: DauMauRow[] }) {
  return (
    <ResponsiveContainer width="100%" height={320}>
      <ComposedChart data={data} margin={{ top: 8, right: 16, bottom: 0, left: 0 }}>
        <XAxis dataKey="date" {...axisProps} interval="preserveStartEnd" />
        <YAxis yAxisId="left" {...axisProps} width={48} tickFormatter={fmtCompact} />
        <YAxis
          yAxisId="right"
          orientation="right"
          tick={{ fill: "#6b7280", fontSize: 10 }}
          axisLine={false}
          tickLine={false}
          width={40}
          tickFormatter={(v: number) => `${Math.round(v)}%`}
          domain={[0, 100]}
        />
        <Tooltip {...tooltipStyle} />
        <Legend wrapperStyle={{ fontSize: "0.7rem", color: "#9ca3af" }} />
        <Line yAxisId="left" type="monotone" dataKey="dau" name="DAU" stroke="#3b82f6" strokeWidth={1.5} dot={false} />
        <Line yAxisId="left" type="monotone" dataKey="mau" name="MAU (trailing 28d)" stroke="#8b5cf6" strokeWidth={1.5} dot={false} />
        <Line yAxisId="right" type="monotone" dataKey="dau_mau_pct" name="DAU/MAU %" stroke="#10b981" strokeWidth={1} dot={false} strokeDasharray="4 3" />
      </ComposedChart>
    </ResponsiveContainer>
  );
}

// ── D1/D3/D7 retention — network-wide ("~ total") trend by creation cohort ──

export function RetentionChart({ data }: { data: RetentionCohortRow[] }) {
  const totals = data.filter(r => r.pds_type === "~ total");
  return (
    <ResponsiveContainer width="100%" height={320}>
      <LineChart data={totals} margin={{ top: 8, right: 16, bottom: 0, left: 0 }}>
        <XAxis dataKey="creation_date" {...axisProps} interval="preserveStartEnd" />
        <YAxis {...axisProps} width={40} tickFormatter={(v: number) => `${v}%`} domain={[0, "auto"]} />
        <Tooltip {...tooltipStyle} />
        <Legend wrapperStyle={{ fontSize: "0.7rem", color: "#9ca3af" }} />
        <Line type="monotone" dataKey="d1_pct" name="D1 %" stroke="#3b82f6" strokeWidth={1.5} dot={false} />
        <Line type="monotone" dataKey="d3_pct" name="D3 %" stroke="#f59e0b" strokeWidth={1.5} dot={false} />
        <Line type="monotone" dataKey="d7_pct" name="D7 %" stroke="#ef4444" strokeWidth={1.5} dot={false} />
      </LineChart>
    </ResponsiveContainer>
  );
}
