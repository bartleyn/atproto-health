"use client";

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
