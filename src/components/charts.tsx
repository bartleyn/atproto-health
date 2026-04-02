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
} from "recharts";

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
