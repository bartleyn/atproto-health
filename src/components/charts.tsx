"use client";

import { sankey, sankeyLinkHorizontal, sankeyJustify, sankeyLeft } from "d3-sankey";
import type { SankeyNode, SankeyLink } from "d3-sankey";
import { useState, useRef, useEffect } from "react";
import type { MigrationFlow, WeeklyMigrationRow, TimeseriesRow, TrajectoryEdge, PdsAgeRow, LangTotal } from "@/lib/db/plc-queries";
import type { CityCluster, PdsProviderLocation, HostingProviderCount } from "@/lib/db/queries";
import { WorldMap, type PdsLangLocation } from "@/components/world-map";

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
  ComposedChart,
  Area,
  Line,
  ScatterChart,
  Scatter,
  CartesianGrid,
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
  logScale?: boolean;
}

export function SimpleBarChart({
  data,
  color = "#3b82f6",
  layout = "vertical",
  xLabel,
  yLabel,
  logScale = false,
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

  const fmtLog = (v: number) => {
    if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(0)}M`;
    if (v >= 1_000) return `${(v / 1_000).toFixed(0)}K`;
    return `${v}`;
  };

  return (
    <ResponsiveContainer width="100%" height={Math.max(300, data.length * 28)}>
      <BarChart data={data} layout="vertical" margin={{ left: 10 }}>
        <XAxis
          type="number"
          scale={logScale ? "log" : "auto"}
          domain={logScale ? [1, "auto"] : [0, "auto"]}
          tick={{ fill: "#9ca3af", fontSize: 12 }}
          axisLine={{ stroke: "#374151" }}
          tickLine={false}
          tickFormatter={logScale ? fmtLog : undefined}
          allowDataOverflow={logScale}
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

// Strip protocol and collapse subdomains: "dev.blacksky.app" → "blacksky.app"
function displayPdsLabel(url: string): string {
  const host = url.replace(/^https?:\/\//, "");
  const parts = host.split(".");
  return parts.length > 2 ? parts.slice(-2).join(".") : host;
}

interface StackedAreaChartProps {
  data: { period: string; pds_url: string; count: number }[];
  allPeriods?: string[]; // force x-axis to cover this full range
  selectedPds?: string | null;
  onPdsClick?: (pds: string | null) => void;
}

export function StackedAreaChart({ data, allPeriods, selectedPds, onPdsClick }: StackedAreaChartProps) {
  const [containerWidth, setContainerWidth] = useState(900);
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const ro = new ResizeObserver(entries => {
      const w = entries[0]?.contentRect.width;
      if (w && w > 0) setContainerWidth(w);
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const isMobile = containerWidth < 500;
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

  let runningCumulative = 0;
  const chartData = [...byPeriod.values()]
    .sort((a, b) => (a.period as string).localeCompare(b.period as string))
    .map(row => {
      const total = pdsKeys.reduce((s, k) => s + ((row[k] as number) ?? 0), 0);
      runningCumulative += total;
      const out: Record<string, number | string> = { period: row.period, __cumulative: runningCumulative };
      for (const k of pdsKeys) {
        const raw = (row[k] as number) ?? 0;
        out[k] = raw;                                                   // raw, for tooltip
        out[`${k}__pct`] = total > 0 ? (raw / total) * 100 : 0;        // normalised, for display
      }
      return out;
    });

  const fmtCumulative = (v: number) => {
    if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
    if (v >= 1_000) return `${(v / 1_000).toFixed(0)}K`;
    return `${v}`;
  };

  // Tooltip: show raw counts even though display is percentage
  const CustomTooltip = ({ active, payload, label }: any) => {
    if (!active || !payload?.length) return null;
    const areaPayload = payload.filter((p: any) => p.dataKey !== "__cumulative");
    const total = areaPayload.reduce((s: number, p: any) => s + (p.payload[p.dataKey.replace("__pct", "")] ?? 0), 0);
    const cumulative = payload[0]?.payload.__cumulative ?? 0;
    return (
      <div style={tooltipStyle.contentStyle}>
        <p className="font-medium mb-1" style={{ color: "#e5e7eb" }}>{label}</p>
        {[...areaPayload].reverse().map((p: any) => {
          const rawKey = p.dataKey.replace("__pct", "");
          const raw = p.payload[rawKey] ?? 0;
          const pct = total > 0 ? ((raw / total) * 100).toFixed(1) : "0.0";
          return (
            <p key={rawKey} style={{ color: p.color, fontSize: "0.8rem" }}>
              {displayPdsLabel(rawKey)}: {raw.toLocaleString()} ({pct}%)
            </p>
          );
        })}
        <p style={{ color: "#6b7280", fontSize: "0.75rem", marginTop: 4 }}>
          Weekly total: {total.toLocaleString()}
        </p>
        <p style={{ color: "#e5e7eb", fontSize: "0.75rem" }}>
          Cumulative: {cumulative.toLocaleString()}
        </p>
      </div>
    );
  };

  return (
    <div ref={containerRef} style={{ width: "100%" }}>
    <ResponsiveContainer width="100%" height={400}>
      <ComposedChart data={chartData} margin={{ top: 8, right: isMobile ? 36 : 56, bottom: 0, left: isMobile ? 4 : 16 }}>
        <XAxis
          dataKey="period"
          tick={{ fill: "#9ca3af", fontSize: isMobile ? 9 : 11 }}
          axisLine={{ stroke: "#374151" }}
          tickLine={false}
          interval="preserveStartEnd"
        />
        <YAxis
          yAxisId="left"
          tick={{ fill: "#9ca3af", fontSize: isMobile ? 9 : 11 }}
          axisLine={{ stroke: "#374151" }}
          tickLine={false}
          width={isMobile ? 32 : 40}
          tickFormatter={(v) => `${Math.round(v)}%`}
          domain={[0, 100]}
        />
        <YAxis
          yAxisId="right"
          orientation="right"
          tick={{ fill: "#6b7280", fontSize: isMobile ? 9 : 10 }}
          axisLine={false}
          tickLine={false}
          width={isMobile ? 36 : 52}
          tickFormatter={fmtCumulative}
        />
        <Tooltip content={<CustomTooltip />} />
        <Legend
          wrapperStyle={{ fontSize: isMobile ? "0.65rem" : "0.75rem", color: "#9ca3af", cursor: onPdsClick ? "pointer" : "default" }}
          formatter={(value: string) => value === "__cumulative" ? "cumulative" : displayPdsLabel(value.replace("__pct", ""))}
          onClick={onPdsClick ? (e: any) => {
            const name = e.value?.replace("__pct", "") ?? e.value;
            if (name === "__cumulative") return;
            onPdsClick(selectedPds === name ? null : name);
          } : undefined}
        />
        {pdsKeys.map((key, i) => {
          const color = COLORS[i % COLORS.length];
          const dimmed = selectedPds && selectedPds !== key;
          return (
            <Area
              key={key}
              yAxisId="left"
              type="monotone"
              dataKey={`${key}__pct`}
              name={key}
              stackId="1"
              stroke={dimmed ? "#374151" : color}
              fill={dimmed ? "#374151" : color}
              fillOpacity={dimmed ? 0.25 : 0.75}
              strokeWidth={1}
              style={{ cursor: onPdsClick ? "pointer" : "default" }}
              onClick={onPdsClick ? () => onPdsClick(selectedPds === key ? null : key) : undefined}
            />
          );
        })}
        <Line
          yAxisId="right"
          type="monotone"
          dataKey="__cumulative"
          name="__cumulative"
          stroke="#ffffff"
          strokeWidth={1.5}
          dot={false}
          strokeOpacity={0.5}
          legendType="none"
        />
      </ComposedChart>
    </ResponsiveContainer>
    </div>
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
  selectedName?: string | null;
  onSliceClick?: (name: string | null) => void;
}

export function DonutChart({ data, maxSlices = 10, selectedName, onSliceClick }: DonutChartProps) {
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
          style={onSliceClick ? { cursor: "pointer" } : undefined}
          onClick={onSliceClick ? (entry) => {
            const name = (entry.name as string | null | undefined) ?? null;
            onSliceClick(selectedName === name ? null : name);
          } : undefined}
        >
          {chartData.map((entry, i) => {
            const dimmed = selectedName && selectedName !== entry.name;
            return (
              <Cell
                key={i}
                fill={dimmed ? "#374151" : COLORS[i % COLORS.length]}
                fillOpacity={dimmed ? 0.4 : 1}
                stroke={selectedName === entry.name ? "#fff" : "#111827"}
                strokeWidth={selectedName === entry.name ? 2 : 2}
              />
            );
          })}
        </Pie>
        <Tooltip {...tooltipStyle} />
        <Legend
          wrapperStyle={{ fontSize: "0.75rem", color: "#9ca3af", cursor: onSliceClick ? "pointer" : "default" }}
          onClick={onSliceClick ? (e: any) => {
            onSliceClick(selectedName === e.value ? null : e.value);
          } : undefined}
        />
      </PieChart>
    </ResponsiveContainer>
  );
}

// ── Infrastructure + Map Section ───────────────────────────────────────────────

interface InfraSectionProps {
  providers: HostingProviderCount[];
  cdnBreakdown: { behindCdn: number; directHosting: number; unknown: number };
  locations: CityCluster[];
  providerLocations: PdsProviderLocation[];
  langLocations?: PdsLangLocation[];
  topLangs?: LangTotal[];
}

export function InfraSection({ providers, cdnBreakdown, locations, providerLocations, langLocations, topLangs }: InfraSectionProps) {
  const [selectedProvider, setSelectedProvider] = useState<string | null>(null);
  const [selectedLang, setSelectedLang] = useState<string | null>(null);
  const [insetTab, setInsetTab] = useState<"provider" | "lang">("provider");
  const [showBskyLang, setShowBskyLang] = useState(false);

  const donutData = providers
    .filter((p) => !p.isCdn)
    .slice(0, 11)
    .map((p) => ({ name: p.provider, value: p.count }));

  const mappedCount = selectedProvider
    ? providerLocations.filter(p => p.provider === selectedProvider).length
    : 0;
  const totalCount = selectedProvider
    ? (providers.find(p => p.provider === selectedProvider)?.count ?? 0)
    : 0;

  const isBskyUrl = (u: string) => /bsky\.network|bsky\.social/.test(u);

  // Filter lang data based on bsky toggle
  const activeLangLocations = langLocations
    ? showBskyLang ? langLocations : langLocations.filter(l => !isBskyUrl(l.url))
    : undefined;
  // Derive per-language totals from the active (filtered) langLocations for the donut.
  // This correctly excludes bsky counts when the toggle is off.
  const langTotalsFromLocs = new Map<string, number>();
  if (activeLangLocations) {
    for (const l of activeLangLocations) {
      langTotalsFromLocs.set(l.lang, (langTotalsFromLocs.get(l.lang) ?? 0) + l.dids);
    }
  }
  // Merge with topLangs to preserve ordering and include langs not in locales.
  // For bsky rows in topLangs, override total_dids with filtered value.
  const filteredTopLangs = topLangs
    ? topLangs
        .map(r => ({
          ...r,
          total_dids: showBskyLang
            ? r.total_dids
            : (langTotalsFromLocs.get(r.lang) ?? 0),
        }))
        .filter(r => r.total_dids > 0)
        .sort((a, b) => b.total_dids - a.total_dids)
    : undefined;

  const langMappedCount = selectedLang && activeLangLocations
    ? new Set(activeLangLocations.filter(l => l.lang === selectedLang).map(l => l.url)).size
    : 0;

  const hasLangData = filteredTopLangs && filteredTopLangs.length > 0;

  function pickProvider(name: string | null) {
    setSelectedProvider(prev => prev === name ? null : name);
    setSelectedLang(null);
    setInsetTab("provider");
  }

  function pickLang(lang: string | null) {
    setSelectedLang(prev => prev === lang ? null : lang);
    setSelectedProvider(null);
    setInsetTab("lang");
  }

  const clearLabel = selectedProvider
    ? `${mappedCount} of ${totalCount} ${selectedProvider} PDSes mapped · clear`
    : selectedLang
    ? `${langMappedCount} PDSes with "${selectedLang}" speakers · clear`
    : null;

  const subLabel = selectedProvider
    ? `amber = cities with ${selectedProvider} PDSes`
    : selectedLang
    ? `amber = cities with "${selectedLang}" speakers`
    : hasLangData
    ? "click a provider or language to highlight on map"
    : "click a provider to highlight on map";

  return (
    <div className="rounded-lg border border-gray-800 bg-gray-900 p-5">
      {/* Header */}
      <div className="flex items-baseline justify-between mb-0.5">
        <h2 className="text-base font-semibold">PDS Geographic Distribution</h2>
        {clearLabel && (
          <button
            onClick={() => { setSelectedProvider(null); setSelectedLang(null); }}
            className="text-xs text-blue-400 hover:text-blue-300 transition-colors"
          >
            {clearLabel}
          </button>
        )}
      </div>
      <p className="text-xs text-gray-500 mb-3">
        {locations.length.toLocaleString()} cities · dot size scales with PDS count · {subLabel}
      </p>

      {/* Map with inset panel (desktop) */}
      <div className="relative">
        <WorldMap
          locations={locations}
          providerLocations={providerLocations}
          selectedProvider={selectedProvider}
          langLocations={activeLangLocations}
          selectedLang={selectedLang}
        />

        {/* Inset panel — visible on md+ only; hidden on mobile */}
        <div className="hidden md:block absolute bottom-3 left-3 w-52 bg-gray-950/90 border border-gray-700 rounded-lg p-3">
          {/* Tab toggle */}
          {hasLangData && (
            <div className="flex gap-1 mb-2">
              <button
                onClick={() => setInsetTab("provider")}
                className={`flex-1 text-xs py-0.5 rounded transition-colors ${insetTab === "provider" ? "bg-gray-700 text-white" : "text-gray-500 hover:text-gray-300"}`}
              >
                Providers
              </button>
              <button
                onClick={() => setInsetTab("lang")}
                className={`flex-1 text-xs py-0.5 rounded transition-colors ${insetTab === "lang" ? "bg-gray-700 text-white" : "text-gray-500 hover:text-gray-300"}`}
              >
                Languages
              </button>
            </div>
          )}

          {insetTab === "provider" && (
            <>
              <p className="text-xs font-medium text-gray-300 mb-0.5">Infrastructure Providers</p>
              <p className="text-xs text-gray-600 mb-2">
                {cdnBreakdown.behindCdn} behind CDN · click to highlight
              </p>
              {/* Fixed-size pie for the inset (no ResponsiveContainer needed) */}
              <div className="flex justify-center">
                <PieChart width={176} height={140}>
                  <Pie
                    data={donutData}
                    cx="50%"
                    cy="50%"
                    innerRadius={34}
                    outerRadius={58}
                    dataKey="value"
                    nameKey="name"
                    stroke="#0a0f1a"
                    strokeWidth={2}
                    style={{ cursor: "pointer" }}
                    onClick={(entry) => pickProvider((entry.name as string) ?? null)}
                  >
                    {donutData.map((entry, i) => {
                      const dimmed = selectedProvider && selectedProvider !== entry.name;
                      return (
                        <Cell
                          key={i}
                          fill={dimmed ? "#374151" : COLORS[i % COLORS.length]}
                          fillOpacity={dimmed ? 0.4 : 1}
                          stroke={selectedProvider === entry.name ? "#fff" : "#0a0f1a"}
                          strokeWidth={2}
                        />
                      );
                    })}
                  </Pie>
                  <Tooltip {...tooltipStyle} />
                </PieChart>
              </div>
              {/* Compact scrollable legend */}
              <div className="space-y-px max-h-28 overflow-y-auto mt-1">
                {donutData.map((entry, i) => (
                  <button
                    key={entry.name}
                    className="flex items-center gap-1.5 w-full text-left px-1 py-px rounded hover:bg-gray-800/60 transition-colors"
                    onClick={() => pickProvider(entry.name)}
                  >
                    <span
                      className="w-2 h-2 rounded-full flex-shrink-0"
                      style={{
                        backgroundColor: selectedProvider && selectedProvider !== entry.name
                          ? "#374151"
                          : COLORS[i % COLORS.length],
                      }}
                    />
                    <span className={`text-xs truncate ${selectedProvider === entry.name ? "text-white font-medium" : "text-gray-400"}`}>
                      {entry.name}
                    </span>
                    <span className="text-xs text-gray-600 ml-auto flex-shrink-0 pl-1">{entry.value}</span>
                  </button>
                ))}
              </div>
            </>
          )}

          {insetTab === "lang" && hasLangData && (
            <>
              <div className="flex items-center justify-between mb-0.5">
                <p className="text-xs font-medium text-gray-300">Languages</p>
                <button
                  onClick={() => setShowBskyLang(v => !v)}
                  className={`text-xs px-1.5 py-0.5 rounded transition-colors ${showBskyLang ? "bg-blue-900/60 text-blue-300" : "text-gray-600 hover:text-gray-400"}`}
                >
                  {showBskyLang ? "bsky on" : "bsky off"}
                </button>
              </div>
              <p className="text-xs text-gray-600 mb-2"># = active speakers</p>
              {(() => {
                const langDonutData = filteredTopLangs!.slice(0, 12).map(r => ({ name: r.lang, value: r.total_dids }));
                return (
                  <>
                    <div className="flex justify-center">
                      <PieChart width={176} height={140}>
                        <Pie
                          data={langDonutData}
                          cx="50%"
                          cy="50%"
                          innerRadius={34}
                          outerRadius={58}
                          dataKey="value"
                          nameKey="name"
                          stroke="#0a0f1a"
                          strokeWidth={2}
                          style={{ cursor: "pointer" }}
                          onClick={(entry) => pickLang((entry.name as string) ?? null)}
                        >
                          {langDonutData.map((entry, i) => {
                            const dimmed = selectedLang && selectedLang !== entry.name;
                            return (
                              <Cell
                                key={i}
                                fill={dimmed ? "#374151" : COLORS[i % COLORS.length]}
                                fillOpacity={dimmed ? 0.4 : 1}
                                stroke={selectedLang === entry.name ? "#fff" : "#0a0f1a"}
                                strokeWidth={2}
                              />
                            );
                          })}
                        </Pie>
                        <Tooltip {...tooltipStyle} />
                      </PieChart>
                    </div>
                    <div className="space-y-px max-h-28 overflow-y-auto mt-1">
                      {filteredTopLangs!.map((row, i) => (
                        <button
                          key={row.lang}
                          className="flex items-center gap-1.5 w-full text-left px-1 py-px rounded hover:bg-gray-800/60 transition-colors"
                          onClick={() => pickLang(row.lang)}
                        >
                          <span
                            className="w-2 h-2 rounded-full flex-shrink-0"
                            style={{
                              backgroundColor: selectedLang && selectedLang !== row.lang
                                ? "#374151"
                                : i < 12 ? COLORS[i % COLORS.length] : "#6366f1",
                            }}
                          />
                          <span className={`text-xs font-mono truncate ${selectedLang === row.lang ? "text-white font-medium" : "text-gray-400"}`}>
                            {row.lang}
                          </span>
                          <span className="text-xs text-gray-600 ml-auto flex-shrink-0 pl-1">{row.total_dids.toLocaleString()}</span>
                        </button>
                      ))}
                    </div>
                  </>
                );
              })()}
            </>
          )}
        </div>
      </div>

      {/* Provider/Language filter — mobile only (stacked below map) */}
      <div className="block md:hidden mt-4 pt-4 border-t border-gray-800">
        {hasLangData && (
          <div className="flex gap-1 mb-3">
            <button
              onClick={() => setInsetTab("provider")}
              className={`flex-1 text-xs py-1 rounded transition-colors ${insetTab === "provider" ? "bg-gray-700 text-white" : "text-gray-500 hover:text-gray-300"}`}
            >
              Providers
            </button>
            <button
              onClick={() => setInsetTab("lang")}
              className={`flex-1 text-xs py-1 rounded transition-colors ${insetTab === "lang" ? "bg-gray-700 text-white" : "text-gray-500 hover:text-gray-300"}`}
            >
              Languages
            </button>
          </div>
        )}

        {insetTab === "provider" && (
          <>
            <p className="text-xs font-medium text-gray-300 mb-0.5">Infrastructure Providers</p>
            <p className="text-xs text-gray-500 mb-3">
              {`${cdnBreakdown.behindCdn} behind CDN · ${cdnBreakdown.directHosting} direct · ${cdnBreakdown.unknown} unknown · click to highlight`}
            </p>
            <DonutChart
              data={donutData}
              maxSlices={donutData.length}
              selectedName={selectedProvider}
              onSliceClick={pickProvider}
            />
          </>
        )}

        {insetTab === "lang" && hasLangData && (
          <>
            <div className="flex items-center justify-between mb-1">
              <p className="text-xs font-medium text-gray-300">Languages</p>
              <button
                onClick={() => setShowBskyLang(v => !v)}
                className={`text-xs px-1.5 py-0.5 rounded transition-colors ${showBskyLang ? "bg-blue-900/60 text-blue-300" : "text-gray-600 hover:text-gray-400"}`}
              >
                {showBskyLang ? "bsky on" : "bsky off"}
              </button>
            </div>
            <p className="text-xs text-gray-500 mb-3"># = active speakers · click to highlight</p>
            <DonutChart
              data={filteredTopLangs!.slice(0, 12).map(r => ({ name: r.lang, value: r.total_dids }))}
              maxSlices={12}
              selectedName={selectedLang}
              onSliceClick={pickLang}
            />
            <div className="grid grid-cols-3 gap-1 mt-3">
              {filteredTopLangs!.map((row, i) => (
                <button
                  key={row.lang}
                  className="flex items-center gap-1.5 text-left px-2 py-1 rounded hover:bg-gray-800/60 transition-colors"
                  onClick={() => pickLang(row.lang)}
                >
                  <span
                    className="w-2 h-2 rounded-full flex-shrink-0"
                    style={{
                      backgroundColor: selectedLang && selectedLang !== row.lang
                        ? "#374151"
                        : i < 12 ? COLORS[i % COLORS.length] : "#6366f1",
                    }}
                  />
                  <span className={`text-xs font-mono truncate ${selectedLang === row.lang ? "text-white font-medium" : "text-gray-400"}`}>
                    {row.lang}
                  </span>
                  <span className="text-xs text-gray-600 ml-auto flex-shrink-0">{row.total_dids.toLocaleString()}</span>
                </button>
              ))}
            </div>
          </>
        )}
      </div>
    </div>
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
  const [mounted, setMounted] = useState(false);
  const svgRef = useRef<SVGSVGElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => setMounted(true), []);

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
  if (!mounted) return <div style={{ height }} />;

  // Treat sources and targets as distinct nodes to avoid cycles (same PDS can be
  // both a source and destination). Suffix internally; strip for display.
  const SRC_SUFFIX = "\u200b";  // zero-width space
  const srcNames  = [...new Set(data.map(d => d.source + SRC_SUFFIX))];
  const tgtNames  = [...new Set(data.map(d => d.target))];
  const nodeNames = [...srcNames, ...tgtNames];
  const nodeIndex = new Map(nodeNames.map((name, i) => [name, i]));

  const labelMargin = width < 500 ? 100 : 160;
  const margin = { top: 10, right: labelMargin, bottom: 10, left: labelMargin };
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

  // BFS: collect all nodes/links connected to selectedSink in either direction
  const highlightedNodes = new Set<string>();
  const highlightedLinks = new Set<number>();
  if (selectedSink) {
    const typedNodes = nodes as SankeyNodeDatum[];
    const typedLinks = links as SankeyLinkDatum[];
    highlightedNodes.add(selectedSink);
    const queue = [typedNodes.find(n => n.name === selectedSink)!];
    const visited = new Set<string>();
    while (queue.length) {
      const n = queue.shift();
      if (!n || visited.has(n.name)) continue;
      visited.add(n.name);
      for (const l of (n.targetLinks as SankeyLinkDatum[])) {
        highlightedLinks.add(typedLinks.indexOf(l));
        const src = l.source as SankeyNodeDatum;
        highlightedNodes.add(src.name);
        queue.push(src);
      }
      for (const l of (n.sourceLinks as SankeyLinkDatum[])) {
        highlightedLinks.add(typedLinks.indexOf(l));
        const tgt = l.target as SankeyNodeDatum;
        highlightedNodes.add(tgt.name);
        queue.push(tgt);
      }
    }
  }

  const svgWidth = Math.max(width, 520);

  return (
    <div ref={containerRef} style={{ width: "100%" }}>
      <div className="overflow-x-auto">
        <div className="relative" style={{ width: svgWidth }}>
          <svg
            ref={svgRef}
            width={svgWidth}
            height={height}
            onMouseLeave={() => setTooltip(null)}
          >
            <g transform={`translate(${margin.left},${margin.top})`}>
              {/* Links */}
              {(links as SankeyLinkDatum[]).map((link, i) => {
                const srcIdx = nodeIndex.get((link.source as SankeyNodeDatum).name) ?? 0;
                const color = COLORS[srcIdx % COLORS.length];
                const isHighlighted = !selectedSink || highlightedLinks.has(i);
                return (
                  <path
                    key={i}
                    d={linkPath(link as any) ?? ""}
                    fill="none"
                    stroke={color}
                    strokeOpacity={isHighlighted ? 0.5 : 0.08}
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
                const isSelected = selectedSink === node.name;
                const isDimmed = selectedSink && !highlightedNodes.has(node.name);
                const clickName = node.name.endsWith(SRC_SUFFIX)
                  ? node.name.slice(0, -1)  // strip zero-width space for source nodes
                  : node.name;
                return (
                  <g
                    key={i}
                    onMouseEnter={(e) => handleNodeHover(e, node)}
                    onMouseMove={(e) => handleNodeHover(e, node)}
                    onClick={() => onSinkClick?.(isSelected ? null : clickName)}
                    style={{ cursor: onSinkClick ? "pointer" : "default", opacity: isDimmed ? 0.25 : 1 }}
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
                      fill={isDimmed ? "#4b5563" : "#d1d5db"}
                      fontSize={width < 500 ? 9 : 11}
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
        </div>
      </div>

      <p style={{ color: "#6b7280", fontSize: "0.7rem", marginTop: 4 }}>
        {selectedSink
          ? <><span style={{ color: "#d1d5db" }}>{selectedSink.replace(/^https?:\/\//, "")} highlighted</span> · click again to clear</>
          : <>click any node to highlight its trajectories</>
        }
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

// ── Creation Weekly Bar Chart ─────────────────────────────────────────────────

interface CreationWeeklyBarChartProps {
  data: TimeseriesRow[];
  selectedPds: string | null;
  onPdsClick: (pds: string | null) => void;
}

function CreationWeeklyBarChart({ data, selectedPds, onPdsClick }: CreationWeeklyBarChartProps) {
  const pdsSet = new Set<string>();
  for (const row of data) {
    if (row.pds_url !== "Other") pdsSet.add(row.pds_url);
  }
  const pdsKeys = [...pdsSet];
  if (data.some(r => r.pds_url === "Other")) pdsKeys.push("Other");

  const byPeriod = new Map<string, Record<string, number | string>>();
  for (const row of data) {
    if (!byPeriod.has(row.period)) byPeriod.set(row.period, { period: row.period });
    (byPeriod.get(row.period)! as Record<string, number>)[row.pds_url] = row.count;
  }
  const chartData = [...byPeriod.values()].sort((a, b) =>
    (a.period as string).localeCompare(b.period as string)
  );

  const CustomTooltip = ({ active, payload, label }: any) => {
    if (!active || !payload?.length) return null;
    const total = payload.reduce((s: number, p: any) => s + (p.value ?? 0), 0);
    return (
      <div style={{ ...tooltipStyle.contentStyle, whiteSpace: "nowrap" }}>
        <p style={{ color: "#9ca3af", fontSize: "0.75rem", marginBottom: 4 }}>{label}</p>
        {[...payload].reverse().map((p: any) => (
          <p key={p.dataKey} style={{ color: p.fill === "#374151" ? "#6b7280" : p.fill, fontSize: "0.8rem" }}>
            {displayPdsLabel(p.dataKey)}: {(p.value ?? 0).toLocaleString()}
          </p>
        ))}
        <p style={{ color: "#6b7280", fontSize: "0.75rem", marginTop: 4 }}>
          Total: {total.toLocaleString()}
        </p>
      </div>
    );
  };

  return (
    <ResponsiveContainer width="100%" height={220}>
      <BarChart data={chartData} margin={{ top: 4, right: 16, bottom: 0, left: 16 }} barCategoryGap="10%">
        <XAxis
          dataKey="period"
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
          width={48}
          tickFormatter={(v: number) => v >= 1000 ? `${(v / 1000).toFixed(0)}k` : String(v)}
        />
        <Tooltip content={<CustomTooltip />} cursor={{ fill: "rgba(255,255,255,0.04)" }} />
        {pdsKeys.map((pds, i) => {
          const color = COLORS[i % COLORS.length];
          const dimmed = selectedPds && selectedPds !== pds;
          return (
            <Bar
              key={pds}
              dataKey={pds}
              stackId="a"
              fill={dimmed ? "#374151" : color}
              isAnimationActive={false}
              style={{ cursor: "pointer" }}
              onClick={() => onPdsClick(selectedPds === pds ? null : pds)}
            />
          );
        })}
      </BarChart>
    </ResponsiveContainer>
  );
}

// ── Creation charts section (area chart + weekly bar, shared selection state) ─

interface CreationChartsSectionProps {
  repoData: TimeseriesRow[];
}

export function CreationChartsSection({ repoData }: CreationChartsSectionProps) {
  const [selectedPds, setSelectedPds] = useState<string | null>(null);

  return (
    <div className="space-y-8">
      <div>
        <p className="text-xs text-gray-500 mb-2">Repo-backed accounts — DIDs with an actual repository (excludes ghosts, morel provisioning artifacts)</p>
        <StackedAreaChart
          data={repoData}
          selectedPds={selectedPds}
          onPdsClick={setSelectedPds}
        />
      </div>
      <div>
        <p className="text-xs text-gray-500 mb-2">
          Raw weekly repo-backed account counts
          {selectedPds && (
            <span className="ml-2 text-blue-400">
              Highlighting: {selectedPds.replace(/^https?:\/\//, "")}
            </span>
          )}
        </p>
        <CreationWeeklyBarChart data={repoData} selectedPds={selectedPds} onPdsClick={setSelectedPds} />
      </div>
    </div>
  );
}

// ── Multi-step Migration Trajectory Sankey ────────────────────────────────────

interface MultiStepSankeyProps {
  data: TrajectoryEdge[];
  height?: number;
}

export function MultiStepSankeyChart({ data, height = 480 }: MultiStepSankeyProps) {
  const [tooltip, setTooltip] = useState<SankeyTooltip | null>(null);
  const [width, setWidth] = useState(900);
  const [selectedNode, setSelectedNode] = useState<string | null>(null);
  const [mounted, setMounted] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const svgRef = useRef<SVGSVGElement>(null);

  useEffect(() => setMounted(true), []);

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

  if (!data.length) return <p className="text-gray-500">No trajectory data.</p>;
  if (!mounted) return <div style={{ height }} />;

  const nodeNames = [...new Set([...data.map(d => d.source), ...data.map(d => d.target)])];
  const nodeIndex = new Map(nodeNames.map((name, i) => [name, i]));

  // Dynamic column labels based on max step in data
  const maxStep = Math.max(...data.flatMap(e => [
    parseInt(e.source.split('@').pop()!),
    parseInt(e.target.split('@').pop()!),
  ]));
  const stepLabels = Array.from({ length: maxStep + 1 }, (_, i) =>
    i === 0 ? "Origin" : i === maxStep ? "Current" : `Hop ${i}`
  );

  // Consistent color per PDS base name (strip @N) so the same PDS has the same
  // color whether it appears as origin, first destination, or second destination.
  const pdsNames = [...new Set(nodeNames.map(n => n.replace(/@\d+$/, "")))];
  const pdsColorMap = new Map(pdsNames.map((pds, i) => [pds, COLORS[i % COLORS.length]]));

  const margin = { top: 36, right: 180, bottom: 10, left: 120 };
  const innerW = width - margin.left - margin.right;
  const innerH = height - margin.top - margin.bottom;

  const sankeyLayout = sankey<N, L>()
    .nodeAlign(sankeyLeft)
    .nodeWidth(14)
    .nodePadding(10)
    .extent([[0, 0], [innerW, innerH]]);

  const { nodes, links } = sankeyLayout({
    nodes: nodeNames.map(name => ({ name })),
    links: data.map(d => ({
      source: nodeIndex.get(d.source)!,
      target: nodeIndex.get(d.target)!,
      value: d.value,
    })) as any,
  });

  const linkPath = sankeyLinkHorizontal();

  // BFS upstream + downstream from selectedNode to find all links on connected paths.
  const highlightedLinkSet = new Set<number>();
  const highlightedNodeSet = new Set<string>();
  if (selectedNode) {
    highlightedNodeSet.add(selectedNode);
    const typedNodes = nodes as SankeyNodeDatum[];
    const typedLinks = links as SankeyLinkDatum[];
    // Upstream: follow targetLinks back to origins
    const upQueue = [typedNodes.find(n => n.name === selectedNode)!];
    const visitedUp = new Set<string>();
    while (upQueue.length > 0) {
      const n = upQueue.shift();
      if (!n || visitedUp.has(n.name)) continue;
      visitedUp.add(n.name);
      for (const link of (n.targetLinks as SankeyLinkDatum[])) {
        highlightedLinkSet.add(typedLinks.indexOf(link));
        const src = link.source as SankeyNodeDatum;
        highlightedNodeSet.add(src.name);
        upQueue.push(src);
      }
    }
    // Downstream: follow sourceLinks forward to destinations
    const downQueue = [typedNodes.find(n => n.name === selectedNode)!];
    const visitedDown = new Set<string>();
    while (downQueue.length > 0) {
      const n = downQueue.shift();
      if (!n || visitedDown.has(n.name)) continue;
      visitedDown.add(n.name);
      for (const link of (n.sourceLinks as SankeyLinkDatum[])) {
        highlightedLinkSet.add(typedLinks.indexOf(link));
        const tgt = link.target as SankeyNodeDatum;
        highlightedNodeSet.add(tgt.name);
        downQueue.push(tgt);
      }
    }
  }

  // Compute representative x0 per step column for header labels
  const stepX = new Map<number, number>();
  for (const node of nodes as SankeyNodeDatum[]) {
    const step = parseInt(node.name.match(/@(\d+)$/)?.[1] ?? "0");
    if (!stepX.has(step)) stepX.set(step, (node.x0 ?? 0) + 7);
  }

  const showTooltip = (e: React.MouseEvent, content: React.ReactNode) => {
    const rect = svgRef.current?.getBoundingClientRect();
    if (!rect) return;
    setTooltip({ x: e.clientX - rect.left, y: e.clientY - rect.top, content });
  };
  const moveTooltip = (e: React.MouseEvent) => {
    const rect = svgRef.current?.getBoundingClientRect();
    if (!rect) return;
    setTooltip(t => t ? { ...t, x: e.clientX - rect.left, y: e.clientY - rect.top } : null);
  };

  const svgWidth = Math.max(width, 560);

  return (
    <div ref={containerRef} style={{ width: "100%" }}>
      <div className="overflow-x-auto">
        <div className="relative" style={{ width: svgWidth }}>
          <svg ref={svgRef} width={svgWidth} height={height} onMouseLeave={() => setTooltip(null)} suppressHydrationWarning>
            <g transform={`translate(${margin.left},${margin.top})`}>

              {/* Column headers */}
              {[...stepX.entries()].sort((a, b) => a[0] - b[0]).map(([step, x]) => (
                <text key={step} x={x} y={-16} textAnchor="middle" fill="#6b7280" fontSize={11}>
                  {stepLabels[step] ?? `Hop ${step}`}
                </text>
              ))}

              {/* Links */}
              {(links as SankeyLinkDatum[]).map((link, i) => {
                const srcName = (link.source as SankeyNodeDatum).name;
                const tgtName = (link.target as SankeyNodeDatum).name;
                const color = pdsColorMap.get(srcName.replace(/@\d+$/, "")) ?? "#6b7280";
                const isActive = !selectedNode || highlightedLinkSet.has(i);
                return (
                  <path
                    key={i}
                    d={linkPath(link as any) ?? ""}
                    fill="none"
                    stroke={color}
                    strokeOpacity={isActive ? 0.5 : 0.05}
                    strokeWidth={Math.max(1, link.width ?? 1)}
                    onMouseEnter={(e) => {
                      const src = srcName.replace(/@\d+$/, "").replace(/^https?:\/\//, "");
                      const tgt = tgtName.replace(/@\d+$/, "").replace(/^https?:\/\//, "");
                      showTooltip(e, (
                        <div>
                          <p className="font-medium mb-1" style={{ color: "#e5e7eb" }}>{src} → {tgt}</p>
                          <p style={{ color: "#f3f4f6", fontSize: "0.8rem" }}>{link.value.toLocaleString()} accounts</p>
                        </div>
                      ));
                    }}
                    onMouseMove={moveTooltip}
                    style={{ cursor: "default" }}
                  />
                );
              })}

              {/* Nodes */}
              {(nodes as SankeyNodeDatum[]).map((node, i) => {
                const pdsBase = node.name.replace(/@\d+$/, "");
                const color = pdsColorMap.get(pdsBase) ?? COLORS[i % COLORS.length];
                const x0 = node.x0 ?? 0, x1 = node.x1 ?? 0;
                const y0 = node.y0 ?? 0, y1 = node.y1 ?? 0;
                const labelRight = x0 > innerW / 2;
                const label = pdsBase.replace(/^https?:\/\//, "");
                const isSelected = selectedNode === node.name;
                const isDimmed = selectedNode && !highlightedNodeSet.has(node.name);
                return (
                  <g
                    key={i}
                    onClick={() => setSelectedNode(isSelected ? null : node.name)}
                    onMouseEnter={(e) => {
                      const inflow  = (node.targetLinks as SankeyLinkDatum[]).reduce((s, l) => s + l.value, 0);
                      const outflow = (node.sourceLinks as SankeyLinkDatum[]).reduce((s, l) => s + l.value, 0);
                      showTooltip(e, (
                        <div>
                          <p className="font-medium mb-1" style={{ color: "#e5e7eb" }}>{label}</p>
                          {inflow  > 0 && <p style={{ color: "#10b981", fontSize: "0.8rem" }}>Inbound: {inflow.toLocaleString()}</p>}
                          {outflow > 0 && <p style={{ color: "#f59e0b", fontSize: "0.8rem" }}>Outbound: {outflow.toLocaleString()}</p>}
                          <p style={{ color: "#6b7280", fontSize: "0.75rem", marginTop: 4 }}>Click to highlight</p>
                        </div>
                      ));
                    }}
                    onMouseMove={moveTooltip}
                    style={{ cursor: "pointer", opacity: isDimmed ? 0.25 : 1 }}
                  >
                    <rect
                      x={x0} y={y0} width={x1 - x0} height={Math.max(1, y1 - y0)}
                      fill={color} rx={2}
                      stroke={isSelected ? "#fff" : "none"}
                      strokeWidth={isSelected ? 1.5 : 0}
                    />
                    <text
                      x={labelRight ? x1 + 6 : x0 - 6}
                      y={(y0 + y1) / 2}
                      textAnchor={labelRight ? "start" : "end"}
                      dominantBaseline="middle"
                      fill={isDimmed ? "#4b5563" : "#d1d5db"}
                      fontSize={11}
                    >
                      {label}
                    </text>
                  </g>
                );
              })}
            </g>
          </svg>

          {tooltip && (
            <div style={{
              position: "absolute",
              left: tooltip.x + 12,
              top: tooltip.y - 8,
              pointerEvents: "none",
              ...tooltipStyle.contentStyle,
            }}>
              {tooltip.content}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// ── PDS Age scatter chart ────────────────────────────────────────────────────

const PDS_AGE_ERAS: { label: string; color: string; from: string; to: string }[] = [
  { label: "Pre-2023",     color: "#6b7280", from: "0000-00", to: "2022-12" },
  { label: "2023",         color: "#3b82f6", from: "2023-01", to: "2023-12" },
  { label: "2024 pre-Nov", color: "#8b5cf6", from: "2024-01", to: "2024-10" },
  { label: "2024 Nov–Dec", color: "#f59e0b", from: "2024-11", to: "2024-12" },
  { label: "2025 H1",      color: "#10b981", from: "2025-01", to: "2025-06" },
  { label: "2025 H2",      color: "#06b6d4", from: "2025-07", to: "2025-12" },
  { label: "2026+",        color: "#ec4899", from: "2026-01", to: "9999-99" },
];

function fmtAccounts(n: number) {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000)     return `${(n / 1_000).toFixed(0)}K`;
  return n.toLocaleString();
}

export function PdsAgeChart({ data }: { data: PdsAgeRow[] }) {
  type Point = { x: number; y: number; name: string; firstMonth: string; total: number };
  const toPoint = (r: PdsAgeRow): Point => ({
    x: new Date(r.first_week).getTime(),
    y: r.total_accounts,
    name: r.pds_url,
    firstMonth: r.first_week,
    total: r.total_accounts,
  });

  const tickFmt = (ts: number) =>
    new Date(ts).toLocaleDateString("en-US", { year: "numeric", month: "short" });

  const CustomTooltip = ({ payload }: { payload?: { payload: Point }[] }) => {
    if (!payload?.length) return null;
    const d = payload[0].payload;
    return (
      <div style={{ ...tooltipStyle.contentStyle, padding: "8px 12px", fontSize: "0.8rem" }}>
        <div className="font-mono text-gray-200 mb-0.5">{d.name.replace(/^https?:\/\//, "")}</div>
        <div className="text-gray-400">First account: {d.firstMonth}</div>
        <div className="text-gray-300">{d.total.toLocaleString()} accounts</div>
      </div>
    );
  };

  return (
    <ResponsiveContainer width="100%" height={420}>
      <ScatterChart margin={{ top: 10, right: 20, bottom: 50, left: 70 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
        <XAxis
          dataKey="x"
          type="number"
          domain={["auto", "auto"]}
          scale="time"
          tickFormatter={tickFmt}
          tick={{ fontSize: 11, fill: "#9ca3af" }}
          label={{ value: "First account date", position: "insideBottom", offset: -30, fill: "#6b7280", fontSize: 12 }}
        />
        <YAxis
          dataKey="y"
          type="number"
          scale="log"
          domain={[10, "auto"]}
          tickFormatter={fmtAccounts}
          tick={{ fontSize: 11, fill: "#9ca3af" }}
          label={{ value: "Total accounts (log scale)", angle: -90, position: "insideLeft", offset: -45, fill: "#6b7280", fontSize: 12 }}
        />
        <Tooltip content={(props) => <CustomTooltip payload={props.payload as unknown as { payload: Point }[] | undefined} />} />
        <Legend
          wrapperStyle={{ fontSize: "0.72rem", paddingTop: "8px" }}
          formatter={(value) => <span style={{ color: "#9ca3af" }}>{value}</span>}
        />
        {PDS_AGE_ERAS.map((era) => (
          <Scatter
            key={era.label}
            name={era.label}
            data={data
              .filter((r) => r.first_week >= era.from && r.first_week <= era.to)
              .map(toPoint)}
            fill={era.color}
            fillOpacity={0.75}
            r={4}
          />
        ))}
      </ScatterChart>
    </ResponsiveContainer>
  );
}
