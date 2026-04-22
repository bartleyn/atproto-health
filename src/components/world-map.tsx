"use client";

import { useState } from "react";
import {
  ComposableMap,
  Geographies,
  Geography,
  Marker,
} from "react-simple-maps";
import type { CityCluster, PdsProviderLocation } from "@/lib/db/queries";

const GEO_URL =
  "https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json";

export interface PdsLangLocation {
  url: string;
  city: string | null;
  country: string | null;
  lang: string;
  dids: number;
}

interface WorldMapProps {
  locations: CityCluster[];
  providerLocations?: PdsProviderLocation[];
  selectedProvider?: string | null;
  langLocations?: PdsLangLocation[];
  selectedLang?: string | null;
}

export function WorldMap({
  locations,
  providerLocations,
  selectedProvider,
  langLocations,
  selectedLang,
}: WorldMapProps) {
  const [hovered, setHovered] = useState<string | null>(null);
  const [mousePos, setMousePos] = useState({ x: 0, y: 0 });
  const [pinnedCluster, setPinnedCluster] = useState<{ key: string; pds: string; dids: number; x: number; y: number } | null>(null);

  // Build highlight counts per cluster.
  // Key by city|country — rounding AVG lat/lon diverges from individual PDS coordinates.
  // Only one filter (provider or lang) can be active at a time.
  const highlightCountByCluster = new Map<string, number>();
  // Top PDS per cluster for the selected language (by dids). Bsky shards collapse to one entry using the real total.
  const topPdsByCluster = new Map<string, { pds: string; dids: number }>();

  if (selectedProvider && providerLocations) {
    for (const p of providerLocations) {
      if (p.provider !== selectedProvider) continue;
      const key = `${p.city ?? ""}|${p.country ?? ""}`;
      highlightCountByCluster.set(key, (highlightCountByCluster.get(key) ?? 0) + 1);
    }
  } else if (selectedLang && langLocations) {
    for (const l of langLocations) {
      if (l.lang !== selectedLang) continue;
      const key = `${l.city ?? ""}|${l.country ?? ""}`;
      highlightCountByCluster.set(key, (highlightCountByCluster.get(key) ?? 0) + l.dids);
      const isBskyShard = /bsky\.network|bsky\.social/.test(l.url);
      const displayPds = isBskyShard ? "bsky.network" : l.url.replace(/^https?:\/\//, "");
      const current = topPdsByCluster.get(key);
      if (!current || l.dids > current.dids) {
        topPdsByCluster.set(key, { pds: displayPds, dids: l.dids });
      }
    }
  }

  const activeFilter = selectedProvider ?? selectedLang ?? null;
  const highlightedClusterKeys = new Set(highlightCountByCluster.keys());

  return (
    <div
      className="relative"
      onMouseMove={(e) => {
        const rect = e.currentTarget.getBoundingClientRect();
        setMousePos({ x: e.clientX - rect.left, y: e.clientY - rect.top });
      }}
      onClick={(e) => {
        // Click outside a dot clears the pinned callout.
        if ((e.target as SVGElement).tagName !== "circle") setPinnedCluster(null);
      }}
    >
      <ComposableMap
        projection="geoNaturalEarth1"
        projectionConfig={{ scale: 153, center: [10, 10] }}
        style={{ width: "100%", height: "auto" }}
      >
        <Geographies geography={GEO_URL}>
          {({ geographies }) =>
            geographies.map((geo) => (
              <Geography
                key={geo.rsmKey}
                geography={geo}
                fill="#1e293b"
                stroke="#334155"
                strokeWidth={0.5}
                style={{
                  default: { outline: "none" },
                  hover: { outline: "none", fill: "#1e293b" },
                  pressed: { outline: "none" },
                }}
              />
            ))
          }
        </Geographies>

        {/* City-cluster dots — render dimmed clusters first, highlighted on top */}
        {[false, true].flatMap(renderHighlighted =>
          locations.flatMap((loc, originalIdx) => {
              const clusterKey = `${loc.city ?? ""}|${loc.country ?? ""}`;
              const isHighlighted = activeFilter ? highlightedClusterKeys.has(clusterKey) : false;
              if (!activeFilter && renderHighlighted) return [];
              if (activeFilter && renderHighlighted !== isHighlighted) return [];
              const dimmed = activeFilter ? !isHighlighted : false;
              const sizeCount = isHighlighted
                ? (highlightCountByCluster.get(clusterKey) ?? 1)
                : loc.pdsCount;
              const r =
                sizeCount > 20 ? 7
                : sizeCount > 10 ? 6
                : sizeCount > 5 ? 5
                : sizeCount > 2 ? 4
                : sizeCount > 1 ? 3
                : 2;
              return (
                <Marker
                  key={`${renderHighlighted ? "h" : "d"}-${originalIdx}`}
                  coordinates={[loc.longitude, loc.latitude]}
                  onMouseEnter={() => {
                    if (dimmed) return;
                    const parts: string[] = [];
                    if (loc.city && loc.country) parts.push(`${loc.city}, ${loc.country}`);
                    else if (loc.country) parts.push(loc.country);
                    if (selectedLang) {
                      parts.push(`${sizeCount.toLocaleString()} ${selectedLang} speaker${sizeCount !== 1 ? "s" : ""}`);
                    } else {
                      parts.push(`${sizeCount} PDS${sizeCount !== 1 ? "es" : ""}`);
                    }
                    setHovered(parts.join(" · "));
                  }}
                  onMouseLeave={() => setHovered(null)}
                  onClick={isHighlighted && selectedLang ? (e) => {
                    e.stopPropagation();
                    const top = topPdsByCluster.get(clusterKey);
                    if (!top) return;
                    const rect = (e.currentTarget as SVGElement).closest(".relative")!.getBoundingClientRect();
                    setPinnedCluster(prev =>
                      prev?.key === clusterKey ? null :
                      { key: clusterKey, pds: top.pds, dids: top.dids, x: mousePos.x, y: mousePos.y }
                    );
                  } : undefined}
                >
                  <circle
                    style={isHighlighted && selectedLang && topPdsByCluster.has(clusterKey) ? { cursor: "pointer" } : undefined}
                    r={r}
                    fill={dimmed ? "#1e3a5f" : isHighlighted ? "#f59e0b" : "#3b82f6"}
                    fillOpacity={dimmed ? 0.25 : 0.75}
                    stroke={dimmed ? "#1e3a5f" : isHighlighted ? "#fcd34d" : "#93c5fd"}
                    strokeWidth={dimmed ? 0.5 : 1}
                  />
                </Marker>
              );
            })
        )}
      </ComposableMap>

      {hovered && (
        <div
          className="absolute pointer-events-none z-10 bg-gray-800 border border-gray-700 rounded px-2 py-1 text-xs text-gray-200 font-mono whitespace-nowrap"
          style={{ left: mousePos.x + 12, top: mousePos.y - 28 }}
        >
          {hovered}
        </div>
      )}
      {pinnedCluster && (
        <div
          className="absolute z-10 bg-gray-900 border border-amber-600/60 rounded px-2.5 py-1.5 text-xs text-gray-200 font-mono whitespace-nowrap shadow-lg"
          style={{ left: pinnedCluster.x + 12, top: pinnedCluster.y - 40 }}
        >
          <span className="text-amber-400">{pinnedCluster.pds}</span>
          <span className="text-gray-500 ml-1.5">{pinnedCluster.dids.toLocaleString()} {selectedLang} speakers</span>
        </div>
      )}
    </div>
  );
}
