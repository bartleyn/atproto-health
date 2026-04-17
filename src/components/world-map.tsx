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

interface WorldMapProps {
  locations: CityCluster[];
  providerLocations?: PdsProviderLocation[];
  selectedProvider?: string | null;
}

export function WorldMap({ locations, providerLocations, selectedProvider }: WorldMapProps) {
  const [hovered, setHovered] = useState<string | null>(null);
  const [mousePos, setMousePos] = useState({ x: 0, y: 0 });

  // For each cluster key, count how many PDSes belong to the selected provider.
  const providerCountByCluster = new Map<string, number>();
  if (selectedProvider && providerLocations) {
    for (const p of providerLocations) {
      if (p.provider !== selectedProvider) continue;
      const key = `${Math.round(p.latitude * 10)},${Math.round(p.longitude * 10)}`;
      providerCountByCluster.set(key, (providerCountByCluster.get(key) ?? 0) + 1);
    }
  }
  const providerClusterKeys = new Set(providerCountByCluster.keys());

  return (
    <div
      className="relative"
      onMouseMove={(e) => {
        const rect = e.currentTarget.getBoundingClientRect();
        setMousePos({ x: e.clientX - rect.left, y: e.clientY - rect.top });
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
              const clusterKey = `${Math.round(loc.latitude * 10)},${Math.round(loc.longitude * 10)}`;
              const hasProvider = selectedProvider ? providerClusterKeys.has(clusterKey) : false;
              if (!selectedProvider && renderHighlighted) return [];
              if (selectedProvider && renderHighlighted !== hasProvider) return [];
              const dimmed = selectedProvider ? !hasProvider : false;
              const sizeCount = hasProvider
                ? (providerCountByCluster.get(clusterKey) ?? 1)
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
                    parts.push(`${sizeCount} PDS${sizeCount !== 1 ? "es" : ""}`);
                    setHovered(parts.join(" · "));
                  }}
                  onMouseLeave={() => setHovered(null)}
                >
                  <circle
                    r={r}
                    fill={dimmed ? "#1e3a5f" : hasProvider ? "#f59e0b" : "#3b82f6"}
                    fillOpacity={dimmed ? 0.25 : 0.75}
                    stroke={dimmed ? "#1e3a5f" : hasProvider ? "#fcd34d" : "#93c5fd"}
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
    </div>
  );
}
