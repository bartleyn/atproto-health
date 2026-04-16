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

  // Build a set of rounded lat/lon keys for clusters that contain at least one
  // PDS from the selected provider (0.1° precision matches cities reliably).
  const providerClusterKeys = selectedProvider && providerLocations
    ? new Set(
        providerLocations
          .filter(p => p.provider === selectedProvider)
          .map(p => `${Math.round(p.latitude * 10)},${Math.round(p.longitude * 10)}`)
      )
    : new Set<string>();

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
          locations
            .filter(loc => {
              const clusterKey = `${Math.round(loc.latitude * 10)},${Math.round(loc.longitude * 10)}`;
              const hasProvider = selectedProvider ? providerClusterKeys.has(clusterKey) : false;
              return renderHighlighted ? hasProvider : !hasProvider;
            })
            .map((loc, i) => {
              const clusterKey = `${Math.round(loc.latitude * 10)},${Math.round(loc.longitude * 10)}`;
              const hasProvider = selectedProvider ? providerClusterKeys.has(clusterKey) : false;
              const dimmed = selectedProvider ? !hasProvider : false;
              const r =
                loc.pdsCount > 20 ? 7
                : loc.pdsCount > 10 ? 6
                : loc.pdsCount > 5 ? 5
                : loc.pdsCount > 2 ? 4
                : loc.pdsCount > 1 ? 3
                : 2;
              return (
                <Marker
                  key={`${renderHighlighted ? "h" : "d"}-${Math.round(loc.latitude * 10)}-${Math.round(loc.longitude * 10)}`}
                  coordinates={[loc.longitude, loc.latitude]}
                  onMouseEnter={() => {
                    if (dimmed) return;
                    const parts: string[] = [];
                    if (loc.city && loc.country) parts.push(`${loc.city}, ${loc.country}`);
                    else if (loc.country) parts.push(loc.country);
                    parts.push(`${loc.pdsCount} PDS${loc.pdsCount !== 1 ? "es" : ""}`);
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
