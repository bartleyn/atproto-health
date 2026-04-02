"use client";

import { useState } from "react";
import {
  ComposableMap,
  Geographies,
  Geography,
  Marker,
} from "react-simple-maps";
import type { CityCluster } from "@/lib/db/queries";

const GEO_URL =
  "https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json";

export function WorldMap({ locations }: { locations: CityCluster[] }) {
  const [hovered, setHovered] = useState<string | null>(null);
  const [mousePos, setMousePos] = useState({ x: 0, y: 0 });

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

        {locations.map((loc, i) => {
          const r =
            loc.pdsCount > 20
              ? 7
              : loc.pdsCount > 10
              ? 6
              : loc.pdsCount > 5
              ? 5
              : loc.pdsCount > 2
              ? 4
              : loc.pdsCount > 1
              ? 3
              : 2;
          return (
            <Marker
              key={i}
              coordinates={[loc.longitude, loc.latitude]}
              onMouseEnter={() => {
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
                fill="#3b82f6"
                fillOpacity={0.65}
                stroke="#93c5fd"
                strokeWidth={0.5}
              />
            </Marker>
          );
        })}
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
