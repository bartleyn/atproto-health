"use client";

import { useState } from "react";
import {
  ScatterChart, Scatter, XAxis, YAxis, ZAxis, Tooltip, ResponsiveContainer, Cell, ReferenceArea,
} from "recharts";

export type SampleAccount = { did: string; labeled: boolean; score: number };
export type Cluster = {
  id: number; size: number; intra_edges: number; density: number;
  n_fraud: number; fraud_rate: number; enrichment: number; n_unlabeled: number;
  samples: SampleAccount[];
};

// amber -> red as enrichment climbs (more fraud-over-represented = hotter)
function color(enr: number): string {
  if (enr >= 15) return "#ef4444";   // red
  if (enr >= 8) return "#f59e0b";    // amber
  if (enr >= 3) return "#eab308";    // yellow
  return "#6b7280";                  // gray (barely enriched)
}

function ClusterTooltip({ active, payload }: { active?: boolean; payload?: { payload: Cluster }[] }) {
  if (!active || !payload?.length) return null;
  const c = payload[0].payload;
  return (
    <div className="bg-gray-900 border border-gray-700 rounded p-2 text-xs text-gray-200">
      <div className="text-white font-semibold">{c.size.toLocaleString()} accounts</div>
      <div>density {c.density.toFixed(0)} · <span className="text-amber-400">{c.enrichment.toFixed(0)}× inauthentic</span></div>
      <div className="text-gray-400">{c.n_fraud} labeled · {c.n_unlabeled.toLocaleString()} unlabeled</div>
      <div className="text-gray-600 mt-1">click to inspect accounts</div>
    </div>
  );
}

export function FraudClusterScatter({ clusters }: { clusters: Cluster[] }) {
  const [sel, setSel] = useState<Cluster | null>(null);
  const maxD = Math.max(...clusters.map((c) => c.density), 10);
  const maxE = Math.max(...clusters.map((c) => c.enrichment), 5);

  return (
    <div>
      <ResponsiveContainer width="100%" height={460}>
        <ScatterChart margin={{ top: 16, right: 24, bottom: 40, left: 8 }}>
          {/* "prime suspect" zone: dense AND highly enriched */}
          <ReferenceArea x1={maxD * 0.4} x2={maxD * 1.05} y1={10} y2={maxE * 1.05}
            fill="#ef4444" fillOpacity={0.06} />
          <XAxis type="number" dataKey="density" name="density" scale="sqrt"
            domain={[0, "dataMax"]} tick={{ fill: "#9ca3af", fontSize: 12 }}
            label={{ value: "intra-cluster follow density →", position: "bottom",
              fill: "#6b7280", fontSize: 12 }} stroke="#374151" />
          <YAxis type="number" dataKey="enrichment" name="enrichment"
            domain={[0, "dataMax"]} tick={{ fill: "#9ca3af", fontSize: 12 }}
            label={{ value: "fraud enrichment (×) →", angle: -90, position: "insideLeft",
              fill: "#6b7280", fontSize: 12 }} stroke="#374151" />
          <ZAxis type="number" dataKey="size" range={[40, 700]} name="size" />
          <Tooltip content={<ClusterTooltip />} cursor={{ strokeDasharray: "3 3", stroke: "#4b5563" }} />
          <Scatter data={clusters} onClick={(d) => setSel(d as unknown as Cluster)}
            style={{ cursor: "pointer" }}>
            {clusters.map((c) => (
              <Cell key={c.id} fill={color(c.enrichment)}
                fillOpacity={sel && sel.id === c.id ? 1 : 0.65}
                stroke={sel && sel.id === c.id ? "#fff" : "none"} strokeWidth={2} />
            ))}
          </Scatter>
        </ScatterChart>
      </ResponsiveContainer>

      <p className="text-xs text-gray-600 -mt-2 ml-2">
        Bubble = cluster size · color = bot/fraud enrichment · red zone (top-right) = dense <em>and</em>
        bot/fraud-enriched = prime candidate rings.
      </p>

      {sel ? (
        <div className="mt-5 bg-gray-900 border border-gray-800 rounded-lg p-4">
          <div className="flex items-baseline justify-between flex-wrap gap-2">
            <h3 className="text-white font-semibold">
              Cluster #{sel.id} — {sel.size.toLocaleString()} accounts
            </h3>
            <div className="text-sm text-gray-400">
              density {sel.density.toFixed(0)} ·{" "}
              <span className="text-amber-400">{sel.enrichment.toFixed(0)}× inauthentic</span> ·{" "}
              {sel.n_fraud} labeled · <span className="text-blue-300">{sel.n_unlabeled.toLocaleString()} unlabeled</span>
            </div>
          </div>
          <div className="mt-2 flex items-center gap-4 text-xs text-gray-500">
            <span><span className="text-red-400">●</span> already flagged (anchor)</span>
            <span><span className="text-blue-400">○</span> unlabeled candidate</span>
            <span className="text-gray-600">% = model score (exploratory)</span>
          </div>
          <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1">
            {sel.samples.map((s) => (
              <a key={s.did} href={`https://bsky.app/profile/${s.did}`} target="_blank" rel="noreferrer"
                className="text-sm hover:underline" title={`${s.did} · model score ${(s.score * 100).toFixed(1)}%`}>
                <span className={s.labeled ? "text-red-400" : "text-blue-400"}>
                  {s.labeled ? "●" : "○"} {s.did.replace("did:plc:", "").slice(0, 10)}
                </span>
                <span className="text-gray-600 ml-1">{(s.score * 100).toFixed(0)}%</span>
              </a>
            ))}
          </div>
          <p className="text-xs text-amber-500/80 mt-3">
            ⚠ Exploratory — these are the unlabeled accounts the model scores most inauthentic-like in this
            cluster. <strong>The model can be wrong</strong>;
            treat these as leads for human review, not determinations.
          </p>
        </div>
      ) : (
        <p className="mt-5 text-sm text-gray-500">Click a bubble to inspect its accounts →</p>
      )}
    </div>
  );
}
