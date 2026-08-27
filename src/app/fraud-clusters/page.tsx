export const dynamic = "force-dynamic";

import { readFileSync } from "fs";
import path from "path";
import { FraudClusterScatter, type Cluster } from "@/components/fraud-cluster-scatter";

type ClusterData = {
  generated_at: string;
  embedding: string;
  k: number;
  n_nodes: number;
  base_fraud_rate: number;
  clusters: Cluster[];
};

function loadClusters(): ClusterData | null {
  try {
    const file = path.join(process.cwd(), "cache", "fraud_clusters.json");
    return JSON.parse(readFileSync(file, "utf8")) as ClusterData;
  } catch {
    return null;
  }
}

function StatCard({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="bg-gray-900 rounded-lg p-4 border border-gray-800">
      <p className="text-xs text-gray-500 uppercase tracking-wide">{label}</p>
      <p className="text-2xl font-bold text-white mt-1">{value}</p>
      {sub && <p className="text-xs text-gray-500 mt-1">{sub}</p>}
    </div>
  );
}

export default function FraudClustersPage() {
  const data = loadClusters();

  if (!data) {
    return (
      <main className="min-h-screen bg-black text-gray-200 p-8 font-mono">
        <h1 className="text-2xl font-bold text-white">Inauthenticity cluster discovery</h1>
        <p className="mt-4 text-gray-400">
          No results yet. Run the clustering job and place its output at{" "}
          <code className="text-amber-400">cache/fraud_clusters.json</code>:
        </p>
        <pre className="mt-3 bg-gray-900 border border-gray-800 rounded p-3 text-sm text-gray-300">
{`modal run scripts/gnn/modal_train.py::cluster_run --embedding degree
modal volume get gnn-data /fraud_clusters.json cache/fraud_clusters.json`}
        </pre>
      </main>
    );
  }

  return (
    <main className="min-h-screen bg-black text-gray-200 p-8">
      <h1 className="text-2xl font-bold text-white">Inauthenticity cluster discovery</h1>
      <p className="mt-2 text-sm text-gray-500">
        GraphSAGE follow-graph embeddings, clustered into {data.k.toLocaleString()} groups.
        This is meant to group users together based on how they look in the follower-graph. Generated{" "}
        {new Date(data.generated_at).toLocaleString()}.
      </p>

      <div className="mt-4 rounded-lg border border-amber-600/40 bg-amber-950/30 p-3 text-sm text-amber-200/90">
        <strong>Exploratory.</strong> These clusters come from a follow-graph model -- its purpose here is to surface coordinated structure for{" "}
        <em>human review</em>, not to label anyone. The per-account scores are model guesses and{" "}
        <strong>can be wrong</strong>; nothing here is a determination of wrongdoing.
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mt-6">
        <StatCard label="Nodes" value={data.n_nodes.toLocaleString()} />
        <StatCard label="Clusters" value={data.k.toLocaleString()} />
        <StatCard label="Base inauthentic rate" value={`${(data.base_fraud_rate * 100).toFixed(3)}%`} />
        <StatCard label="Candidate rings" value={data.clusters.length.toLocaleString()} />
      </div>

      <div className="mt-8">
        <FraudClusterScatter clusters={data.clusters} />
      </div>

      <p className="mt-6 text-xs text-gray-600">
        Note: this is surfacing coordinated structure the labelers and per-node models miss. High-enrichment, high-density clusters with many unlabeled members are
        the candidates worth a manual look.
      </p>
    </main>
  );
}
