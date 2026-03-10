/**
 * CLI runner for ecosystem stats collection.
 *
 * Usage:
 *   npm run collect:ecosystem
 */

import { getDb } from "../db/schema";
import { collectGithubStats } from "./github-stats";

async function main() {
  const db = getDb();
  console.log("\n=== ATProto Ecosystem Stats Collection ===\n");

  // GitHub
  console.log("--- GitHub ---");
  const githubResults = await collectGithubStats();
  const insertGithub = db.prepare(`
    INSERT INTO github_stats (query, repo_count, top_repos)
    VALUES (?, ?, ?)
  `);
  const githubTx = db.transaction(() => {
    for (const r of githubResults) {
      insertGithub.run(r.query, r.repoCount, JSON.stringify(r.topRepos));
    }
  });
  githubTx();
  console.log(`[github] Saved ${githubResults.length} query results\n`);

  console.log("=== Ecosystem Collection Complete ===\n");
}

main().catch((err) => {
  console.error("Ecosystem collection failed:", err);
  process.exit(1);
});
