/**
 * CLI runner for ecosystem stats collection.
 *
 * Usage:
 *   npm run collect:ecosystem
 */

import sql from "../db/pg";
import { collectGithubStats } from "./github-stats";

async function main() {
  console.log("\n=== ATProto Ecosystem Stats Collection ===\n");

  // GitHub
  console.log("--- GitHub ---");
  const githubResults = await collectGithubStats();
  for (const r of githubResults) {
    await sql`
      INSERT INTO health.github_stats (query, repo_count, top_repos)
      VALUES (${r.query}, ${r.repoCount}, ${sql.json(r.topRepos as never)})
    `;
  }
  console.log(`[github] Saved ${githubResults.length} query results\n`);

  console.log("=== Ecosystem Collection Complete ===\n");
}

main()
  .catch((err) => {
    console.error("Ecosystem collection failed:", err);
    process.exit(1);
  })
  .finally(() => sql.end());
