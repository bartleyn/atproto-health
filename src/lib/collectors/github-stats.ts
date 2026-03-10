/**
 * Collects GitHub repository stats for ATProto-related topics.
 * Uses the GitHub Search API (no auth required for our volume).
 * Set GITHUB_TOKEN env var for higher rate limits if needed.
 */

export interface GithubRepo {
  name: string;
  fullName: string;
  stars: number;
  url: string;
  description: string | null;
}

export interface GithubQueryResult {
  query: string;
  repoCount: number;
  topRepos: GithubRepo[];
}

const QUERIES = [
  "topic:atproto",
  "topic:bluesky",
  "topic:at-protocol",
];

async function searchRepos(query: string): Promise<GithubQueryResult> {
  const headers: Record<string, string> = {
    Accept: "application/vnd.github.v3+json",
    "User-Agent": "atproto-health-dashboard",
  };
  if (process.env.GITHUB_TOKEN) {
    headers["Authorization"] = `Bearer ${process.env.GITHUB_TOKEN}`;
  }

  const url = `https://api.github.com/search/repositories?q=${encodeURIComponent(query)}&sort=stars&order=desc&per_page=25`;
  const res = await fetch(url, { headers });

  if (!res.ok) {
    throw new Error(`GitHub API error for "${query}": ${res.status} ${res.statusText}`);
  }

  const data = await res.json() as {
    total_count: number;
    items: Array<{
      name: string;
      full_name: string;
      stargazers_count: number;
      html_url: string;
      description: string | null;
    }>;
  };

  return {
    query,
    repoCount: data.total_count,
    topRepos: data.items.map((r) => ({
      name: r.name,
      fullName: r.full_name,
      stars: r.stargazers_count,
      url: r.html_url,
      description: r.description,
    })),
  };
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function collectGithubStats(): Promise<GithubQueryResult[]> {
  const results: GithubQueryResult[] = [];

  for (const query of QUERIES) {
    console.log(`[github] Searching: ${query}`);
    const result = await searchRepos(query);
    results.push(result);
    console.log(`[github]   ${result.repoCount} repos, top star count: ${result.topRepos[0]?.stars ?? 0}`);
    // Stay well under GitHub's 10 req/min unauthenticated limit
    await sleep(8000);
  }

  return results;
}
