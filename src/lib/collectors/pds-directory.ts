/**
 * Fetches the PDS directory from mary-ext/atproto-scraping state.json.
 * This is the canonical source for all known ATProto PDS instances.
 */

const STATE_JSON_URL =
  "https://raw.githubusercontent.com/mary-ext/atproto-scraping/refs/heads/trunk/dist/instances.json";

export interface PdsDirectoryEntry {
  url: string;
  version: string | null;
  inviteCodeRequired: boolean;
  isOnline: boolean;
  errorAt: number | null;
}

interface StateJson {
  pdses: Record<
    string,
    {
      status?: "online" | "offline";
      version?: string | null;
      inviteCodeRequired?: boolean;
    }
  >;
}

export async function fetchPdsDirectory(): Promise<PdsDirectoryEntry[]> {
  const response = await fetch(STATE_JSON_URL);
  if (!response.ok) {
    throw new Error(
      `Failed to fetch instances.json: ${response.status} ${response.statusText}`
    );
  }

  const data: StateJson = await response.json();
  const entries: PdsDirectoryEntry[] = [];

  for (const [url, info] of Object.entries(data.pdses)) {
    entries.push({
      url,
      version: info.version ?? null,
      inviteCodeRequired: info.inviteCodeRequired ?? false,
      isOnline: info.status === "online",
      errorAt: null,
    });
  }

  console.log(`[pds-directory] Fetched ${entries.length} PDS instances`);
  return entries;
}
