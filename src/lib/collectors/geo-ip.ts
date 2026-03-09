/**
 * Resolves PDS hostnames to IPs and geolocates them via ip-api.com.
 * Free tier: 45 requests/minute, batch up to 100 IPs per request.
 */

import { resolve4 } from "dns/promises";

export interface GeoIpResult {
  pdsUrl: string;
  ip: string | null;
  country: string | null;
  countryCode: string | null;
  region: string | null;
  city: string | null;
  lat: number | null;
  lon: number | null;
  isp: string | null;
  org: string | null;
  asNumber: string | null;
}

interface IpApiResponse {
  status: string;
  country?: string;
  countryCode?: string;
  regionName?: string;
  city?: string;
  lat?: number;
  lon?: number;
  isp?: string;
  org?: string;
  as?: string;
  query: string;
}

const BATCH_SIZE = 100;
const RATE_LIMIT_DELAY_MS = 2000; // ~30 req/min to stay well under 45
const MAX_RETRIES = 3;

export async function resolveHostname(hostname: string): Promise<string | null> {
  try {
    const addresses = await resolve4(hostname);
    return addresses[0] ?? null;
  } catch {
    return null;
  }
}

async function geolocateBatch(ips: string[]): Promise<IpApiResponse[]> {
  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    const response = await fetch("http://ip-api.com/batch", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(ips),
    });

    if (response.status === 429) {
      const backoff = (attempt + 1) * 15000; // 15s, 30s, 45s
      console.log(`[geo-ip] Rate limited, waiting ${backoff / 1000}s...`);
      await sleep(backoff);
      continue;
    }

    if (!response.ok) {
      throw new Error(`ip-api batch failed: ${response.status}`);
    }

    return response.json();
  }

  throw new Error("ip-api batch failed after retries");
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function geolocatePdses(
  pdsUrls: string[]
): Promise<Map<string, GeoIpResult>> {
  const results = new Map<string, GeoIpResult>();

  // Resolve all hostnames to IPs
  console.log(`[geo-ip] Resolving ${pdsUrls.length} hostnames...`);
  const urlToIp = new Map<string, string>();
  const ipToUrls = new Map<string, string[]>();

  const DNS_CONCURRENCY = 50;
  for (let i = 0; i < pdsUrls.length; i += DNS_CONCURRENCY) {
    const batch = pdsUrls.slice(i, i + DNS_CONCURRENCY);
    const resolved = await Promise.all(
      batch.map(async (url) => {
        const hostname = new URL(url).hostname;
        const ip = await resolveHostname(hostname);
        return { url, ip };
      })
    );

    for (const { url, ip } of resolved) {
      if (ip) {
        urlToIp.set(url, ip);
        const existing = ipToUrls.get(ip) ?? [];
        existing.push(url);
        ipToUrls.set(ip, existing);
      } else {
        results.set(url, {
          pdsUrl: url,
          ip: null,
          country: null,
          countryCode: null,
          region: null,
          city: null,
          lat: null,
          lon: null,
          isp: null,
          org: null,
          asNumber: null,
        });
      }
    }
  }

  // Deduplicate IPs for batch lookup
  const uniqueIps = [...ipToUrls.keys()];
  console.log(
    `[geo-ip] Resolved to ${uniqueIps.length} unique IPs. Geolocating...`
  );

  // Batch geolocate
  for (let i = 0; i < uniqueIps.length; i += BATCH_SIZE) {
    const batch = uniqueIps.slice(i, i + BATCH_SIZE);
    const geoResults = await geolocateBatch(batch);

    for (const geo of geoResults) {
      const urls = ipToUrls.get(geo.query) ?? [];
      for (const url of urls) {
        results.set(url, {
          pdsUrl: url,
          ip: geo.query,
          country: geo.status === "success" ? (geo.country ?? null) : null,
          countryCode:
            geo.status === "success" ? (geo.countryCode ?? null) : null,
          region: geo.status === "success" ? (geo.regionName ?? null) : null,
          city: geo.status === "success" ? (geo.city ?? null) : null,
          lat: geo.status === "success" ? (geo.lat ?? null) : null,
          lon: geo.status === "success" ? (geo.lon ?? null) : null,
          isp: geo.status === "success" ? (geo.isp ?? null) : null,
          org: geo.status === "success" ? (geo.org ?? null) : null,
          asNumber: geo.status === "success" ? (geo.as ?? null) : null,
        });
      }
    }

    const progress = Math.min(i + BATCH_SIZE, uniqueIps.length);
    console.log(`[geo-ip] Geolocated ${progress}/${uniqueIps.length} IPs`);

    if (i + BATCH_SIZE < uniqueIps.length) {
      await sleep(RATE_LIMIT_DELAY_MS);
    }
  }

  return results;
}
