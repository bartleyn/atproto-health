import postgres from "postgres";

if (!process.env.DATABASE_URL) {
  throw new Error("DATABASE_URL is not set");
}

const sql = postgres(process.env.DATABASE_URL, {
  max: 10,
  idle_timeout: 30,
  connect_timeout: 30,
  types: {
    // Return bigint columns as JS number rather than string.
    // All our IDs fit comfortably within Number.MAX_SAFE_INTEGER.
    bigint: postgres.BigInt,
  },
});

// Transient connection errors that should be retried rather than crash a
// long-running collector. postgres.js opens a fresh pooled connection for the
// retry, so the dropped socket recovers transparently.
const TRANSIENT_DB_ERRORS = new Set([
  "CONNECTION_ENDED", "CONNECTION_CLOSED", "CONNECTION_DESTROYED",
  "ECONNRESET", "EPIPE", "ETIMEDOUT", "57P01", // 57P01 = admin shutdown
  "CONNECT_TIMEOUT", // postgres.js: TCP connect didn't complete within connect_timeout
]);

/**
 * Run a DB operation, retrying transient connection drops with backoff.
 * Wrap individual writes in the hot path of multi-hour collectors so a single
 * blip (machine sleep, network hiccup, server restart) doesn't kill the whole run.
 */
export async function withDbRetry<T>(fn: () => Promise<T>, label = "db", maxAttempts = 5): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    try {
      return await fn();
    } catch (err: unknown) {
      const code = (err as { code?: string; errno?: string })?.code
        ?? (err as { errno?: string })?.errno;
      if (attempt >= maxAttempts || !code || !TRANSIENT_DB_ERRORS.has(code)) throw err;
      const delay = Math.min(attempt * 2000, 10_000);
      console.warn(`[${label}] transient DB error ${code}, retry ${attempt}/${maxAttempts - 1} in ${delay}ms`);
      await new Promise(r => setTimeout(r, delay));
    }
  }
}

export default sql;
