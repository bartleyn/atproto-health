import postgres from "postgres";

if (!process.env.DATABASE_URL) {
  throw new Error("DATABASE_URL is not set");
}

const sql = postgres(process.env.DATABASE_URL, {
  max: 10,
  idle_timeout: 30,
  connect_timeout: 10,
  types: {
    // Return bigint columns as JS number rather than string.
    // All our IDs fit comfortably within Number.MAX_SAFE_INTEGER.
    bigint: postgres.BigInt,
  },
});

export default sql;
