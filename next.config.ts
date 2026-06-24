import type { NextConfig } from "next";
import path from "path";

const securityHeaders = [
  { key: "X-Content-Type-Options", value: "nosniff" },
  { key: "X-Frame-Options", value: "DENY" },
  { key: "Referrer-Policy", value: "strict-origin-when-cross-origin" },
  {
    key: "Content-Security-Policy",
    value: [
      "default-src 'self'",
      // Recharts and react-simple-maps inject inline styles at runtime
      "style-src 'self' 'unsafe-inline'",
      // Next.js requires unsafe-inline for its hydration bootstrap scripts
      "script-src 'self' 'unsafe-inline'",
      // react-simple-maps fetches world-atlas GeoJSON from jsDelivr at runtime
      "connect-src 'self' https://cdn.jsdelivr.net",
      // SVG map assets and chart rendering
      "img-src 'self' data:",
      "font-src 'self'",
      "object-src 'none'",
      "base-uri 'self'",
      "form-action 'self'",
      "frame-ancestors 'none'",
    ].join("; "),
  },
];

const nextConfig: NextConfig = {
  turbopack: {
    root: path.resolve(__dirname),
  },
  async headers() {
    return [{ source: "/(.*)", headers: securityHeaders }];
  },
};

export default nextConfig;
