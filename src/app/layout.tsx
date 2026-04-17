import type { Metadata } from "next";
import Link from "next/link";
import "./globals.css";

export const metadata: Metadata = {
  title: "ATProto Health",
  description: "AT Protocol ecosystem health dashboard",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className="bg-gray-950 text-gray-100 min-h-screen antialiased">
        <nav className="px-8 py-3">
          <div className="max-w-6xl mx-auto flex justify-center gap-8 text-sm">
            <Link href="/" className="text-gray-400 underline hover:text-gray-100 transition-colors">
              PDS Network
            </Link>
            <Link href="/migrations" className="text-gray-400 underline hover:text-gray-100 transition-colors">
              Migrations
            </Link>
            <Link href="/longevity" className="text-gray-400 underline hover:text-gray-100 transition-colors">
              Longevity
            </Link>
          </div>
        </nav>
        {children}
      </body>
    </html>
  );
}
