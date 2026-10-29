import Link from "next/link";

const ACCENTS = {
  blue: { text: "text-blue-400", border: "hover:border-blue-700 hover:shadow-blue-900/20" },
  green: { text: "text-green-400", border: "hover:border-green-700 hover:shadow-green-900/20" },
  purple: { text: "text-purple-400", border: "hover:border-purple-700 hover:shadow-purple-900/20" },
};

export default function LandingPage() {
  return (
    <main className="max-w-5xl mx-auto px-6 py-20">
      <div className="text-center mb-14">
        <h1 className="text-4xl font-bold">ATProto Health</h1>
        <p className="text-gray-400 mt-3 max-w-xl mx-auto">
          Measurements of the AT Protocol network: its servers, its users and its feeds.
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <LandingCard
          href="/overview"
          eyebrow="Infrastructure"
          title="PDS Network"
          description="Personal data servers by country, version and size, account migrations between them, and PDS and repo ages."
          accent="blue"
        />
        <LandingCard
          href="/analytics"
          eyebrow="Activity"
          title="Active Users"
          description="Daily, weekly and monthly active accounts, WAU by account-age cohort, and new account retention."
          accent="green"
        />
        <LandingCard
          href="https://explorer.barn.city"
          eyebrow="Feeds"
          title="Feed Explorer"
          description="A map of Bluesky custom feeds grouped by topic, with search and liveness checks."
          accent="purple"
        />
      </div>
    </main>
  );
}

function LandingCard({
  href,
  eyebrow,
  title,
  description,
  accent,
}: {
  href: string;
  eyebrow: string;
  title: string;
  description: string;
  accent: keyof typeof ACCENTS;
}) {
  const { text, border } = ACCENTS[accent];

  return (
    <Link
      href={href}
      className={`group block rounded-xl border border-gray-800 bg-gray-900 p-8 transition-all hover:shadow-lg ${border}`}
    >
      <p className={`text-xs font-medium uppercase tracking-wide ${text}`}>
        {eyebrow}
      </p>
      <h2 className="text-xl font-semibold mt-2">{title}</h2>
      <p className="text-sm text-gray-400 mt-2 leading-relaxed">
        {description}
      </p>
      <span
        className={`inline-flex items-center gap-1 text-sm mt-5 ${text} opacity-80 group-hover:opacity-100 group-hover:gap-2 transition-all`}
      >
        Explore
        <span aria-hidden="true">→</span>
      </span>
    </Link>
  );
}
