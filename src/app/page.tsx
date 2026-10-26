import Link from "next/link";

export default function LandingPage() {
  return (
    <main className="max-w-5xl mx-auto px-6 py-20">
      <div className="text-center mb-14">
        <h1 className="text-4xl font-bold">ATProto Health</h1>
        <p className="text-gray-400 mt-3 max-w-xl mx-auto">
          A dashboard for the AT Protocol ecosystem — and a look at what
          decentralization actually buys you.
        </p>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-6">
        <LandingCard
          href="/overview"
          eyebrow="Data"
          title="Analysis & Charts"
          description="PDS infrastructure, geography, user distribution, migrations, and account longevity across the network."
          accent="blue"
        />
        <LandingCard
          href="/decentralization"
          eyebrow="Concepts"
          title="How Decentralization Works"
          description="What a PDS is, why your identity and data aren't locked to one company, and what that changes."
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
  accent: "blue" | "purple";
}) {
  const accentText = accent === "blue" ? "text-blue-400" : "text-purple-400";
  const accentBorder =
    accent === "blue"
      ? "hover:border-blue-700 hover:shadow-blue-900/20"
      : "hover:border-purple-700 hover:shadow-purple-900/20";

  return (
    <Link
      href={href}
      className={`group block rounded-xl border border-gray-800 bg-gray-900 p-8 transition-all hover:shadow-lg ${accentBorder}`}
    >
      <p className={`text-xs font-medium uppercase tracking-wide ${accentText}`}>
        {eyebrow}
      </p>
      <h2 className="text-xl font-semibold mt-2">{title}</h2>
      <p className="text-sm text-gray-400 mt-2 leading-relaxed">
        {description}
      </p>
      <span
        className={`inline-flex items-center gap-1 text-sm mt-5 ${accentText} opacity-80 group-hover:opacity-100 group-hover:gap-2 transition-all`}
      >
        Explore
        <span aria-hidden="true">→</span>
      </span>
    </Link>
  );
}
