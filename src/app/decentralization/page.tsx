export default function DecentralizationPage() {
  return (
    <main className="max-w-3xl mx-auto px-6 py-16">
      <h1 className="text-3xl font-bold">How Decentralization Works</h1>
      <p className="text-gray-400 mt-2">
        A quick tour of the pieces that make AT Protocol different from a
        typical social platform.
      </p>

      <div className="mt-12 space-y-10">
        <Section title="Your data lives on a PDS, not inside an app">
          A Personal Data Server (PDS) stores your posts, likes, and follows
          as a signed repository of records — independent of any single
          application. Bluesky the app is one client that reads and writes
          to that repository; it isn&apos;t the repository&apos;s owner.
          Anyone can run a PDS, from a company&apos;s datacenter to a
          Raspberry Pi at home.
        </Section>

        <Section title="Your identity is a DID, not a username">
          Every account is a Decentralized Identifier (DID) — a permanent
          ID that doesn&apos;t depend on which server hosts you. Handles
          (the @name.bsky.social you see) are just a human-friendly label
          pointed at that DID, and can be repointed at any domain you
          control.
        </Section>

        <Section title="You can move your PDS without losing your identity">
          Because identity (DID) is separate from storage (PDS), an account
          can migrate to a different PDS — including a self-hosted one —
          and keep its followers, history, and handle. This dashboard&apos;s{" "}
          <a href="/overview" className="underline hover:text-gray-100">
            Analysis &amp; Charts
          </a>{" "}
          section tracks how much of that actually happens in practice.
        </Section>

        <Section title="The network is the protocol, not one company's servers">
          Apps, feed generators, and moderation services are all separate,
          swappable pieces built on the same open protocol. A single
          operator going down, changing policy, or shutting off — including
          Bluesky PBC itself — doesn&apos;t erase anyone&apos;s data or
          social graph, because nothing lives exclusively on its
          infrastructure.
        </Section>

        <Section title="Decentralized in design, concentrated in practice">
          Design and reality can diverge: most accounts today still sit on
          Bluesky-operated PDSes, which recreates some of the centralization
          the protocol is meant to avoid. The{" "}
          <a href="/overview" className="underline hover:text-gray-100">
            Analysis &amp; Charts
          </a>{" "}
          page measures exactly how concentrated the network currently is —
          by host, by geography, and over time.
        </Section>
      </div>

      <p className="text-xs text-gray-600 mt-16">
        For the full protocol spec, see{" "}
        <a
          href="https://atproto.com/guides/overview"
          target="_blank"
          rel="noopener noreferrer"
          className="underline hover:text-gray-400"
        >
          atproto.com
        </a>
        .
      </p>
    </main>
  );
}

function Section({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <section>
      <h2 className="text-lg font-semibold text-gray-100">{title}</h2>
      <p className="text-sm text-gray-400 mt-2 leading-relaxed">{children}</p>
    </section>
  );
}
