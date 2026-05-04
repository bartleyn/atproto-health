"use client";

import { useState, useEffect } from "react";

interface CollapsibleSectionProps {
  title: string;
  subtitle?: string;
  storageKey: string;
  defaultOpen?: boolean;
  children: React.ReactNode;
}

export function CollapsibleSection({
  title,
  subtitle,
  storageKey,
  defaultOpen = true,
  children,
}: CollapsibleSectionProps) {
  const [open, setOpen] = useState(defaultOpen);
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
    const stored = localStorage.getItem(`collapse:${storageKey}`);
    if (stored !== null) setOpen(stored === "1");
  }, [storageKey]);

  function toggle() {
    setOpen(v => {
      localStorage.setItem(`collapse:${storageKey}`, !v ? "1" : "0");
      return !v;
    });
  }

  return (
    <section>
      <button
        onClick={toggle}
        className="flex items-center justify-between gap-2 w-full text-left group py-2 -my-2 mb-1"
      >
        <h2 className="text-xl font-semibold text-gray-200">{title}</h2>
        <svg
          className={`w-5 h-5 text-gray-500 group-hover:text-gray-300 transition-transform flex-shrink-0 ${open ? "" : "-rotate-90"}`}
          fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
        >
          <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
        </svg>
      </button>
      {subtitle && <p className="text-xs text-gray-500 mb-4">{subtitle}</p>}
      {(!mounted || open) && <div>{children}</div>}
    </section>
  );
}
