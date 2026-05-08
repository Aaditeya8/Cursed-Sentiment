import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./app/**/*.{ts,tsx}", "./components/**/*.{ts,tsx}"],
  theme: {
    extend: {
      // Field-manual palette: muted, slightly cold, with reserved accents.
      // Crimson is held back for *one* visual purpose only — the
      // chapter-236 marker on the headline chart. Used everywhere else
      // it would lose meaning.
      colors: {
        ink: "#0a0a0a",       // page background
        bone: "#e8e6e3",      // primary text
        smoke: "#9a9890",     // secondary text, axis labels
        moss: "#8a8f7a",      // chart line — neutral character
        gold: "#e0a82e",      // chart line — Sukuna; warning labels
        indigo: "#6366f1",    // chart line — Yuji; info labels
        crimson: "#c92a2a",   // RESERVED for chapter-236 only
      },
      fontFamily: {
        // Display: Fraunces italic at large sizes only — the section
        // titles you'd see at the top of a printed manual chapter.
        display: ["var(--font-fraunces)", "Georgia", "serif"],
        // Body: IBM Plex Sans. Wide enough to read at small sizes,
        // boring enough not to draw attention.
        sans: ["var(--font-plex-sans)", "system-ui", "sans-serif"],
        // Data and code: IBM Plex Mono. Tabular-nums for chart labels.
        mono: ["var(--font-plex-mono)", "monospace"],
      },
      // Tighter line-height for headings; default for body text.
      fontSize: {
        "headline": ["3rem", { lineHeight: "1.05", letterSpacing: "-0.02em" }],
        "section":  ["1.5rem", { lineHeight: "1.2", letterSpacing: "-0.01em" }],
      },
    },
  },
  plugins: [],
};

export default config;