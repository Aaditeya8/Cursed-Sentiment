import type { Metadata } from "next";
import { Fraunces, IBM_Plex_Mono, IBM_Plex_Sans } from "next/font/google";
import "./globals.css";

// Display: italic Fraunces for headings only — the section titles you'd
// see at the top of a printed manual chapter.
const fraunces = Fraunces({
  subsets: ["latin"],
  weight: ["400", "500"],
  style: ["italic"],
  variable: "--font-fraunces",
  display: "swap",
});

// Body: IBM Plex Sans. Reads cleanly at small sizes, doesn't draw
// attention away from the data.
const plexSans = IBM_Plex_Sans({
  subsets: ["latin"],
  weight: ["400", "500", "600"],
  variable: "--font-plex-sans",
  display: "swap",
});

// Mono: tabular-nums for chart axes and data tables.
const plexMono = IBM_Plex_Mono({
  subsets: ["latin"],
  weight: ["400", "500"],
  variable: "--font-plex-mono",
  display: "swap",
});

export const metadata: Metadata = {
  title: "Cursed Sentiment",
  description:
    "What 280,000 fans really feel about Gojo Satoru. " +
    "Sentiment analytics over five years of r/JuJutsuKaisen discussion.",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html
      lang="en"
      className={`${fraunces.variable} ${plexSans.variable} ${plexMono.variable}`}
    >
      <body className="bg-ink text-bone min-h-screen">
        <div className="mx-auto max-w-6xl px-6 py-12 md:px-12 md:py-20">
          {children}
        </div>
      </body>
    </html>
  );
}