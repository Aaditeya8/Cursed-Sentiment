"use client";

import { useState } from "react";
import { useQuery } from "@/lib/duckdb";
import {
  Q_CHAR_SUMMARIES,
  Q_TOP_CHARACTERS,
  type CharSummaryRow,
  type PolarisationRow,
} from "@/lib/queries";
import { CharacterModal } from "./CharacterModal";

/**
 * Four stat counters across the top of the page. Three of them
 * (most polarising, warmest, coldest) are clickable — opening a modal
 * with a pre-computed LLM synthesis of what the fandom is saying about
 * that character.
 */
export function HeroStats() {
  const top = useQuery<PolarisationRow>(Q_TOP_CHARACTERS);
  const summaries = useQuery<CharSummaryRow>(Q_CHAR_SUMMARIES);
  const [openCategory, setOpenCategory] = useState<string | null>(null);

  if (top.loading) return <HeroSkeleton />;
  if (top.error || !top.data || top.data.length === 0) return <HeroEmpty />;

  const totalMentions = top.data.reduce(
    (sum, r) => sum + Number(r.total_mentions),
    0,
  );
  const mostPolarising = [...top.data].sort(
    (a, b) => (b.polarisation_index ?? 0) - (a.polarisation_index ?? 0),
  )[0];
  const mostPositive = [...top.data].sort(
    (a, b) => b.mean_sentiment_score - a.mean_sentiment_score,
  )[0];
  const mostNegative = [...top.data].sort(
    (a, b) => a.mean_sentiment_score - b.mean_sentiment_score,
  )[0];

  // Find the matching summary for the currently-open category.
  const openSummary =
    openCategory && summaries.data
      ? summaries.data.find((s) => s.category === openCategory)
      : null;

  // Only enable click if we actually have a summary cached for this category.
  const hasSummary = (cat: string) =>
    !!summaries.data?.find((s) => s.category === cat);

  return (
    <>
      <div className="grid grid-cols-2 gap-8 md:grid-cols-4 mb-16 mt-12">
        <Stat label="mentions tracked" value={fmt(totalMentions)} />
        <Stat
          label="most polarising"
          value={mostPolarising?.display_name ?? "—"}
          sublabel={
            mostPolarising
              ? `index ${(mostPolarising.polarisation_index ?? 0).toFixed(2)}`
              : undefined
          }
          onClick={
            hasSummary("most_polarising")
              ? () => setOpenCategory("most_polarising")
              : undefined
          }
        />
        <Stat
          label="warmest reception"
          value={mostPositive?.display_name ?? "—"}
          sublabel={
            mostPositive
              ? `score ${mostPositive.mean_sentiment_score.toFixed(2)}`
              : undefined
          }
          onClick={
            hasSummary("warmest")
              ? () => setOpenCategory("warmest")
              : undefined
          }
        />
        <Stat
          label="coldest reception"
          value={mostNegative?.display_name ?? "—"}
          sublabel={
            mostNegative
              ? `score ${mostNegative.mean_sentiment_score.toFixed(2)}`
              : undefined
          }
          onClick={
            hasSummary("coldest")
              ? () => setOpenCategory("coldest")
              : undefined
          }
        />
      </div>

      {openSummary && (
        <CharacterModal
          summary={openSummary}
          onClose={() => setOpenCategory(null)}
        />
      )}
    </>
  );
}

function Stat({
  label,
  value,
  sublabel,
  onClick,
}: {
  label: string;
  value: string;
  sublabel?: string;
  onClick?: () => void;
}) {
  const interactive = !!onClick;
  return (
    <div
      onClick={onClick}
      className={
        interactive
          ? "cursor-pointer group transition-colors"
          : ""
      }
      role={interactive ? "button" : undefined}
      tabIndex={interactive ? 0 : undefined}
      onKeyDown={(e) => {
        if (interactive && (e.key === "Enter" || e.key === " ")) {
          e.preventDefault();
          onClick?.();
        }
      }}
    >
      <div className="font-mono text-xs uppercase tracking-wider text-smoke mb-2">
        {label}
        {interactive && (
          <span className="ml-2 text-smoke/50 group-hover:text-gold transition-colors">
            ↗
          </span>
        )}
      </div>
      <div
        className={`font-display text-2xl md:text-3xl tabular ${
          interactive ? "text-bone group-hover:text-gold transition-colors" : "text-bone"
        }`}
      >
        {value}
      </div>
      {sublabel && (
        <div className="font-mono text-xs text-smoke mt-1 tabular">
          {sublabel}
        </div>
      )}
    </div>
  );
}

function fmt(n: number): string {
  return n.toLocaleString();
}

function HeroSkeleton() {
  return (
    <div className="grid grid-cols-2 gap-8 md:grid-cols-4 mb-16 mt-12">
      {[0, 1, 2, 3].map((i) => (
        <div key={i} className="h-20 bg-smoke/5 animate-pulse" />
      ))}
    </div>
  );
}

function HeroEmpty() {
  return (
    <div className="font-mono text-xs text-smoke mb-16 mt-12">
      No headline stats yet. Run the gold pipeline.
    </div>
  );
}