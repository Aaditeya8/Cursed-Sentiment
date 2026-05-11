"use client";

import { useEffect, useState } from "react";
import { useQuery } from "@/lib/duckdb";
import {
  Q_CHAR_SUMMARIES,
  Q_TOP_CHARACTERS,
  type CharSummaryRow,
  type PolarisationRow,
} from "@/lib/queries";
import { CharacterModal } from "./CharacterModal";

/**
 * Four-card stats strip across the top of the page. New layout uses
 * the .stats grid + .stat flex-column cards defined in globals.css so
 * labels, values, and sublabels align consistently regardless of content.
 *
 *   1. Mentions tracked       (big italic counter, animates on load)
 *   2. Most polarising        (character name, clickable → modal)
 *   3. Warmest reception      (character name, clickable → modal)
 *   4. Coldest reception      (character name, clickable → modal)
 *
 * The three character cards open a CharacterModal showing the LLM
 * synthesis cached by `cursed reason`. Modal layout adapts by category
 * (single camp for warmest/coldest, both for polarising) — that logic
 * lives in CharacterModal.tsx.
 */
export function HeroStats() {
  const top = useQuery<PolarisationRow>(Q_TOP_CHARACTERS);
  const summaries = useQuery<CharSummaryRow>(Q_CHAR_SUMMARIES);
  const [openCategory, setOpenCategory] = useState<string | null>(null);

  if (top.loading) return <HeroStatsSkeleton />;
  if (top.error || !top.data || top.data.length === 0) return <HeroStatsEmpty />;

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

  const openSummary =
    openCategory && summaries.data
      ? summaries.data.find((s) => s.category === openCategory)
      : null;

  const hasSummary = (cat: string) =>
    !!summaries.data?.find((s) => s.category === cat);

  return (
    <>
      <section className="stats">
        <MentionsCard count={totalMentions} characters={top.data.length} />

        <CharStat
          label="Most polarising"
          name={mostPolarising?.display_name ?? "—"}
          sublabel={
            mostPolarising
              ? `index ${(mostPolarising.polarisation_index ?? 0).toFixed(2)} · ${Number(mostPolarising.total_mentions).toLocaleString()} mentions`
              : undefined
          }
          subTone="default"
          onClick={
            hasSummary("most_polarising")
              ? () => setOpenCategory("most_polarising")
              : undefined
          }
        />

        <CharStat
          label="Warmest reception"
          name={mostPositive?.display_name ?? "—"}
          sublabel={
            mostPositive
              ? `score ${formatScore(mostPositive.mean_sentiment_score)} · ${Number(mostPositive.total_mentions).toLocaleString()} mentions`
              : undefined
          }
          subTone="gojo"
          onClick={
            hasSummary("warmest")
              ? () => setOpenCategory("warmest")
              : undefined
          }
        />

        <CharStat
          label="Coldest reception"
          name={mostNegative?.display_name ?? "—"}
          sublabel={
            mostNegative
              ? `score ${formatScore(mostNegative.mean_sentiment_score)} · ${Number(mostNegative.total_mentions).toLocaleString()} mentions`
              : undefined
          }
          subTone="default"
          onClick={
            hasSummary("coldest")
              ? () => setOpenCategory("coldest")
              : undefined
          }
        />
      </section>

      {openSummary && (
        <CharacterModal
          summary={openSummary}
          onClose={() => setOpenCategory(null)}
        />
      )}
    </>
  );
}

// --- mentions card with animated counter ----------------------------------

function MentionsCard({
  count,
  characters,
}: {
  count: number;
  characters: number;
}) {
  const animated = useAnimatedCount(count, 1400);
  return (
    <div className="stat">
      <div className="stat-label">Mentions tracked</div>
      <div className="stat-value big">{animated.toLocaleString()}</div>
      <div className="stat-sub dim">
        across {characters} characters · daily-refreshed
      </div>
    </div>
  );
}

// --- character stat card (clickable) --------------------------------------

interface CharStatProps {
  label: string;
  name: string;
  sublabel?: string;
  subTone: "default" | "gojo";
  onClick?: () => void;
}

function CharStat({ label, name, sublabel, subTone, onClick }: CharStatProps) {
  const interactive = !!onClick;
  return (
    <div
      className={interactive ? "stat clickable" : "stat"}
      onClick={onClick}
      role={interactive ? "button" : undefined}
      tabIndex={interactive ? 0 : undefined}
      onKeyDown={(e) => {
        if (interactive && (e.key === "Enter" || e.key === " ")) {
          e.preventDefault();
          onClick?.();
        }
      }}
    >
      <div className="stat-label">
        {label}
        {interactive && <span className="stat-arrow">↗</span>}
      </div>
      <div className="stat-value name">{name}</div>
      {sublabel && (
        <div className={subTone === "gojo" ? "stat-sub gojo" : "stat-sub"}>
          {sublabel}
        </div>
      )}
    </div>
  );
}

// --- helpers --------------------------------------------------------------

function formatScore(score: number): string {
  return `${score >= 0 ? "+" : ""}${score.toFixed(2)}`;
}

function useAnimatedCount(target: number, duration: number): number {
  const [value, setValue] = useState(0);
  useEffect(() => {
    if (target === 0) {
      setValue(0);
      return;
    }
    const start = performance.now();
    const ease = (t: number) => 1 - Math.pow(1 - t, 3);
    let frame: number;
    const tick = (now: number) => {
      const t = Math.min((now - start) / duration, 1);
      setValue(Math.floor(ease(t) * target));
      if (t < 1) frame = requestAnimationFrame(tick);
    };
    frame = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(frame);
  }, [target, duration]);
  return value;
}

// --- loading / empty states ----------------------------------------------

function HeroStatsSkeleton() {
  return (
    <section className="stats" aria-hidden="true">
      {[0, 1, 2, 3].map((i) => (
        <div key={i} className="stat" style={{ opacity: 0.4 }}>
          <div className="stat-label">&nbsp;</div>
          <div className="stat-value big">—</div>
          <div className="stat-sub dim">&nbsp;</div>
        </div>
      ))}
    </section>
  );
}

function HeroStatsEmpty() {
  return (
    <section className="stats">
      <div className="stat" style={{ gridColumn: "1 / -1" }}>
        <div className="stat-label">No headline stats yet</div>
        <div className="stat-value name">—</div>
        <div className="stat-sub dim">Run the gold pipeline to populate.</div>
      </div>
    </section>
  );
}
