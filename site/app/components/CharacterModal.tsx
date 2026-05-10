"use client";

import { useEffect } from "react";
import type { CharSummaryRow } from "@/lib/queries";

/**
 * Modal that opens when a user clicks one of the three hero stats
 * (warmest / coldest / most polarising). Shows the LLM-generated
 * synthesis of what each fandom camp is saying, plus actual
 * top-upvoted post quotes with Reddit permalinks.
 *
 * The summaries are pre-computed by `cursed reason` and cached in
 * gold/char_summary.parquet. No LLM call happens at view time.
 */
export function CharacterModal({
  summary,
  onClose,
}: {
  summary: CharSummaryRow;
  onClose: () => void;
}) {
  // Close on Escape; restore body scroll on unmount.
  useEffect(() => {
    const handleEsc = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", handleEsc);
    document.body.style.overflow = "hidden";
    return () => {
      document.removeEventListener("keydown", handleEsc);
      document.body.style.overflow = "";
    };
  }, [onClose]);

  const categoryLabel: Record<string, string> = {
    warmest: "warmest reception",
    coldest: "coldest reception",
    most_polarising: "most polarising",
  };

  return (
    <div
      className="fixed inset-0 z-50 flex items-start justify-center overflow-y-auto bg-ink/80 backdrop-blur-sm pt-12 pb-12 px-4"
      onClick={onClose}
    >
      <div
        className="bg-ink border border-smoke/30 max-w-3xl w-full p-8 md:p-12"
        onClick={(e) => e.stopPropagation()}
      >
        {/* header */}
        <div className="flex items-start justify-between mb-8 pb-6 border-b border-smoke/20">
          <div>
            <div className="font-mono text-xs uppercase tracking-wider text-smoke mb-2">
              {categoryLabel[summary.category] ?? summary.category}
            </div>
            <h2 className="font-display italic text-3xl md:text-4xl text-bone">
              {summary.display_name}
            </h2>
            <div className="font-mono text-xs text-smoke mt-2 tabular">
              {summary.mentions.toLocaleString()} mentions ·{" "}
              mean {summary.mean_sentiment_score >= 0 ? "+" : ""}
              {summary.mean_sentiment_score.toFixed(2)} ·{" "}
              polarisation {summary.polarisation_index.toFixed(2)}
            </div>
          </div>
          <button
            onClick={onClose}
            className="font-mono text-smoke hover:text-bone text-sm"
            aria-label="Close"
          >
            close [esc]
          </button>
        </div>

        {/* positive camp */}
        <Camp
          title="The positive camp"
          tone="positive"
          summary={summary.positive_summary}
          examples={summary.positive_examples}
        />

        {/* negative camp */}
        <Camp
          title="The critical camp"
          tone="negative"
          summary={summary.negative_summary}
          examples={summary.negative_examples}
        />

        {/* footer */}
        <div className="mt-10 pt-6 border-t border-smoke/20 font-mono text-xs text-smoke">
          Synthesis by <span className="text-bone">{summary.model}</span>{" "}
          from top-upvoted posts. Refreshed daily after gold rebuild.
        </div>
      </div>
    </div>
  );
}

function Camp({
  title,
  tone,
  summary,
  examples,
}: {
  title: string;
  tone: "positive" | "negative";
  summary: string;
  examples: Array<{
    text: string;
    score: number;
    subreddit: string;
    permalink: string;
  }>;
}) {
  const accentClass = tone === "positive" ? "border-gold/60" : "border-crimson/60";
  return (
    <section className="mt-8">
      <h3 className={`font-display italic text-xl text-bone mb-4 pl-3 border-l-2 ${accentClass}`}>
        {title}
      </h3>
      <p className="text-bone leading-relaxed mb-6">{summary}</p>
      {examples.length > 0 && (
        <div className="space-y-3">
          <div className="font-mono text-xs uppercase tracking-wider text-smoke">
            top-upvoted quotes
          </div>
          {examples.map((q, i) => (
            <a
              key={i}
              href={q.permalink}
              target="_blank"
              rel="noopener noreferrer"
              className="block border-l border-smoke/30 pl-4 py-1 hover:border-bone transition-colors"
            >
              <div className="font-mono text-sm text-bone leading-relaxed">
                &ldquo;{q.text}&rdquo;
              </div>
              <div className="font-mono text-xs text-smoke mt-1 tabular">
                r/{q.subreddit} · {q.score.toLocaleString()}↑ · permalink ↗
              </div>
            </a>
          ))}
        </div>
      )}
    </section>
  );
}
