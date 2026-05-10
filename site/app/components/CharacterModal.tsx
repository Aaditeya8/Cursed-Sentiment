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
              {Number(summary.mentions).toLocaleString()} mentions ·{" "}
              mean {Number(summary.mean_sentiment_score) >= 0 ? "+" : ""}
              {Number(summary.mean_sentiment_score).toFixed(2)} ·{" "}
              polarisation {Number(summary.polarisation_index ?? 0).toFixed(2)}
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
          examples={toArray(summary.positive_examples)}
        />

        {/* negative camp */}
        <Camp
          title="The critical camp"
          tone="negative"
          summary={summary.negative_summary}
          examples={toArray(summary.negative_examples)}
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

interface QuoteRow {
  text: string;
  score: number;
  subreddit: string;
  permalink: string;
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
  examples: QuoteRow[];
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
            <a key={i} href={q.permalink} target="_blank" rel="noopener noreferrer" className="block border-l border-smoke/30 pl-4 py-1 hover:border-bone transition-colors">
              <div className="font-mono text-sm text-bone leading-relaxed">
                &ldquo;{q.text}&rdquo;
              </div>
              <div className="font-mono text-xs text-smoke mt-1 tabular">
                r/{q.subreddit} · {Number(q.score).toLocaleString()}↑ · permalink ↗
              </div>
            </a>
          ))}
        </div>
      )}
    </section>
  );
}

/**
 * DuckDB-WASM returns list<struct> columns as Arrow Vector objects, not
 * plain JS arrays. We coerce to a plain array of plain objects before
 * mapping; .toJSON() on each Arrow row flattens nested fields too.
 */
function toArray(v: unknown): QuoteRow[] {
  if (!v) return [];
  if (Array.isArray(v)) return v as QuoteRow[];

  // Arrow Vector — has a toArray() method
  const maybe = v as { toArray?: () => unknown[] };
  if (typeof maybe.toArray === "function") {
    return maybe.toArray().map((row) => {
      const r = row as { toJSON?: () => QuoteRow };
      if (r && typeof r.toJSON === "function") return r.toJSON();
      return row as QuoteRow;
    });
  }
  return [];
}