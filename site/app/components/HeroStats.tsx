"use client";

import { useQuery } from "@/lib/duckdb";
import { Q_TOP_CHARACTERS, type PolarisationRow } from "@/lib/queries";

/**
 * Four stat counters across the top of the page.
 *
 * The dashboard hero is a "field manual masthead": small text labels
 * with large monospace numbers. The values come from the polarisation
 * rankings (top 6 characters by mention volume) so the counters are
 * always grounded in the freshest gold rebuild.
 */
export function HeroStats() {
  const { data, error, loading } = useQuery<PolarisationRow>(Q_TOP_CHARACTERS);

  if (loading) return <HeroSkeleton />;
  if (error || !data || data.length === 0) return <HeroEmpty />;

  const totalMentions = data.reduce((sum, r) => sum + Number(r.total_mentions), 0);
  const mostPolarising = [...data].sort(
    (a, b) => (b.polarisation_index ?? 0) - (a.polarisation_index ?? 0),
  )[0];
  const mostPositive = [...data].sort(
    (a, b) => b.mean_sentiment_score - a.mean_sentiment_score,
  )[0];
  const mostNegative = [...data].sort(
    (a, b) => a.mean_sentiment_score - b.mean_sentiment_score,
  )[0];

  return (
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
      />
      <Stat
        label="warmest reception"
        value={mostPositive?.display_name ?? "—"}
        sublabel={
          mostPositive
            ? `score ${mostPositive.mean_sentiment_score.toFixed(2)}`
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
      />
    </div>
  );
}

function Stat({
  label,
  value,
  sublabel,
}: {
  label: string;
  value: string;
  sublabel?: string;
}) {
  return (
    <div>
      <div className="font-mono text-xs uppercase tracking-wider text-smoke mb-2">
        {label}
      </div>
      <div className="font-display text-2xl md:text-3xl tabular text-bone">
        {value}
      </div>
      {sublabel && (
        <div className="font-mono text-xs text-smoke mt-1 tabular">{sublabel}</div>
      )}
    </div>
  );
}

function HeroSkeleton() {
  return (
    <div className="grid grid-cols-2 gap-8 md:grid-cols-4 mb-16 mt-12">
      {Array.from({ length: 4 }).map((_, i) => (
        <div key={i}>
          <div className="h-3 w-24 bg-smoke/20 rounded mb-3" />
          <div className="h-7 w-16 bg-smoke/10 rounded" />
        </div>
      ))}
    </div>
  );
}

function HeroEmpty() {
  return (
    <div className="border border-smoke/30 p-8 mb-16 mt-12 font-mono text-sm text-smoke">
      No gold data yet — run <code className="text-bone">uv run cursed gold</code>{" "}
      after the pipeline produces silver.
    </div>
  );
}

const fmt = (n: number) => n.toLocaleString("en-US");