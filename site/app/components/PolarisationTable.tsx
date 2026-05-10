"use client";

import { useQuery } from "@/lib/duckdb";
import { Q_POLARISATION_RANKING, type PolarisationRow } from "@/lib/queries";

/**
 * Top characters by polarisation index (variance, not just mean).
 *
 * The methodology page explains polarisation in detail. The summary:
 * 1.0 means the fandom is split exactly 50/50 positive vs negative on
 * this character; 0.0 means everyone agrees one way or the other.
 */
export function PolarisationTable() {
  const { data, error, loading } = useQuery<PolarisationRow>(Q_POLARISATION_RANKING);

  if (loading) return <TableSkeleton />;
  if (error || !data || data.length === 0) return <TableEmpty />;

  return (
    <div className="mt-16">
      <div className="font-display italic text-section text-bone mb-1">
        The polarisation index
      </div>
      <div className="font-mono text-xs uppercase tracking-wider text-smoke mb-2">
        characters who split the fandom most · top 10
      </div>
      <div className="font-mono text-xs text-smoke max-w-2xl mb-6 leading-relaxed normal-case tracking-normal">
        Polarisation 1.0 = fandom evenly split positive vs. negative, not hated.
        Mean is the average sentiment score across all mentions, scaled
        −1 (entirely negative) to +1 (entirely positive); it shows which
        way the disagreement leans on average. Min 50 mentions; smaller
        samples are too noisy to rank fairly.
      </div>

      <table className="w-full font-mono text-sm">
        <thead>
          <tr className="border-b border-smoke/20 text-smoke text-xs uppercase tracking-wider">
            <th className="text-left py-3 pr-4 font-normal">#</th>
            <th className="text-left py-3 pr-4 font-normal">character</th>
            <th className="text-right py-3 px-4 font-normal">mentions</th>
            <th className="text-right py-3 px-4 font-normal">mean</th>
            <th className="text-right py-3 pl-4 font-normal">polarisation</th>
          </tr>
        </thead>
        <tbody className="text-bone">
          {data
            .filter((r) => r.polarisation_index !== null && !isNaN(r.polarisation_index))
            .map((r, i) => (
            <tr key={r.character_id} className="border-b border-smoke/10">
              <td className="py-3 pr-4 text-smoke tabular">{i + 1}</td>
              <td className="py-3 pr-4">{r.display_name}</td>
              <td className="py-3 px-4 text-right tabular">
                {Number(r.total_mentions).toLocaleString()}
              </td>
              <td className="py-3 px-4 text-right tabular">
                {r.mean_sentiment_score >= 0 ? "+" : ""}
                {r.mean_sentiment_score.toFixed(2)}
              </td>
              <td className="py-3 pl-4 text-right tabular text-gold">
                {r.polarisation_index?.toFixed(2) ?? "—"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function TableSkeleton() {
  return (
    <div className="mt-16">
      <div className="h-5 w-48 bg-smoke/20 mb-2" />
      <div className="h-3 w-64 bg-smoke/10 mb-6" />
      <div className="h-64 bg-smoke/5" />
    </div>
  );
}

function TableEmpty() {
  return (
    <div className="mt-16 border border-smoke/30 p-8 font-mono text-sm text-smoke">
      No polarisation data yet — run the gold build.
    </div>
  );
}