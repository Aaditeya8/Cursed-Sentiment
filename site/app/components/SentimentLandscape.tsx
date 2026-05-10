"use client";

import {
  CartesianGrid,
  ReferenceLine,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
} from "recharts";

import { useQuery } from "@/lib/duckdb";
import { Q_TOP_CHARACTERS, type PolarisationRow } from "@/lib/queries";

/**
 * Sentiment landscape — every character placed in 2D by:
 *   X = mean sentiment score (warmth, -1 to +1)
 *   Y = polarisation index (controversy, 0 to 1)
 *   bubble size = total mention count (volume)
 *
 * Not an embedding projection (we don't have vectors), but the shape
 * tells the same kind of story: clusters in the upper-right are
 * "talked about a lot, fandom split" — characters who divide the
 * audience. Lower-left is "low volume, settled opinion."
 */
export function SentimentLandscape() {
  const { data, error, loading } = useQuery<PolarisationRow>(Q_TOP_CHARACTERS);

  if (loading) return <ChartSkeleton />;
  if (error || !data || data.length === 0) return <ChartEmpty />;

  const points = data
    .filter(
      (r) =>
        !isNaN(r.polarisation_index) && r.mean_sentiment_score !== undefined,
    )
    .map((r) => ({
      x: r.mean_sentiment_score,
      y: isNaN(r.polarisation_index) ? 0 : r.polarisation_index,
      z: Math.max(20, Math.min(400, Number(r.total_mentions) * 2)),
      name: r.display_name ?? r.character_id,
      mentions: Number(r.total_mentions),
      polarisation: r.polarisation_index,
      sentiment: r.mean_sentiment_score,
    }));

  return (
    <div className="mt-16">
      <div className="font-display italic text-section text-bone mb-1">
        The sentiment landscape
      </div>
      <div className="font-mono text-xs uppercase tracking-wider text-smoke mb-6">
        warmth × controversy × volume · every tracked character
      </div>

      <div className="h-96 -mx-2">
        <ResponsiveContainer width="100%" height="100%">
          <ScatterChart margin={{ top: 20, right: 32, left: 32, bottom: 48 }}>
            <CartesianGrid />
            <XAxis
              type="number"
              dataKey="x"
              domain={[-1, 1]}
              ticks={[-1, -0.5, 0, 0.5, 1]}
              stroke="var(--color-smoke)"
              tickLine={false}
              axisLine={false}
              tickFormatter={(v) =>
                v === 0 ? "neutral" : v > 0 ? `+${v.toFixed(1)}` : v.toFixed(1)
              }
              label={{
                value: "← cold        warmth        warm →",
                position: "insideBottom",
                offset: -16,
                fill: "var(--color-smoke)",
                fontSize: 11,
                fontFamily: "var(--font-plex-mono)",
              }}
            />
            <YAxis
              type="number"
              dataKey="y"
              domain={[0, 1]}
              ticks={[0, 0.25, 0.5, 0.75, 1]}
              stroke="var(--color-smoke)"
              tickLine={false}
              axisLine={false}
              tickFormatter={(v) => v.toFixed(2)}
              label={{
                value: "polarisation",
                angle: -90,
                position: "insideLeft",
                offset: -8,
                fill: "var(--color-smoke)",
                fontSize: 11,
                fontFamily: "var(--font-plex-mono)",
              }}
            />
            <ZAxis type="number" dataKey="z" range={[40, 400]} />
            <ReferenceLine
              x={0}
              stroke="var(--color-smoke)"
              strokeDasharray="2 2"
              opacity={0.4}
            />
            <Tooltip content={<LandscapeTooltip />} />
            <Scatter
              data={points}
              fill="var(--color-gold)"
              fillOpacity={0.6}
              stroke="var(--color-bone)"
              strokeWidth={1}
            />
          </ScatterChart>
        </ResponsiveContainer>
      </div>

      <div className="mt-2 font-mono text-xs text-smoke max-w-2xl leading-relaxed">
        Each bubble is a character. Position on the X axis is mean sentiment
        across all classifications. Y is the polarisation index — how
        evenly the fandom is split positive vs. negative. Bubble size is
        total mention volume. The interesting region is upper-right: high
        volume, evenly split — characters the fandom is loud about and
        can&apos;t agree on.
      </div>
    </div>
  );
}

function LandscapeTooltip({ active, payload }: any) {
  if (!active || !payload || payload.length === 0) return null;
  const p = payload[0].payload;
  return (
    <div className="bg-ink border border-smoke/40 p-3 font-mono text-xs">
      <div className="text-bone font-medium mb-2">{p.name}</div>
      <div className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1">
        <span className="text-smoke">mentions</span>
        <span className="text-bone tabular text-right">
          {p.mentions.toLocaleString()}
        </span>
        <span className="text-smoke">sentiment</span>
        <span className="text-bone tabular text-right">
          {p.sentiment >= 0 ? "+" : ""}
          {p.sentiment.toFixed(2)}
        </span>
        <span className="text-smoke">polarisation</span>
        <span className="text-bone tabular text-right">
          {isNaN(p.polarisation) ? "—" : p.polarisation.toFixed(2)}
        </span>
      </div>
    </div>
  );
}

function ChartSkeleton() {
  return <div className="h-96 mt-16 bg-smoke/5 animate-pulse" />;
}

function ChartEmpty() {
  return (
    <div className="h-96 mt-16 border border-smoke/30 flex items-center justify-center font-mono text-sm text-smoke">
      No landscape data yet.
    </div>
  );
}