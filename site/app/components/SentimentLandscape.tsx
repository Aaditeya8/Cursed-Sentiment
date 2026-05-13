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

interface ScatterPoint {
  x: number;
  y: number;
  z: number;
  name: string;
  mentions: number;
  polarisation: number;
  sentiment: number;
}

/**
 * The sentiment landscape — every tracked character as a bubble.
 *   X = mean sentiment    (warmth, -1 to +1)
 *   Y = polarisation index (controversy, 0 to 1)
 *   Z = total mentions     (bubble area)
 *
 * Bubbles render outlined-bone with low-opacity cursed-red fill;
 * hovering pops a Recharts Tooltip with the character name and
 * full stats. Four corner quadrant labels orient the reader without
 * cluttering the data area.
 */
export function SentimentLandscape() {
  const { data, error, loading } = useQuery<PolarisationRow>(Q_TOP_CHARACTERS);

  if (loading) return <Shell />;
  if (error || !data || data.length === 0) return <Shell empty />;

  const points: ScatterPoint[] = data
    .filter(
      (r) =>
        !isNaN(r.polarisation_index) && r.mean_sentiment_score !== undefined,
    )
    .map((r) => ({
      x: Number(r.mean_sentiment_score),
      y: isNaN(r.polarisation_index) ? 0 : Number(r.polarisation_index),
      z: Math.max(60, Math.min(900, Number(r.total_mentions) * 1.2)),
      name: r.display_name ?? r.character_id,
      mentions: Number(r.total_mentions),
      polarisation: Number(r.polarisation_index),
      sentiment: Number(r.mean_sentiment_score),
    }));

  return (
    <div className="landscape-wrap" data-corner="呪 / 03">
      {/* quadrant labels — orient the reader without cluttering data area */}
      <span className="quadrant-label tl">
        Resented<span className="desc">cold · split fandom</span>
      </span>
      <span className="quadrant-label tr">
        Adored<span className="desc">warm · split fandom</span>
      </span>
      <span className="quadrant-label bl">
        Dismissed<span className="desc">cold · settled</span>
      </span>
      <span className="quadrant-label br">
        Liked<span className="desc">warm · settled</span>
      </span>

      <div style={{ height: 520 }}>
        <ResponsiveContainer width="100%" height="100%">
          <ScatterChart margin={{ top: 32, right: 48, left: 48, bottom: 48 }}>
            <CartesianGrid stroke="#2a2024" />
            <XAxis
              type="number"
              dataKey="x"
              domain={[-1, 1]}
              ticks={[-1, -0.5, 0, 0.5, 1]}
              stroke="#6a6055"
              tickLine={false}
              axisLine={false}
              tickFormatter={(v) =>
                v === 0 ? "0" : v > 0 ? `+${v.toFixed(1)}` : v.toFixed(1)
              }
              tick={{ fill: "#b8ac9a", fontSize: 11, fontFamily: "JetBrains Mono, monospace" }}
            />
            <YAxis
              type="number"
              dataKey="y"
              domain={[0, 1]}
              ticks={[0, 0.25, 0.5, 0.75, 1]}
              stroke="#6a6055"
              tickLine={false}
              axisLine={false}
              tickFormatter={(v) => v.toFixed(2)}
              tick={{ fill: "#b8ac9a", fontSize: 11, fontFamily: "JetBrains Mono, monospace" }}
            />
            <ZAxis type="number" dataKey="z" range={[80, 900]} />
            <ReferenceLine
              x={0}
              stroke="#6a6055"
              strokeDasharray="2 4"
              opacity={0.6}
            />
            <ReferenceLine
              y={0.5}
              stroke="#6a6055"
              strokeDasharray="2 4"
              opacity={0.6}
            />
            <Tooltip content={<LandscapeTooltip />} cursor={false} />
            <Scatter
              data={points}
              fill="#c8102e"
              fillOpacity={0.35}
              stroke="#f4ede0"
              strokeWidth={1.2}
            />
          </ScatterChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

function LandscapeTooltip({ active, payload }: { active?: boolean; payload?: Array<{ payload: ScatterPoint }> }) {
  if (!active || !payload || payload.length === 0) return null;
  const p = payload[0].payload;
  return (
    <div
      style={{
        background: "#181214",
        border: "1px solid #2a2024",
        padding: "0.875rem 1.125rem",
        fontFamily: "JetBrains Mono, monospace",
        fontSize: "0.75rem",
        minWidth: 180,
      }}
    >
      <div
        style={{
          fontFamily: "Fraunces, serif",
          fontStyle: "italic",
          fontSize: "1.05rem",
          color: "#f4ede0",
          marginBottom: "0.625rem",
        }}
      >
        {p.name}
      </div>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "auto 1fr",
          columnGap: "1.5rem",
          rowGap: "0.25rem",
        }}
      >
        <span style={{ color: "#6a6055" }}>mentions</span>
        <span
          style={{
            color: "#f4ede0",
            fontVariantNumeric: "tabular-nums",
            textAlign: "right",
          }}
        >
          {p.mentions.toLocaleString()}
        </span>
        <span style={{ color: "#6a6055" }}>sentiment</span>
        <span
          style={{
            color: "#f4ede0",
            fontVariantNumeric: "tabular-nums",
            textAlign: "right",
          }}
        >
          {p.sentiment >= 0 ? "+" : ""}
          {p.sentiment.toFixed(2)}
        </span>
        <span style={{ color: "#6a6055" }}>polarisation</span>
        <span
          style={{
            color: "#f4ede0",
            fontVariantNumeric: "tabular-nums",
            textAlign: "right",
          }}
        >
          {isNaN(p.polarisation) ? "—" : p.polarisation.toFixed(2)}
        </span>
      </div>
    </div>
  );
}

function Shell({ empty = false }: { empty?: boolean }) {
  return (
    <div
      className="landscape-wrap"
      data-corner="呪 / 03"
      style={{
        height: 520,
        display: empty ? "flex" : "block",
        alignItems: "center",
        justifyContent: "center",
        color: "#6a6055",
        fontFamily: "JetBrains Mono, monospace",
        fontSize: "0.875rem",
      }}
    >
      {empty && "No landscape data yet."}
    </div>
  );
}