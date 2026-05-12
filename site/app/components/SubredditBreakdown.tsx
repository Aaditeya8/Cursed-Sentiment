"use client";

import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { useQuery } from "@/lib/duckdb";
import {
  Q_SUBREDDIT_BREAKDOWN,
  type SubredditBreakdownRow,
} from "@/lib/queries";

const SUBREDDIT_COLORS: Record<string, string> = {
  jujutsukaisen: "#e6203f", // cursed-red — main hub, flagship
  jujutsushi:    "#5a9fe8", // gojo-blue — analytical / manga-only
  jujutsufolk:   "#d4a857", // gold — memes / casual chaos
};

const SUBREDDIT_LABELS: Record<string, string> = {
  jujutsukaisen: "r/JuJutsuKaisen",
  jujutsushi:    "r/Jujutsushi",
  jujutsufolk:   "r/Jujutsufolk",
};

/**
 * Per-character mention split across the three subreddits. Wrapped in
 * a paper chart-card with the 呪 / 02 corner mark. Stacked horizontal
 * bars reveal where each character's conversation actually lives.
 */
export function SubredditBreakdown() {
  const { data, error, loading } = useQuery<SubredditBreakdownRow>(
    Q_SUBREDDIT_BREAKDOWN,
  );

  if (loading) return <ChartShell />;
  if (error || !data || data.length === 0) return <ChartShell empty />;

  const pivoted = pivotBySubreddit(data);
  const subreddits = Array.from(new Set(data.map((r) => r.subreddit))).sort();

  return (
    <div className="chart-card" data-corner="呪 / 02">
      <div style={{ height: 420 }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={pivoted}
            layout="vertical"
            margin={{ top: 16, right: 32, left: 16, bottom: 24 }}
          >
            <CartesianGrid horizontal={false} stroke="#2a2024" />
            <XAxis
              type="number"
              stroke="#6a6055"
              tickLine={false}
              axisLine={false}
              tick={{ fill: "#b8ac9a", fontSize: 11, fontFamily: "JetBrains Mono, monospace" }}
            />
            <YAxis
              type="category"
              dataKey="display_name"
              stroke="#6a6055"
              tickLine={false}
              axisLine={false}
              width={140}
              tick={{ fill: "#f4ede0", fontSize: 13, fontFamily: "Fraunces, serif", fontStyle: "italic" }}
            />
            <Tooltip content={<TooltipBox />} cursor={{ fill: "rgba(255,255,255,0.03)" }} />
            <Legend
              wrapperStyle={{ paddingTop: 16, fontSize: 11 }}
              iconType="square"
              formatter={(value: string) => SUBREDDIT_LABELS[value] ?? value}
            />
            {subreddits.map((sub) => (
              <Bar
                key={sub}
                dataKey={sub}
                stackId="a"
                fill={SUBREDDIT_COLORS[sub] ?? "#7c8089"}
                isAnimationActive={false}
              />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

function pivotBySubreddit(rows: SubredditBreakdownRow[]) {
  const byChar = new Map<string, Record<string, string | number>>();
  for (const r of rows) {
    if (!byChar.has(r.display_name)) {
      byChar.set(r.display_name, { display_name: r.display_name });
    }
    byChar.get(r.display_name)![r.subreddit] = Number(r.mention_count);
  }
  return Array.from(byChar.values()).sort((a, b) => {
    const aTotal = Object.entries(a)
      .filter(([k]) => k !== "display_name")
      .reduce((sum, [, v]) => sum + (v as number), 0);
    const bTotal = Object.entries(b)
      .filter(([k]) => k !== "display_name")
      .reduce((sum, [, v]) => sum + (v as number), 0);
    return bTotal - aTotal;
  });
}

function TooltipBox({ active, payload, label }: { active?: boolean; payload?: Array<{ dataKey: string; value: number; color: string }>; label?: string }) {
  if (!active || !payload || payload.length === 0) return null;
  const total = payload.reduce(
    (s, p) => s + Number(p.value || 0),
    0,
  );
  return (
    <div
      style={{
        background: "#181214",
        border: "1px solid #2a2024",
        padding: "0.75rem 1rem",
        fontFamily: "JetBrains Mono, monospace",
        fontSize: "0.75rem",
      }}
    >
      <div
        style={{
          color: "#f4ede0",
          marginBottom: "0.5rem",
          fontFamily: "Fraunces, serif",
          fontStyle: "italic",
          fontSize: "0.95rem",
        }}
      >
        {label}
      </div>
      {payload.map((p) => (
        <div
          key={p.dataKey}
          style={{
            display: "flex",
            justifyContent: "space-between",
            gap: "1.5rem",
          }}
        >
          <span style={{ color: p.color }}>
            {SUBREDDIT_LABELS[p.dataKey] ?? p.dataKey}
          </span>
          <span style={{ color: "#f4ede0", fontVariantNumeric: "tabular-nums" }}>
            {Number(p.value).toLocaleString()}
          </span>
        </div>
      ))}
      <div
        style={{
          marginTop: "0.5rem",
          paddingTop: "0.5rem",
          borderTop: "1px solid #2a2024",
          display: "flex",
          justifyContent: "space-between",
          gap: "1.5rem",
        }}
      >
        <span style={{ color: "#6a6055" }}>total</span>
        <span style={{ color: "#f4ede0", fontVariantNumeric: "tabular-nums" }}>
          {total.toLocaleString()}
        </span>
      </div>
    </div>
  );
}

function ChartShell({ empty = false }: { empty?: boolean }) {
  return (
    <div
      className="chart-card"
      data-corner="呪 / 02"
      style={{
        height: 420,
        display: empty ? "flex" : "block",
        alignItems: "center",
        justifyContent: "center",
        color: "#6a6055",
        fontFamily: "JetBrains Mono, monospace",
        fontSize: "0.875rem",
      }}
    >
      {empty && "No subreddit data yet."}
    </div>
  );
}