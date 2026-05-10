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
  jujutsukaisen: "var(--color-bone)",      // main hub — high contrast
  jujutsushi:    "var(--color-indigo)",    // analysis-heavy
  jujutsufolk:   "var(--color-gold)",      // memes
};

const SUBREDDIT_LABELS: Record<string, string> = {
  jujutsukaisen: "r/JuJutsuKaisen",
  jujutsushi:    "r/Jujutsushi",
  jujutsufolk:   "r/Jujutsufolk",
};

/**
 * Stacked horizontal bar showing how each top character's mentions
 * distribute across the three subreddits. The interesting insight this
 * surfaces: r/Jujutsufolk dominates volume for some characters (memes
 * find a home), while r/Jujutsushi tends to skew analytical.
 */
export function SubredditBreakdown() {
  const { data, error, loading } = useQuery<SubredditBreakdownRow>(
    Q_SUBREDDIT_BREAKDOWN,
  );

  if (loading) return <ChartSkeleton />;
  if (error || !data || data.length === 0) return <ChartEmpty />;

  // Pivot from long format (display_name, subreddit, count)
  // to wide format (one row per character with one column per subreddit).
  const pivoted = pivotBySubreddit(data);
  const subreddits = Array.from(
    new Set(data.map((r) => r.subreddit)),
  ).sort();

  return (
    <div className="mt-16">
      <div className="font-display italic text-section text-bone mb-1">
        Where the conversation lives
      </div>
      <div className="font-mono text-xs uppercase tracking-wider text-smoke mb-6">
        per-character mention split across the three subreddits · top 6
      </div>

      <div className="h-80 -mx-2">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={pivoted}
            layout="vertical"
            margin={{ top: 10, right: 24, left: 24, bottom: 24 }}
          >
            <CartesianGrid horizontal={false} />
            <XAxis
              type="number"
              stroke="var(--color-smoke)"
              tickLine={false}
              axisLine={false}
            />
            <YAxis
              type="category"
              dataKey="display_name"
              stroke="var(--color-smoke)"
              tickLine={false}
              axisLine={false}
              width={120}
            />
            <Tooltip content={<TooltipBox />} />
            <Legend
              wrapperStyle={{ paddingTop: 16, fontSize: 11 }}
              iconType="square"
              formatter={(value: string) =>
                SUBREDDIT_LABELS[value] ?? value
              }
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
  // Sort by total mentions descending so the heavy hitters are at top.
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

function TooltipBox({ active, payload, label }: any) {
  if (!active || !payload || payload.length === 0) return null;
  const total = payload.reduce(
    (s: number, p: any) => s + Number(p.value || 0),
    0,
  );
  return (
    <div className="bg-ink border border-smoke/40 p-3 font-mono text-xs">
      <div className="text-bone mb-2 font-medium">{label}</div>
      {payload.map((p: any) => (
        <div key={p.dataKey} className="flex justify-between gap-6">
          <span style={{ color: p.color }}>
            {SUBREDDIT_LABELS[p.dataKey] ?? p.dataKey}
          </span>
          <span className="text-bone tabular">{Number(p.value)}</span>
        </div>
      ))}
      <div className="mt-2 pt-2 border-t border-smoke/20 flex justify-between gap-6">
        <span className="text-smoke">total</span>
        <span className="text-bone tabular">{total}</span>
      </div>
    </div>
  );
}

function ChartSkeleton() {
  return <div className="h-80 mt-16 bg-smoke/5 animate-pulse" />;
}

function ChartEmpty() {
  return (
    <div className="h-80 mt-16 border border-smoke/30 flex items-center justify-center font-mono text-sm text-smoke">
      No subreddit data yet.
    </div>
  );
}