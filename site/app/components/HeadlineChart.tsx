"use client";

import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { useQuery } from "@/lib/duckdb";
import {
  Q_EVENTS_TIMELINE,
  Q_HEADLINE_WEEKLY,
  type CharWeekRow,
  type EventRow,
} from "@/lib/queries";

const CHARACTER_COLORS: Record<string, string> = {
  "Gojo Satoru":     "var(--color-bone)",
  "Ryomen Sukuna":   "var(--color-gold)",
  "Itadori Yuji":    "var(--color-indigo)",
  "Fushiguro Megumi": "#7c9b8a",
  "Kugisaki Nobara": "#b88a4d",
  "Geto Suguru":     "var(--color-moss)",
};

const fallbackColor = "#7c8089";

/**
 * Sentiment-over-time for the top 6 characters, with a single reserved
 * crimson guideline at chapter 236 (the moment Gojo dies in the manga).
 *
 * The chart pivots gold-layer rows from long format into wide
 * (one column per character) so Recharts can draw multiple Lines from
 * the same dataset. The pivot happens client-side because pivots in
 * DuckDB-WASM are awkward and our data is small enough.
 */
export function HeadlineChart() {
  const weeks = useQuery<CharWeekRow>(Q_HEADLINE_WEEKLY);
  const events = useQuery<EventRow>(Q_EVENTS_TIMELINE);

  if (weeks.loading || events.loading) return <ChartSkeleton />;
  if (weeks.error || events.error) return <ChartError />;
  if (!weeks.data || weeks.data.length === 0) return <ChartEmpty />;

  const pivoted = pivotByCharacter(weeks.data);
  const characters = Array.from(
    new Set(weeks.data.map((r) => r.display_name).filter(Boolean) as string[]),
  );

  // Find chapter 236 if present in events. Reserved crimson, single emphasis.
  const ch236 = events.data?.find((e) => e.chapter === 236);

  return (
    <div className="mt-8">
      <div className="font-display italic text-section text-bone mb-1">
        Sentiment over time
      </div>
      <div className="font-mono text-xs uppercase tracking-wider text-smoke mb-6">
        weekly mean · top 6 characters by mention volume
      </div>

      <div className="h-96 -mx-2">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart
            data={pivoted}
            margin={{ top: 10, right: 12, left: 8, bottom: 32 }}
          >
            <CartesianGrid vertical={false} />
            <XAxis
              dataKey="week_label"
              stroke="var(--color-smoke)"
              tickLine={false}
              axisLine={false}
              minTickGap={32}
            />
            <YAxis
              domain={[-1, 1]}
              ticks={[-1, -0.5, 0, 0.5, 1]}
              stroke="var(--color-smoke)"
              tickLine={false}
              axisLine={false}
              tickFormatter={(v) => v.toFixed(1)}
            />
            <Tooltip content={<TooltipBox />} />

            {characters.map((name) => (
              <Line
                key={name}
                type="monotone"
                dataKey={name}
                stroke={CHARACTER_COLORS[name] ?? fallbackColor}
                strokeWidth={1.5}
                dot={false}
                isAnimationActive={false}
                connectNulls
              />
            ))}

            {ch236 && (
              <ReferenceLine
                x={isoToWeekLabel(ch236.event_date)}
                stroke="var(--color-crimson)"
                strokeWidth={1.5}
                label={{
                  value: "Ch. 236 — Gojo dies",
                  position: "insideTopRight",
                  fill: "var(--color-crimson)",
                  fontSize: 11,
                  fontFamily: "var(--font-plex-mono)",
                }}
              />
            )}

            <Legend
              wrapperStyle={{ paddingTop: 16, fontSize: 11 }}
              iconType="plainline"
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

function pivotByCharacter(rows: CharWeekRow[]) {
  // Map week_label → { week_label, "Gojo Satoru": 0.4, "Ryomen Sukuna": -0.1, ... }
  const byWeek = new Map<string, Record<string, string | number>>();
  for (const r of rows) {
    const label = isoToWeekLabel(r.week_start);
    if (!byWeek.has(label)) byWeek.set(label, { week_label: label });
    if (r.display_name) {
      byWeek.get(label)![r.display_name] = Number(r.mean_sentiment_score);
    }
  }
  return Array.from(byWeek.values()).sort((a, b) =>
    String(a.week_label).localeCompare(String(b.week_label)),
  );
}

function isoToWeekLabel(d: Date | string): string {
  const date = d instanceof Date ? d : new Date(d);
  return `${date.getUTCFullYear()}-W${String(getISOWeek(date)).padStart(2, "0")}`;
}

function getISOWeek(date: Date): number {
  const d = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  d.setUTCDate(d.getUTCDate() + 4 - (d.getUTCDay() || 7));
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  return Math.ceil(((d.getTime() - yearStart.getTime()) / 86400000 + 1) / 7);
}

function TooltipBox({ active, payload, label }: any) {
  if (!active || !payload || payload.length === 0) return null;
  return (
    <div className="bg-ink border border-smoke/40 p-3 font-mono text-xs">
      <div className="text-bone mb-2">{label}</div>
      {payload.map((p: any) => (
        <div key={p.dataKey} className="flex justify-between gap-6">
          <span style={{ color: p.color }}>{p.dataKey}</span>
          <span className="text-bone tabular">{Number(p.value).toFixed(2)}</span>
        </div>
      ))}
    </div>
  );
}

function ChartSkeleton() {
  return <div className="h-96 mt-8 bg-smoke/5 animate-pulse" />;
}

function ChartError() {
  return (
    <div className="h-96 mt-8 border border-crimson/40 flex items-center justify-center font-mono text-sm text-crimson">
      Failed to load weekly sentiment.
    </div>
  );
}

function ChartEmpty() {
  return (
    <div className="h-96 mt-8 border border-smoke/30 flex items-center justify-center font-mono text-sm text-smoke">
      No weekly data yet.
    </div>
  );
}