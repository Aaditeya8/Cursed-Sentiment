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

// Six visually distinct hex colors for the top-6 characters. Literal
// hex (not CSS vars) because Recharts passes them straight to SVG
// attributes where var() doesn't resolve reliably in this setup.
const CHARACTER_COLORS: Record<string, string> = {
  "Gojo Satoru":      "#f4ede0", // bone
  "Ryomen Sukuna":    "#d4a857", // gold
  "Itadori Yuji":     "#8da0cb", // soft indigo
  "Fushiguro Megumi": "#7fc8a9", // sage
  "Kugisaki Nobara":  "#e07b91", // rose
  "Geto Suguru":      "#c084fc", // violet
};

const FALLBACK_COLORS = ["#f7a072", "#5eb3d6", "#a8d672", "#b88a4d"];

/**
 * Sentiment-over-time for the top 6 characters. Wrapped in a paper
 * chart-card with the 呪 / 01 corner mark. Chapter 236 reference line
 * renders as a dashed crimson rule with an italic Fraunces label
 * rendered via a custom SVG callback (position="top" gets clipped by
 * the SVG viewport; the callback renders inside the chart area).
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

  const ch236 = events.data?.find((e) => e.chapter === 236);

  return (
    <div className="chart-card" data-corner="呪 / 01">
      <div style={{ height: 420 }}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart
            data={pivoted}
            margin={{ top: 32, right: 16, left: 8, bottom: 32 }}
          >
            <CartesianGrid vertical={false} stroke="#2a2024" />
            <XAxis
              dataKey="week_label"
              stroke="#6a6055"
              tickLine={false}
              axisLine={false}
              minTickGap={32}
              tick={{ fill: "#b8ac9a", fontSize: 11, fontFamily: "JetBrains Mono, monospace" }}
            />
            <YAxis
              domain={[-1, 1]}
              ticks={[-1, -0.5, 0, 0.5, 1]}
              stroke="#6a6055"
              tickLine={false}
              axisLine={false}
              tickFormatter={(v) => v.toFixed(1)}
              tick={{ fill: "#b8ac9a", fontSize: 11, fontFamily: "JetBrains Mono, monospace" }}
            />
            <Tooltip content={<TooltipBox />} />

            {characters.map((name, i) => (
              <Line
                key={name}
                type="monotone"
                dataKey={name}
                stroke={
                  CHARACTER_COLORS[name] ??
                  FALLBACK_COLORS[i % FALLBACK_COLORS.length]
                }
                strokeWidth={1.5}
                dot={false}
                isAnimationActive={false}
                connectNulls
              />
            ))}

            {ch236 && (
              <ReferenceLine
                x={isoToWeekLabel(ch236.event_date)}
                stroke="#c8102e"
                strokeWidth={2}
                strokeDasharray="4 4"
                label={(props: { viewBox?: { x: number; y: number } }) => {
                  const vb = props.viewBox;
                  if (!vb) return <g />;
                  return (
                    <g>
                      <text
                        x={vb.x}
                        y={vb.y + 18}
                        textAnchor="middle"
                        fill="#ff2d4d"
                        fontSize={13}
                        fontStyle="italic"
                        fontFamily="Fraunces, serif"
                        style={{
                          filter:
                            "drop-shadow(0 0 6px rgba(255,45,77,0.45))",
                        }}
                      >
                        Ch. 236 — Gojo dies
                      </text>
                    </g>
                  );
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

function TooltipBox({ active, payload, label }: { active?: boolean; payload?: Array<{ dataKey: string; value: number; color: string }>; label?: string }) {
  if (!active || !payload || payload.length === 0) return null;
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
      <div style={{ color: "#f4ede0", marginBottom: "0.5rem" }}>{label}</div>
      {payload.map((p) => (
        <div
          key={p.dataKey}
          style={{
            display: "flex",
            justifyContent: "space-between",
            gap: "1.5rem",
          }}
        >
          <span style={{ color: p.color }}>{p.dataKey}</span>
          <span style={{ color: "#f4ede0", fontVariantNumeric: "tabular-nums" }}>
            {Number(p.value).toFixed(2)}
          </span>
        </div>
      ))}
    </div>
  );
}

function ChartSkeleton() {
  return <div className="chart-card" data-corner="呪 / 01" style={{ height: 420 }} />;
}

function ChartError() {
  return (
    <div
      className="chart-card"
      data-corner="呪 / 01"
      style={{
        height: 420,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        color: "#ff2d4d",
        fontFamily: "JetBrains Mono, monospace",
        fontSize: "0.875rem",
      }}
    >
      Failed to load weekly sentiment.
    </div>
  );
}

function ChartEmpty() {
  return (
    <div
      className="chart-card"
      data-corner="呪 / 01"
      style={{
        height: 420,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        color: "#6a6055",
        fontFamily: "JetBrains Mono, monospace",
        fontSize: "0.875rem",
      }}
    >
      No weekly data yet.
    </div>
  );
}
