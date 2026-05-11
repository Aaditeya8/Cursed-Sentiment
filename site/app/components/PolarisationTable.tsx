"use client";

import { useEffect, useRef } from "react";

import { useQuery } from "@/lib/duckdb";
import { Q_POLARISATION_RANKING, type PolarisationRow } from "@/lib/queries";

const MENTION_FLOOR = 50;
const TOP_N = 10;

/**
 * Top characters by polarisation index. Rebuilt as a CSS grid of
 * .polar-row entries instead of an HTML table, so the gradient bar
 * can use the proper styling from globals.css with the centre-line
 * indicator and red glow shadow.
 *
 * Rows fade up in sequence via IntersectionObserver as they scroll
 * into view — a 40ms stagger feels like a deck of cards being dealt.
 */
export function PolarisationTable() {
  const { data, error, loading } = useQuery<PolarisationRow>(Q_POLARISATION_RANKING);
  const rootRef = useRef<HTMLDivElement>(null);

  // IntersectionObserver-driven row reveal. Each .polar-row starts at
  // opacity 0 and translated 16px down; flips on intersection.
  useEffect(() => {
    if (!rootRef.current) return;
    const rows = rootRef.current.querySelectorAll<HTMLDivElement>(".polar-row");
    rows.forEach((row, i) => {
      row.style.opacity = "0";
      row.style.transform = "translateY(16px)";
      row.style.transition = `opacity .7s cubic-bezier(.2,.8,.2,1) ${i * 0.04}s, transform .7s cubic-bezier(.2,.8,.2,1) ${i * 0.04}s`;
    });
    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting) {
            const el = entry.target as HTMLDivElement;
            el.style.opacity = "1";
            el.style.transform = "translateY(0)";
            observer.unobserve(el);
          }
        });
      },
      { threshold: 0.15 },
    );
    rows.forEach((row) => observer.observe(row));
    return () => observer.disconnect();
  }, [data]);

  if (loading) return <Caption text="Loading polarisation data…" />;
  if (error) return <Caption text="Failed to load polarisation data." tone="error" />;
  if (!data || data.length === 0) return <Caption text="No polarisation data yet." />;

  const filtered = data
    .filter((r) => Number(r.total_mentions) >= MENTION_FLOOR)
    .sort((a, b) => (b.polarisation_index ?? 0) - (a.polarisation_index ?? 0))
    .slice(0, TOP_N);

  if (filtered.length === 0) {
    return <Caption text={`No characters reach the ${MENTION_FLOOR}-mention floor yet.`} />;
  }

  return (
    <>
      <div className="polar-wrap" ref={rootRef}>
        {filtered.map((row, i) => {
          const pol = Number(row.polarisation_index ?? 0);
          const mean = Number(row.mean_sentiment_score ?? 0);
          return (
            <div key={row.character_id} className="polar-row">
              <div className="polar-rank">{String(i + 1).padStart(2, "0")}</div>
              <div className="polar-name">{row.display_name ?? row.character_id}</div>
              <div className="polar-bar">
                <div
                  className="polar-fill"
                  style={{ width: `${Math.max(0, Math.min(1, pol)) * 100}%` }}
                />
              </div>
              <div className="polar-mentions">
                {Number(row.total_mentions).toLocaleString()} mentions
              </div>
              <div className="polar-score">
                {pol.toFixed(2)}
                <span
                  style={{
                    display: "block",
                    fontSize: "0.6875rem",
                    color: "#6a6055",
                    marginTop: "0.125rem",
                    fontVariantNumeric: "tabular-nums",
                  }}
                >
                  mean {mean >= 0 ? "+" : ""}
                  {mean.toFixed(2)}
                </span>
              </div>
            </div>
          );
        })}
      </div>
      <div
        style={{
          marginTop: "1rem",
          fontFamily: "JetBrains Mono, monospace",
          fontSize: "0.6875rem",
          color: "#6a6055",
          textTransform: "uppercase",
          letterSpacing: "0.08em",
        }}
      >
        characters with fewer than {MENTION_FLOOR} mentions excluded as
        statistical noise
      </div>
    </>
  );
}

function Caption({ text, tone = "muted" }: { text: string; tone?: "muted" | "error" }) {
  return (
    <div
      style={{
        padding: "2rem",
        fontFamily: "JetBrains Mono, monospace",
        fontSize: "0.875rem",
        color: tone === "error" ? "#ff2d4d" : "#6a6055",
        textAlign: "center",
      }}
    >
      {text}
    </div>
  );
}