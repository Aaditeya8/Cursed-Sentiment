"use client";

import { useEffect, useRef } from "react";

import { useQuery } from "@/lib/duckdb";
import { Q_GEGE_MOMENTS, type GegeMomentRow } from "@/lib/queries";

/**
 * Moments where weekly sentiment for a character shifted >2σ from
 * its trailing baseline — flagged automatically by the pipeline and
 * paired with the closest chapter event.
 *
 * Rendered as a 2-column grid of .moment cards with a cursed-red
 * left border that intensifies on hover. Same IntersectionObserver
 * stagger pattern as PolarisationTable — feels like the moments
 * are unsealed one by one as you scroll.
 */
export function GegeMomentsList() {
  const { data, error, loading } = useQuery<GegeMomentRow>(Q_GEGE_MOMENTS);
  const rootRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!rootRef.current) return;
    const cards = rootRef.current.querySelectorAll<HTMLElement>(".moment");
    cards.forEach((card, i) => {
      card.style.opacity = "0";
      card.style.transform = "translateY(16px)";
      card.style.transition = `opacity .7s cubic-bezier(.2,.8,.2,1) ${i * 0.05}s, transform .7s cubic-bezier(.2,.8,.2,1) ${i * 0.05}s, border-left-color .25s ease`;
    });
    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting) {
            const el = entry.target as HTMLElement;
            el.style.opacity = "1";
            el.style.transform = "translateY(0)";
            observer.unobserve(el);
          }
        });
      },
      { threshold: 0.15 },
    );
    cards.forEach((card) => observer.observe(card));
    return () => observer.disconnect();
  }, [data]);

  if (loading) return <Caption text="Loading moments…" />;
  if (error) return <Caption text="Failed to load moments." tone="error" />;
  if (!data || data.length === 0) {
    return (
      <Caption text="No moments flagged yet — needs more historical data for the baseline." />
    );
  }

  return (
    <div className="moments-grid" ref={rootRef}>
      {data.map((m, i) => {
        const sigma = Number(getField(m, ["sigma_shift", "z_score", "sigma"]) ?? 0);
        const character =
          (getField(m, ["display_name", "character_name", "character_id"]) as string) ?? "—";
        const chapter = getField(m, ["chapter", "chapter_number"]) as number | string | undefined;
        const chapterTitle = getField(m, ["chapter_title", "event_title", "title"]) as
          | string
          | undefined;
        const weekStart = getField(m, ["week_start", "date", "event_date"]) as
          | Date
          | string
          | undefined;

        return (
          <article key={`${character}-${chapter ?? i}`} className="moment">
            <div className="moment-date">{formatDateLine(weekStart)}</div>
            <div className="moment-chapter">
              {chapter !== undefined && `Ch. ${chapter}`}
              {chapter !== undefined && chapterTitle && " — "}
              {chapterTitle && <em>&ldquo;{chapterTitle}&rdquo;</em>}
              {!chapter && !chapterTitle && "Untagged moment"}
            </div>
            <h3 className="moment-title">{renderHeadline(character, sigma)}</h3>
            <div className="moment-shift">
              <span>
                <strong>{character}</strong> sentiment{" "}
                <span className={`delta${sigma > 0 ? " pos" : ""}`}>
                  {sigma >= 0 ? "+" : ""}
                  {sigma.toFixed(2)}σ
                </span>
              </span>
            </div>
          </article>
        );
      })}
    </div>
  );
}

// --- helpers --------------------------------------------------------------

/**
 * Defensive field accessor — the gege_moments parquet schema has shifted
 * a few times over development; this looks up the first present field
 * from a candidate list so the component doesn't crash on a schema change.
 */
function getField(row: GegeMomentRow, candidates: string[]): unknown {
  const r = row as unknown as Record<string, unknown>;
  for (const c of candidates) {
    if (r[c] !== undefined && r[c] !== null) return r[c];
  }
  return undefined;
}

function formatDateLine(d: Date | string | undefined): string {
  if (!d) return "Undated";
  const date = d instanceof Date ? d : new Date(d);
  if (isNaN(date.getTime())) return "Undated";
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(date.getUTCDate()).padStart(2, "0");
  const dow = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"][date.getUTCDay()];
  return `${y}.${m}.${dd} · ${dow}`;
}

/**
 * Synthesises a one-line headline from the data — not editorial copy,
 * just a deterministic description of the shift's shape so each card
 * has a memorable title without needing a separate column in the parquet.
 */
function renderHeadline(character: string, sigma: number): string {
  const direction = sigma > 0 ? "rose" : "fell";
  const magnitude =
    Math.abs(sigma) > 3
      ? "the floor fell out"
      : Math.abs(sigma) > 2.5
        ? "sharply"
        : "noticeably";
  if (sigma < -2.5) return `The week ${magnitude} for ${character}.`;
  if (sigma > 2.5) return `${character} ${direction} ${magnitude}.`;
  return `${character} sentiment ${direction} ${magnitude}.`;
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