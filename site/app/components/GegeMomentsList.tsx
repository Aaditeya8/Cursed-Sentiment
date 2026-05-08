"use client";

import { useQuery } from "@/lib/duckdb";
import { Q_GEGE_MOMENTS, type GegeMomentRow } from "@/lib/queries";

/**
 * Weeks where a character's sentiment shifted more than 2 standard
 * deviations from its trailing 12-week baseline, paired with the
 * closest chapter event within ±7 days.
 *
 * This is the storytelling component of the dashboard — it pairs
 * cold quantitative anomalies with the qualitative reason the fandom
 * had for them.
 */
export function GegeMomentsList() {
  const { data, error, loading } = useQuery<GegeMomentRow>(Q_GEGE_MOMENTS);

  if (loading) return <ListSkeleton />;
  if (error || !data || data.length === 0) return <ListEmpty />;

  return (
    <div className="mt-16">
      <div className="font-display italic text-section text-bone mb-1">
        Gege moments
      </div>
      <div className="font-mono text-xs uppercase tracking-wider text-smoke mb-6">
        sentiment shifts &gt;2σ from baseline · paired with chapter releases
      </div>

      <ul className="space-y-4">
        {data.map((m, i) => (
          <li
            key={`${m.character_id}-${m.week_start}`}
            className="border-b border-smoke/15 pb-4"
          >
            <div className="flex items-baseline justify-between gap-4">
              <div className="font-mono text-xs text-smoke tabular">
                {fmtDate(m.week_start)}
              </div>
              <div
                className={`font-mono text-xs tabular ${
                  m.z_score > 0 ? "text-indigo" : "text-gold"
                }`}
              >
                {m.z_score > 0 ? "+" : ""}
                {m.z_score.toFixed(1)}σ
              </div>
            </div>
            <div className="mt-1 text-bone">
              <span className="font-medium">{m.display_name}</span>
              {m.paired_event_title ? (
                <>
                  {" — "}
                  <span className="text-smoke italic">
                    {m.paired_event_title}
                    {m.paired_event_distance_days != null &&
                      ` (±${m.paired_event_distance_days}d)`}
                  </span>
                </>
              ) : (
                <span className="text-smoke italic"> — unpaired</span>
              )}
            </div>
          </li>
        ))}
      </ul>
    </div>
  );
}

function fmtDate(d: Date | string): string {
  const date = d instanceof Date ? d : new Date(d);
  return date.toISOString().slice(0, 10);
}

function ListSkeleton() {
  return (
    <div className="mt-16">
      <div className="h-5 w-32 bg-smoke/20 mb-2" />
      <div className="h-3 w-64 bg-smoke/10 mb-6" />
      <div className="space-y-4">
        {Array.from({ length: 5 }).map((_, i) => (
          <div key={i} className="h-12 bg-smoke/5" />
        ))}
      </div>
    </div>
  );
}

function ListEmpty() {
  return (
    <div className="mt-16 border border-smoke/30 p-8 font-mono text-sm text-smoke">
      No Gege moments detected yet — needs at least 12 weeks of baseline data.
    </div>
  );
}