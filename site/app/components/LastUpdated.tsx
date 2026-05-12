"use client";

import { useEffect, useState } from "react";

export function LastUpdated() {
  const [stamp, setStamp] = useState<string | null>(null);

  useEffect(() => {
    async function load() {
      let timestamp: string | null = null;

      // primary: pipeline-status file written every cron run
      try {
        const r = await fetch("/data/_pipeline_status.json", { cache: "no-store" });
        if (r.ok) {
          const data = await r.json();
          if (data?.last_ran_at) timestamp = data.last_ran_at;
        }
      } catch {
        /* fall through */
      }

      // fallback: eval results — updated only when eval is re-run
      if (!timestamp) {
        try {
          const r = await fetch("/data/eval_results.json", { cache: "no-store" });
          if (r.ok) {
            const data = await r.json();
            if (data?.ran_at) timestamp = data.ran_at;
          }
        } catch {
          /* silent — footer just doesn't show a stamp */
        }
      }

      if (timestamp) {
        setStamp(humanAgo(new Date(timestamp)));
      }
    }
    load();
  }, []);

  if (!stamp) return null;
  return <span className="tabular">last refreshed {stamp}</span>;
}

function humanAgo(then: Date): string {
  const seconds = Math.floor((Date.now() - then.getTime()) / 1000);
  if (seconds < 60) return "just now";
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 48) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}