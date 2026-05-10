"use client";

import { useEffect, useState } from "react";

/**
 * Pulls the most recent classification timestamp from eval_results.json
 * and displays it in the footer. Makes the "daily-refreshed" claim
 * visible — a hiring manager sees "last updated 14h ago" and immediately
 * understands the cron is real.
 */
export function LastUpdated() {
  const [stamp, setStamp] = useState<string | null>(null);

  useEffect(() => {
    fetch("/data/eval_results.json")
      .then((r) => (r.ok ? r.json() : Promise.reject()))
      .then((data) => {
        if (!data?.ran_at) return;
        const ago = humanAgo(new Date(data.ran_at));
        setStamp(ago);
      })
      .catch(() => {
        /* silent — footer just doesn't show a stamp */
      });
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