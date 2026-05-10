"use client";

/**
 * DuckDB-WASM singleton + React query hook.
 *
 * The dashboard's backend is DuckDB running entirely in the user's browser.
 * It loads parquets from `/data/*.parquet` (synced from `../data/gold/`
 * by `scripts/sync-gold.mjs`) over HTTP range requests — only the bytes
 * needed for a given query are fetched, so a 50KB parquet might only
 * require 5KB of network traffic to scan.
 *
 * Architecture:
 *   - One DuckDB instance per browser tab (singleton)
 *   - Lazy-initialized on first query
 *   - All queries go through `useQuery`, a React hook that handles
 *     loading/error/data states uniformly
 *   - Every gold parquet is registered against DuckDB's virtual filesystem
 *     at init time so queries can reference it by bare filename
 */

import * as duckdb from "@duckdb/duckdb-wasm";
import { useEffect, useState } from "react";

// Every parquet the dashboard queries. Must match the filenames produced
// by `pipeline/load/build_gold.py` and synced into public/data by
// scripts/sync-gold.mjs. Adding a new parquet to gold? Add it here.
const GOLD_FILES = [
  "agg_char_week.parquet",
  "agg_polarisation.parquet",
  "char_summary.parquet",
  "dim_character.parquet",
  "dim_event.parquet",
  "fact_comment_sentiment.parquet",
  "fact_post_sentiment.parquet",
  "gege_moments.parquet",
];

let dbInstance: duckdb.AsyncDuckDB | null = null;
let dbPromise: Promise<duckdb.AsyncDuckDB> | null = null;

async function initDuckDB(): Promise<duckdb.AsyncDuckDB> {
  if (dbInstance) return dbInstance;

  // The bundles object maps each WASM build (mvp, eh, coi) to its files.
  // selectBundle picks the best build the browser supports.
  const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

  // The worker is a separate JS file that hosts the WASM runtime so the
  // main thread isn't blocked during heavy queries. We construct it from
  // a blob URL because the worker file lives on jsDelivr (cross-origin).
  const workerUrl = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker!}");`], {
      type: "text/javascript",
    }),
  );

  const worker = new Worker(workerUrl);
  const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(workerUrl);

  // Register every gold parquet against DuckDB's virtual filesystem.
  // Without this, queries fail with "No files found that match the
  // pattern" — DuckDB-WASM doesn't fetch arbitrary URLs from the page
  // origin, it needs each filename mapped explicitly to a fetch URL.
  const baseUrl =
    typeof window !== "undefined"
      ? `${window.location.origin}/data`
      : "/data";
  for (const file of GOLD_FILES) {
    await db.registerFileURL(
      file,
      `${baseUrl}/${file}`,
      duckdb.DuckDBDataProtocol.HTTP,
      false,
    );
  }

  dbInstance = db;
  return db;
}

/**
 * Get the singleton DB instance, initializing it on first call.
 * Subsequent calls return the same instance instantly.
 */
export async function getDB(): Promise<duckdb.AsyncDuckDB> {
  if (dbInstance) return dbInstance;
  if (!dbPromise) dbPromise = initDuckDB();
  return dbPromise;
}

/**
 * Run one SQL query and return the rows as a plain JS array.
 *
 * Arrow tables don't serialize to JSON cleanly out of the box (they're
 * columnar and lazy), so we explicitly call `.toJSON()` on each row to
 * get a plain object. The cast is needed because Arrow's StructRow type
 * has a more permissive index signature than TypeScript can infer.
 */
export async function runQuery<T = Record<string, unknown>>(
  sql: string,
): Promise<T[]> {
  const db = await getDB();
  const conn = await db.connect();
  try {
    const result = await conn.query(sql);
    return result
      .toArray()
      .map((row: { toJSON: () => Record<string, unknown> }) =>
        row.toJSON(),
      ) as T[];
  } finally {
    await conn.close();
  }
}

/**
 * React hook: run a query, render with the result. Handles the three
 * states most charts care about — loading, error, data — without each
 * component reimplementing them.
 *
 * Usage:
 *   const { data, error, loading } = useQuery<MyRow>(
 *     `SELECT * FROM read_parquet('agg_polarisation.parquet')`,
 *   );
 */
export function useQuery<T = Record<string, unknown>>(
  sql: string | null,
): { data: T[] | null; error: Error | null; loading: boolean } {
  const [data, setData] = useState<T[] | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [loading, setLoading] = useState<boolean>(true);

  useEffect(() => {
    if (!sql) {
      setLoading(false);
      return;
    }
    let cancelled = false;
    setLoading(true);
    setError(null);

    runQuery<T>(sql)
      .then((rows) => {
        if (!cancelled) {
          setData(rows);
          setLoading(false);
        }
      })
      .catch((err: Error) => {
        if (!cancelled) {
          setError(err);
          setLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [sql]);

  return { data, error, loading };
}