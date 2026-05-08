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
 *
 * Why this is in `lib/` not `app/`:
 *   `lib/` is for runtime utilities that don't render UI. The hook does
 *   manage React state (useState, useEffect) but it's exported, not
 *   consumed in place — same way you'd put `useAuth` in a lib folder.
 */

import * as duckdb from "@duckdb/duckdb-wasm";
import { useEffect, useState } from "react";

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
    // The cast is needed because Arrow's row.toJSON() returns a
    // Record-shaped object but TypeScript can't see that through the
    // generic StructRow type — we know the shape from the SQL.
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
 *     `SELECT * FROM read_parquet('/data/agg_polarisation.parquet')`,
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