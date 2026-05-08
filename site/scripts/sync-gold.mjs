#!/usr/bin/env node
/**
 * Sync gold parquets from the Python pipeline output into the site's
 * public folder so they ship with the static build.
 *
 * Runs automatically before `npm run dev` and `npm run build` (via the
 * predev / prebuild hooks in package.json).
 *
 * The dashboard reads these files via DuckDB-WASM with HTTP range
 * requests — committing them to git keeps the deploy entirely
 * server-runtime-free.
 */

import { copyFile, mkdir, readdir } from "node:fs/promises";
import { existsSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const SOURCE = resolve(__dirname, "..", "..", "data", "gold");
const DEST = resolve(__dirname, "..", "public", "data");

async function main() {
  if (!existsSync(SOURCE)) {
    console.warn(
      `[sync-gold] source ${SOURCE} doesn't exist yet — ` +
        `the dashboard will render in empty-state. ` +
        `Run \`uv run cursed gold\` to populate.`,
    );
    return;
  }

  await mkdir(DEST, { recursive: true });

  const files = await readdir(SOURCE);
  const parquets = files.filter((f) => f.endsWith(".parquet"));
  const json = files.filter((f) => f.endsWith(".json"));

  for (const f of [...parquets, ...json]) {
    await copyFile(join(SOURCE, f), join(DEST, f));
  }

  console.log(
    `[sync-gold] copied ${parquets.length} parquet + ${json.length} json from ` +
      `data/gold/ to site/public/data/`,
  );
}

main().catch((err) => {
  console.error(`[sync-gold] failed: ${err.message}`);
  process.exit(1);
});