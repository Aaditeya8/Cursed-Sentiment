Cursed Sentiment

What 280,000 fans really feel about Gojo Satoru — a daily-refreshed sentiment analytics pipeline over five years of r/JuJutsuKaisen discussion.

🔗 Live dashboard · Methodology · Architecture deep-dive · Known limitations
What this is
A character-aware sentiment analytics pipeline over five years of Reddit fan discussion, built as a portfolio piece. Three subreddits, ~30 named characters tracked via a curated alias dictionary, sentiment classified along three axes (polarity, intensity, target), aggregated into weekly rollups and a "Gege moment" detector that pairs sentiment shifts with chapter releases.
What's interesting about this technically

Medallion-style batch pipeline (bronze → silver → gold) running daily via GitHub Actions on a free Groq tier — total infra cost is $0/month.
Character-aware NLP: hand-curated alias dictionary with three match modes (substring, word-boundary, requires-context), weighted by ambiguity. Documented in KNOWN_LIMITATIONS.md.
Three-axis sentiment classification via Llama 3.1 8B with eight few-shot examples covering JJK-fandom idioms ("this killed me" 💀, sarcastic 🙏, "Gege you genius bastard"). Persistent JSONL cache committed to git so collaborators don't pay the API bill twice.
Self-aware eval methodology: 55-case synthetic eval set with per-idiom regression detection, results published on the dashboard's /methodology page. The eval has a self-validation hazard (same person wrote prompt and eval) — documented honestly.
DuckDB-WASM in the browser: the dashboard reads gold parquets directly via HTTP range requests. No server runtime, deploys as static files.

Run it locally
uv sync                                                # installs Python deps
cp .env.example .env                                   # fill in Reddit + Groq creds
uv run cursed backfill --after 2023-09-23 --before 2023-09-27 --subreddit JuJutsuKaisen
uv run cursed silver
uv run cursed classify
uv run cursed gold

cd site && npm install && npm run dev                  # http://localhost:3000
Architecture
See ARCHITECTURE.md for the full diagram and design decisions.
What it cost
Total cumulative infra spend: $0. Groq free tier handles the classification load (~14,400 requests/day cap is well above the daily incremental rate); GitHub Actions hosts the cron; Vercel hosts the static dashboard; the gold parquets are small enough to commit (~50KB total).
What I'd do differently

Hand-label a 200-post real eval set first, before committing to the full backfill. The synthetic eval validates the prompt against named idioms but not unknown unknowns.
Move classifier confidence into the polarisation math. Right now polarisation is unweighted; a 0.6-confidence positive carries the same weight as a 0.95-confidence positive in the index calculation. This is on the methodology page as a known limitation.
Try fine-tuning a small BERT on the labeled real eval as an A/B against the LLM. Probably wouldn't beat Llama 3.1 on edge cases, but the eval discipline would tell us for sure.

Built by
Aaditeya Sharma — data engineer in Delhi-NCR