# Architecture

```
            ┌───────────────────┐
            │ Reddit API (PRAW) │  ┌───────────────────┐
            │  + scheduled cron │  │   Arctic Shift    │  (historical backfill)
            └──────────┬────────┘  └─────────┬─────────┘
                       │                     │
                       └──────────┬──────────┘
                                  ▼
                         ┌────────────────┐
                         │ Bronze (raw)   │  parquet, append-only
                         │  /data/bronze  │  partitioned by ingest date
                         └──────┬─────────┘
                                │
                                ▼
                         ┌────────────────┐
                         │ Silver         │  cleaned, deduplicated,
                         │  /data/silver  │  one row per post / comment
                         └──────┬─────────┘
                                │
                   ┌────────────┴────────────┐
                   ▼                         ▼
           ┌────────────────┐       ┌────────────────┐
           │ Character      │       │ Sentiment      │
           │ resolution     │       │ classification │
           │ alias-dict +   │       │ Llama 3.1 8B   │
           │ word-boundary  │       │ JSONL cache    │
           └────────┬───────┘       └────────┬───────┘
                    │                        │
                    └────────────┬───────────┘
                                 ▼
                         ┌────────────────┐
                         │ Gold           │  7 analytical tables,
                         │  /data/gold    │  pre-aggregated for charts
                         └──────┬─────────┘
                                │
                                ▼
                         ┌────────────────┐
                         │ Static site    │  Next.js + Recharts
                         │  (Vercel)      │  reads parquets via DuckDB-WASM
                         └────────────────┘
```

## Why these choices

### DuckDB over Snowflake / BigQuery

Free, zero-ops, fast enough. Runs locally during development and in the user's browser via DuckDB-WASM in production. The dashboard queries parquet files directly through HTTP range requests — no server runtime, no warehouse bill.

### Parquet on disk over a hosted warehouse

Total infra cost is $0/month. Bronze and silver are gitignored (heavy, recoverable from the API). Gold parquets are small enough (~50KB total) to commit alongside code, which means the dashboard deploys as static files and the cron can rebuild gold deterministically.

### Llama 3.1 8B on Groq over GPT-4 / fine-tuned BERT

Groq's free tier is 14,400 requests/day with no card required, enough for the daily incremental load. 8B is small but the few-shot prompt with eight JJK-fandom examples handles the idiom edge cases that VADER and base sentiment models miss ("this killed me", sarcastic 🙏, affectionate Gege-cursing). Fine-tuning a BERT was rejected as v1 scope creep — see `KNOWN_LIMITATIONS.md`.

### GitHub Actions for the daily cron

Free for public repos. The watermark file and classifier cache are committed back to the repo each run, so the cron has continuity across job invocations without needing external state.

### Static Next.js + DuckDB-WASM over Streamlit

Streamlit looks like every other data-person side project. The static-site approach matches the design quality of the existing portfolio and lets the dashboard be embedded anywhere (a tweet, a Reddit post, a hiring manager's tab) with zero cold-start latency.

## The hard problems and how they're solved

### Character disambiguation

`reference/characters.yaml` maps canonical character ids to lists of aliases with three match modes:

- **substring** — for unambiguous strings (`"Yuji Itadori"`, `"Higuruma"`)
- **word_boundary** — for shorter strings that need to not match inside other words (`"Yuji"` matches `Yuji` but not `Yujiro`)
- **requires_context** — for ambiguous aliases (`"the strongest"`, `"the honored one"`) that only resolve when other character indicators appear in the same post

Aliases also carry a weight in [0, 1] which propagates through to the gold-layer fact tables, so downstream aggregations can discount low-confidence mentions. Bare `"Satoru"` is weight 0.7, full `"Gojo Satoru"` is weight 1.0.

### Sentiment beyond library defaults

Three axes per classification:

- **sentiment**: positive / negative / mixed / neutral, plus a confidence in [0, 1]
- **intensity**: low / medium / high (separate from polarity — short reactions like "PEAK" are high-intensity)
- **target**: about_character / about_arc / about_meta (does the post evaluate the character, the plot, or the work itself?)

The eight few-shot examples each teach a specific JJK-fandom idiom, tagged in the prompt module by `teaches=` so a regression test can verify coverage. The eval harness reports per-idiom-category accuracy, which surfaces *which* idiom regressed when the prompt is edited — not just an aggregate accuracy.

### Chapter-event timeline

`reference/events.yaml` is hand-curated: chapter number, release date, arc, one-line description, spoiler-intensity tag. The dashboard overlays these as vertical guidelines on the headline chart; the Gege-moment detector pairs each detected sentiment shift with the closest event within ±7 days.

### Single-source ingestion via Arctic Shift

The original v1 plan was a dual-source ingestion pattern: PRAW for the daily incremental scrape, Arctic Shift for the historical backfill. In November 2025 Reddit shipped the Responsible Builder Policy and closed self-service OAuth approval — new PRAW credentials now require Reddit's manual review with no committed turnaround time.

Rather than block the project on a manual approval pipeline, the daily cron pulls a 2-day rolling window from Arctic Shift, which already serves both historical and recent data and remains freely accessible. The silver-layer dedup collapses the 24-hour overlap so a missed run doesn't create gaps. The tradeoff is ~12-24 hours of additional latency on the freshest data — invisible at the weekly-aggregation grain the dashboard ships.

The `pipeline/extract/reddit_praw.py` module is retained, untested-against-live, in case Reddit's policy posture changes.

## Cost ceiling

| Component | Free tier | Daily usage |
|---|---|---|
| Groq Llama 3.1 8B | 14,400 req/day | ~200–500 with cache |
| GitHub Actions | 2,000 min/month | ~3 min/day |
| Vercel hosting | 100 GB bandwidth | <1 GB/day |
| Reddit API | 60 req/min | ~10 req/min during scrape |

Total monthly spend: $0. Comfortable headroom on every axis.