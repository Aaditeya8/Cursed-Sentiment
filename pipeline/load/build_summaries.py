"""Build per-character LLM-generated reasoning summaries.

For three "headline" characters from the gold layer — warmest, coldest, most
polarising — gather their top-upvoted positive and negative posts and have
Llama 3.3 70B synthesize what the fandom is actually saying about them.

Cached as gold/char_summary.parquet. Runs after build_gold; the dashboard
reads from this cache, never calls the LLM at view time.

Cost: 3 characters × ~3K input tokens each × Dev tier rate ≈ $0.01/run.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from groq import Groq
from pydantic import BaseModel, Field
from tenacity import retry, stop_after_attempt, wait_exponential

from pipeline.extract._common import UTC, DataPaths, write_parquet_atomic

log = structlog.get_logger(__name__)

# Use 70B for synthesis quality. 8B works but 70B is dramatically better at
# distilling 30 fan posts into a coherent paragraph that reads like real
# analysis. Cost is rounding error at this volume.
SUMMARY_MODEL = "llama-3.3-70b-versatile"
SUMMARY_PROMPT_VERSION = "v1"

# How many top-upvoted quotes per camp (positive/negative) to give the LLM.
# 8 is enough for the model to synthesize patterns; more gets repetitive.
QUOTES_PER_CAMP = 8

SCHEMA = pa.schema([
    ("character_id", pa.string()),
    ("display_name", pa.string()),
    ("category", pa.string()),  # 'warmest' | 'coldest' | 'most_polarising'
    ("mentions", pa.int64()),
    ("mean_sentiment_score", pa.float64()),
    ("polarisation_index", pa.float64()),
    ("positive_summary", pa.string()),
    ("negative_summary", pa.string()),
    ("positive_examples", pa.list_(pa.struct([
        ("text", pa.string()),
        ("score", pa.int64()),
        ("subreddit", pa.string()),
        ("permalink", pa.string()),
    ]))),
    ("negative_examples", pa.list_(pa.struct([
        ("text", pa.string()),
        ("score", pa.int64()),
        ("subreddit", pa.string()),
        ("permalink", pa.string()),
    ]))),
    ("generated_at", pa.timestamp("us", tz="UTC")),
    ("model", pa.string()),
    ("prompt_version", pa.string()),
])


class CampSummary(BaseModel):
    """Structured output for one camp (positive or negative)."""

    summary: str = Field(
        description=(
            "1-2 paragraph synthesis of what fans in this camp are saying. "
            "Written in third person about the fandom, NOT in the voice of a "
            "fan. Reference specific themes, not generic praise/criticism."
        ),
    )


class CharacterReasoning(BaseModel):
    """Two-camp synthesis for a single character."""

    positive_camp: CampSummary
    negative_camp: CampSummary


SYSTEM_PROMPT = """You are a careful fandom analyst. Your job is to read 16 Reddit posts about a Jujutsu Kaisen character — 8 most-upvoted positive, 8 most-upvoted negative — and produce a synthesis of what each camp is saying.

Style requirements:
- Third person about the fandom, NEVER first person ("Fans argue..." not "I think...")
- Reference SPECIFIC themes from the posts, not generic praise/criticism
- 1-2 paragraphs per camp, ~80-120 words each
- No spoilers about resolutions; describe what's said, not future story beats
- Avoid the words "complex", "nuanced", "iconic" — they're filler
- Don't use emoji or all-caps
- Don't quote the posts directly; the dashboard shows the quotes separately

Return valid JSON matching this schema:
{
  "positive_camp": { "summary": "..." },
  "negative_camp": { "summary": "..." }
}
"""


def build_summaries(paths: DataPaths | None = None) -> int:
    """Build char_summary.parquet for warmest, coldest, most polarising characters."""
    paths = paths or DataPaths.from_env()
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY not set")
    client = Groq(api_key=api_key)

    # Pick the three headliner characters from the gold layer.
    headliners = _pick_headliners(paths)
    if not headliners:
        log.warning("no_headliners_to_summarise")
        write_parquet_atomic([], paths.gold / "char_summary.parquet", SCHEMA)
        return 0

    rows: list[dict[str, Any]] = []
    generated_at = datetime.now(tz=UTC)

    for category, char in headliners.items():
        log.info(
            "summarise_start",
            category=category,
            character_id=char["character_id"],
            display_name=char["display_name"],
        )
        positives, negatives = _gather_quotes(paths, char["character_id"])
        if not positives or not negatives:
            log.warning(
                "skip_summary_insufficient_quotes",
                character_id=char["character_id"],
                positives=len(positives),
                negatives=len(negatives),
            )
            continue

        try:
            reasoning = _call_llm(client, char["display_name"], positives, negatives)
        except Exception as e:  # noqa: BLE001
            log.warning(
                "summary_llm_failed",
                character_id=char["character_id"],
                error=str(e),
            )
            continue

        rows.append({
            "character_id": char["character_id"],
            "display_name": char["display_name"],
            "category": category,
            "mentions": char["mentions"],
            "mean_sentiment_score": float(char["mean_sentiment_score"]),
            "polarisation_index": (
                float(char["polarisation_index"])
                if char["polarisation_index"] is not None
                else 0.0
            ),
            "positive_summary": reasoning.positive_camp.summary,
            "negative_summary": reasoning.negative_camp.summary,
            "positive_examples": positives,
            "negative_examples": negatives,
            "generated_at": generated_at,
            "model": SUMMARY_MODEL,
            "prompt_version": SUMMARY_PROMPT_VERSION,
        })

    dest = paths.gold / "char_summary.parquet"
    write_parquet_atomic(rows, dest, SCHEMA)
    log.info("char_summary_built", rows=len(rows), path=str(dest))
    return len(rows)


def _pick_headliners(paths: DataPaths) -> dict[str, dict[str, Any]]:
    """Pick warmest, coldest, and most polarising — same logic as HeroStats.

    Source of truth is the top 6 characters by mention volume (matches what
    Q_TOP_CHARACTERS returns to the dashboard). Sorting that set by mean
    sentiment gives warmest/coldest; sorting by polarisation gives most
    polarising. This guarantees clicking a hero stat shows that exact
    character's summary, no surprises.
    """
    pol = pq.read_table(paths.gold / "agg_polarisation.parquet").to_pylist()
    chars = pq.read_table(paths.gold / "dim_character.parquet").to_pylist()
    name_by_id = {c["character_id"]: c["display_name"] for c in chars}

    # Same as Q_TOP_CHARACTERS: top 6 by total_mentions DESC.
    top6 = sorted(pol, key=lambda r: -int(r["total_mentions"]))[:6]
    if not top6:
        return {}

    warmest = max(top6, key=lambda r: r["mean_sentiment_score"])
    coldest = min(top6, key=lambda r: r["mean_sentiment_score"])
    most_pol = max(
        top6,
        key=lambda r: r["polarisation_index"] if r["polarisation_index"] is not None else -1,
    )

    headliners: dict[str, dict[str, Any]] = {}
    for category, row in [("warmest", warmest), ("coldest", coldest), ("most_polarising", most_pol)]:
        if row["character_id"] in {h["character_id"] for h in headliners.values()}:
            continue
        headliners[category] = {
            "character_id": row["character_id"],
            "display_name": name_by_id.get(row["character_id"], row["character_id"]),
            "mentions": int(row["total_mentions"]),
            "mean_sentiment_score": row["mean_sentiment_score"],
            "polarisation_index": row["polarisation_index"],
        }
    return headliners


def _gather_quotes(
    paths: DataPaths, character_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Find top-upvoted positive and negative posts for a given character."""
    facts = pq.read_table(paths.gold / "fact_post_sentiment.parquet").to_pylist()
    posts = pq.read_table(paths.silver / "posts.parquet").to_pylist()
    posts_by_id = {p["id"]: p for p in posts}

    char_facts = [f for f in facts if f["character_id"] == character_id]
    enriched = []
    for f in char_facts:
        p = posts_by_id.get(f["post_id"])
        if not p:
            continue
        title = (p.get("title_clean") or p.get("title") or "").strip()
        body = (p.get("selftext_clean") or "").strip()
        text = title + ((" — " + body[:200]) if body else "")
        if not text or len(text) < 10:
            continue
        enriched.append({
            "text": text[:300],
            "score": int(p.get("score") or 0),
            "subreddit": p.get("subreddit", ""),
            "permalink": "https://reddit.com" + (p.get("permalink") or ""),
            "sentiment": f["sentiment"],
        })

    positives = sorted(
        [r for r in enriched if r["sentiment"] == "positive"],
        key=lambda r: r["score"], reverse=True,
    )[:QUOTES_PER_CAMP]
    negatives = sorted(
        [r for r in enriched if r["sentiment"] == "negative"],
        key=lambda r: r["score"], reverse=True,
    )[:QUOTES_PER_CAMP]

    # Strip helper fields the parquet schema doesn't have.
    for r in positives + negatives:
        r.pop("sentiment", None)
    return positives, negatives


@retry(
    wait=wait_exponential(multiplier=2, min=2, max=30),
    stop=stop_after_attempt(4),
    reraise=True,
)
def _call_llm(
    client: Groq,
    display_name: str,
    positives: list[dict[str, Any]],
    negatives: list[dict[str, Any]],
) -> CharacterReasoning:
    """Call Llama 3.3 70B for one character; return validated reasoning."""
    pos_block = "\n".join(f"  - ({p['score']}↑) {p['text']}" for p in positives)
    neg_block = "\n".join(f"  - ({n['score']}↑) {n['text']}" for n in negatives)
    user_msg = f"""Character: {display_name}

Top-upvoted POSITIVE posts ({len(positives)}):
{pos_block}

Top-upvoted NEGATIVE posts ({len(negatives)}):
{neg_block}

Synthesize each camp following the rules in the system prompt. Return only the JSON object."""

    response = client.chat.completions.create(
        model=SUMMARY_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ],
        temperature=0.4,
        response_format={"type": "json_object"},
        max_tokens=1000,
    )
    raw = response.choices[0].message.content or "{}"
    parsed = json.loads(raw)
    return CharacterReasoning.model_validate(parsed)