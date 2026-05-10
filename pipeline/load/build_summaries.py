"""Build per-character LLM-generated reasoning summaries.

For three "headline" characters from the dashboard — warmest reception,
coldest reception, most polarising — gather their top-upvoted positive
and/or negative posts and have Llama 3.3 70B synthesize what the fandom
actually thinks about them.

Per-category specialisation:
  - warmest         → deep positive-only synthesis
  - coldest         → deep negative-only synthesis
  - most_polarising → both camps; the split IS the story

Cached as gold/char_summary.parquet. Runs after build_gold; the dashboard
reads from this cache, never calls the LLM at view time.

Cost: 3 characters × ~3K input + ~500 output tokens × Dev tier rate
≈ $0.005/run.
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

# Use 70B for synthesis quality. 8B works but 70B is dramatically better
# at distilling 16 fan posts into a coherent paragraph that reads like
# real analysis. Cost is rounding error at this volume.
SUMMARY_MODEL = "llama-3.3-70b-versatile"
SUMMARY_PROMPT_VERSION = "v2"  # bumped for the deeper-interpretation prompt

# How many top-upvoted quotes per camp. 8 gives the LLM enough variety
# to spot patterns; more becomes redundant.
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
    """Structured output for one camp."""
    summary: str = Field(
        description=(
            "An interpretive paragraph (140-220 words) that explains the "
            "fandom's actual thesis about the character — not a description "
            "of what individual posts say."
        ),
    )


class TwoCampReasoning(BaseModel):
    """For polarising characters where both camps matter."""
    positive_camp: CampSummary
    negative_camp: CampSummary


class SingleCampReasoning(BaseModel):
    """For warmest/coldest where only one camp is the story."""
    summary: CampSummary


# === System prompts =========================================================

# The shared writing guide all three prompts share. The point is to push
# the model from descriptive ("fans discuss X") to interpretive ("the
# fandom's reading of X is that..."). Banned words remove filler.
COMMON_RULES = """\
Style requirements:
- Third person about the fandom, NEVER first person ("Fans see..." not "I think...")
- Reference SPECIFIC story moments by name when fans bring them up — chapters,
  arcs, deaths, fights, moments. Don't paraphrase to "the recent events."
- INTERPRETIVE not DESCRIPTIVE — explain WHY fans hold this view, what it
  reveals about the character's role in the manga. Avoid summarizing posts.
- 140-220 words. Substantial enough to actually develop a thesis.
- No spoilers about future story beats; describe what's said, not predictions.
- Banned filler words: "appreciate", "celebrate", "iconic", "complex",
  "nuanced", "showcase", "dive deep", "give justice". Remove them and
  rewrite with concrete language.
- Don't quote the posts; the dashboard shows the quotes separately.
- Don't use emoji or all-caps for emphasis.
"""

WARMEST_PROMPT = f"""You are a careful fandom analyst writing about a Jujutsu Kaisen character who the fandom warmly receives.

Your job: read 8 top-upvoted positive Reddit posts about this character and produce ONE interpretive paragraph that explains why the fandom is drawn to them. What does this character represent in the story? What thematic role do fans see them filling? What recurring framings appear across these posts?

Do not list what each post says. Synthesize the underlying thesis.

{COMMON_RULES}

Return valid JSON matching this schema:
{{
  "summary": {{ "summary": "..." }}
}}
"""

COLDEST_PROMPT = f"""You are a careful fandom analyst writing about a Jujutsu Kaisen character the fandom is cold toward.

Your job: read 8 top-upvoted negative Reddit posts about this character and produce ONE interpretive paragraph that explains the fandom's frustration. What is the core complaint? Is it about the character's writing, their arc, their use, their power level, their treatment by Gege? What does this resentment reveal about how fans expected the character to function in the story?

Do not list what each post says. Synthesize the underlying thesis.

{COMMON_RULES}

Return valid JSON matching this schema:
{{
  "summary": {{ "summary": "..." }}
}}
"""

POLARISING_PROMPT = f"""You are a careful fandom analyst writing about a Jujutsu Kaisen character the fandom is sharply divided about.

Your job: read 8 top-upvoted positive AND 8 top-upvoted negative Reddit posts about this character. Produce TWO interpretive paragraphs:

1. The positive camp — what's the fandom's case for this character? What do they represent in the story to people who love them?

2. The critical camp — what's the case against? What's the recurring complaint, and what does it reveal about how fans wanted the character to be handled?

The point is to make the disagreement legible. Don't soften either side; both are real positions held by real fans. Don't list posts; synthesize the theses.

{COMMON_RULES}

Return valid JSON matching this schema:
{{
  "positive_camp": {{ "summary": "..." }},
  "negative_camp": {{ "summary": "..." }}
}}
"""


def build_summaries(paths: DataPaths | None = None) -> int:
    """Build char_summary.parquet for warmest, coldest, most polarising."""
    paths = paths or DataPaths.from_env()
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY not set")
    client = Groq(api_key=api_key)

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

        # Pick prompt and required quotes by category.
        if category == "warmest":
            if not positives:
                log.warning("skip_warmest_no_positives", character_id=char["character_id"])
                continue
            try:
                pos_text = _call_single_camp(
                    client, char["display_name"], WARMEST_PROMPT, positives, "POSITIVE"
                )
            except Exception as e:  # noqa: BLE001
                log.warning("summary_failed", category=category, error=str(e))
                continue
            neg_text = ""
            negatives = []  # don't show negative quotes for warmest
        elif category == "coldest":
            if not negatives:
                log.warning("skip_coldest_no_negatives", character_id=char["character_id"])
                continue
            try:
                neg_text = _call_single_camp(
                    client, char["display_name"], COLDEST_PROMPT, negatives, "NEGATIVE"
                )
            except Exception as e:  # noqa: BLE001
                log.warning("summary_failed", category=category, error=str(e))
                continue
            pos_text = ""
            positives = []  # don't show positive quotes for coldest
        else:  # most_polarising
            if not positives or not negatives:
                log.warning(
                    "skip_polarising_insufficient",
                    character_id=char["character_id"],
                    positives=len(positives),
                    negatives=len(negatives),
                )
                continue
            try:
                pos_text, neg_text = _call_two_camps(
                    client, char["display_name"], POLARISING_PROMPT, positives, negatives
                )
            except Exception as e:  # noqa: BLE001
                log.warning("summary_failed", category=category, error=str(e))
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
            "positive_summary": pos_text,
            "negative_summary": neg_text,
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

    Source of truth is the top 6 characters by mention volume (matches
    Q_TOP_CHARACTERS on the dashboard). Sorting that set by mean sentiment
    gives warmest/coldest; sorting by polarisation gives most polarising.
    Guarantees clicking a hero stat shows that exact character's summary.
    """
    pol = pq.read_table(paths.gold / "agg_polarisation.parquet").to_pylist()
    chars = pq.read_table(paths.gold / "dim_character.parquet").to_pylist()
    name_by_id = {c["character_id"]: c["display_name"] for c in chars}

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

    for r in positives + negatives:
        r.pop("sentiment", None)
    return positives, negatives


@retry(
    wait=wait_exponential(multiplier=2, min=2, max=30),
    stop=stop_after_attempt(4),
    reraise=True,
)
def _call_single_camp(
    client: Groq,
    display_name: str,
    system_prompt: str,
    quotes: list[dict[str, Any]],
    camp_label: str,
) -> str:
    """Call LLM for warmest or coldest — one camp only, longer paragraph."""
    quote_block = "\n".join(f"  - ({q['score']}↑) {q['text']}" for q in quotes)
    user_msg = f"""Character: {display_name}

Top-upvoted {camp_label} posts ({len(quotes)}):
{quote_block}

Write the interpretive paragraph following the rules in the system prompt. Return only the JSON object."""
    response = client.chat.completions.create(
        model=SUMMARY_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_msg},
        ],
        temperature=0.5,  # slightly higher for richer interpretation
        response_format={"type": "json_object"},
        max_tokens=900,
    )
    raw = response.choices[0].message.content or "{}"
    parsed = json.loads(raw)
    validated = SingleCampReasoning.model_validate(parsed)
    return validated.summary.summary


@retry(
    wait=wait_exponential(multiplier=2, min=2, max=30),
    stop=stop_after_attempt(4),
    reraise=True,
)
def _call_two_camps(
    client: Groq,
    display_name: str,
    system_prompt: str,
    positives: list[dict[str, Any]],
    negatives: list[dict[str, Any]],
) -> tuple[str, str]:
    """Call LLM for polarising — both camps, the split is the story."""
    pos_block = "\n".join(f"  - ({p['score']}↑) {p['text']}" for p in positives)
    neg_block = "\n".join(f"  - ({n['score']}↑) {n['text']}" for n in negatives)
    user_msg = f"""Character: {display_name}

Top-upvoted POSITIVE posts ({len(positives)}):
{pos_block}

Top-upvoted NEGATIVE posts ({len(negatives)}):
{neg_block}

Write both interpretive paragraphs following the rules in the system prompt. Return only the JSON object."""
    response = client.chat.completions.create(
        model=SUMMARY_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_msg},
        ],
        temperature=0.5,
        response_format={"type": "json_object"},
        max_tokens=1400,
    )
    raw = response.choices[0].message.content or "{}"
    parsed = json.loads(raw)
    validated = TwoCampReasoning.model_validate(parsed)
    return validated.positive_camp.summary, validated.negative_camp.summary