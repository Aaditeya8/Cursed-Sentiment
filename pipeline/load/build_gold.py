"""Silver → Gold: denormalized dims, fact tables, weekly aggregations.

This is the layer the dashboard reads. Every table here is a query result
materialized as parquet so DuckDB-WASM in the browser can scan it in
milliseconds; nothing here is JOIN-heavy at read time.

Build order (each step depends on prior steps):

1. ``dim_character`` -- denormalized from ``reference/characters.yaml``
2. ``dim_event``     -- denormalized from ``reference/events.yaml``
3. ``fact_post_sentiment``    -- silver/posts × character mentions × classifications
4. ``fact_comment_sentiment`` -- silver/comments × character mentions × classifications
5. ``agg_char_week``  -- weekly per-character rollup with mean sentiment + polarisation
6. ``agg_polarisation`` -- top-line per-character rankings
7. ``gege_moments``   -- weeks with z-score > 2 vs trailing baseline, paired with events

Idempotence: every step reads input parquets and overwrites its output
parquet atomically. Re-running is safe and produces the same result.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import structlog
import typer
import yaml

from pipeline.extract._common import UTC, DataPaths, write_parquet_atomic
from pipeline.transform.resolve_characters import (
    REGISTRY_PATH as CHAR_REGISTRY_PATH,
    CharacterRegistry,
)

log = structlog.get_logger(__name__)

# Sentiment numeric encoding for averaging. Mixed and neutral both map to 0,
# but they're distinguished in the *share* columns -- we don't want a single
# user's mixed feelings to look like absent feelings in the aggregate.
SENTIMENT_SCORE = {
    "positive": 1.0,
    "negative": -1.0,
    "mixed": 0.0,
    "neutral": 0.0,
}
INTENSITY_NUMERIC = {"low": 1.0, "medium": 2.0, "high": 3.0}

# Z-score threshold for the Gege moment detector.
GEGE_Z_THRESHOLD = 2.0
# Don't fire a moment unless the week has at least this many mentions; otherwise
# noise on small-sample weeks dominates.
GEGE_MIN_MENTIONS = 10
# Trailing window for the z-score baseline. 12 weeks is a quarter -- short
# enough to track shifting baselines, long enough to be statistically meaningful.
GEGE_BASELINE_WEEKS = 12
# Pair a moment with the closest event within this many days, or leave unpaired.
GEGE_PAIR_WINDOW_DAYS = 7

EVENTS_PATH = CHAR_REGISTRY_PATH.parent / "events.yaml"


# --- schemas ------------------------------------------------------------------


DIM_CHARACTER_SCHEMA = pa.schema(
    [
        ("character_id", pa.string()),
        ("display_name", pa.string()),
        ("role", pa.string()),
        ("affiliation", pa.string()),
        ("status", pa.string()),
        ("alias_count", pa.int32()),
        ("notes", pa.string()),
    ]
)


DIM_EVENT_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("event_date", pa.timestamp("us", tz="UTC")),
        ("chapter", pa.int32()),
        ("arc", pa.string()),
        ("title", pa.string()),
        ("description", pa.string()),
        ("spoiler_intensity", pa.string()),
        ("medium", pa.string()),
        ("verified", pa.bool_()),
        ("characters", pa.list_(pa.string())),
    ]
)


FACT_POST_SENTIMENT_SCHEMA = pa.schema(
    [
        ("post_id", pa.string()),
        ("character_id", pa.string()),
        ("alias_matched", pa.string()),
        ("mention_weight", pa.float64()),
        ("sentiment", pa.string()),
        ("sentiment_score", pa.float64()),
        ("sentiment_confidence", pa.float64()),
        ("intensity", pa.string()),
        ("target", pa.string()),
        ("subreddit", pa.string()),
        ("created_at", pa.timestamp("us", tz="UTC")),
        ("created_utc", pa.int64()),
        ("score", pa.int64()),
        ("num_comments", pa.int64()),
        ("spoiler", pa.bool_()),
        ("is_deleted", pa.bool_()),
        ("permalink", pa.string()),
        ("title", pa.string()),
    ]
)


FACT_COMMENT_SENTIMENT_SCHEMA = pa.schema(
    [
        ("comment_id", pa.string()),
        ("post_id", pa.string()),
        ("character_id", pa.string()),
        ("alias_matched", pa.string()),
        ("mention_weight", pa.float64()),
        ("sentiment", pa.string()),
        ("sentiment_score", pa.float64()),
        ("sentiment_confidence", pa.float64()),
        ("intensity", pa.string()),
        ("target", pa.string()),
        ("subreddit", pa.string()),
        ("created_at", pa.timestamp("us", tz="UTC")),
        ("created_utc", pa.int64()),
        ("score", pa.int64()),
        ("is_deleted", pa.bool_()),
        ("permalink", pa.string()),
    ]
)


AGG_CHAR_WEEK_SCHEMA = pa.schema(
    [
        ("character_id", pa.string()),
        ("year", pa.int32()),
        ("week", pa.int32()),
        ("week_start", pa.timestamp("us", tz="UTC")),
        ("mention_count", pa.int64()),
        ("post_count", pa.int64()),
        ("comment_count", pa.int64()),
        ("weighted_sentiment", pa.float64()),
        ("mean_sentiment_score", pa.float64()),
        ("share_positive", pa.float64()),
        ("share_negative", pa.float64()),
        ("share_mixed", pa.float64()),
        ("share_neutral", pa.float64()),
        ("share_high_intensity", pa.float64()),
        ("polarisation_index", pa.float64()),
        ("polarisation_entropy", pa.float64()),
    ]
)


AGG_POLARISATION_SCHEMA = pa.schema(
    [
        ("character_id", pa.string()),
        ("total_mentions", pa.int64()),
        ("mean_sentiment_score", pa.float64()),
        ("polarisation_index", pa.float64()),
        ("polarisation_entropy", pa.float64()),
        ("polarisation_rank", pa.int32()),
        ("most_mentioned_rank", pa.int32()),
    ]
)


GEGE_MOMENTS_SCHEMA = pa.schema(
    [
        ("character_id", pa.string()),
        ("week_start", pa.timestamp("us", tz="UTC")),
        ("sentiment_score", pa.float64()),
        ("baseline_mean", pa.float64()),
        ("baseline_std", pa.float64()),
        ("z_score", pa.float64()),
        ("mention_count", pa.int64()),
        ("paired_event_id", pa.string()),
        ("paired_event_title", pa.string()),
        ("paired_event_distance_days", pa.int32()),
    ]
)


# --- dim_character / dim_event ------------------------------------------------


def build_dim_character(paths: DataPaths | None = None) -> int:
    """Denormalize ``characters.yaml`` into a parquet the dashboard can read."""
    paths = paths or DataPaths.from_env()
    with CHAR_REGISTRY_PATH.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    rows: list[dict[str, Any]] = []
    for c in raw:
        rows.append(
            {
                "character_id": c["id"],
                "display_name": c["display_name"],
                "role": c["role"],
                "affiliation": c["affiliation"],
                "status": c["status"],
                "alias_count": len(c["aliases"]),
                "notes": c.get("notes", "") or "",
            }
        )

    paths.gold.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(rows, paths.gold / "dim_character.parquet", DIM_CHARACTER_SCHEMA)
    return len(rows)


def build_dim_event(paths: DataPaths | None = None) -> int:
    """Denormalize ``events.yaml`` into a parquet for the chart overlay."""
    paths = paths or DataPaths.from_env()
    with EVENTS_PATH.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    rows: list[dict[str, Any]] = []
    for e in raw:
        # YAML loads dates as date objects; we want full timestamps in UTC.
        d = e["date"]
        if hasattr(d, "year") and not isinstance(d, datetime):
            event_date = datetime(d.year, d.month, d.day, tzinfo=UTC)
        else:
            event_date = d
        rows.append(
            {
                "event_id": e["id"],
                "event_date": event_date,
                "chapter": e.get("chapter") if e.get("chapter") is not None else None,
                "arc": e["arc"],
                "title": e["title"],
                "description": e.get("description", "") or "",
                "spoiler_intensity": e.get("spoiler_intensity", "low") or "low",
                "medium": e.get("medium", "manga") or "manga",
                "verified": bool(e.get("verified", False)),
                "characters": e.get("characters") or [],
            }
        )

    paths.gold.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(rows, paths.gold / "dim_event.parquet", DIM_EVENT_SCHEMA)
    return len(rows)


# --- fact tables --------------------------------------------------------------


def _post_text(row: dict[str, Any]) -> str:
    """Same combiner as the classifier uses; ensures the character resolver
    sees the same text the classifier saw."""
    title = (row.get("title") or "").strip()
    body = (row.get("selftext") or "").strip()
    if title and body:
        return f"{title}\n\n{body}"
    return title or body


def _strip_t3_prefix(link_id: str) -> str:
    """Reddit's ``link_id`` prefixes the post id with ``t3_``; we strip it
    so the fact table's ``post_id`` matches silver/posts.id directly."""
    return link_id[3:] if link_id.startswith("t3_") else link_id


def build_fact_post_sentiment(
    paths: DataPaths | None = None,
    registry: CharacterRegistry | None = None,
) -> int:
    """Join silver posts with character mentions and classifications.

    One row per (post, character_mention). A post mentioning two characters
    yields two rows. A post mentioning none yields zero rows -- by design,
    the fact table is only about character-attributable sentiment.
    """
    paths = paths or DataPaths.from_env()
    registry = registry or CharacterRegistry.from_yaml()

    posts_path = paths.silver / "posts.parquet"
    cls_path = paths.silver / "post_classifications.parquet"
    if not posts_path.exists():
        log.warning("gold_no_silver_posts", path=str(posts_path))
        write_parquet_atomic(
            [], paths.gold / "fact_post_sentiment.parquet", FACT_POST_SENTIMENT_SCHEMA
        )
        return 0

    posts = pq.read_table(posts_path).to_pylist()
    classifications = (
        {r["id"]: r for r in pq.read_table(cls_path).to_pylist()}
        if cls_path.exists()
        else {}
    )

    rows: list[dict[str, Any]] = []
    for p in posts:
        cls = classifications.get(p["id"])
        if cls is None:
            continue
        text = _post_text(p)
        for m in registry.find_mentions(text):
            rows.append(
                {
                    "post_id": p["id"],
                    "character_id": m.character_id,
                    "alias_matched": m.alias_matched,
                    "mention_weight": m.weight,
                    "sentiment": cls["sentiment"],
                    "sentiment_score": SENTIMENT_SCORE[cls["sentiment"]],
                    "sentiment_confidence": cls["confidence"],
                    "intensity": cls["intensity"],
                    "target": cls["target"],
                    "subreddit": p["subreddit"],
                    "created_at": p["created_at"],
                    "created_utc": p["created_utc"],
                    "score": p["score"],
                    "num_comments": p["num_comments"],
                    "spoiler": p["spoiler"],
                    "is_deleted": p["is_deleted"],
                    "permalink": p["permalink"],
                    "title": p["title"],
                }
            )

    paths.gold.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(
        rows, paths.gold / "fact_post_sentiment.parquet", FACT_POST_SENTIMENT_SCHEMA
    )
    log.info("fact_post_sentiment_built", rows=len(rows), input_posts=len(posts))
    return len(rows)


def build_fact_comment_sentiment(
    paths: DataPaths | None = None,
    registry: CharacterRegistry | None = None,
) -> int:
    """Same shape as the post fact table, for comments."""
    paths = paths or DataPaths.from_env()
    registry = registry or CharacterRegistry.from_yaml()

    comments_path = paths.silver / "comments.parquet"
    cls_path = paths.silver / "comment_classifications.parquet"
    if not comments_path.exists():
        log.warning("gold_no_silver_comments", path=str(comments_path))
        write_parquet_atomic(
            [], paths.gold / "fact_comment_sentiment.parquet",
            FACT_COMMENT_SENTIMENT_SCHEMA,
        )
        return 0

    comments = pq.read_table(comments_path).to_pylist()
    classifications = (
        {r["id"]: r for r in pq.read_table(cls_path).to_pylist()}
        if cls_path.exists()
        else {}
    )

    rows: list[dict[str, Any]] = []
    for c in comments:
        cls = classifications.get(c["id"])
        if cls is None:
            continue
        for m in registry.find_mentions(c.get("body") or ""):
            rows.append(
                {
                    "comment_id": c["id"],
                    "post_id": _strip_t3_prefix(c.get("link_id") or ""),
                    "character_id": m.character_id,
                    "alias_matched": m.alias_matched,
                    "mention_weight": m.weight,
                    "sentiment": cls["sentiment"],
                    "sentiment_score": SENTIMENT_SCORE[cls["sentiment"]],
                    "sentiment_confidence": cls["confidence"],
                    "intensity": cls["intensity"],
                    "target": cls["target"],
                    "subreddit": c["subreddit"],
                    "created_at": c["created_at"],
                    "created_utc": c["created_utc"],
                    "score": c["score"],
                    "is_deleted": c["is_deleted"],
                    "permalink": c["permalink"],
                }
            )

    paths.gold.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(
        rows, paths.gold / "fact_comment_sentiment.parquet",
        FACT_COMMENT_SENTIMENT_SCHEMA,
    )
    log.info(
        "fact_comment_sentiment_built", rows=len(rows), input_comments=len(comments)
    )
    return len(rows)


# --- aggregations -------------------------------------------------------------


def _entropy(shares: list[float]) -> float:
    """Shannon entropy of a probability distribution, normalized to [0, 1]
    by dividing by ``log(K)``. NaN if all shares are zero."""
    nonzero = [s for s in shares if s > 0]
    if not nonzero:
        return float("nan")
    h = -sum(s * math.log(s) for s in nonzero)
    return h / math.log(len(shares))


def _polarisation_index(positive_count: float, negative_count: float) -> float:
    """1 - 2·|p - 0.5| where p = positive / (positive + negative).

    1.0 means perfect 50/50 split (maximum polarisation).
    0.0 means one side dominates entirely.
    NaN if neither sentiment was expressed at all.
    """
    total = positive_count + negative_count
    if total == 0:
        return float("nan")
    p = positive_count / total
    return 1.0 - 2.0 * abs(p - 0.5)


def build_agg_char_week(paths: DataPaths | None = None) -> int:
    """Weekly per-character rollup. Pulls from BOTH fact tables."""
    paths = paths or DataPaths.from_env()
    fact_post = paths.gold / "fact_post_sentiment.parquet"
    fact_comment = paths.gold / "fact_comment_sentiment.parquet"

    if not fact_post.exists() and not fact_comment.exists():
        write_parquet_atomic([], paths.gold / "agg_char_week.parquet", AGG_CHAR_WEEK_SCHEMA)
        return 0

    con = duckdb.connect(":memory:")

    parts: list[str] = []
    if fact_post.exists():
        parts.append(f"""
            SELECT character_id, sentiment, sentiment_score, sentiment_confidence,
                   intensity, mention_weight, created_at, 'post' AS kind
            FROM read_parquet('{fact_post}')
        """)
    if fact_comment.exists():
        parts.append(f"""
            SELECT character_id, sentiment, sentiment_score, sentiment_confidence,
                   intensity, mention_weight, created_at, 'comment' AS kind
            FROM read_parquet('{fact_comment}')
        """)
    union_sql = " UNION ALL ".join(parts)

    rows = con.execute(f"""
        WITH facts AS ({union_sql}),
        weekly AS (
            SELECT
                character_id,
                CAST(strftime(created_at, '%Y') AS INTEGER) AS year,
                CAST(strftime(created_at, '%V') AS INTEGER) AS week,
                date_trunc('week', created_at AT TIME ZONE 'UTC') AS week_start,
                sentiment,
                intensity,
                kind,
                mention_weight * sentiment_confidence AS row_weight,
                sentiment_score
            FROM facts
        )
        SELECT
            character_id, year, week, week_start,
            COUNT(*) AS mention_count,
            SUM(CASE WHEN kind = 'post' THEN 1 ELSE 0 END) AS post_count,
            SUM(CASE WHEN kind = 'comment' THEN 1 ELSE 0 END) AS comment_count,
            SUM(sentiment_score * row_weight) / NULLIF(SUM(row_weight), 0) AS mean_sentiment_score,
            SUM(row_weight) AS total_weight,
            SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) AS pos_count,
            SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) AS neg_count,
            SUM(CASE WHEN sentiment = 'mixed'    THEN 1 ELSE 0 END) AS mix_count,
            SUM(CASE WHEN sentiment = 'neutral'  THEN 1 ELSE 0 END) AS neu_count,
            SUM(CASE WHEN intensity = 'high'     THEN 1 ELSE 0 END) AS high_count
        FROM weekly
        GROUP BY character_id, year, week, week_start
        ORDER BY character_id, year, week
    """).fetchall()
    columns = [d[0] for d in con.description]

    out: list[dict[str, Any]] = []
    for raw in rows:
        d = dict(zip(columns, raw))
        n = d["mention_count"]
        shares = [d["pos_count"] / n, d["neg_count"] / n, d["mix_count"] / n, d["neu_count"] / n]
        out.append(
            {
                "character_id": d["character_id"],
                "year": d["year"],
                "week": d["week"],
                "week_start": d["week_start"],
                "mention_count": n,
                "post_count": d["post_count"],
                "comment_count": d["comment_count"],
                "weighted_sentiment": d["total_weight"] or 0.0,
                "mean_sentiment_score": d["mean_sentiment_score"] or 0.0,
                "share_positive": shares[0],
                "share_negative": shares[1],
                "share_mixed": shares[2],
                "share_neutral": shares[3],
                "share_high_intensity": d["high_count"] / n,
                "polarisation_index": _polarisation_index(d["pos_count"], d["neg_count"]),
                "polarisation_entropy": _entropy(shares),
            }
        )

    paths.gold.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(out, paths.gold / "agg_char_week.parquet", AGG_CHAR_WEEK_SCHEMA)
    log.info("agg_char_week_built", rows=len(out))
    return len(out)


def build_agg_polarisation(paths: DataPaths | None = None) -> int:
    """Per-character all-time aggregates with polarisation and mention rankings."""
    paths = paths or DataPaths.from_env()
    week = paths.gold / "agg_char_week.parquet"
    if not week.exists():
        write_parquet_atomic(
            [], paths.gold / "agg_polarisation.parquet", AGG_POLARISATION_SCHEMA
        )
        return 0

    con = duckdb.connect(":memory:")
    rows = con.execute(f"""
        WITH agg AS (
            SELECT
                character_id,
                SUM(mention_count) AS total_mentions,
                SUM(mean_sentiment_score * mention_count) / NULLIF(SUM(mention_count), 0)
                    AS mean_sentiment_score,
                SUM(share_positive * mention_count) AS pos_count,
                SUM(share_negative * mention_count) AS neg_count,
                SUM(share_mixed * mention_count)    AS mix_count,
                SUM(share_neutral * mention_count)  AS neu_count
            FROM read_parquet('{week}')
            GROUP BY character_id
        )
        SELECT
            character_id, total_mentions, mean_sentiment_score,
            pos_count, neg_count, mix_count, neu_count
        FROM agg
        ORDER BY total_mentions DESC
    """).fetchall()
    columns = [d[0] for d in con.description]

    summaries = [dict(zip(columns, r)) for r in rows]
    for s in summaries:
        n = s["total_mentions"] or 0
        if n == 0:
            s["polarisation_index"] = float("nan")
            s["polarisation_entropy"] = float("nan")
            continue
        shares = [s["pos_count"] / n, s["neg_count"] / n, s["mix_count"] / n, s["neu_count"] / n]
        s["polarisation_index"] = _polarisation_index(s["pos_count"], s["neg_count"])
        s["polarisation_entropy"] = _entropy(shares)

    pol_sorted = sorted(
        summaries,
        key=lambda x: x["polarisation_index"] if not math.isnan(x["polarisation_index"]) else -1,
        reverse=True,
    )
    pol_rank = {s["character_id"]: i + 1 for i, s in enumerate(pol_sorted)}
    mention_sorted = sorted(summaries, key=lambda x: x["total_mentions"], reverse=True)
    mention_rank = {s["character_id"]: i + 1 for i, s in enumerate(mention_sorted)}

    out = [
        {
            "character_id": s["character_id"],
            "total_mentions": int(s["total_mentions"] or 0),
            "mean_sentiment_score": s["mean_sentiment_score"] or 0.0,
            "polarisation_index": s["polarisation_index"],
            "polarisation_entropy": s["polarisation_entropy"],
            "polarisation_rank": pol_rank[s["character_id"]],
            "most_mentioned_rank": mention_rank[s["character_id"]],
        }
        for s in summaries
    ]

    paths.gold.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(out, paths.gold / "agg_polarisation.parquet", AGG_POLARISATION_SCHEMA)
    log.info("agg_polarisation_built", rows=len(out))
    return len(out)


# --- Gege moments -------------------------------------------------------------


def build_gege_moments(paths: DataPaths | None = None) -> int:
    """Weeks where a character's sentiment shifted >2σ from the trailing baseline."""
    paths = paths or DataPaths.from_env()
    week = paths.gold / "agg_char_week.parquet"
    events = paths.gold / "dim_event.parquet"
    if not week.exists():
        write_parquet_atomic([], paths.gold / "gege_moments.parquet", GEGE_MOMENTS_SCHEMA)
        return 0

    weeks = pq.read_table(week).to_pylist()
    weeks.sort(key=lambda r: (r["character_id"], r["week_start"]))

    event_rows = pq.read_table(events).to_pylist() if events.exists() else []

    moments: list[dict[str, Any]] = []
    by_char: dict[str, list[dict[str, Any]]] = {}
    for r in weeks:
        by_char.setdefault(r["character_id"], []).append(r)

    for char_id, char_weeks in by_char.items():
        for i, current in enumerate(char_weeks):
            if current["mention_count"] < GEGE_MIN_MENTIONS:
                continue
            baseline_window = char_weeks[max(0, i - GEGE_BASELINE_WEEKS) : i]
            if len(baseline_window) < GEGE_BASELINE_WEEKS:
                continue
            scores = [w["mean_sentiment_score"] for w in baseline_window]
            mean = sum(scores) / len(scores)
            variance = sum((s - mean) ** 2 for s in scores) / len(scores)
            std = math.sqrt(variance) if variance > 0 else 0.0
            if std == 0.0:
                continue
            z = (current["mean_sentiment_score"] - mean) / std
            if abs(z) <= GEGE_Z_THRESHOLD:
                continue

            paired = _pair_with_event(current["week_start"], event_rows)
            moments.append(
                {
                    "character_id": char_id,
                    "week_start": current["week_start"],
                    "sentiment_score": current["mean_sentiment_score"],
                    "baseline_mean": mean,
                    "baseline_std": std,
                    "z_score": z,
                    "mention_count": current["mention_count"],
                    "paired_event_id": paired["event_id"] if paired else None,
                    "paired_event_title": paired["title"] if paired else None,
                    "paired_event_distance_days": (
                        paired["distance_days"] if paired else None
                    ),
                }
            )

    paths.gold.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(moments, paths.gold / "gege_moments.parquet", GEGE_MOMENTS_SCHEMA)
    log.info("gege_moments_built", rows=len(moments))
    return len(moments)


def _pair_with_event(week_start: datetime, events: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Find the closest event within ±GEGE_PAIR_WINDOW_DAYS, or return None."""
    if not events:
        return None
    week_dt = (
        week_start
        if isinstance(week_start, datetime)
        else datetime.combine(week_start, datetime.min.time(), tzinfo=UTC)
    )
    best = None
    best_dist = math.inf
    for e in events:
        ev_dt = e["event_date"]
        if not isinstance(ev_dt, datetime):
            ev_dt = datetime(ev_dt.year, ev_dt.month, ev_dt.day, tzinfo=UTC)
        dist_days = abs((ev_dt - week_dt).days)
        if dist_days <= GEGE_PAIR_WINDOW_DAYS and dist_days < best_dist:
            best = {
                "event_id": e["event_id"],
                "title": e["title"],
                "distance_days": dist_days,
            }
            best_dist = dist_days
    return best


# --- top-level orchestrator ---------------------------------------------------


@dataclass
class GoldBuildResult:
    dim_character: int
    dim_event: int
    fact_post_sentiment: int
    fact_comment_sentiment: int
    agg_char_week: int
    agg_polarisation: int
    gege_moments: int


def build_all(paths: DataPaths | None = None) -> GoldBuildResult:
    """Build every gold table in dependency order. Idempotent."""
    paths = paths or DataPaths.from_env()
    registry = CharacterRegistry.from_yaml()

    return GoldBuildResult(
        dim_character=build_dim_character(paths),
        dim_event=build_dim_event(paths),
        fact_post_sentiment=build_fact_post_sentiment(paths, registry),
        fact_comment_sentiment=build_fact_comment_sentiment(paths, registry),
        agg_char_week=build_agg_char_week(paths),
        agg_polarisation=build_agg_polarisation(paths),
        gege_moments=build_gege_moments(paths),
    )


# --- CLI ----------------------------------------------------------------------

app = typer.Typer(help="Build the gold layer (dims + facts + aggregations)")


@app.command()
def run() -> None:
    result = build_all()
    typer.echo(
        f"Gold built:\n"
        f"  dim_character:           {result.dim_character}\n"
        f"  dim_event:               {result.dim_event}\n"
        f"  fact_post_sentiment:     {result.fact_post_sentiment}\n"
        f"  fact_comment_sentiment:  {result.fact_comment_sentiment}\n"
        f"  agg_char_week:           {result.agg_char_week}\n"
        f"  agg_polarisation:        {result.agg_polarisation}\n"
        f"  gege_moments:            {result.gege_moments}"
    )


if __name__ == "__main__":
    app()