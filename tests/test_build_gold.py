"""Tests for ``pipeline.load.build_gold``.

Three layers:

1. Pure math helpers (``_polarisation_index``, ``_entropy``, ``_pair_with_event``)
2. Individual builders against parquet fixtures on tmp_path
3. End-to-end ``build_all`` with a minimal silver dataset
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pipeline.extract._common import DataPaths, write_parquet_atomic
from pipeline.load.build_gold import (
    AGG_CHAR_WEEK_SCHEMA,
    AGG_POLARISATION_SCHEMA,
    DIM_CHARACTER_SCHEMA,
    DIM_EVENT_SCHEMA,
    FACT_POST_SENTIMENT_SCHEMA,
    GEGE_MOMENTS_SCHEMA,
    SENTIMENT_SCORE,
    _entropy,
    _pair_with_event,
    _polarisation_index,
    _strip_t3_prefix,
    build_agg_char_week,
    build_agg_polarisation,
    build_all,
    build_dim_character,
    build_dim_event,
    build_fact_post_sentiment,
    build_gege_moments,
)
from pipeline.transform.classify_sentiment import CLASSIFICATION_SCHEMA
from pipeline.transform.clean import COMMENT_SILVER_SCHEMA, POST_SILVER_SCHEMA

UTC = timezone.utc


# --- pure math helpers --------------------------------------------------------


def test_polarisation_index_perfect_split_is_one() -> None:
    assert _polarisation_index(50, 50) == pytest.approx(1.0)


def test_polarisation_index_one_sided_is_zero() -> None:
    assert _polarisation_index(100, 0) == pytest.approx(0.0)
    assert _polarisation_index(0, 100) == pytest.approx(0.0)


def test_polarisation_index_three_to_one_is_intermediate() -> None:
    """75/25 should produce 0.5 — exactly halfway between perfect-split and
    one-sided. Sanity check that the formula is symmetric and linear."""
    assert _polarisation_index(75, 25) == pytest.approx(0.5)
    assert _polarisation_index(25, 75) == pytest.approx(0.5)


def test_polarisation_index_no_opinionated_mentions_is_nan() -> None:
    """If neither positive nor negative is present at all, polarisation is
    undefined — not zero. We surface that with NaN."""
    assert math.isnan(_polarisation_index(0, 0))


def test_entropy_uniform_distribution_is_one() -> None:
    """Maximum entropy: all four classes equally represented."""
    assert _entropy([0.25, 0.25, 0.25, 0.25]) == pytest.approx(1.0)


def test_entropy_single_class_is_zero() -> None:
    assert _entropy([1.0, 0.0, 0.0, 0.0]) == pytest.approx(0.0)


def test_entropy_all_zero_is_nan() -> None:
    assert math.isnan(_entropy([0.0, 0.0, 0.0, 0.0]))


def test_entropy_handles_two_class_split() -> None:
    """50/50 across two classes (others zero): partial entropy of log(2)/log(4) = 0.5."""
    assert _entropy([0.5, 0.5, 0.0, 0.0]) == pytest.approx(0.5)


# --- _strip_t3_prefix ---------------------------------------------------------


def test_strip_t3_prefix_removes_t3_when_present() -> None:
    assert _strip_t3_prefix("t3_abc123") == "abc123"


def test_strip_t3_prefix_passes_through_unprefixed() -> None:
    assert _strip_t3_prefix("abc123") == "abc123"


def test_strip_t3_prefix_handles_empty() -> None:
    assert _strip_t3_prefix("") == ""


# --- _pair_with_event ---------------------------------------------------------


def _event(event_id: str, date: datetime, title: str = "x") -> dict:
    return {"event_id": event_id, "event_date": date, "title": title}


def test_pair_with_event_returns_none_when_no_events_in_window() -> None:
    week = datetime(2024, 1, 8, tzinfo=UTC)
    far_event = _event("far", datetime(2023, 1, 1, tzinfo=UTC))
    assert _pair_with_event(week, [far_event]) is None


def test_pair_with_event_returns_closest_within_window() -> None:
    week = datetime(2024, 1, 8, tzinfo=UTC)
    events = [
        _event("close", datetime(2024, 1, 9, tzinfo=UTC), title="close"),
        _event("further", datetime(2024, 1, 14, tzinfo=UTC), title="further"),
    ]
    paired = _pair_with_event(week, events)
    assert paired is not None
    assert paired["event_id"] == "close"
    assert paired["distance_days"] == 1


def test_pair_with_event_returns_none_for_empty_event_list() -> None:
    assert _pair_with_event(datetime(2024, 1, 8, tzinfo=UTC), []) is None


# --- dim builders -------------------------------------------------------------


def test_build_dim_character_writes_all_yaml_chars(tmp_path: Path) -> None:
    paths = DataPaths(root=tmp_path)
    n = build_dim_character(paths)
    assert n >= 25
    table = pq.read_table(paths.gold / "dim_character.parquet")
    assert table.schema.equals(DIM_CHARACTER_SCHEMA)
    ids = set(table.column("character_id").to_pylist())
    assert "gojo_satoru" in ids
    assert "ryomen_sukuna" in ids


def test_build_dim_event_writes_yaml_events(tmp_path: Path) -> None:
    paths = DataPaths(root=tmp_path)
    n = build_dim_event(paths)
    assert n > 0
    table = pq.read_table(paths.gold / "dim_event.parquet")
    assert table.schema.equals(DIM_EVENT_SCHEMA)
    titles = table.column("title").to_pylist()
    # Chapter 236 is hand-anchored in events.yaml as the verified marker.
    assert any("236" in t or "Gojo dies" in t for t in titles)


# --- fact_post_sentiment helpers ---------------------------------------------


def _silver_post(**overrides) -> dict:
    base = {
        "id": "p1",
        "subreddit": "jujutsukaisen",
        "author_hash": "deadbeef0123",
        "created_utc": 1700000000,
        "created_at": datetime.fromtimestamp(1700000000, tz=UTC),
        "retrieved_at": datetime(2024, 1, 1, tzinfo=UTC),
        "title": "default title",
        "title_clean": "default title",
        "selftext": "",
        "selftext_clean": "",
        "score": 100,
        "num_comments": 5,
        "over_18": False,
        "spoiler": False,
        "link_flair_text": None,
        "permalink": "/r/jujutsukaisen/comments/p1/",
        "is_self": True,
        "is_deleted": False,
        "_source": "test",
        "_silver_processed_at": datetime(2024, 1, 1, tzinfo=UTC),
    }
    base.update(overrides)
    return base


def _classification(**overrides) -> dict:
    base = {
        "id": "p1",
        "sentiment": "positive",
        "confidence": 0.9,
        "intensity": "high",
        "target": "about_arc",
        "classified_at": datetime.now(tz=UTC),
        "prompt_version": "v1",
        "model": "test",
    }
    base.update(overrides)
    return base


def _write_silver_posts(paths: DataPaths, posts: list[dict]) -> None:
    paths.silver.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(posts, paths.silver / "posts.parquet", POST_SILVER_SCHEMA)


def _write_post_classifications(paths: DataPaths, classifications: list[dict]) -> None:
    paths.silver.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(
        classifications, paths.silver / "post_classifications.parquet",
        CLASSIFICATION_SCHEMA,
    )


# --- fact_post_sentiment ------------------------------------------------------


def test_fact_post_sentiment_is_empty_when_no_silver(tmp_path: Path) -> None:
    paths = DataPaths(root=tmp_path)
    n = build_fact_post_sentiment(paths)
    assert n == 0


def test_fact_post_sentiment_one_row_per_character_mention(tmp_path: Path) -> None:
    """A post mentioning Gojo and Sukuna yields exactly two fact rows
    with the same classification fanned out."""
    paths = DataPaths(root=tmp_path)
    _write_silver_posts(
        paths,
        [_silver_post(id="p1", title="Gojo vs Sukuna was peak", selftext="")],
    )
    _write_post_classifications(paths, [_classification(id="p1")])
    n = build_fact_post_sentiment(paths)
    assert n == 2
    rows = pq.read_table(paths.gold / "fact_post_sentiment.parquet").to_pylist()
    char_ids = {r["character_id"] for r in rows}
    assert char_ids == {"gojo_satoru", "ryomen_sukuna"}
    assert all(r["sentiment"] == "positive" for r in rows)


def test_fact_post_sentiment_zero_rows_for_post_without_character_mentions(
    tmp_path: Path,
) -> None:
    """A post that's pure meta — no character mentioned — produces no fact rows."""
    paths = DataPaths(root=tmp_path)
    _write_silver_posts(
        paths,
        [_silver_post(id="p1", title="MAPPA's animation is great", selftext="")],
    )
    _write_post_classifications(paths, [_classification(id="p1")])
    n = build_fact_post_sentiment(paths)
    assert n == 0


def test_fact_post_sentiment_skips_unclassified_posts(tmp_path: Path) -> None:
    """If a post is in silver but not yet classified, we skip rather than
    fabricate a classification value."""
    paths = DataPaths(root=tmp_path)
    _write_silver_posts(paths, [_silver_post(id="p1", title="Gojo is great")])
    _write_post_classifications(paths, [])  # no classifications
    n = build_fact_post_sentiment(paths)
    assert n == 0


def test_fact_post_sentiment_carries_mention_weight(tmp_path: Path) -> None:
    """A post mentioning bare 'Satoru' (weight 0.7) should propagate the
    weight into the fact row so downstream aggregations can discount it."""
    paths = DataPaths(root=tmp_path)
    _write_silver_posts(
        paths,
        [_silver_post(id="p1", title="I drew Satoru today", selftext="")],
    )
    _write_post_classifications(paths, [_classification(id="p1")])
    build_fact_post_sentiment(paths)
    rows = pq.read_table(paths.gold / "fact_post_sentiment.parquet").to_pylist()
    gojo_row = [r for r in rows if r["character_id"] == "gojo_satoru"][0]
    assert gojo_row["mention_weight"] == pytest.approx(0.7)


def test_fact_post_sentiment_maps_sentiment_to_score(tmp_path: Path) -> None:
    paths = DataPaths(root=tmp_path)
    _write_silver_posts(paths, [_silver_post(id="p1", title="Gojo")])
    _write_post_classifications(
        paths, [_classification(id="p1", sentiment="negative")]
    )
    build_fact_post_sentiment(paths)
    row = pq.read_table(paths.gold / "fact_post_sentiment.parquet").to_pylist()[0]
    assert row["sentiment_score"] == SENTIMENT_SCORE["negative"]


def test_fact_post_sentiment_schema_matches(tmp_path: Path) -> None:
    paths = DataPaths(root=tmp_path)
    _write_silver_posts(paths, [_silver_post(id="p1", title="Gojo")])
    _write_post_classifications(paths, [_classification(id="p1")])
    build_fact_post_sentiment(paths)
    table = pq.read_table(paths.gold / "fact_post_sentiment.parquet")
    assert table.schema.equals(FACT_POST_SENTIMENT_SCHEMA)


# --- agg_char_week ------------------------------------------------------------


def test_agg_char_week_empty_when_no_facts(tmp_path: Path) -> None:
    paths = DataPaths(root=tmp_path)
    n = build_agg_char_week(paths)
    assert n == 0


def test_agg_char_week_aggregates_mentions_by_iso_week(tmp_path: Path) -> None:
    """Two posts in the same ISO week mentioning Gojo collapse to one
    weekly row with mention_count=2."""
    paths = DataPaths(root=tmp_path)
    same_week_a = datetime(2024, 1, 8, tzinfo=UTC)
    same_week_b = datetime(2024, 1, 10, tzinfo=UTC)
    _write_silver_posts(
        paths,
        [
            _silver_post(
                id="p1", title="Gojo deserved better",
                created_utc=int(same_week_a.timestamp()),
                created_at=same_week_a,
            ),
            _silver_post(
                id="p2", title="Gojo's death still hurts",
                created_utc=int(same_week_b.timestamp()),
                created_at=same_week_b,
            ),
        ],
    )
    _write_post_classifications(
        paths,
        [
            _classification(id="p1", sentiment="positive"),
            _classification(id="p2", sentiment="negative"),
        ],
    )
    build_fact_post_sentiment(paths)
    n = build_agg_char_week(paths)
    rows = pq.read_table(paths.gold / "agg_char_week.parquet").to_pylist()
    gojo_rows = [r for r in rows if r["character_id"] == "gojo_satoru"]
    assert len(gojo_rows) == 1
    assert gojo_rows[0]["mention_count"] == 2


def test_agg_char_week_polarisation_index_max_for_50_50_split(tmp_path: Path) -> None:
    """One positive + one negative post in the same week → polarisation 1.0."""
    paths = DataPaths(root=tmp_path)
    week = datetime(2024, 1, 8, tzinfo=UTC)
    _write_silver_posts(
        paths,
        [
            _silver_post(id="p1", title="Gojo great",
                         created_utc=int(week.timestamp()), created_at=week),
            _silver_post(id="p2", title="Gojo overrated",
                         created_utc=int(week.timestamp()), created_at=week),
        ],
    )
    _write_post_classifications(
        paths,
        [
            _classification(id="p1", sentiment="positive"),
            _classification(id="p2", sentiment="negative"),
        ],
    )
    build_fact_post_sentiment(paths)
    build_agg_char_week(paths)
    row = pq.read_table(paths.gold / "agg_char_week.parquet").to_pylist()[0]
    assert row["polarisation_index"] == pytest.approx(1.0)


def test_agg_char_week_polarisation_index_zero_when_one_sided(tmp_path: Path) -> None:
    """All positive → polarisation 0."""
    paths = DataPaths(root=tmp_path)
    week = datetime(2024, 1, 8, tzinfo=UTC)
    _write_silver_posts(
        paths,
        [
            _silver_post(id=f"p{i}", title="Gojo amazing",
                         created_utc=int(week.timestamp()), created_at=week)
            for i in range(3)
        ],
    )
    _write_post_classifications(
        paths,
        [_classification(id=f"p{i}", sentiment="positive") for i in range(3)],
    )
    build_fact_post_sentiment(paths)
    build_agg_char_week(paths)
    row = pq.read_table(paths.gold / "agg_char_week.parquet").to_pylist()[0]
    assert row["polarisation_index"] == pytest.approx(0.0)


# --- agg_polarisation ---------------------------------------------------------


def test_agg_polarisation_ranks_by_mentions(tmp_path: Path) -> None:
    """Build a tiny dataset with Gojo > Sukuna in mention count and verify
    the most_mentioned_rank reflects it."""
    paths = DataPaths(root=tmp_path)
    week = datetime(2024, 1, 8, tzinfo=UTC)
    _write_silver_posts(
        paths,
        [
            _silver_post(id=f"p{i}", title="Gojo great",
                         created_utc=int(week.timestamp()), created_at=week)
            for i in range(3)
        ] + [
            _silver_post(id="ps", title="Sukuna won",
                         created_utc=int(week.timestamp()), created_at=week),
        ],
    )
    _write_post_classifications(
        paths,
        [_classification(id=f"p{i}") for i in range(3)] + [_classification(id="ps")],
    )
    build_fact_post_sentiment(paths)
    build_agg_char_week(paths)
    build_agg_polarisation(paths)

    rows = pq.read_table(paths.gold / "agg_polarisation.parquet").to_pylist()
    by_id = {r["character_id"]: r for r in rows}
    assert by_id["gojo_satoru"]["most_mentioned_rank"] == 1
    assert by_id["ryomen_sukuna"]["most_mentioned_rank"] == 2


# --- gege moments -------------------------------------------------------------


def _agg_week_row(
    *, character_id: str, week_start: datetime, score: float, count: int = 50
) -> dict:
    """Build a row matching AGG_CHAR_WEEK_SCHEMA for direct injection."""
    return {
        "character_id": character_id,
        "year": week_start.isocalendar().year,
        "week": week_start.isocalendar().week,
        "week_start": week_start,
        "mention_count": count,
        "post_count": count,
        "comment_count": 0,
        "weighted_sentiment": float(count),
        "mean_sentiment_score": score,
        "share_positive": 0.5 + score / 2,
        "share_negative": 0.5 - score / 2,
        "share_mixed": 0.0,
        "share_neutral": 0.0,
        "share_high_intensity": 0.0,
        "polarisation_index": 0.0,
        "polarisation_entropy": 0.0,
    }


def test_gege_moments_finds_a_real_spike(tmp_path: Path) -> None:
    """Stable score for 12 weeks, then a 2.5σ-equivalent spike → one moment."""
    paths = DataPaths(root=tmp_path)
    base_week = datetime(2023, 1, 2, tzinfo=UTC)
    rows = []
    for i in range(12):
        rows.append(
            _agg_week_row(
                character_id="gojo_satoru",
                week_start=base_week + timedelta(weeks=i),
                score=0.30 + (i % 3) * 0.05,
            )
        )
    rows.append(
        _agg_week_row(
            character_id="gojo_satoru",
            week_start=base_week + timedelta(weeks=12),
            score=-0.50,
            count=100,
        )
    )

    paths.gold.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(rows, paths.gold / "agg_char_week.parquet", AGG_CHAR_WEEK_SCHEMA)
    write_parquet_atomic([], paths.gold / "dim_event.parquet", DIM_EVENT_SCHEMA)

    n = build_gege_moments(paths)
    assert n == 1
    moment = pq.read_table(paths.gold / "gege_moments.parquet").to_pylist()[0]
    assert moment["character_id"] == "gojo_satoru"
    assert moment["z_score"] < -2.0
    assert moment["paired_event_id"] is None


def test_gege_moments_skip_low_mention_weeks(tmp_path: Path) -> None:
    """A spike with too few mentions is noise; it must not fire."""
    paths = DataPaths(root=tmp_path)
    base_week = datetime(2023, 1, 2, tzinfo=UTC)
    rows = []
    for i in range(12):
        rows.append(
            _agg_week_row(
                character_id="gojo_satoru",
                week_start=base_week + timedelta(weeks=i),
                score=0.3,
            )
        )
    rows.append(
        _agg_week_row(
            character_id="gojo_satoru",
            week_start=base_week + timedelta(weeks=12),
            score=-1.0,
            count=3,
        )
    )

    paths.gold.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(rows, paths.gold / "agg_char_week.parquet", AGG_CHAR_WEEK_SCHEMA)
    write_parquet_atomic([], paths.gold / "dim_event.parquet", DIM_EVENT_SCHEMA)

    assert build_gege_moments(paths) == 0


def test_gege_moments_skip_until_baseline_window_full(tmp_path: Path) -> None:
    """The first 12 weeks of a character's data don't have enough history
    to compute a baseline; we don't fire moments on them."""
    paths = DataPaths(root=tmp_path)
    base_week = datetime(2023, 1, 2, tzinfo=UTC)
    rows = []
    for i in range(9):
        rows.append(
            _agg_week_row(
                character_id="gojo_satoru",
                week_start=base_week + timedelta(weeks=i),
                score=0.3,
            )
        )
    rows.append(
        _agg_week_row(
            character_id="gojo_satoru",
            week_start=base_week + timedelta(weeks=10),
            score=-1.0,
            count=200,
        )
    )

    paths.gold.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(rows, paths.gold / "agg_char_week.parquet", AGG_CHAR_WEEK_SCHEMA)
    write_parquet_atomic([], paths.gold / "dim_event.parquet", DIM_EVENT_SCHEMA)

    assert build_gege_moments(paths) == 0


def test_gege_moments_pair_with_chapter_event(tmp_path: Path) -> None:
    """A spike that lines up with a known chapter event should pair with it."""
    paths = DataPaths(root=tmp_path)
    base_week = datetime(2023, 1, 2, tzinfo=UTC)
    spike_week = base_week + timedelta(weeks=12)
    rows = [
        _agg_week_row(
            character_id="gojo_satoru",
            week_start=base_week + timedelta(weeks=i),
            # Vary the baseline so std > 0 and the z-score is defined.
            score=0.30 + (i % 3) * 0.05,
        )
        for i in range(12)
    ] + [
        _agg_week_row(
            character_id="gojo_satoru",
            week_start=spike_week,
            score=-0.5,
            count=200,
        )
    ]

    paths.gold.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(rows, paths.gold / "agg_char_week.parquet", AGG_CHAR_WEEK_SCHEMA)
    write_parquet_atomic(
        [
            {
                "event_id": "chapter_X",
                "event_date": spike_week + timedelta(days=1),
                "chapter": 236,
                "arc": "shinjuku_showdown",
                "title": "Chapter X",
                "description": "test",
                "spoiler_intensity": "high",
                "medium": "manga",
                "verified": True,
                "characters": ["gojo_satoru"],
            }
        ],
        paths.gold / "dim_event.parquet",
        DIM_EVENT_SCHEMA,
    )

    build_gege_moments(paths)
    moment = pq.read_table(paths.gold / "gege_moments.parquet").to_pylist()[0]
    assert moment["paired_event_id"] == "chapter_X"
    assert moment["paired_event_distance_days"] == 1


# --- end-to-end build_all -----------------------------------------------------


def test_build_all_against_minimal_silver(tmp_path: Path) -> None:
    """Run the entire gold build against a hand-built minimal silver dataset
    and verify all seven gold tables exist with expected row counts."""
    paths = DataPaths(root=tmp_path)
    week = datetime(2024, 1, 8, tzinfo=UTC)
    _write_silver_posts(
        paths,
        [
            _silver_post(
                id="p1", title="Gojo vs Sukuna was peak",
                created_utc=int(week.timestamp()), created_at=week,
            ),
            _silver_post(
                id="p2", title="Megumi deserved better",
                created_utc=int(week.timestamp()), created_at=week,
            ),
        ],
    )
    _write_post_classifications(
        paths,
        [_classification(id="p1"), _classification(id="p2", sentiment="negative")],
    )

    result = build_all(paths)
    assert result.dim_character > 0
    assert result.dim_event > 0
    # p1 mentions 2 chars (Gojo, Sukuna), p2 mentions 1 (Megumi). 3 fact rows.
    assert result.fact_post_sentiment == 3
    assert result.fact_comment_sentiment == 0
    assert result.agg_char_week >= 1
    assert result.agg_polarisation >= 1
    # gege_moments needs >12 weeks of history; minimal dataset → 0.
    assert result.gege_moments == 0

    for name in (
        "dim_character", "dim_event",
        "fact_post_sentiment", "fact_comment_sentiment",
        "agg_char_week", "agg_polarisation", "gege_moments",
    ):
        assert (paths.gold / f"{name}.parquet").exists()