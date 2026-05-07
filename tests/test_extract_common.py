"""Tests for ``pipeline.extract._common``."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from pipeline.extract._common import (
    COMMENT_SCHEMA,
    POST_SCHEMA,
    DataPaths,
    RateLimiter,
    WatermarkStore,
    make_bronze_path,
    write_parquet_atomic,
)

UTC = timezone.utc


# --- DataPaths ---------------------------------------------------------------


def test_data_paths_constructs_subdirs(tmp_path: Path) -> None:
    paths = DataPaths(root=tmp_path)
    assert paths.bronze == tmp_path / "bronze"
    assert paths.silver == tmp_path / "silver"
    assert paths.gold == tmp_path / "gold"
    assert paths.state_dir == tmp_path / "_state"


def test_data_paths_from_env_uses_env_var(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CURSED_DATA_DIR", str(tmp_path))
    paths = DataPaths.from_env()
    assert paths.root == tmp_path


def test_data_paths_from_env_defaults_to_data(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CURSED_DATA_DIR", raising=False)
    paths = DataPaths.from_env()
    assert paths.root.name == "data"


# --- RateLimiter -------------------------------------------------------------


def test_rate_limiter_allows_immediate_first_acquire() -> None:
    """First acquire should not block — there's no prior call to wait against."""
    limiter = RateLimiter(2.0)  # 2 req/sec
    start = time.monotonic()
    with limiter.acquire():
        pass
    assert time.monotonic() - start < 0.05


def test_rate_limiter_blocks_second_acquire() -> None:
    """Second acquire within the rate window should sleep."""
    limiter = RateLimiter(10.0)  # 10 req/sec → 100ms minimum gap
    with limiter.acquire():
        pass
    start = time.monotonic()
    with limiter.acquire():
        pass
    elapsed = time.monotonic() - start
    assert elapsed >= 0.09  # leeway for clock granularity


def test_rate_limiter_rejects_invalid_rate() -> None:
    with pytest.raises(ValueError):
        RateLimiter(0)
    with pytest.raises(ValueError):
        RateLimiter(-1)


# --- WatermarkStore ----------------------------------------------------------


def test_watermark_starts_empty(tmp_path: Path) -> None:
    store = WatermarkStore(tmp_path / "watermarks.json")
    assert store.get("JuJutsuKaisen", "posts") == 0


def test_watermark_persists_across_instances(tmp_path: Path) -> None:
    path = tmp_path / "watermarks.json"
    store1 = WatermarkStore(path)
    store1.set("JuJutsuKaisen", "posts", 1700000000)
    store2 = WatermarkStore(path)
    assert store2.get("JuJutsuKaisen", "posts") == 1700000000


def test_watermark_only_advances_forward(tmp_path: Path) -> None:
    """Watermarks are monotonic — setting a lower value is a no-op."""
    store = WatermarkStore(tmp_path / "watermarks.json")
    store.set("JuJutsuKaisen", "posts", 1700000000)
    store.set("JuJutsuKaisen", "posts", 1600000000)  # earlier
    assert store.get("JuJutsuKaisen", "posts") == 1700000000


def test_watermark_handles_corrupted_json(tmp_path: Path) -> None:
    """A malformed watermark file should not crash the loader."""
    path = tmp_path / "watermarks.json"
    path.write_text("{ this is not valid json")
    store = WatermarkStore(path)
    # Loader silently resets on parse error.
    assert store.get("JuJutsuKaisen", "posts") == 0


def test_watermark_keys_are_per_subreddit_per_kind(tmp_path: Path) -> None:
    store = WatermarkStore(tmp_path / "watermarks.json")
    store.set("JuJutsuKaisen", "posts", 100)
    store.set("Jujutsushi", "posts", 200)
    store.set("JuJutsuKaisen", "comments", 300)
    assert store.get("JuJutsuKaisen", "posts") == 100
    assert store.get("Jujutsushi", "posts") == 200
    assert store.get("JuJutsuKaisen", "comments") == 300


# --- write_parquet_atomic ----------------------------------------------------


def test_write_parquet_round_trips_rows(tmp_path: Path) -> None:
    rows = [
        {
            "id": "p1",
            "subreddit": "JuJutsuKaisen",
            "author": "tester",
            "created_utc": 1700000000,
            "retrieved_at": datetime.now(tz=UTC),
            "title": "Chapter 236",
            "selftext": "I'm crying",
            "score": 100,
            "num_comments": 50,
            "over_18": False,
            "spoiler": True,
            "link_flair_text": "Spoiler",
            "permalink": "/r/JuJutsuKaisen/comments/p1/",
            "is_self": True,
            "_source": "test",
        }
    ]
    dest = tmp_path / "posts.parquet"
    write_parquet_atomic(rows, dest, POST_SCHEMA)
    table = pq.read_table(dest)
    assert table.num_rows == 1
    assert table.column("id").to_pylist() == ["p1"]


def test_write_parquet_handles_empty_rows(tmp_path: Path) -> None:
    """An empty row list should produce a valid empty parquet, not skip writing."""
    dest = tmp_path / "empty.parquet"
    write_parquet_atomic([], dest, POST_SCHEMA)
    assert dest.exists()
    table = pq.read_table(dest)
    assert table.num_rows == 0
    assert table.schema.equals(POST_SCHEMA)


def test_write_parquet_does_not_leave_tmp_files(tmp_path: Path) -> None:
    dest = tmp_path / "posts.parquet"
    write_parquet_atomic([], dest, POST_SCHEMA)
    tmp_files = list(tmp_path.glob("*.tmp"))
    assert tmp_files == []


# --- make_bronze_path --------------------------------------------------------


def test_bronze_path_partitions_by_date_and_source(tmp_path: Path) -> None:
    paths = DataPaths(root=tmp_path)
    fixed_date = datetime(2024, 1, 15, 12, 30, 45, tzinfo=UTC)
    p = make_bronze_path(paths, "arctic_shift", "posts", ingest_date=fixed_date)
    assert "bronze/2024-01-15/arctic_shift" in str(p)
    assert p.name.startswith("posts.")
    assert p.suffix == ".parquet"