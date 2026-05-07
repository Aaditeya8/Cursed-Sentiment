"""Shared utilities for the extract layer.

Everything in this module is imported by both arctic_shift.py and reddit_praw.py
so they produce the same bronze-layer schema regardless of source. The bronze
schema is the contract between extraction and the rest of the pipeline; if you
change a column here, downstream silver/gold/dashboard all need updating.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import pyarrow as pa
import pyarrow.parquet as pq

UTC = timezone.utc

# The three subreddits the pipeline pulls from. Order matters for log output
# but not for correctness.
SUBREDDITS = ("JuJutsuKaisen", "Jujutsushi", "Jujutsufolk")


# --- DataPaths ----------------------------------------------------------------


@dataclass(frozen=True)
class DataPaths:
    """Resolved filesystem paths for the data directory.

    Constructed from the CURSED_DATA_DIR environment variable, defaulting to
    ``./data``. All other modules use this rather than hardcoding paths so
    tests can point at a tmp_path without monkeypatching.
    """

    root: Path

    @property
    def bronze(self) -> Path:
        return self.root / "bronze"

    @property
    def silver(self) -> Path:
        return self.root / "silver"

    @property
    def gold(self) -> Path:
        return self.root / "gold"

    @property
    def state_dir(self) -> Path:
        return self.root / "_state"

    @classmethod
    def from_env(cls) -> DataPaths:
        return cls(root=Path(os.environ.get("CURSED_DATA_DIR", "./data")).resolve())


# --- Schemas ------------------------------------------------------------------
# These are the canonical bronze-layer schemas. Both arctic_shift and praw
# produce rows matching these schemas exactly so silver doesn't have to know
# which source it's reading from.

POST_SCHEMA = pa.schema(
    [
        ("id", pa.string()),
        ("subreddit", pa.string()),
        ("author", pa.string()),
        ("created_utc", pa.int64()),
        ("retrieved_at", pa.timestamp("us", tz="UTC")),
        ("title", pa.string()),
        ("selftext", pa.string()),
        ("score", pa.int64()),
        ("num_comments", pa.int64()),
        ("over_18", pa.bool_()),
        ("spoiler", pa.bool_()),
        ("link_flair_text", pa.string()),
        ("permalink", pa.string()),
        ("is_self", pa.bool_()),
        ("_source", pa.string()),
    ]
)

COMMENT_SCHEMA = pa.schema(
    [
        ("id", pa.string()),
        ("subreddit", pa.string()),
        ("author", pa.string()),
        ("created_utc", pa.int64()),
        ("retrieved_at", pa.timestamp("us", tz="UTC")),
        ("body", pa.string()),
        ("score", pa.int64()),
        ("link_id", pa.string()),
        ("parent_id", pa.string()),
        ("permalink", pa.string()),
        ("_source", pa.string()),
    ]
)


# --- RateLimiter --------------------------------------------------------------


class RateLimiter:
    """Token-bucket-style rate limiter with a context manager interface.

    ``acquire()`` blocks until at least ``1 / rate`` seconds have passed since
    the last acquisition. Used by both extract modules to be polite to free
    APIs (Arctic Shift, Reddit) regardless of how fast our code calls them.

    Not thread-safe — these pipelines are sequential by design.
    """

    def __init__(self, rate_per_second: float) -> None:
        if rate_per_second <= 0:
            raise ValueError("rate_per_second must be > 0")
        self.rate = rate_per_second
        self._last: float = 0.0

    @contextmanager
    def acquire(self) -> Iterator[None]:
        elapsed = time.monotonic() - self._last
        wait = (1.0 / self.rate) - elapsed
        if wait > 0:
            time.sleep(wait)
        try:
            yield
        finally:
            self._last = time.monotonic()


# --- WatermarkStore -----------------------------------------------------------


class WatermarkStore:
    """Per-subreddit high-water-mark store.

    Tracks the most recent ``created_utc`` we've ingested per (subreddit, kind),
    so the daily cron knows where to resume. Persists to JSON so it survives
    across job invocations; committed to git via the .gitignore whitelist
    pattern so GitHub Actions cron has continuity.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self._data: dict[str, int] = self._load()

    def _load(self) -> dict[str, int]:
        if not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text())
        except (json.JSONDecodeError, OSError):
            return {}

    def get(self, subreddit: str, kind: str) -> int:
        return self._data.get(f"{subreddit}:{kind}", 0)

    def set(self, subreddit: str, kind: str, watermark: int) -> None:
        key = f"{subreddit}:{kind}"
        existing = self._data.get(key, 0)
        # Watermarks are monotonic — never go backwards.
        if watermark > existing:
            self._data[key] = watermark
            self._flush()

    def _flush(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # Atomic write via tmp + rename so a crash doesn't leave a partial file.
        tmp = self.path.with_suffix(f".{uuid.uuid4().hex}.tmp")
        tmp.write_text(json.dumps(self._data, indent=2, sort_keys=True))
        tmp.replace(self.path)


# --- Parquet writers ----------------------------------------------------------


def write_parquet_atomic(
    rows: list[dict[str, Any]],
    dest: Path,
    schema: pa.Schema,
) -> Path:
    """Write rows to ``dest`` atomically: write to tmp, then rename.

    Crashes mid-write leave only the tmp file, never a half-written parquet
    in the canonical location. This is critical because downstream readers
    treat any file at ``dest`` as committed and complete.

    Empty row lists produce a valid empty parquet file with the correct
    schema so downstream code doesn't have to special-case "file missing".
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(f".{uuid.uuid4().hex}.tmp")
    table = pa.Table.from_pylist(rows, schema=schema) if rows else pa.Table.from_pydict(
        {field.name: [] for field in schema}, schema=schema
    )
    pq.write_table(table, tmp, compression="zstd")
    tmp.replace(dest)
    return dest


def make_bronze_path(
    paths: DataPaths,
    source: str,
    kind: str,
    *,
    ingest_date: datetime | None = None,
) -> Path:
    """Construct the partitioned bronze path: ``bronze/{date}/{source}/{kind}.{ts}.parquet``.

    Append-only — every ingestion run gets a fresh timestamp suffix. Silver
    deduplicates across all bronze files, so producing duplicates here is
    cheap and safe.
    """
    if ingest_date is None:
        ingest_date = datetime.now(tz=UTC)
    date_str = ingest_date.strftime("%Y-%m-%d")
    ts = ingest_date.strftime("%Y%m%dT%H%M%S")
    return paths.bronze / date_str / source / f"{kind}.{ts}.parquet"