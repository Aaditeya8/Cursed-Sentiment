"""Bronze → Silver cleaning.

Three concerns layered together:

1. Idempotent processing
   We track which bronze files have been seen via
   ``data/_state/silver_processed.json``. Re-runs only process new bronze
   files; silver gets rewritten from union(all known bronze) so dedup is
   always against the full corpus.

2. Cleaning
   Unicode NFKC normalization, smart-quote folding, HTML entity decoding,
   deletion-marker detection (six patterns), whitespace collapsing. Both
   raw and clean text are kept in silver so methodology can be audited.

3. Privacy
   Author names are SHA-256-hashed with a salt loaded from
   ``CURSED_AUTHOR_SALT``. Public Reddit usernames are technically not
   secrets, but committing 200K of them in a public repo would be a poor
   look. The salt is per-deployment so different forks don't produce the
   same hashes.

The state file (``silver_processed.json``) is committed via the .gitignore
whitelist pattern so the daily cron has continuity across runs.
"""

from __future__ import annotations

import hashlib
import html
import json
import os
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from pipeline.extract._common import (
    COMMENT_SCHEMA,
    POST_SCHEMA,
    UTC,
    DataPaths,
    write_parquet_atomic,
)

log = structlog.get_logger(__name__)


# --- silver schemas ----------------------------------------------------------
# Silver keeps both raw and clean text so the methodology page can show
# before/after examples. We add ``author_hash`` (replaces ``author``),
# ``created_at`` (timestamp from created_utc), ``is_deleted`` (computed),
# and ``_silver_processed_at`` (audit timestamp).

POST_SILVER_SCHEMA = pa.schema(
    [
        ("id", pa.string()),
        ("subreddit", pa.string()),
        ("author_hash", pa.string()),
        ("created_utc", pa.int64()),
        ("created_at", pa.timestamp("us", tz="UTC")),
        ("retrieved_at", pa.timestamp("us", tz="UTC")),
        ("title", pa.string()),
        ("title_clean", pa.string()),
        ("selftext", pa.string()),
        ("selftext_clean", pa.string()),
        ("score", pa.int64()),
        ("num_comments", pa.int64()),
        ("over_18", pa.bool_()),
        ("spoiler", pa.bool_()),
        ("link_flair_text", pa.string()),
        ("permalink", pa.string()),
        ("is_self", pa.bool_()),
        ("is_deleted", pa.bool_()),
        ("_source", pa.string()),
        ("_silver_processed_at", pa.timestamp("us", tz="UTC")),
    ]
)


COMMENT_SILVER_SCHEMA = pa.schema(
    [
        ("id", pa.string()),
        ("subreddit", pa.string()),
        ("author_hash", pa.string()),
        ("created_utc", pa.int64()),
        ("created_at", pa.timestamp("us", tz="UTC")),
        ("retrieved_at", pa.timestamp("us", tz="UTC")),
        ("body", pa.string()),
        ("body_clean", pa.string()),
        ("score", pa.int64()),
        ("link_id", pa.string()),
        ("parent_id", pa.string()),
        ("permalink", pa.string()),
        ("is_deleted", pa.bool_()),
        ("_source", pa.string()),
        ("_silver_processed_at", pa.timestamp("us", tz="UTC")),
    ]
)


# --- text cleaning ----------------------------------------------------------

# Smart quotes → straight quotes. Treat as a fold table; many sources mix.
SMART_QUOTE_FOLDS = str.maketrans({
    "\u2018": "'", "\u2019": "'",  # single curly
    "\u201c": '"', "\u201d": '"',  # double curly
    "\u2013": "-", "\u2014": "-",  # en/em dashes
    "\u2026": "...",                # ellipsis
})

# Six patterns we treat as "the body was deleted by Reddit or a mod."
# Tested against real ch.236-week data — all six show up in production.
_DELETION_PATTERNS = (
    r"^\[deleted\]$",
    r"^\[removed\]$",
    r"^\[removed by reddit\]",
    r"^\[removed by .+?\]",
    r"^this comment was removed",
    r"^this post was removed",
)
_DELETION_RE = re.compile("|".join(_DELETION_PATTERNS), re.IGNORECASE)


def _is_deleted(text: str) -> bool:
    """Return True if text matches any known deletion-marker pattern."""
    if not text:
        return False
    return bool(_DELETION_RE.match(text.strip()))


def clean_text(raw: str | None) -> str:
    """Normalize a raw post/comment string for downstream processing.

    Steps in order:
        1. None → empty string
        2. NFKC unicode normalization (folds half-width / full-width, ligatures)
        3. HTML entity decode (&amp; → &)
        4. Smart-quote fold (curly → straight)
        5. Whitespace collapse (multiple spaces/newlines → single)
    """
    if not raw:
        return ""
    text = unicodedata.normalize("NFKC", raw)
    text = html.unescape(text)
    text = text.translate(SMART_QUOTE_FOLDS)
    # Collapse internal whitespace runs but preserve paragraph breaks.
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def hash_author(author: str | None, salt: str) -> str:
    """SHA-256 hash of author name with salt, truncated to 12 hex chars.

    12 chars = 48 bits of entropy. Collision probability is irrelevant for
    aggregate analysis; the hash exists for privacy, not uniqueness.
    """
    name = (author or "[deleted]").strip().lower()
    digest = hashlib.sha256(f"{salt}:{name}".encode("utf-8")).hexdigest()
    return digest[:12]


# --- normalize one bronze row to silver -------------------------------------


def _post_to_silver(row: dict[str, Any], salt: str, processed_at: datetime) -> dict[str, Any]:
    raw_title = row.get("title") or ""
    raw_selftext = row.get("selftext") or ""
    title_deleted = _is_deleted(raw_title)
    body_deleted = _is_deleted(raw_selftext)
    return {
        "id": row["id"],
        "subreddit": (row.get("subreddit") or "").lower(),
        "author_hash": hash_author(row.get("author"), salt),
        "created_utc": int(row.get("created_utc", 0)),
        "created_at": datetime.fromtimestamp(int(row.get("created_utc", 0)), tz=UTC),
        "retrieved_at": row.get("retrieved_at"),
        "title": raw_title,
        "title_clean": "" if title_deleted else clean_text(raw_title),
        "selftext": raw_selftext,
        "selftext_clean": "" if body_deleted else clean_text(raw_selftext),
        "score": int(row.get("score", 0)),
        "num_comments": int(row.get("num_comments", 0)),
        "over_18": bool(row.get("over_18", False)),
        "spoiler": bool(row.get("spoiler", False)),
        "link_flair_text": row.get("link_flair_text"),
        "permalink": row.get("permalink") or "",
        "is_self": bool(row.get("is_self", False)),
        # is_deleted true if EITHER title or body was a deletion marker.
        # In practice posts have informative titles + [removed] bodies, so
        # this conservatively flags only fully-erased posts.
        "is_deleted": title_deleted or body_deleted,
        "_source": row.get("_source") or "unknown",
        "_silver_processed_at": processed_at,
    }


def _comment_to_silver(row: dict[str, Any], salt: str, processed_at: datetime) -> dict[str, Any]:
    raw_body = row.get("body") or ""
    body_deleted = _is_deleted(raw_body)
    return {
        "id": row["id"],
        "subreddit": (row.get("subreddit") or "").lower(),
        "author_hash": hash_author(row.get("author"), salt),
        "created_utc": int(row.get("created_utc", 0)),
        "created_at": datetime.fromtimestamp(int(row.get("created_utc", 0)), tz=UTC),
        "retrieved_at": row.get("retrieved_at"),
        "body": raw_body,
        "body_clean": "" if body_deleted else clean_text(raw_body),
        "score": int(row.get("score", 0)),
        "link_id": row.get("link_id") or "",
        "parent_id": row.get("parent_id") or "",
        "permalink": row.get("permalink") or "",
        "is_deleted": body_deleted,
        "_source": row.get("_source") or "unknown",
        "_silver_processed_at": processed_at,
    }


# --- bronze discovery -------------------------------------------------------


def _list_bronze_files(paths: DataPaths, kind: str) -> list[Path]:
    """Find all bronze parquets for a given kind (posts | comments)."""
    if not paths.bronze.exists():
        return []
    return sorted(paths.bronze.rglob(f"{kind}.*.parquet"))


# --- top-level run ----------------------------------------------------------


@dataclass
class CleanResult:
    posts_processed: int
    comments_processed: int
    posts_silver_total: int
    comments_silver_total: int


def run(
    paths: DataPaths | None = None,
    salt: str | None = None,
) -> CleanResult:
    """Read all bronze files, dedup by id (keep latest retrieved_at), write silver.

    Always rewrites silver from scratch — silver is a derived view, not
    append-only. The state file just records which bronze files we've seen
    so we can short-circuit when nothing's changed.
    """
    paths = paths or DataPaths.from_env()
    salt = salt or os.environ.get("CURSED_AUTHOR_SALT", "change-me")
    if salt == "change-me":
        log.warning("clean_default_salt", message="CURSED_AUTHOR_SALT not set in env")

    state_path = paths.state_dir / "silver_processed.json"
    state = json.loads(state_path.read_text()) if state_path.exists() else {}
    posts_files = _list_bronze_files(paths, "posts")
    comments_files = _list_bronze_files(paths, "comments")

    new_posts = [p for p in posts_files if str(p) not in state]
    new_comments = [p for p in comments_files if str(p) not in state]

    log.info(
        "clean_start",
        new_posts_files=len(new_posts),
        new_comments_files=len(new_comments),
        already_processed=len(state),
    )

    processed_at = datetime.now(tz=UTC)

    # Posts: read all bronze files, dedup by id (latest retrieved_at wins).
    posts_silver = _process_kind(
        all_files=posts_files,
        new_files=new_posts,
        normalize=lambda r: _post_to_silver(r, salt, processed_at),
        dest=paths.silver / "posts.parquet",
        schema=POST_SILVER_SCHEMA,
    )

    comments_silver = _process_kind(
        all_files=comments_files,
        new_files=new_comments,
        normalize=lambda r: _comment_to_silver(r, salt, processed_at),
        dest=paths.silver / "comments.parquet",
        schema=COMMENT_SILVER_SCHEMA,
    )

    # Update state to mark ALL bronze files (not just new) as processed.
    new_state = {str(p): processed_at.isoformat() for p in posts_files + comments_files}
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(new_state, indent=2, sort_keys=True))

    log.info(
        "clean_done",
        posts_in=len(new_posts), posts_total=posts_silver,
        comments_in=len(new_comments), comments_total=comments_silver,
    )
    return CleanResult(
        posts_processed=len(new_posts),
        comments_processed=len(new_comments),
        posts_silver_total=posts_silver,
        comments_silver_total=comments_silver,
    )


def _process_kind(
    *,
    all_files: list[Path],
    new_files: list[Path],
    normalize,
    dest: Path,
    schema: pa.Schema,
) -> int:
    """Read every bronze file of one kind, dedup by id, write silver.

    Always processes ALL bronze files (not just new ones) because silver is
    a full rewrite. ``new_files`` is informational only — for logging.
    """
    if not all_files:
        write_parquet_atomic([], dest, schema)
        return 0

    by_id: dict[str, dict[str, Any]] = {}
    for path in all_files:
        rows = pq.read_table(path).to_pylist()
        for row in rows:
            silver = normalize(row)
            existing = by_id.get(silver["id"])
            if existing is None:
                by_id[silver["id"]] = silver
                continue
            # Keep the row with the most recent retrieved_at — Reddit data
            # mutates after creation (scores, deletions), latest wins.
            if (silver.get("retrieved_at") or datetime.min.replace(tzinfo=UTC)) > (
                existing.get("retrieved_at") or datetime.min.replace(tzinfo=UTC)
            ):
                by_id[silver["id"]] = silver

    out = list(by_id.values())
    write_parquet_atomic(out, dest, schema)
    return len(out)