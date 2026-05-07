"""Historical backfill via Arctic Shift.

Arctic Shift is a community-run successor to Pushshift that exposes a free
JSON API for historical Reddit posts and comments. We use it for the multi-year
backfill; PRAW handles daily incremental.

Why this module exists separately from reddit_praw:
    PRAW can't naively serve five years of historical data — Reddit's listing
    API caps out at a few hundred posts per query. Arctic Shift archives the
    entire historical corpus and serves it via cursor-paginated endpoints.

Pagination strategy:
    Arctic Shift returns results sorted ascending by created_utc. We slide
    ``after`` forward to the last seen timestamp + 1 each page. Three stop
    conditions: empty page, no progress (cursor stuck), or hitting ``before``.

Field-list quirk:
    The Arctic Shift API documentation lists ``permalink`` and ``is_self`` as
    valid fields, but the live API returns 400 if you actually request them.
    We omit both from the request and reconstruct ``permalink`` client-side
    from subreddit + id. Found during real-data validation; covered by
    regression tests.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx
import structlog
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from pipeline.extract._common import (
    COMMENT_SCHEMA,
    POST_SCHEMA,
    SUBREDDITS,
    UTC,
    DataPaths,
    RateLimiter,
    make_bronze_path,
    write_parquet_atomic,
)

log = structlog.get_logger(__name__)

ARCTIC_SHIFT_BASE = "https://arctic-shift.photon-reddit.com/api"
DEFAULT_RATE_PER_SECOND = 0.5  # be polite to a free community service
MAX_PAGES_PER_BACKFILL = 200
PAGE_LIMIT = "auto"  # Arctic Shift's heuristic for picking page size

# Fields we ask for explicitly. Reduces payload size and pins the contract.
# NOTE: ``permalink`` and ``is_self`` are NOT in Arctic Shift's selectable
# fields list (despite being normal Reddit fields). The API returns 400 if
# we ask for them. We reconstruct permalink from subreddit+id and infer
# is_self from selftext presence in :func:`_normalize_post`.
POST_FIELDS = (
    "id,subreddit,author,created_utc,title,selftext,score,num_comments,"
    "over_18,spoiler,link_flair_text"
)
COMMENT_FIELDS = "id,subreddit,author,created_utc,body,score,link_id,parent_id"


@dataclass
class BackfillCounts:
    posts_fetched: int = 0
    comments_fetched: int = 0


# --- Normalizers --------------------------------------------------------------


def _normalize_post(raw: dict[str, Any], retrieved_at: datetime) -> dict[str, Any]:
    subreddit = raw.get("subreddit", "")
    post_id = raw.get("id", "")
    selftext = raw.get("selftext") or ""
    return {
        "id": post_id,
        "subreddit": subreddit,
        "author": raw.get("author") or "[deleted]",
        "created_utc": int(raw.get("created_utc", 0)),
        "retrieved_at": retrieved_at,
        "title": raw.get("title") or "",
        "selftext": selftext,
        "score": int(raw.get("score") or 0),
        "num_comments": int(raw.get("num_comments") or 0),
        "over_18": bool(raw.get("over_18", False)),
        "spoiler": bool(raw.get("spoiler", False)),
        "link_flair_text": raw.get("link_flair_text"),
        # Reconstructed: Arctic Shift doesn't expose permalink as a selectable
        # field. We rebuild the canonical Reddit URL form.
        "permalink": f"/r/{subreddit}/comments/{post_id}/" if subreddit and post_id else "",
        # Inferred: if there's a body, it's a self post. ~95% accurate;
        # the rare false-positive (e.g., crossposts with copied text)
        # doesn't affect sentiment analysis.
        "is_self": bool(selftext),
        "_source": "arctic_shift",
    }


def _normalize_comment(raw: dict[str, Any], retrieved_at: datetime) -> dict[str, Any]:
    subreddit = raw.get("subreddit", "")
    comment_id = raw.get("id", "")
    link_id = raw.get("link_id") or ""
    # Reddit's link_id is the parent post id with a "t3_" prefix; strip it
    # so the permalink path is well-formed.
    post_id = link_id[3:] if link_id.startswith("t3_") else link_id
    return {
        "id": comment_id,
        "subreddit": subreddit,
        "author": raw.get("author") or "[deleted]",
        "created_utc": int(raw.get("created_utc", 0)),
        "retrieved_at": retrieved_at,
        "body": raw.get("body") or "",
        "score": int(raw.get("score") or 0),
        "link_id": link_id,
        "parent_id": raw.get("parent_id") or "",
        "permalink": (
            f"/r/{subreddit}/comments/{post_id}/_/{comment_id}/"
            if subreddit and post_id and comment_id
            else ""
        ),
        "_source": "arctic_shift",
    }


# --- HTTP fetcher with retries and rate limiting ----------------------------


@retry(
    retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TransportError)),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(4),
    reraise=True,
)
def _fetch_page(
    client: httpx.Client,
    endpoint: str,
    params: dict[str, Any],
    rate_limiter: RateLimiter,
) -> dict[str, Any]:
    """Fetch one page from Arctic Shift with retries and rate limiting."""
    with rate_limiter.acquire():
        resp = client.get(f"{ARCTIC_SHIFT_BASE}/{endpoint}", params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


# --- Pagination loop --------------------------------------------------------


def _paginate(
    client: httpx.Client,
    endpoint: str,
    *,
    subreddit: str,
    after: int,
    before: int,
    fields: str,
    rate_limiter: RateLimiter,
) -> list[dict[str, Any]]:
    """Slide a cursor through Arctic Shift, returning all rows in [after, before).

    Three stop conditions:
        1. Page returned no rows (we've passed the end of available data)
        2. Cursor didn't advance (Arctic Shift is stuck — bail)
        3. Page count exceeds MAX_PAGES_PER_BACKFILL (safety valve)
    """
    rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    cursor = after
    pages = 0

    while pages < MAX_PAGES_PER_BACKFILL:
        params: dict[str, Any] = {
            "subreddit": subreddit,
            "after": cursor,
            "before": before,
            "sort": "asc",
            "limit": PAGE_LIMIT,
            "fields": fields,
        }
        body = _fetch_page(client, endpoint, params, rate_limiter)
        page_rows = body.get("data", []) or []
        if not page_rows:
            break

        # Dedup within this run via id-set; Arctic Shift can return overlap
        # at page boundaries.
        new_count = 0
        for row in page_rows:
            rid = row.get("id")
            if rid and rid not in seen_ids:
                seen_ids.add(rid)
                rows.append(row)
                new_count += 1

        last_ts = max(int(r.get("created_utc", 0)) for r in page_rows)
        if last_ts <= cursor:
            log.warning("arctic_shift_cursor_stuck", endpoint=endpoint, cursor=cursor)
            break
        cursor = last_ts + 1
        pages += 1
        if new_count == 0:
            # Page was all duplicates — we're past the data we want.
            break

    log.info(
        "arctic_shift_paginated",
        endpoint=endpoint,
        subreddit=subreddit,
        rows=len(rows),
        pages=pages,
    )
    return rows


# --- Top-level backfill -------------------------------------------------------


def backfill(
    subreddits: tuple[str, ...] = SUBREDDITS,
    *,
    after: datetime,
    before: datetime,
    paths: DataPaths | None = None,
    rate_limiter: RateLimiter | None = None,
) -> BackfillCounts:
    """Backfill bronze parquets for the given subreddits and time window.

    Each (subreddit, kind) pair produces one parquet file in
    ``bronze/{date}/arctic_shift/`` with the full window's data. Re-runnable:
    silver dedups by id across all bronze files.
    """
    paths = paths or DataPaths.from_env()
    rate_limiter = rate_limiter or RateLimiter(DEFAULT_RATE_PER_SECOND)
    counts = BackfillCounts()
    after_ts = int(after.timestamp())
    before_ts = int(before.timestamp())
    retrieved_at = datetime.now(tz=UTC)

    with httpx.Client() as client:
        for sub in subreddits:
            log.info("arctic_shift_subreddit_start", subreddit=sub)

            posts_raw = _paginate(
                client, "posts/search",
                subreddit=sub, after=after_ts, before=before_ts,
                fields=POST_FIELDS, rate_limiter=rate_limiter,
            )
            posts = [_normalize_post(r, retrieved_at) for r in posts_raw]
            if posts:
                dest = make_bronze_path(paths, "arctic_shift", "posts")
                write_parquet_atomic(posts, dest, POST_SCHEMA)
                counts.posts_fetched += len(posts)

            comments_raw = _paginate(
                client, "comments/search",
                subreddit=sub, after=after_ts, before=before_ts,
                fields=COMMENT_FIELDS, rate_limiter=rate_limiter,
            )
            comments = [_normalize_comment(r, retrieved_at) for r in comments_raw]
            if comments:
                dest = make_bronze_path(paths, "arctic_shift", "comments")
                write_parquet_atomic(comments, dest, COMMENT_SCHEMA)
                counts.comments_fetched += len(comments)

    return counts