"""Daily incremental ingestion via PRAW.

PRAW is the official Python wrapper for Reddit's API. We use it for the daily
cron because Reddit's own listing endpoints serve "new" posts cleanly with
proper rate limit handling — Arctic Shift is for historical backfill only.

Watermarking
------------
Each (subreddit, kind) pair has a high-water-mark stored in
``data/_state/watermarks.json``. On each run we fetch posts created after the
watermark and update it to the latest created_utc seen. The watermark file is
committed to git via the .gitignore whitelist pattern so the GitHub Actions
cron has continuity across runs.

Replace-more
------------
PRAW's ``comment.replies.replace_more(limit=N)`` expands collapsed "load more"
stubs in deep comment threads. We use a finite limit (16) rather than 0
(unlimited) because deep threads have diminishing analytical value and burn
through rate limit fast.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable

import praw
import structlog

from pipeline.extract._common import (
    COMMENT_SCHEMA,
    POST_SCHEMA,
    SUBREDDITS,
    UTC,
    DataPaths,
    RateLimiter,
    WatermarkStore,
    make_bronze_path,
    write_parquet_atomic,
)

log = structlog.get_logger(__name__)

# Reddit's free API tier is 60 req/min. PRAW handles rate limiting itself,
# but we add a thin layer because we make a lot of comment-tree calls.
DEFAULT_RATE_PER_SECOND = 1.0
# Cap on comment-tree expansion. 16 catches most real conversations without
# burning rate limit on rambling deep threads.
REPLACE_MORE_LIMIT = 16
# Cap on posts pulled per subreddit per run. Daily incremental shouldn't see
# more than this; if it does, we're probably re-running on a multi-day gap.
MAX_NEW_POSTS_PER_RUN = 500


@dataclass
class IncrementalCounts:
    posts_fetched: int = 0
    comments_fetched: int = 0


# --- Reddit client construction ----------------------------------------------


def make_reddit_client() -> praw.Reddit:
    """Construct a PRAW Reddit client from environment variables.

    Required env vars: REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT.
    See .env.example for setup instructions.
    """
    client_id = os.environ.get("REDDIT_CLIENT_ID")
    client_secret = os.environ.get("REDDIT_CLIENT_SECRET")
    user_agent = os.environ.get("REDDIT_USER_AGENT")
    if not all([client_id, client_secret, user_agent]):
        raise RuntimeError(
            "Missing Reddit credentials. Set REDDIT_CLIENT_ID, "
            "REDDIT_CLIENT_SECRET, and REDDIT_USER_AGENT in .env. "
            "Register a script app at https://www.reddit.com/prefs/apps "
            "(it's free)."
        )
    return praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent,
        check_for_async=False,  # we're sync
    )


# --- Normalizers --------------------------------------------------------------


def _normalize_post(submission: Any, retrieved_at: datetime) -> dict[str, Any]:
    """Convert a PRAW Submission to a bronze row.

    Duck-typed: tests pass plain stand-in objects with the same attribute
    names rather than mocking PRAW's full object hierarchy.
    """
    author = getattr(submission, "author", None)
    author_name = author.name if author and hasattr(author, "name") else "[deleted]"
    return {
        "id": submission.id,
        "subreddit": getattr(submission.subreddit, "display_name", str(submission.subreddit)),
        "author": author_name,
        "created_utc": int(submission.created_utc),
        "retrieved_at": retrieved_at,
        "title": submission.title or "",
        "selftext": submission.selftext or "",
        "score": int(getattr(submission, "score", 0)),
        "num_comments": int(getattr(submission, "num_comments", 0)),
        "over_18": bool(getattr(submission, "over_18", False)),
        "spoiler": bool(getattr(submission, "spoiler", False)),
        "link_flair_text": getattr(submission, "link_flair_text", None),
        "permalink": getattr(submission, "permalink", "") or "",
        "is_self": bool(getattr(submission, "is_self", False)),
        "_source": "praw",
    }


def _normalize_comment(comment: Any, retrieved_at: datetime) -> dict[str, Any]:
    author = getattr(comment, "author", None)
    author_name = author.name if author and hasattr(author, "name") else "[deleted]"
    return {
        "id": comment.id,
        "subreddit": getattr(comment.subreddit, "display_name", str(comment.subreddit)),
        "author": author_name,
        "created_utc": int(comment.created_utc),
        "retrieved_at": retrieved_at,
        "body": getattr(comment, "body", "") or "",
        "score": int(getattr(comment, "score", 0)),
        "link_id": getattr(comment, "link_id", "") or "",
        "parent_id": getattr(comment, "parent_id", "") or "",
        "permalink": getattr(comment, "permalink", "") or "",
        "_source": "praw",
    }


# --- Per-subreddit incremental scrape ----------------------------------------


def _scrape_subreddit(
    reddit: praw.Reddit,
    subreddit_name: str,
    watermark: int,
    rate_limiter: RateLimiter,
    retrieved_at: datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    """Scrape new posts and their comment trees for one subreddit.

    Returns (post_rows, comment_rows, new_watermark).
    """
    subreddit = reddit.subreddit(subreddit_name)
    posts: list[dict[str, Any]] = []
    comments: list[dict[str, Any]] = []
    new_watermark = watermark

    with rate_limiter.acquire():
        # PRAW's iterator handles pagination internally. ``new()`` returns
        # most recent first; we filter by created_utc > watermark.
        new_posts = list(subreddit.new(limit=MAX_NEW_POSTS_PER_RUN))

    for submission in new_posts:
        if int(submission.created_utc) <= watermark:
            # Already ingested in a previous run; skip.
            continue
        posts.append(_normalize_post(submission, retrieved_at))
        new_watermark = max(new_watermark, int(submission.created_utc))

        # Best-effort comment expansion. Failures (rare deleted threads, etc.)
        # should not abort the whole subreddit scrape.
        try:
            with rate_limiter.acquire():
                submission.comments.replace_more(limit=REPLACE_MORE_LIMIT)
                for comment in submission.comments.list():
                    comments.append(_normalize_comment(comment, retrieved_at))
        except Exception as e:  # noqa: BLE001
            log.warning(
                "praw_comment_expansion_failed",
                submission_id=submission.id,
                error=str(e),
            )

    log.info(
        "praw_subreddit_done",
        subreddit=subreddit_name,
        posts=len(posts),
        comments=len(comments),
        watermark=new_watermark,
    )
    return posts, comments, new_watermark


# --- Top-level daily incremental ---------------------------------------------


def daily(
    subreddits: Iterable[str] = SUBREDDITS,
    *,
    paths: DataPaths | None = None,
    rate_limiter: RateLimiter | None = None,
    reddit: praw.Reddit | None = None,
) -> IncrementalCounts:
    """Run the daily incremental scrape across all subreddits.

    Idempotent within a single day: re-running pulls only posts created since
    the last watermark advance.
    """
    paths = paths or DataPaths.from_env()
    rate_limiter = rate_limiter or RateLimiter(DEFAULT_RATE_PER_SECOND)
    reddit = reddit or make_reddit_client()
    watermarks = WatermarkStore(paths.state_dir / "watermarks.json")
    counts = IncrementalCounts()
    retrieved_at = datetime.now(tz=UTC)

    for sub in subreddits:
        post_watermark = watermarks.get(sub, "posts")
        posts, comments, new_watermark = _scrape_subreddit(
            reddit, sub, post_watermark, rate_limiter, retrieved_at,
        )

        if posts:
            dest = make_bronze_path(paths, "praw", "posts")
            write_parquet_atomic(posts, dest, POST_SCHEMA)
            counts.posts_fetched += len(posts)
        if comments:
            dest = make_bronze_path(paths, "praw", "comments")
            write_parquet_atomic(comments, dest, COMMENT_SCHEMA)
            counts.comments_fetched += len(comments)

        if new_watermark > post_watermark:
            watermarks.set(sub, "posts", new_watermark)

    return counts