"""Tests for ``pipeline.extract.reddit_praw``.

PRAW's API is duck-typed in normalize_*, so we test with plain stand-in
objects rather than mocking PRAW's full hierarchy.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from pipeline.extract.reddit_praw import _normalize_comment, _normalize_post

UTC = timezone.utc


def _author(name: str | None) -> SimpleNamespace | None:
    """PRAW exposes deleted authors as None; otherwise as an object with .name."""
    return SimpleNamespace(name=name) if name else None


def _subreddit(name: str) -> SimpleNamespace:
    return SimpleNamespace(display_name=name)


# --- _normalize_post ---------------------------------------------------------


def test_normalize_post_handles_full_submission() -> None:
    submission = SimpleNamespace(
        id="abc",
        subreddit=_subreddit("JuJutsuKaisen"),
        author=_author("tester"),
        created_utc=1700000000,
        title="Chapter 236",
        selftext="content",
        score=42,
        num_comments=7,
        over_18=False,
        spoiler=True,
        link_flair_text="Manga Spoilers",
        permalink="/r/JuJutsuKaisen/comments/abc/",
        is_self=True,
    )
    out = _normalize_post(submission, datetime.now(tz=UTC))
    assert out["id"] == "abc"
    assert out["subreddit"] == "JuJutsuKaisen"
    assert out["author"] == "tester"
    assert out["spoiler"] is True
    assert out["_source"] == "praw"


def test_normalize_post_handles_deleted_author() -> None:
    submission = SimpleNamespace(
        id="abc", subreddit=_subreddit("j"), author=None,
        created_utc=1, title="t", selftext="",
        score=0, num_comments=0,
        over_18=False, spoiler=False, link_flair_text=None,
        permalink="/p", is_self=False,
    )
    out = _normalize_post(submission, datetime.now(tz=UTC))
    assert out["author"] == "[deleted]"


def test_normalize_post_handles_missing_optional_fields() -> None:
    """Submissions can be missing some fields; defaults should kick in."""
    # Use a class instead of SimpleNamespace because getattr defaults
    # only fire when the attribute genuinely doesn't exist.
    class MinimalSubmission:
        id = "abc"
        subreddit = _subreddit("j")
        author = _author("tester")
        created_utc = 1
        title = "t"
        selftext = ""
    out = _normalize_post(MinimalSubmission(), datetime.now(tz=UTC))
    assert out["score"] == 0
    assert out["num_comments"] == 0
    assert out["over_18"] is False
    assert out["spoiler"] is False
    assert out["link_flair_text"] is None


# --- _normalize_comment ------------------------------------------------------


def test_normalize_comment_handles_full_comment() -> None:
    comment = SimpleNamespace(
        id="c1",
        subreddit=_subreddit("JuJutsuKaisen"),
        author=_author("tester"),
        created_utc=1700000000,
        body="Sukuna won",
        score=10,
        link_id="t3_abc",
        parent_id="t1_xyz",
        permalink="/r/JuJutsuKaisen/comments/abc/_/c1/",
    )
    out = _normalize_comment(comment, datetime.now(tz=UTC))
    assert out["id"] == "c1"
    assert out["body"] == "Sukuna won"
    assert out["link_id"] == "t3_abc"
    assert out["_source"] == "praw"


def test_normalize_comment_handles_deleted_author() -> None:
    comment = SimpleNamespace(
        id="c1", subreddit=_subreddit("j"), author=None,
        created_utc=1, body="x", score=0,
        link_id="", parent_id="", permalink="",
    )
    out = _normalize_comment(comment, datetime.now(tz=UTC))
    assert out["author"] == "[deleted]"