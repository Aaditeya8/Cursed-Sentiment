"""Tests for ``pipeline.extract.arctic_shift``."""

from __future__ import annotations

from datetime import datetime, timezone

from pipeline.extract.arctic_shift import (
    COMMENT_FIELDS,
    POST_FIELDS,
    _normalize_comment,
    _normalize_post,
)

UTC = timezone.utc


# --- field-list contract -----------------------------------------------------
# Regression tests for the v0 bug where we requested ``permalink`` and
# ``is_self`` and got 400 from the live API.


def test_post_fields_does_not_request_permalink_or_is_self() -> None:
    """Arctic Shift returns 400 if permalink or is_self appears in the fields
    parameter, even though both are normal Reddit post fields."""
    fields = set(POST_FIELDS.split(","))
    assert "permalink" not in fields
    assert "is_self" not in fields


def test_comment_fields_does_not_request_permalink() -> None:
    """Same restriction applies to comments."""
    fields = set(COMMENT_FIELDS.split(","))
    assert "permalink" not in fields


# --- _normalize_post ---------------------------------------------------------


def test_normalize_post_handles_complete_payload() -> None:
    raw = {
        "id": "abc",
        "subreddit": "JuJutsuKaisen",
        "author": "tester",
        "created_utc": 1700000000,
        "title": "Chapter 236 reaction thread",
        "selftext": "I'm crying.",
        "score": 4200,
        "num_comments": 1337,
        "over_18": False,
        "spoiler": True,
        "link_flair_text": "Manga Spoilers",
    }
    pulled = datetime.now(tz=UTC)
    out = _normalize_post(raw, pulled)
    assert out["id"] == "abc"
    assert out["score"] == 4200
    assert out["spoiler"] is True
    assert out["_source"] == "arctic_shift"
    assert out["retrieved_at"] == pulled
    # permalink is reconstructed, not passed through:
    assert out["permalink"] == "/r/JuJutsuKaisen/comments/abc/"
    # is_self is inferred from selftext presence:
    assert out["is_self"] is True


def test_normalize_post_reconstructs_permalink_from_id_and_subreddit() -> None:
    """Arctic Shift can't return permalink, so we rebuild the canonical form."""
    out = _normalize_post(
        {"id": "abc123", "subreddit": "JuJutsuKaisen", "created_utc": 1},
        datetime.now(tz=UTC),
    )
    assert out["permalink"] == "/r/JuJutsuKaisen/comments/abc123/"


def test_normalize_post_reconstructs_empty_permalink_when_id_missing() -> None:
    """Defensive: if Arctic Shift somehow returns a row without an id, we
    don't produce a malformed permalink — just an empty one."""
    out = _normalize_post(
        {"subreddit": "JuJutsuKaisen", "created_utc": 1, "id": ""},
        datetime.now(tz=UTC),
    )
    assert out["permalink"] == ""


def test_normalize_post_infers_is_self_from_selftext_presence() -> None:
    """Arctic Shift can't return is_self either; we infer from whether the
    post has a body. ~95% accurate."""
    has_body = _normalize_post(
        {"id": "x", "subreddit": "j", "created_utc": 1, "selftext": "hello"},
        datetime.now(tz=UTC),
    )
    assert has_body["is_self"] is True

    no_body = _normalize_post(
        {"id": "x", "subreddit": "j", "created_utc": 1, "selftext": ""},
        datetime.now(tz=UTC),
    )
    assert no_body["is_self"] is False


def test_normalize_post_handles_missing_author() -> None:
    """Deleted authors come back as null — coerce to '[deleted]' for consistency."""
    out = _normalize_post(
        {"id": "x", "subreddit": "j", "created_utc": 1, "author": None},
        datetime.now(tz=UTC),
    )
    assert out["author"] == "[deleted]"


def test_normalize_post_coerces_numeric_fields() -> None:
    """Score and num_comments come back as int strings sometimes; coerce."""
    out = _normalize_post(
        {
            "id": "x", "subreddit": "j", "created_utc": "1700000000",
            "score": "100", "num_comments": "5",
        },
        datetime.now(tz=UTC),
    )
    assert out["created_utc"] == 1700000000
    assert out["score"] == 100
    assert out["num_comments"] == 5


# --- _normalize_comment ------------------------------------------------------


def test_normalize_comment_handles_complete_payload() -> None:
    raw = {
        "id": "c1",
        "subreddit": "JuJutsuKaisen",
        "author": "tester",
        "created_utc": 1700000000,
        "body": "Sukuna deserved every bit of that loss",
        "score": 50,
        "link_id": "t3_abc",
        "parent_id": "t1_xyz",
    }
    out = _normalize_comment(raw, datetime.now(tz=UTC))
    assert out["body"].startswith("Sukuna")
    assert out["link_id"] == "t3_abc"
    assert out["_source"] == "arctic_shift"


def test_normalize_comment_reconstructs_permalink_with_post_id_stripped() -> None:
    """Reddit's link_id is t3_<post_id>; the permalink path uses the
    bare post_id without prefix."""
    out = _normalize_comment(
        {
            "id": "cmt1",
            "subreddit": "JuJutsuKaisen",
            "created_utc": 1,
            "link_id": "t3_post1",
        },
        datetime.now(tz=UTC),
    )
    assert out["permalink"] == "/r/JuJutsuKaisen/comments/post1/_/cmt1/"