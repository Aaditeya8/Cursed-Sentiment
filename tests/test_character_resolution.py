"""Tests for ``pipeline.transform.resolve_characters``.

Three test layers:

  1. Pydantic validation — the YAML schema rejects malformed entries
  2. Match-mode primitives — substring vs word_boundary vs requires_context
  3. The full registry against the real shipped characters.yaml
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from pipeline.transform.resolve_characters import (
    REGISTRY_PATH,
    AliasEntry,
    CharacterEntry,
    CharacterRegistry,
    _alias_matches,
)


# --- pydantic validation -----------------------------------------------------


def test_alias_entry_accepts_valid_payload() -> None:
    a = AliasEntry(string="Gojo", mode="word_boundary", weight=1.0)
    assert a.string == "Gojo"


def test_alias_entry_rejects_empty_string() -> None:
    with pytest.raises(ValueError):
        AliasEntry(string="", mode="word_boundary", weight=1.0)


def test_alias_entry_rejects_whitespace_string() -> None:
    with pytest.raises(ValueError):
        AliasEntry(string="   ", mode="word_boundary", weight=1.0)


def test_alias_entry_rejects_invalid_mode() -> None:
    with pytest.raises(ValueError):
        AliasEntry(string="Gojo", mode="not_a_real_mode", weight=1.0)


def test_alias_entry_rejects_weight_above_one() -> None:
    with pytest.raises(ValueError):
        AliasEntry(string="Gojo", mode="word_boundary", weight=1.5)


def test_alias_entry_rejects_negative_weight() -> None:
    with pytest.raises(ValueError):
        AliasEntry(string="Gojo", mode="word_boundary", weight=-0.1)


# --- _alias_matches: substring mode ------------------------------------------


def _alias(string: str, mode: str = "substring", weight: float = 1.0) -> AliasEntry:
    return AliasEntry(string=string, mode=mode, weight=weight)


def test_substring_mode_matches_anywhere() -> None:
    text = "thinking about Gojo Satoru today"
    assert _alias_matches(_alias("Gojo Satoru"), text, text.lower(), False)


def test_substring_mode_is_case_insensitive() -> None:
    text = "GOJO SATORU is the strongest"
    assert _alias_matches(_alias("Gojo Satoru"), text, text.lower(), False)


def test_substring_mode_does_not_require_word_boundary() -> None:
    """Substring mode is for unambiguous full names — boundary doesn't matter."""
    text = "BeforeGojo Satoruafter"
    # Substring mode doesn't enforce word boundaries — any literal
    # occurrence of the alias matches, even glued to other characters.
    # That's the documented tradeoff; reserve substring for unambiguous
    # multi-word aliases where this rarely matters in practice.
    assert _alias_matches(_alias("Gojo Satoru"), text, text.lower(), False)


# --- _alias_matches: word_boundary mode --------------------------------------


def test_word_boundary_matches_isolated_token() -> None:
    text = "I love Gojo so much"
    assert _alias_matches(_alias("Gojo", mode="word_boundary"), text, text.lower(), False)


def test_word_boundary_does_not_match_inside_other_word() -> None:
    """Yuji should match 'Yuji' but not 'Yujiro' (different character)."""
    text = "Yujiro Hanma is from a different anime"
    assert not _alias_matches(_alias("Yuji", mode="word_boundary"), text, text.lower(), False)


def test_word_boundary_matches_at_punctuation() -> None:
    text = "Gojo, the strongest, is dead"
    assert _alias_matches(_alias("Gojo", mode="word_boundary"), text, text.lower(), False)


def test_word_boundary_matches_at_sentence_end() -> None:
    text = "I miss Gojo."
    assert _alias_matches(_alias("Gojo", mode="word_boundary"), text, text.lower(), False)


# --- _alias_matches: requires_context mode -----------------------------------


def test_requires_context_fires_when_indicator_present() -> None:
    text = "the strongest goes up against Sukuna"
    assert _alias_matches(
        _alias("the strongest", mode="requires_context"),
        text, text.lower(), has_any_indicator=True,
    )


def test_requires_context_does_not_fire_without_indicator() -> None:
    """Without another JJK marker in the text, ambiguous aliases don't resolve.
    Otherwise 'the strongest' fires on gym subreddit posts."""
    text = "I'm hitting the gym, gotta become the strongest"
    assert not _alias_matches(
        _alias("the strongest", mode="requires_context"),
        text, text.lower(), has_any_indicator=False,
    )


def test_requires_context_still_needs_alias_in_text() -> None:
    """The alias must literally appear; an indicator alone isn't enough."""
    text = "Gojo is so strong"  # has indicator but doesn't say 'the strongest'
    assert not _alias_matches(
        _alias("the strongest", mode="requires_context"),
        text, text.lower(), has_any_indicator=True,
    )


# --- full registry against the shipped characters.yaml ----------------------


def _registry() -> CharacterRegistry:
    """Load the real registry once per test session."""
    return CharacterRegistry.from_yaml()


def test_registry_loads_real_characters_yaml() -> None:
    reg = _registry()
    assert len(reg.characters) >= 25
    ids = {c.id for c in reg.characters}
    assert "gojo_satoru" in ids
    assert "ryomen_sukuna" in ids
    assert "itadori_yuji" in ids


def test_registry_finds_gojo_in_post() -> None:
    reg = _registry()
    mentions = reg.find_mentions("Gojo Satoru is the protagonist of JJK")
    char_ids = {m.character_id for m in mentions}
    assert "gojo_satoru" in char_ids


def test_registry_finds_multiple_characters_in_one_post() -> None:
    reg = _registry()
    mentions = reg.find_mentions("Gojo vs Sukuna was the fight of the decade")
    char_ids = {m.character_id for m in mentions}
    assert "gojo_satoru" in char_ids
    assert "ryomen_sukuna" in char_ids


def test_registry_picks_highest_weight_alias_per_character() -> None:
    """If both 'Gojo Satoru' (1.0) and 'Satoru' (0.7) fire on the same text,
    we get one mention at weight 1.0, not two mentions."""
    reg = _registry()
    mentions = reg.find_mentions("Gojo Satoru, also known as Satoru, is dead")
    gojo_mentions = [m for m in mentions if m.character_id == "gojo_satoru"]
    assert len(gojo_mentions) == 1
    assert gojo_mentions[0].weight == 1.0


def test_registry_returns_empty_for_text_without_characters() -> None:
    reg = _registry()
    mentions = reg.find_mentions("the weather is nice today")
    assert mentions == []


def test_registry_returns_empty_for_empty_text() -> None:
    reg = _registry()
    assert reg.find_mentions("") == []


def test_registry_does_not_match_yuji_inside_yujiro() -> None:
    """Cross-anime collision: Baki's Yujiro Hanma should not match Yuji Itadori."""
    reg = _registry()
    mentions = reg.find_mentions("Yujiro Hanma from Baki is wild")
    char_ids = {m.character_id for m in mentions}
    assert "itadori_yuji" not in char_ids


def test_registry_resolves_panda_only_with_jjk_context() -> None:
    """'Panda' alone doesn't fire — too easily confused with the animal.
    Only resolves if another JJK character is mentioned alongside."""
    reg = _registry()
    # Without context: should NOT match Panda the character
    mentions = reg.find_mentions("I saw a panda at the zoo today")
    assert "panda" not in {m.character_id for m in mentions}

    # With context: SHOULD match
    mentions_with_context = reg.find_mentions("Panda and Itadori are squad")
    assert "panda" in {m.character_id for m in mentions_with_context}