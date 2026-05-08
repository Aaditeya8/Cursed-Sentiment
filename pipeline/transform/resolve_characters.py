"""Character mention resolution from arbitrary text.

Reads the alias dictionary from ``reference/characters.yaml`` and finds
every canonical character mentioned in a given string. Used by the gold
layer to fan one classification out to per-character fact rows.

Three match modes per alias:
    substring        — match anywhere in text. Use for unambiguous full names.
    word_boundary    — match only at word boundaries. Use for short tokens.
    requires_context — only resolve if another character indicator appears
                       in the same text. Use for ambiguous tokens like
                       'the strongest' or 'Panda'.

Each alias also carries a weight in [0, 1] which propagates to the gold
fact tables, so downstream aggregations can discount low-confidence
mentions. Bare 'Satoru' is 0.7; full 'Gojo Satoru' is 1.0.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, Literal

import yaml
from pydantic import BaseModel, Field, field_validator

# Reference YAML lives at the project root, three directories above this file:
# pipeline/transform/resolve_characters.py → ../../reference/characters.yaml
REGISTRY_PATH = Path(__file__).resolve().parent.parent.parent / "reference" / "characters.yaml"


# --- pydantic schema for characters.yaml validation --------------------------


class AliasEntry(BaseModel):
    """One alias with its match mode and weight.

    pydantic validates the shape on load, which is the cheapest place to
    catch a misformed yaml entry — better than discovering it during a
    backfill three hours into a job.
    """

    string: str
    mode: Literal["substring", "word_boundary", "requires_context"]
    weight: float = Field(ge=0.0, le=1.0)

    @field_validator("string")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("alias string must be non-empty")
        return v


class CharacterEntry(BaseModel):
    """One character with metadata and its alias list."""

    id: str
    display_name: str
    role: str
    affiliation: str
    status: str
    aliases: list[AliasEntry]
    notes: str | None = None


# --- match result ------------------------------------------------------------


@dataclass(frozen=True)
class CharacterMention:
    """One character mention found in text.

    ``alias_matched`` records which alias string fired so the gold layer can
    show evidence on the methodology page (e.g., "matched 'Satoru' at weight
    0.7"). ``weight`` is the alias's confidence weight, which gold-layer
    aggregations use to discount low-confidence mentions.
    """

    character_id: str
    alias_matched: str
    weight: float


# --- registry ----------------------------------------------------------------


@dataclass
class CharacterRegistry:
    """In-memory copy of the alias dictionary, keyed for fast lookup.

    Constructed once per pipeline run via ``from_yaml``. The internal
    ``_indicators`` set holds every high-confidence alias across all
    characters — used by the requires_context check to detect "is there
    any character mention at all in this text" before resolving ambiguous
    aliases.
    """

    characters: list[CharacterEntry]
    _indicators: set[str] = field(default_factory=set)

    @classmethod
    def from_yaml(cls, path: Path = REGISTRY_PATH) -> CharacterRegistry:
        with path.open(encoding="utf-8") as f:
            raw = yaml.safe_load(f)
        characters = [CharacterEntry.model_validate(c) for c in raw]
        # An "indicator" is any alias with weight >= 0.85 — high enough that
        # if it appears, we trust it as evidence the text is about a JJK
        # character generally, even if we don't yet know which.
        indicators: set[str] = set()
        for c in characters:
            for a in c.aliases:
                if a.weight >= 0.85 and a.mode != "requires_context":
                    indicators.add(a.string.lower())
        return cls(characters=characters, _indicators=indicators)

    def find_mentions(self, text: str) -> list[CharacterMention]:
        """Find every character mention in ``text``.

        A character can be mentioned multiple times via different aliases;
        we deduplicate to one mention per character_id, taking the highest
        weight that fired. (A post mentioning both 'Gojo Satoru' (1.0) and
        'Satoru' (0.7) gets one mention at weight 1.0.)
        """
        if not text:
            return []
        text_lower = text.lower()
        has_any_indicator = any(ind in text_lower for ind in self._indicators)

        # Per-character: collect the highest-weight match across all aliases
        # that fired. We track the matched alias too so the gold layer can
        # show what evidence supported the mention.
        best: dict[str, CharacterMention] = {}
        for char in self.characters:
            for alias in char.aliases:
                if not _alias_matches(alias, text, text_lower, has_any_indicator):
                    continue
                existing = best.get(char.id)
                if existing is None or alias.weight > existing.weight:
                    best[char.id] = CharacterMention(
                        character_id=char.id,
                        alias_matched=alias.string,
                        weight=alias.weight,
                    )
                    break  # each character only fires once per text
        return list(best.values())


# --- match mode implementations ----------------------------------------------


def _alias_matches(
    alias: AliasEntry,
    text: str,
    text_lower: str,
    has_any_indicator: bool,
) -> bool:
    """Test whether one alias matches according to its mode."""
    needle = alias.string.lower()

    if alias.mode == "substring":
        return needle in text_lower

    if alias.mode == "word_boundary":
        # Word boundaries are tricky for non-ASCII text; the regex \b
        # respects unicode word characters. Build per-call rather than
        # caching because the cache key (alias.string) is small.
        pattern = r"\b" + re.escape(needle) + r"\b"
        return re.search(pattern, text_lower, re.IGNORECASE) is not None

    if alias.mode == "requires_context":
        # Two-stage: first the alias must literally appear; then there has
        # to be some other indicator (a high-confidence alias from any
        # character) in the same text. Otherwise 'the strongest' would
        # fire on every workout post.
        if needle not in text_lower:
            return False
        return has_any_indicator

    # Unknown mode is a YAML error pydantic should have caught earlier.
    return False