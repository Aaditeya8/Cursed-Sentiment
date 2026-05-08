"""Prompt template and few-shot examples for the sentiment classifier.

Why this is a Python module (not a text file)
---------------------------------------------
Two reasons:

1. The prompt and the cache key are coupled. ``PROMPT_VERSION`` is the
   invalidation signal — bump it and every cached classification is treated
   as stale. Keeping it in code means the version is right next to the
   content, and refactoring tools see the dependency.
2. The few-shot examples are unit-testable as data. We assert that the
   prompt covers each named idiom; that's the line of defense against
   regressing the prompt by accident.

Design rules for the few-shot examples
--------------------------------------
* Each example must teach the model ONE specific idiom that a generic
  sentiment classifier gets wrong on this subreddit.
* Examples are short and look like real Reddit posts. No Markdown headers,
  no academic tone — that would teach the model the wrong distribution.
* Outputs are minimal valid JSON. The system prompt forbids prose around
  the JSON; the examples reinforce by example.
* Every axis label appears at least once across the examples — otherwise
  the model can drift to never producing that label.

When to bump PROMPT_VERSION
---------------------------
Bump on any change that could materially shift the output distribution:
* New or removed few-shot example
* Substantive system-prompt change (definitions, ordering of rules)
* Output schema change (new axis, renamed enum value)

DO NOT bump for typo fixes or formatting tweaks that don't change semantics.
That just wastes the cache.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

# Bump this when the prompt's behavior changes. Cache invalidation depends on it.
PROMPT_VERSION = "v1"


# --- output schema ------------------------------------------------------------
# These literal lists are the source of truth for valid output values. The
# system prompt below references them by name, the parser validates against
# them, and the eval harness uses them to enumerate confusion-matrix axes.

SENTIMENT_LABELS = ("positive", "negative", "mixed", "neutral")
INTENSITY_LABELS = ("low", "medium", "high")
TARGET_LABELS = ("about_character", "about_arc", "about_meta")


# --- system prompt ------------------------------------------------------------
# Written for Llama 3.1 8B-instant on Groq. Llama follows numbered rules well
# and respects "JSON only" instructions without elaborate cajoling. The rules
# are ordered with the highest-leverage corrections first (the "killed me"
# idiom is the single most common failure mode for off-the-shelf classifiers).

SYSTEM_PROMPT = """You classify posts and comments from r/JuJutsuKaisen, the manga and anime subreddit.

For each input, output exactly one JSON object with these four fields:

- "sentiment": one of "positive", "negative", "mixed", "neutral"
- "confidence": a number between 0 and 1 representing how sure you are
- "intensity": one of "low", "medium", "high"  (how emotionally charged the post is, INDEPENDENT of whether it's positive or negative)
- "target": one of "about_character", "about_arc", "about_meta"

GUIDELINES SPECIFIC TO THIS COMMUNITY (read carefully):

1. EMOTIONAL INTENSITY DOES NOT MEAN NEGATIVE. Phrases like "this killed me", "I'm crying", "I'm not okay", and "I'm dead" almost always express ENJOYMENT of something the writer just read or watched. Treat these as POSITIVE with HIGH intensity.

2. SKULL EMOJI 💀 indicates amusement or disbelief, not sadness. Almost always positive or mixed (rarely negative).

3. AFFECTIONATE PROFANITY toward the author Gege Akutami — "Gege you genius bastard", "I hate this man", "fuck Gege" paired with engagement markers — is POSITIVE. The author is being praised through mock-anger.

4. SARCASTIC PRAYING-HANDS 🙏🙏🙏 after wishing a dead character would return is RESIGNED NEGATIVE or MIXED. The fan knows the wish won't be granted.

5. POSTS CAN MENTION A CHARACTER WITHOUT BEING ABOUT THAT CHARACTER. Distinguish:
   - "about_character": the post evaluates a character (their writing, design, arc satisfaction). Example: "Sukuna's writing is incredible, his philosophy actually tracks"
   - "about_arc": the post evaluates a plot decision or storyline event. Example: "Why did Gege kill Gojo, this ruined the pacing" — mentions Gojo but is about the plot choice
   - "about_meta": the post is about the work itself — adaptation quality, art style, anime vs manga comparisons, fandom dynamics. Example: "MAPPA's animation in s2 is mid"

6. BREVITY IS INTENSITY. Short reactions ("PEAK", "tragic", "actually insane") are HIGH intensity even with few words.

7. MIXED SENTIMENT IS COMMON AND VALID. "Sukuna is amazingly written but I hate what he did to Megumi" is MIXED, not negative.

8. NEUTRAL IS FOR INFORMATION-SEEKING OR FACTUAL POSTS. "What chapter is Shibuya?" is NEUTRAL with LOW intensity.

OUTPUT EXACTLY ONE JSON OBJECT. NO MARKDOWN FENCES. NO PROSE BEFORE OR AFTER THE JSON."""


# --- few-shot examples --------------------------------------------------------
# Eight examples, each teaching a specific idiom. Internal-use rationale is
# kept ALONGSIDE each example as a Python field, NOT in the prompt sent to
# the model — JSON has no comments and we don't want the model parsing them.


@dataclass(frozen=True)
class FewShotExample:
    """One few-shot example for the prompt.

    ``rationale`` is documentation for the prompt designer. It is NEVER sent
    to the model — only ``input`` and ``output`` are.
    """

    input: str
    output: dict
    rationale: str
    teaches: str  # one-word tag matched by tests


FEW_SHOT_EXAMPLES: tuple[FewShotExample, ...] = (
    FewShotExample(
        input="this killed me 💀💀💀 chapter 236 was actually peak",
        output={
            "sentiment": "positive",
            "confidence": 0.95,
            "intensity": "high",
            "target": "about_arc",
        },
        rationale=(
            'The "killed me" + skull-emoji idiom plus "peak" makes this '
            "high-intensity positive. It targets a specific plot event "
            "(chapter 236), so about_arc."
        ),
        teaches="killed_me_idiom",
    ),
    FewShotExample(
        input="Gege you genius bastard. you killed our king. I'll never forgive you. 10/10 chapter",
        output={
            "sentiment": "positive",
            "confidence": 0.9,
            "intensity": "high",
            "target": "about_meta",
        },
        rationale=(
            "Affectionate cursing toward Gege paired with '10/10 chapter' "
            "anchors this as praise of authorial choices, not anger. "
            "Targets the author/work — about_meta."
        ),
        teaches="gege_affectionate_curse",
    ),
    FewShotExample(
        input="bring back gojo gege 🙏🙏🙏 it's been 2 years",
        output={
            "sentiment": "negative",
            "confidence": 0.75,
            "intensity": "medium",
            "target": "about_arc",
        },
        rationale=(
            "Sarcastic 🙏🙏🙏 after a known-impossible wish is resigned "
            "negativity. About a plot decision — about_arc."
        ),
        teaches="praying_hands_resignation",
    ),
    FewShotExample(
        input="Sukuna's writing post-Heian flashback is genuinely some of the best villain work I've read. the philosophy actually tracks.",
        output={
            "sentiment": "positive",
            "confidence": 0.95,
            "intensity": "medium",
            "target": "about_character",
        },
        rationale=(
            "Pure character-craft praise. Specific (his writing, his "
            "philosophy), measured tone — about_character, medium intensity."
        ),
        teaches="character_craft_praise",
    ),
    FewShotExample(
        input="PEAK",
        output={
            "sentiment": "positive",
            "confidence": 0.85,
            "intensity": "high",
            "target": "about_arc",
        },
        rationale=(
            "Brevity is intensity. One-word 'PEAK' reactions are how this "
            "community signals strong approval of a moment they just read."
        ),
        teaches="brevity_intensity",
    ),
    FewShotExample(
        input="love Sukuna as a written character, but I hate what he did to Megumi. this isn't the redemption arc I wanted",
        output={
            "sentiment": "mixed",
            "confidence": 0.9,
            "intensity": "medium",
            "target": "about_arc",
        },
        rationale=(
            "Explicit love/hate split — the canonical mixed-sentiment shape. "
            "Targets a plot event (what was done to Megumi) — about_arc."
        ),
        teaches="mixed_love_hate",
    ),
    FewShotExample(
        input="what chapter does the shibuya arc start? trying to plan a reread",
        output={
            "sentiment": "neutral",
            "confidence": 0.95,
            "intensity": "low",
            "target": "about_meta",
        },
        rationale=(
            "Information-seeking, no evaluative content. Neutral, low. "
            "Targets the work as object — about_meta."
        ),
        teaches="neutral_question",
    ),
    FewShotExample(
        input="MAPPA's animation in s2 was honestly carrying a script that didn't always deserve it. the rooftop scene though, that frame composition...",
        output={
            "sentiment": "mixed",
            "confidence": 0.85,
            "intensity": "medium",
            "target": "about_meta",
        },
        rationale=(
            "Praise for animation, criticism of script — mixed. Specifically "
            "about the adaptation quality — about_meta."
        ),
        teaches="adaptation_meta",
    ),
)


# --- prompt assembly ----------------------------------------------------------


def build_messages(text: str) -> list[dict]:
    """Build OpenAI-format messages array for one classification.

    The returned list alternates user/assistant for each few-shot example
    and ends with the actual input as a fresh user message. This is the
    standard "in-context learning" shape for chat-completions endpoints
    and is what Llama responds best to.
    """
    messages: list[dict] = [{"role": "system", "content": SYSTEM_PROMPT}]
    for ex in FEW_SHOT_EXAMPLES:
        messages.append({"role": "user", "content": ex.input})
        messages.append({"role": "assistant", "content": json.dumps(ex.output)})
    messages.append({"role": "user", "content": text})
    return messages


def example_idioms() -> set[str]:
    """Return the set of idiom tags covered by the few-shot examples.

    Used by tests to verify the prompt covers every JJK-specific failure
    mode named in the spec.
    """
    return {ex.teaches for ex in FEW_SHOT_EXAMPLES}