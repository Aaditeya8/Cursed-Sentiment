"""Tests for ``pipeline.transform.eval_runner``.

The runner is a stack of pure functions over a list of cases and a
predictable classifier. We test by building tiny eval sets and stub
classifiers that produce known outputs.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from pipeline.transform.classify_sentiment import (
    ClassificationResult,
    GroqClassifier,
)
from pipeline.transform.eval_runner import (
    DEFAULT_EVAL_PATH,
    EvalCase,
    _collect_misses,
    _compute_axis_metrics,
    _per_category_breakdown,
    evaluate,
    load_eval_set,
)
from pipeline.transform.prompts import (
    INTENSITY_LABELS,
    SENTIMENT_LABELS,
    TARGET_LABELS,
)


# --- _compute_axis_metrics ----------------------------------------------------


def test_metrics_all_correct_gives_perfect_scores() -> None:
    metrics = _compute_axis_metrics(
        "sentiment",
        SENTIMENT_LABELS,
        actual=["positive", "negative", "mixed"],
        predicted=["positive", "negative", "mixed"],
    )
    assert metrics.accuracy == 1.0
    for cls in ("positive", "negative", "mixed"):
        assert metrics.per_class[cls]["precision"] == 1.0
        assert metrics.per_class[cls]["recall"] == 1.0
        assert metrics.per_class[cls]["f1"] == 1.0


def test_metrics_all_wrong_gives_zero() -> None:
    metrics = _compute_axis_metrics(
        "sentiment",
        SENTIMENT_LABELS,
        actual=["positive", "positive"],
        predicted=["negative", "negative"],
    )
    assert metrics.accuracy == 0.0
    assert metrics.per_class["positive"]["recall"] == 0.0
    assert metrics.per_class["negative"]["precision"] == 0.0


def test_metrics_class_collapse_is_visible() -> None:
    """A classifier that always predicts 'positive' should have high
    positive-recall but low everything-else-recall. This is the failure
    pattern the test exists to catch."""
    metrics = _compute_axis_metrics(
        "sentiment",
        SENTIMENT_LABELS,
        actual=["positive", "negative", "mixed", "neutral"],
        predicted=["positive", "positive", "positive", "positive"],
    )
    assert metrics.per_class["positive"]["recall"] == 1.0
    assert metrics.per_class["positive"]["precision"] == 0.25
    assert metrics.per_class["negative"]["recall"] == 0.0
    assert metrics.per_class["mixed"]["recall"] == 0.0
    assert metrics.per_class["neutral"]["recall"] == 0.0


def test_metrics_confusion_matrix_counts_correctly() -> None:
    metrics = _compute_axis_metrics(
        "sentiment",
        SENTIMENT_LABELS,
        actual=["positive", "positive", "negative"],
        predicted=["positive", "negative", "negative"],
    )
    assert metrics.confusion[("positive", "positive")] == 1
    assert metrics.confusion[("positive", "negative")] == 1
    assert metrics.confusion[("negative", "negative")] == 1


def test_metrics_handles_class_with_zero_support() -> None:
    """A class that doesn't appear at all shouldn't crash with division-by-zero."""
    metrics = _compute_axis_metrics(
        "sentiment",
        SENTIMENT_LABELS,
        actual=["positive", "positive"],
        predicted=["positive", "positive"],
    )
    assert metrics.per_class["mixed"]["support"] == 0
    assert metrics.per_class["mixed"]["f1"] == 0.0


# --- per-category breakdown ---------------------------------------------------


def _case(id_: str, category: str, sent: str = "positive", inten: str = "high",
          tgt: str = "about_arc") -> EvalCase:
    return EvalCase(
        id=id_, text="x", category=category,
        expected_sentiment=sent, expected_intensity=inten, expected_target=tgt,
    )


def test_per_category_breakdown_counts_all_axes_correct() -> None:
    cases = [
        _case("a", "killed_me_idiom"),
        _case("b", "killed_me_idiom", sent="negative"),
    ]
    preds = [
        ClassificationResult(sentiment="positive", confidence=0.9,
                             intensity="high", target="about_arc"),
        ClassificationResult(sentiment="positive", confidence=0.9,
                             intensity="high", target="about_arc"),
    ]
    out = _per_category_breakdown(cases, preds)
    assert out["killed_me_idiom"]["total"] == 2
    assert out["killed_me_idiom"]["all_correct"] == 1


# --- _collect_misses ----------------------------------------------------------


def test_collect_misses_records_only_wrong_cases() -> None:
    cases = [
        _case("right", "cat1"),
        _case("wrong_sentiment", "cat2"),
    ]
    preds = [
        ClassificationResult(sentiment="positive", confidence=0.9,
                             intensity="high", target="about_arc"),
        ClassificationResult(sentiment="negative", confidence=0.9,
                             intensity="high", target="about_arc"),
    ]
    misses = _collect_misses(cases, preds)
    assert len(misses) == 1
    assert misses[0]["id"] == "wrong_sentiment"
    assert "sentiment" in misses[0]["wrong_axes"]


def test_collect_misses_records_multi_axis_misses() -> None:
    cases = [_case("triple_wrong", "cat1")]
    preds = [
        ClassificationResult(
            sentiment="negative", confidence=0.9, intensity="low", target="about_meta"
        )
    ]
    misses = _collect_misses(cases, preds)
    assert sorted(misses[0]["wrong_axes"]) == ["intensity", "sentiment", "target"]


# --- top-level evaluate -------------------------------------------------------


def _stub_classifier(predictions: list[ClassificationResult]) -> GroqClassifier:
    """Build a GroqClassifier whose classify() returns predetermined results."""
    classifier = MagicMock(spec=GroqClassifier)
    classifier.model = "test-model"
    classifier.classify.side_effect = predictions
    return classifier


def test_evaluate_produces_complete_report() -> None:
    cases = [
        _case("a", "cat1"),
        _case("b", "cat2", sent="negative"),
    ]
    preds = [
        ClassificationResult(sentiment="positive", confidence=0.9,
                             intensity="high", target="about_arc"),
        ClassificationResult(sentiment="negative", confidence=0.9,
                             intensity="high", target="about_arc"),
    ]
    report = evaluate(_stub_classifier(preds), cases=cases)
    assert report.eval_set_size == 2
    assert report.sentiment.accuracy == 1.0
    serialized = json.dumps(report.to_json())
    assert "sentiment" in serialized


def test_evaluate_surfaces_misses_in_report() -> None:
    cases = [_case("wrong", "cat1")]
    preds = [
        ClassificationResult(sentiment="negative", confidence=0.9,
                             intensity="high", target="about_arc"),
    ]
    report = evaluate(_stub_classifier(preds), cases=cases)
    assert len(report.misses) == 1
    assert report.misses[0]["id"] == "wrong"


# --- eval set loader ---------------------------------------------------------


def test_load_eval_set_reads_real_synthetic_file() -> None:
    """Smoke test that the shipped eval JSONL loads cleanly."""
    cases = load_eval_set(DEFAULT_EVAL_PATH)
    assert len(cases) >= 50, "spec calls for ~50 eval cases minimum"
    categories = {c.category for c in cases}
    assert "killed_me_idiom" in categories
    assert "praying_hands_resignation" in categories
    assert "neutral_question" in categories


def test_load_eval_set_uses_valid_enum_values() -> None:
    """Every expected output in the eval set must be a valid enum value —
    otherwise the eval would silently mark legitimate model outputs as wrong."""
    cases = load_eval_set(DEFAULT_EVAL_PATH)
    for c in cases:
        assert c.expected_sentiment in SENTIMENT_LABELS, (
            f"{c.id}: bad sentiment {c.expected_sentiment!r}"
        )
        assert c.expected_intensity in INTENSITY_LABELS, (
            f"{c.id}: bad intensity {c.expected_intensity!r}"
        )
        assert c.expected_target in TARGET_LABELS, (
            f"{c.id}: bad target {c.expected_target!r}"
        )


def test_load_eval_set_has_unique_ids() -> None:
    """ID collisions in the eval set would cause confusing metric attribution."""
    cases = load_eval_set(DEFAULT_EVAL_PATH)
    ids = [c.id for c in cases]
    assert len(ids) == len(set(ids))