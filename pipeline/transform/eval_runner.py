"""Evaluation harness for the sentiment classifier.

Runs the classifier against ``reference/eval_synthetic.jsonl``, computes
precision / recall / F1 per class for each of the three axes, and writes
the results to ``data/gold/eval_results.json`` for the dashboard's
methodology page to read.

Two failure modes this harness specifically catches:

1. **Class collapse.** A classifier that always says "positive" gets 60% of
   our eval set right by base rate alone. Per-class metrics surface this:
   recall on neutral and mixed will be near zero.
2. **Idiom regression.** When a few-shot example is removed or rewritten,
   the corresponding category's accuracy drops. The category-level breakdown
   in the output JSON pinpoints which idiom regressed.

The synthetic eval is the *first* of two evaluation surfaces. Once the
historical backfill produces real silver data, a 200-post hand-labeled
real eval set will join this one. Both stay live: synthetic for prompt
regression, real for distribution validity.
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import structlog
import typer

from pipeline.extract._common import UTC, DataPaths
from pipeline.transform.classify_sentiment import (
    ClassificationResult,
    GroqClassifier,
)
from pipeline.transform.prompts import (
    INTENSITY_LABELS,
    PROMPT_VERSION,
    SENTIMENT_LABELS,
    TARGET_LABELS,
)

log = structlog.get_logger(__name__)

DEFAULT_EVAL_PATH = Path(__file__).resolve().parent.parent.parent / "reference" / "eval_synthetic.jsonl"


# --- eval set loading ---------------------------------------------------------


@dataclass(frozen=True)
class EvalCase:
    id: str
    text: str
    category: str
    expected_sentiment: str
    expected_intensity: str
    expected_target: str


def load_eval_set(path: Path = DEFAULT_EVAL_PATH) -> list[EvalCase]:
    """Read eval_synthetic.jsonl into a list of EvalCase objects."""
    cases: list[EvalCase] = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            raw = json.loads(line)
            exp = raw["expected"]
            cases.append(
                EvalCase(
                    id=raw["id"],
                    text=raw["text"],
                    category=raw["category"],
                    expected_sentiment=exp["sentiment"],
                    expected_intensity=exp["intensity"],
                    expected_target=exp["target"],
                )
            )
    return cases


# --- metrics ------------------------------------------------------------------


@dataclass
class AxisMetrics:
    """Per-axis precision/recall/F1 plus a full confusion matrix.

    The confusion matrix is keyed ``(actual_label, predicted_label) -> count``,
    which is the natural shape for both grouped tables and image rendering.
    """

    axis: str
    accuracy: float
    per_class: dict[str, dict[str, float]]
    confusion: dict[tuple[str, str], int]


def _compute_axis_metrics(
    axis_name: str,
    labels: tuple[str, ...],
    actual: list[str],
    predicted: list[str],
) -> AxisMetrics:
    """Standard precision/recall/F1 implementation, no sklearn dependency.

    sklearn is a heavy dep for one function we'll call once per run; the
    canonical formula is short and worth keeping inline so the eval has no
    hidden behavior.
    """
    n = len(actual)
    correct = sum(1 for a, p in zip(actual, predicted) if a == p)
    accuracy = correct / n if n else 0.0

    confusion: dict[tuple[str, str], int] = defaultdict(int)
    for a, p in zip(actual, predicted):
        confusion[(a, p)] += 1

    per_class: dict[str, dict[str, float]] = {}
    for cls in labels:
        tp = confusion[(cls, cls)]
        fp = sum(c for (a, p), c in confusion.items() if p == cls and a != cls)
        fn = sum(c for (a, p), c in confusion.items() if a == cls and p != cls)
        support = sum(c for (a, _), c in confusion.items() if a == cls)
        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
        per_class[cls] = {
            "precision": round(precision, 3),
            "recall": round(recall, 3),
            "f1": round(f1, 3),
            "support": support,
        }

    return AxisMetrics(
        axis=axis_name,
        accuracy=round(accuracy, 3),
        per_class=per_class,
        confusion=dict(confusion),
    )


@dataclass
class EvalReport:
    """Bundle of all metrics from one eval run."""

    eval_set_size: int
    prompt_version: str
    model: str
    ran_at: datetime
    sentiment: AxisMetrics
    intensity: AxisMetrics
    target: AxisMetrics
    per_category: dict[str, dict[str, int]] = field(default_factory=dict)
    misses: list[dict[str, Any]] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        def axis_dict(m: AxisMetrics) -> dict[str, Any]:
            return {
                "accuracy": m.accuracy,
                "per_class": m.per_class,
                "confusion": [
                    {"actual": a, "predicted": p, "count": c}
                    for (a, p), c in sorted(m.confusion.items())
                ],
            }

        return {
            "eval_set_size": self.eval_set_size,
            "prompt_version": self.prompt_version,
            "model": self.model,
            "ran_at": self.ran_at.isoformat(),
            "sentiment": axis_dict(self.sentiment),
            "intensity": axis_dict(self.intensity),
            "target": axis_dict(self.target),
            "per_category": self.per_category,
            "misses": self.misses,
        }


def _per_category_breakdown(
    cases: list[EvalCase],
    predictions: list[ClassificationResult],
) -> dict[str, dict[str, int]]:
    """Per-idiom-category accuracy. Surfaces *which* idiom regressed on a
    bad run, which is the diagnostic the prompt designer actually needs."""
    out: dict[str, dict[str, int]] = defaultdict(lambda: {"total": 0, "all_correct": 0})
    for c, p in zip(cases, predictions):
        out[c.category]["total"] += 1
        if (
            p.sentiment == c.expected_sentiment
            and p.intensity == c.expected_intensity
            and p.target == c.expected_target
        ):
            out[c.category]["all_correct"] += 1
    return dict(out)


def _collect_misses(
    cases: list[EvalCase],
    predictions: list[ClassificationResult],
) -> list[dict[str, Any]]:
    """Return only the cases the model got wrong on at least one axis."""
    misses: list[dict[str, Any]] = []
    for c, p in zip(cases, predictions):
        wrong = []
        if p.sentiment != c.expected_sentiment:
            wrong.append("sentiment")
        if p.intensity != c.expected_intensity:
            wrong.append("intensity")
        if p.target != c.expected_target:
            wrong.append("target")
        if wrong:
            misses.append(
                {
                    "id": c.id,
                    "category": c.category,
                    "text": c.text,
                    "wrong_axes": wrong,
                    "expected": {
                        "sentiment": c.expected_sentiment,
                        "intensity": c.expected_intensity,
                        "target": c.expected_target,
                    },
                    "predicted": {
                        "sentiment": p.sentiment,
                        "intensity": p.intensity,
                        "target": p.target,
                    },
                }
            )
    return misses


# --- top-level run ------------------------------------------------------------


def evaluate(
    classifier: GroqClassifier,
    cases: list[EvalCase] | None = None,
) -> EvalReport:
    """Run the classifier against the eval set and return a structured report.

    No I/O — caller is responsible for persisting the report. This separation
    means tests can build a classifier with a mocked client, run evaluate,
    and assert on the metrics without ever touching disk.
    """
    cases = cases or load_eval_set()

    predictions = [classifier.classify(c.text) for c in cases]

    actual_sent = [c.expected_sentiment for c in cases]
    pred_sent = [p.sentiment for p in predictions]
    actual_int = [c.expected_intensity for c in cases]
    pred_int = [p.intensity for p in predictions]
    actual_tgt = [c.expected_target for c in cases]
    pred_tgt = [p.target for p in predictions]

    return EvalReport(
        eval_set_size=len(cases),
        prompt_version=PROMPT_VERSION,
        model=classifier.model,
        ran_at=datetime.now(tz=UTC),
        sentiment=_compute_axis_metrics("sentiment", SENTIMENT_LABELS, actual_sent, pred_sent),
        intensity=_compute_axis_metrics("intensity", INTENSITY_LABELS, actual_int, pred_int),
        target=_compute_axis_metrics("target", TARGET_LABELS, actual_tgt, pred_tgt),
        per_category=_per_category_breakdown(cases, predictions),
        misses=_collect_misses(cases, predictions),
    )


def write_report(report: EvalReport, paths: DataPaths | None = None) -> Path:
    """Persist a report to ``gold/eval_results.json``."""
    paths = paths or DataPaths.from_env()
    paths.gold.mkdir(parents=True, exist_ok=True)
    dest = paths.gold / "eval_results.json"
    dest.write_text(json.dumps(report.to_json(), indent=2))
    log.info("eval_report_written", path=str(dest))
    return dest


# --- CLI ----------------------------------------------------------------------

app = typer.Typer(help="Run the synthetic-eval harness against Groq")


@app.command()
def run() -> None:
    """Classify the eval set, print metrics, write results JSON."""
    classifier = GroqClassifier.from_env()
    report = evaluate(classifier)
    write_report(report)
    _print_summary(report)


def _print_summary(report: EvalReport) -> None:
    """Pretty-print the report to the console."""
    typer.echo(f"\nEval against prompt version {report.prompt_version} / model {report.model}")
    typer.echo(f"  {report.eval_set_size} cases")
    for axis in (report.sentiment, report.intensity, report.target):
        typer.echo(f"\n  {axis.axis} accuracy: {axis.accuracy:.1%}")
        for cls, m in axis.per_class.items():
            typer.echo(
                f"    {cls:>17}: P={m['precision']:.2f} R={m['recall']:.2f} "
                f"F1={m['f1']:.2f} (n={m['support']})"
            )
    if report.misses:
        typer.echo(f"\n  {len(report.misses)} cases missed at least one axis")


if __name__ == "__main__":
    app()