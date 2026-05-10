"""Sentiment classifier — calls a Groq-hosted Llama via the OpenAI-compatible API.

Architecture
------------
One classification per post or comment (not per character). A post mentioning
both Gojo and Sukuna gets ONE sentiment, which is fanned out to both characters
in the gold layer. The spec is explicit about this tradeoff: it introduces
noise on multi-character posts but keeps cost bounded and the eval honest.

Cache
-----
Hash-keyed JSONL at ``data/_state/classifier_cache.jsonl``. The key is
``{PROMPT_VERSION}:{sha256(text)[:12]}``. Bumping the prompt version
invalidates everything cleanly without deleting any files. The cache is
loaded once into memory at startup; misses append a single line. The file
is small enough to commit to git so collaborators don't pay the API bill
a second time.

Output schema
-------------
The classifier writes to ``silver/post_classifications.parquet`` and
``silver/comment_classifications.parquet``, keyed by the original post or
comment id. The gold layer joins these against character mentions to
produce the per-character sentiment fact table.
"""

from __future__ import annotations

import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import structlog
import typer
from openai import OpenAI
from pydantic import BaseModel, Field, ValidationError, field_validator
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from pipeline.extract._common import UTC, DataPaths, RateLimiter, write_parquet_atomic
from pipeline.transform.prompts import (
    INTENSITY_LABELS,
    PROMPT_VERSION,
    SENTIMENT_LABELS,
    TARGET_LABELS,
    build_messages,
)

log = structlog.get_logger(__name__)

## Groq free tier on llama-3.1-8b-instant caps at 6000 TPM. Each classification
# is ~1200 tokens (the 8-example few-shot eats budget). 4 concurrent workers
# × 1200 tokens = 4800 TPM, leaving headroom under the 6000 cap. The rate
# limiter still throttles RPS as a secondary guard.
DEFAULT_RATE_PER_SECOND = 16.0
DEFAULT_CONCURRENCY = 16

# Model and endpoint defaults. Override in the environment to swap providers.
DEFAULT_MODEL = "llama-3.1-8b-instant"
DEFAULT_BASE_URL = "https://api.groq.com/openai/v1"


# --- result schema (pydantic for validation, dataclass for the cache) --------


class ClassifierResponse(BaseModel):
    """The model's response, validated against our enums."""

    sentiment: str
    confidence: float = Field(ge=0.0, le=1.0)
    intensity: str
    target: str

    @field_validator("sentiment")
    @classmethod
    def _check_sentiment(cls, v: str) -> str:
        if v not in SENTIMENT_LABELS:
            raise ValueError(f"sentiment must be one of {SENTIMENT_LABELS}, got {v!r}")
        return v

    @field_validator("intensity")
    @classmethod
    def _check_intensity(cls, v: str) -> str:
        if v not in INTENSITY_LABELS:
            raise ValueError(f"intensity must be one of {INTENSITY_LABELS}, got {v!r}")
        return v

    @field_validator("target")
    @classmethod
    def _check_target(cls, v: str) -> str:
        if v not in TARGET_LABELS:
            raise ValueError(f"target must be one of {TARGET_LABELS}, got {v!r}")
        return v


@dataclass(frozen=True)
class ClassificationResult:
    """The cache value type. Mirrors :class:`ClassifierResponse` but is
    a plain dataclass so it serializes cleanly to JSONL without pydantic
    metadata, and round-trips through the cache without revalidation.
    """

    sentiment: str
    confidence: float
    intensity: str
    target: str


# --- cache --------------------------------------------------------------------


def cache_key(text: str, prompt_version: str = PROMPT_VERSION) -> str:
    h = hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]
    return f"{prompt_version}:{h}"


class ClassifierCache:
    """JSONL-backed cache loaded once into memory.

    On miss we append a single line; reads are dict lookups. The cache is
    intentionally append-only — no in-place edits — so a crash mid-write
    leaves at most one trailing partial line that the loader skips.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self._mem: dict[str, ClassificationResult] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        with self.path.open(encoding="utf-8") as f:
            for raw in f:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    entry = json.loads(raw)
                    self._mem[entry["key"]] = ClassificationResult(**entry["result"])
                except (json.JSONDecodeError, KeyError, TypeError):
                    # Skip malformed lines; could be a crashed-mid-write partial.
                    continue
        log.info("classifier_cache_loaded", entries=len(self._mem), path=str(self.path))

    def get(self, key: str) -> ClassificationResult | None:
        return self._mem.get(key)

    def put(self, key: str, result: ClassificationResult) -> None:
        self._mem[key] = result
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as f:
            entry = {"key": key, "result": asdict(result), "cached_at": datetime.now(tz=UTC).isoformat()}
            f.write(json.dumps(entry) + "\n")

    def __len__(self) -> int:
        return len(self._mem)


# --- classifier ---------------------------------------------------------------


class ClassifierError(Exception):
    """Raised when the model produces output we can't validate even after retries."""


class GroqClassifier:
    """Stateful classifier wrapping the OpenAI SDK pointed at Groq."""

    def __init__(
        self,
        client: OpenAI,
        model: str,
        cache: ClassifierCache,
        rate_limiter: RateLimiter | None = None,
    ) -> None:
        self.client = client
        self.model = model
        self.cache = cache
        self.rate_limiter = rate_limiter or RateLimiter(DEFAULT_RATE_PER_SECOND)

    @classmethod
    def from_env(cls, paths: DataPaths | None = None) -> GroqClassifier:
        """Build a classifier from environment variables.

        Required: ``GROQ_API_KEY``. Optional: ``LLM_BASE_URL``, ``LLM_MODEL``.
        """
        paths = paths or DataPaths.from_env()
        api_key = os.environ.get("GROQ_API_KEY")
        if not api_key:
            raise RuntimeError(
                "Missing GROQ_API_KEY. Get one at https://console.groq.com — "
                "free tier, no card needed."
            )
        client = OpenAI(
            api_key=api_key,
            base_url=os.environ.get("LLM_BASE_URL", DEFAULT_BASE_URL),
        )
        model = os.environ.get("LLM_MODEL", DEFAULT_MODEL)
        cache = ClassifierCache(paths.state_dir / "classifier_cache.jsonl")
        return cls(client=client, model=model, cache=cache)

    def classify(self, text: str) -> ClassificationResult:
        """Return the classification for ``text``, hitting cache when possible."""
        if not text or not text.strip():
            return ClassificationResult(
                sentiment="neutral", confidence=1.0, intensity="low", target="about_meta"
            )

        key = cache_key(text)
        hit = self.cache.get(key)
        if hit is not None:
            return hit

        result = self._call_api(text)
        self.cache.put(key, result)
        return result

    @retry(
        retry=retry_if_exception_type(
            (json.JSONDecodeError, ValidationError, Exception)
        ),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        stop=stop_after_attempt(6),
        reraise=True,
    )
    def _call_api(self, text: str) -> ClassificationResult:
        """Single API call with retries on parse/validation failures."""
        with self.rate_limiter.acquire():
            response = self.client.chat.completions.create(
                model=self.model,
                messages=build_messages(text),
                response_format={"type": "json_object"},
                temperature=0.0,
                max_tokens=200,
            )
        raw = response.choices[0].message.content or ""
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as e:
            log.warning("classifier_bad_json", raw=raw[:200], error=str(e))
            raise
        validated = ClassifierResponse.model_validate(payload)
        return ClassificationResult(
            sentiment=validated.sentiment,
            confidence=validated.confidence,
            intensity=validated.intensity,
            target=validated.target,
        )


# --- silver→silver classification orchestrator --------------------------------


CLASSIFICATION_SCHEMA = pa.schema(
    [
        ("id", pa.string()),
        ("sentiment", pa.string()),
        ("confidence", pa.float64()),
        ("intensity", pa.string()),
        ("target", pa.string()),
        ("classified_at", pa.timestamp("us", tz="UTC")),
        ("prompt_version", pa.string()),
        ("model", pa.string()),
    ]
)


def _post_text(row: dict[str, Any]) -> str:
    """Combine post title and body for classification."""
    title = (row.get("title_clean") or row.get("title") or "").strip()
    body = (row.get("selftext_clean") or row.get("selftext") or "").strip()
    if title and body:
        return f"{title}\n\n{body}"
    return title or body


def classify_posts(
    classifier: GroqClassifier | None = None,
    paths: DataPaths | None = None,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> int:
    """Classify every post in silver, write results parquet.

    Concurrent: ``concurrency`` posts are in flight against Groq at any given
    moment. The rate limiter is shared across threads, so the effective
    throughput is bounded by ``DEFAULT_RATE_PER_SECOND`` regardless of
    concurrency. Cache hits skip API calls entirely.
    """
    paths = paths or DataPaths.from_env()
    classifier = classifier or GroqClassifier.from_env(paths)

    posts_path = paths.silver / "posts.parquet"
    if not posts_path.exists():
        log.warning("classify_no_silver_posts", path=str(posts_path))
        write_parquet_atomic(
            [], paths.silver / "post_classifications.parquet", CLASSIFICATION_SCHEMA
        )
        return 0

    rows = pq.read_table(posts_path).to_pylist()
    classified_at = datetime.now(tz=UTC)

    # Pre-build (post_id, text) tuples so the worker function is closure-free.
    work: list[tuple[str, str]] = []
    for r in rows:
        text = _post_text(r)
        if text:
            work.append((r["id"], text))

    log.info("classify_posts_start", total=len(work), concurrency=concurrency)
    out = _classify_concurrent(classifier, work, classified_at, concurrency)

    dest = paths.silver / "post_classifications.parquet"
    write_parquet_atomic(out, dest, CLASSIFICATION_SCHEMA)
    log.info("classify_posts_done", classified=len(out), input_rows=len(rows))
    return len(out)


def classify_comments(
    classifier: GroqClassifier | None = None,
    paths: DataPaths | None = None,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> int:
    """Classify every comment in silver, write results parquet."""
    paths = paths or DataPaths.from_env()
    classifier = classifier or GroqClassifier.from_env(paths)

    comments_path = paths.silver / "comments.parquet"
    if not comments_path.exists():
        log.warning("classify_no_silver_comments", path=str(comments_path))
        write_parquet_atomic(
            [], paths.silver / "comment_classifications.parquet",
            CLASSIFICATION_SCHEMA,
        )
        return 0

    rows = pq.read_table(comments_path).to_pylist()
    classified_at = datetime.now(tz=UTC)

    work: list[tuple[str, str]] = []
    for r in rows:
        body = (r.get("body_clean") or r.get("body") or "").strip()
        if body:
            work.append((r["id"], body))

    log.info("classify_comments_start", total=len(work), concurrency=concurrency)
    out = _classify_concurrent(classifier, work, classified_at, concurrency)

    dest = paths.silver / "comment_classifications.parquet"
    write_parquet_atomic(out, dest, CLASSIFICATION_SCHEMA)
    log.info("classify_comments_done", classified=len(out), input_rows=len(rows))
    return len(out)


def _classify_concurrent(
    classifier: GroqClassifier,
    work: list[tuple[str, str]],
    classified_at: datetime,
    concurrency: int,
) -> list[dict[str, Any]]:
    """Run ``classifier.classify`` concurrently across (id, text) pairs.

    The rate limiter inside the classifier is shared across threads, so the
    effective throughput is bounded by ``DEFAULT_RATE_PER_SECOND`` regardless
    of how high concurrency is set. Failures on one row don't abort the
    whole run — they just produce a warning and that row is skipped.
    """
    out: list[dict[str, Any]] = []
    completed = 0
    total = len(work)
    next_log = max(1, total // 20)  # log progress at 5% increments

    def _classify_one(item: tuple[str, str]) -> dict[str, Any] | None:
        row_id, text = item
        try:
            result = classifier.classify(text)
            return {
                "id": row_id,
                "sentiment": result.sentiment,
                "confidence": result.confidence,
                "intensity": result.intensity,
                "target": result.target,
                "classified_at": classified_at,
                "prompt_version": PROMPT_VERSION,
                "model": classifier.model,
            }
        except Exception as e:  # noqa: BLE001
            log.warning("classify_row_failed", id=row_id, error=str(e))
            return None

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(_classify_one, item) for item in work]
        for fut in as_completed(futures):
            row = fut.result()
            if row is not None:
                out.append(row)
            completed += 1
            if completed % next_log == 0:
                log.info(
                    "classify_progress",
                    completed=completed,
                    total=total,
                    pct=f"{100 * completed / total:.0f}%",
                )

    return out


def classify_all(paths: DataPaths | None = None) -> tuple[int, int]:
    """Run both posts and comments classification."""
    paths = paths or DataPaths.from_env()
    classifier = GroqClassifier.from_env(paths)
    return classify_posts(classifier, paths), classify_comments(classifier, paths)


# --- CLI ----------------------------------------------------------------------

app = typer.Typer(help="Sentiment classification via Groq")


@app.command()
def run() -> None:
    """Classify all silver posts and comments."""
    posts, comments = classify_all()
    typer.echo(f"Classified: posts={posts} comments={comments}")


if __name__ == "__main__":
    app()