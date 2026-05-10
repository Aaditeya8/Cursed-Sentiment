"""The ``cursed`` CLI — entry point for every pipeline stage.

One Typer app with subcommands for each layer of the pipeline. Wired into
``pyproject.toml`` via ``[project.scripts] cursed = "..."``, so you can
invoke ``uv run cursed <command>`` from anywhere in the project.

Commands
--------

  cursed daily        PRAW incremental into bronze (needs Reddit creds)
  cursed silver       Bronze → Silver clean (no creds)
  cursed classify     Silver → silver+sentiment (needs Groq key)
  cursed gold         Silver → 7 gold parquets (no creds)
  cursed eval         Synthetic eval harness (needs Groq key)
  cursed spot-check   Eyeball N classifications on real silver (needs Groq key)
  cursed backfill     Arctic Shift historical → bronze (no creds, polite)
  cursed status       Report watermarks and silver-processed file counts

Designed so the daily cron can be expressed as a sequence of these:

  cursed daily        # incremental bronze
  cursed silver       # bronze → silver
  cursed classify     # silver → silver+sentiment
  cursed gold         # silver → gold
"""

from __future__ import annotations

import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

import pyarrow.parquet as pq
import structlog
import typer

from pipeline.extract import arctic_shift
from pipeline.extract._common import UTC, DataPaths, SUBREDDITS, WatermarkStore
from pipeline.load import build_gold
from pipeline.transform import clean as silver_clean
from pipeline.transform import classify_sentiment, eval_runner

log = structlog.get_logger(__name__)
app = typer.Typer(
    help="Cursed Sentiment — pipeline orchestrator",
    no_args_is_help=True,
)


# --- daily incremental --------------------------------------------------------

@app.command()
def daily(
    days: int = typer.Option(2, help="How many trailing days of data to pull"),
) -> None:
    """Daily incremental ingestion via Arctic Shift.

    Pulls the trailing N days of posts and comments from each subreddit.
    Two days by default — gives a 24-hour overlap that silver-layer dedup
    will collapse, so a missed run doesn't create gaps.

    Arctic Shift is the project's only live data source as of v1: Reddit
    closed self-service OAuth in Nov 2025 (Responsible Builder Policy),
    making PRAW unavailable without prior approval.
    """
    now = datetime.now(tz=UTC)
    after = now - timedelta(days=days)
    counts = arctic_shift.backfill(after=after, before=now)
    typer.echo(
        f"Daily ingest (Arctic Shift, last {days}d): "
        f"posts={counts.posts_fetched} comments={counts.comments_fetched}"
    )

@app.command()
def classify(
    posts_only: bool = typer.Option(False, "--posts-only", help="Skip comment classification"),
) -> None:
    """Run sentiment classification across silver posts and comments.

    Idempotent and cache-aware: only un-cached texts hit the Groq API.
    Bumping ``PROMPT_VERSION`` in ``pipeline.transform.prompts`` invalidates
    every cache entry cleanly.

    Use ``--posts-only`` to skip comments — useful for first-run dashboards
    where post-level signal is plenty and comments would dominate API budget.
    """
    classifier = classify_sentiment.GroqClassifier.from_env()
    posts = classify_sentiment.classify_posts(classifier)
    comments = 0
    if not posts_only:
        comments = classify_sentiment.classify_comments(classifier)
    typer.echo(
        f"\nClassified: posts={posts} comments={comments}\n"
        f"  Bumping PROMPT_VERSION invalidates the cache."
    )


# --- silver / classify / gold -------------------------------------------------


@app.command()
def silver() -> None:
    """Bronze → Silver clean.

    Idempotent: re-runs only process new bronze files (tracked via the
    silver_processed.json state file), but always rewrites silver from
    the full corpus so dedup is always against everything ingested.
    """
    result = silver_clean.run()
    typer.echo(
        f"Silver: posts {result.posts_processed} new / "
        f"{result.posts_silver_total} total · "
        f"comments {result.comments_processed} new / "
        f"{result.comments_silver_total} total"
    )


@app.command()
def eval() -> None:
    """Run the synthetic eval harness against Groq.

    Reads ``reference/eval_synthetic.jsonl``, runs the classifier on each
    case, computes per-class precision/recall/F1, and writes
    ``data/gold/eval_results.json`` for the dashboard's methodology page.
    """
    classifier = classify_sentiment.GroqClassifier.from_env()
    report = eval_runner.evaluate(classifier)
    eval_runner.write_report(report)
    eval_runner._print_summary(report)


@app.command()
def spot_check(
    n: int = typer.Option(20, help="Number of silver rows to sample and classify"),
    seed: int = typer.Option(0, help="Random seed for reproducible sampling"),
) -> None:
    """Sample N silver posts, classify each, print results for eyeballing.

    The qualitative companion to ``cursed eval``. Useful right after a
    prompt change or to validate that a real backfill slice produces
    sensible classifications before committing to the full backfill.

    Requires silver/posts.parquet to exist; run ``cursed silver`` first.
    """
    paths = DataPaths.from_env()
    posts_path = paths.silver / "posts.parquet"
    if not posts_path.exists():
        typer.echo(
            f"No silver/posts.parquet at {posts_path}.\n"
            f"Run a backfill first, e.g.:\n"
            f"  cursed backfill --after 2023-09-23 --before 2023-09-27 "
            f"--subreddit JuJutsuKaisen\n"
            f"  cursed silver"
        )
        raise typer.Exit(code=1)

    rows = pq.read_table(posts_path).to_pylist()
    candidates = [r for r in rows if (r.get("title") or "").strip()]
    if not candidates:
        typer.echo("Silver has no posts with non-empty titles.")
        raise typer.Exit(code=1)

    random.seed(seed)
    sample = random.sample(candidates, min(n, len(candidates)))

    classifier = classify_sentiment.GroqClassifier.from_env(paths)
    typer.echo(f"\nSpot-checking {len(sample)} posts against {classifier.model}\n")
    typer.echo(f"{'sentiment':<10} {'inten':<7} {'target':<18} title")
    typer.echo("-" * 100)

    for r in sample:
        title = (r.get("title") or "").strip()
        body = (r.get("selftext") or "").strip()
        text = f"{title}\n\n{body}" if body else title
        result = classifier.classify(text)
        truncated = title[:60].replace("\n", " ")
        typer.echo(
            f"{result.sentiment:<10} {result.intensity:<7} "
            f"{result.target:<18} {truncated}"
        )

    typer.echo(
        f"\nCache now has {len(classifier.cache)} entries. "
        f"Re-run with same seed for free (cache hit) or different seed for fresh draws."
    )


@app.command()
def gold() -> None:
    """Build the gold layer (dims + facts + weekly aggregations + gege moments).

    Idempotent: each step reads input parquets and overwrites its output
    atomically. Run after ``cursed classify`` to refresh the dashboard view.
    """
    result = build_gold.build_all()
    typer.echo(
        f"Gold built:\n"
        f"  dim_character:           {result.dim_character}\n"
        f"  dim_event:               {result.dim_event}\n"
        f"  fact_post_sentiment:     {result.fact_post_sentiment}\n"
        f"  fact_comment_sentiment:  {result.fact_comment_sentiment}\n"
        f"  agg_char_week:           {result.agg_char_week}\n"
        f"  agg_polarisation:        {result.agg_polarisation}\n"
        f"  gege_moments:            {result.gege_moments}"
    )


# --- backfill -----------------------------------------------------------------


@app.command()
def backfill(
    after: str = typer.Option(..., help="ISO date, e.g. 2023-09-23"),
    before: str = typer.Option(..., help="ISO date, e.g. 2023-09-27"),
    subreddit: str = typer.Option(
        None, help="Single subreddit; defaults to all three configured ones"
    ),
) -> None:
    """Arctic Shift historical backfill into bronze.

    Polite to a free community service: runs at ~0.5 req/sec. Re-runnable;
    silver dedup handles overlap.
    """
    after_dt = datetime.fromisoformat(after).replace(tzinfo=UTC)
    before_dt = datetime.fromisoformat(before).replace(tzinfo=UTC)
    subs = (subreddit,) if subreddit else SUBREDDITS
    counts = arctic_shift.backfill(subreddits=subs, after=after_dt, before=before_dt)
    typer.echo(
        f"Backfill: posts={counts.posts_fetched} "
        f"comments={counts.comments_fetched}"
    )


# --- status -------------------------------------------------------------------


@app.command()
def status() -> None:
    """Report watermarks and silver-processed file counts.

    Useful for verifying cron continuity: if watermarks haven't advanced
    since yesterday, the daily run probably failed.
    """
    paths = DataPaths.from_env()
    typer.echo(f"\nData root: {paths.root}")

    wm_path = paths.state_dir / "watermarks.json"
    if wm_path.exists():
        watermarks = json.loads(wm_path.read_text())
        typer.echo(f"\nWatermarks ({len(watermarks)} entries):")
        for key, ts in sorted(watermarks.items()):
            dt = datetime.fromtimestamp(ts, tz=UTC).isoformat() if ts else "—"
            typer.echo(f"  {key:<40} {ts:>12}  {dt}")
    else:
        typer.echo("\nNo watermarks file yet — daily ingest hasn't run.")

    sp_path = paths.state_dir / "silver_processed.json"
    if sp_path.exists():
        processed = json.loads(sp_path.read_text())
        typer.echo(f"\nSilver-processed bronze files: {len(processed)}")
    else:
        typer.echo("\nNo silver_processed.json yet — silver hasn't run.")

    bronze_files = list(paths.bronze.rglob("*.parquet")) if paths.bronze.exists() else []
    silver_files = list(paths.silver.glob("*.parquet")) if paths.silver.exists() else []
    gold_files = list(paths.gold.glob("*.parquet")) if paths.gold.exists() else []
    typer.echo(
        f"\nFile counts: "
        f"bronze={len(bronze_files)} silver={len(silver_files)} gold={len(gold_files)}"
    )

    cache_path = paths.state_dir / "classifier_cache.jsonl"
    if cache_path.exists():
        cache_lines = sum(1 for _ in cache_path.open())
        typer.echo(f"Classifier cache: {cache_lines} entries")


if __name__ == "__main__":
    app()