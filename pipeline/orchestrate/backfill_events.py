"""Backfill bronze for every event in reference/events.yaml.

Iterates over the event timeline, pulls a 7-day window centered on each
event's release date from Arctic Shift, and lands it in bronze. Use after
the v1 dashboard ships to populate the historical time series.

Usage:
    uv run python -m pipeline.orchestrate.backfill_events
    uv run python -m pipeline.orchestrate.backfill_events --window-days 5

Each event takes 2-5 minutes (varies by subreddit traffic that week).
Twenty events ≈ 60-90 minutes total. Polite to Arctic Shift; safe to
re-run, silver dedup collapses overlap.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import structlog
import typer
import yaml

from pipeline.extract import arctic_shift
from pipeline.extract._common import UTC

log = structlog.get_logger(__name__)
app = typer.Typer(help="Backfill bronze for every chapter event")

EVENTS_PATH = Path(__file__).resolve().parent.parent.parent / "reference" / "events.yaml"


@app.command()
def run(
    window_days: int = typer.Option(7, help="Days on each side of the event date"),
    skip_unverified: bool = typer.Option(
        False, help="Skip events with verified=false (most are estimated)"
    ),
) -> None:
    """Iterate events.yaml; backfill a window around each event date."""
    with EVENTS_PATH.open(encoding="utf-8") as f:
        events = yaml.safe_load(f)

    typer.echo(f"\nFound {len(events)} events. Starting backfill...\n")

    total_posts = 0
    total_comments = 0
    skipped = 0

    for i, event in enumerate(events, 1):
        if skip_unverified and not event.get("verified", False):
            skipped += 1
            continue

        event_date = event["date"]
        if not isinstance(event_date, datetime):
            event_date = datetime(event_date.year, event_date.month, event_date.day, tzinfo=UTC)

        after = event_date - timedelta(days=window_days)
        before = event_date + timedelta(days=window_days)

        typer.echo(
            f"[{i}/{len(events)}] {event['id']:<28} "
            f"{event_date.strftime('%Y-%m-%d')} "
            f"(±{window_days}d)"
        )

        try:
            counts = arctic_shift.backfill(after=after, before=before)
            total_posts += counts.posts_fetched
            total_comments += counts.comments_fetched
            typer.echo(
                f"   ✓ posts={counts.posts_fetched} comments={counts.comments_fetched}"
            )
        except Exception as e:  # noqa: BLE001
            log.warning("event_backfill_failed", event_id=event["id"], error=str(e))
            typer.echo(f"   ✗ failed: {e}")

    typer.echo(
        f"\nDone. Total: posts={total_posts} comments={total_comments} "
        f"(skipped {skipped} unverified)"
    )
    typer.echo("\nNext steps:")
    typer.echo("  uv run cursed silver")
    typer.echo("  uv run cursed classify --posts-only")
    typer.echo("  uv run cursed gold")


if __name__ == "__main__":
    app()