"""Click-based CLI entry point.

Registered as the `ftth-compete` console script in pyproject.toml.
"""

from __future__ import annotations

import json
import logging

import click

from .config import get_settings
from .data import fcc_bdc
from .pipeline import run_market


def _parse_market(text: str) -> tuple[str, str]:
    """Parse 'City, ST' (or 'City,ST'). Returns (city, state).

    Raises click.BadParameter on bad input.
    """
    if "," not in text:
        raise click.BadParameter("expected 'City, ST' (comma required)")
    city, state = (s.strip() for s in text.split(",", 1))
    if not city or len(state) != 2 or not state.isalpha():
        raise click.BadParameter("expected 'City, ST' with a 2-letter state")
    return city, state.upper()


@click.group()
@click.version_option()
@click.option("-v", "--verbose", is_flag=True, help="Show INFO log messages.")
def cli(verbose: bool) -> None:
    """ftth-compete - FTTH market competitive intelligence."""
    level = logging.INFO if verbose else logging.WARNING
    logging.basicConfig(level=level, format="%(levelname)s %(name)s: %(message)s")


@cli.command()
@click.argument("market_str", metavar="MARKET")
@click.option(
    "--include-boundary",
    is_flag=True,
    help="Include tracts that intersect the city but whose centroid is outside.",
)
@click.option(
    "--no-providers",
    is_flag=True,
    help="Skip FCC BDC provider lookup (faster; demographics-only).",
)
@click.option(
    "--no-speeds",
    is_flag=True,
    help="Skip Ookla measured-speed query.",
)
@click.option(
    "--no-ratings",
    is_flag=True,
    help="Skip Google Places rating lookups.",
)
@click.option(
    "--no-ias",
    is_flag=True,
    help="Skip FCC IAS subscription-density anchor (estimates stay heuristic).",
)
@click.option(
    "--include-velocity",
    is_flag=True,
    help="Also fetch a prior BDC release for per-provider 12-month coverage deltas.",
)
@click.option(
    "--include-trajectory",
    is_flag=True,
    help="Fetch 4 BDC releases (~6mo apart) for per-provider coverage trajectory sparklines. Slow (~20 min cold first state).",
)
def market(
    market_str: str,
    include_boundary: bool,
    no_providers: bool,
    no_speeds: bool,
    no_ratings: bool,
    no_ias: bool,
    include_velocity: bool,
    include_trajectory: bool,
) -> None:
    """Look up a market and emit a JSON tear-sheet to stdout.

    MARKET is "City, ST" - e.g. "Evans, CO".
    """
    city, state = _parse_market(market_str)
    sheet = run_market(
        city,
        state,
        include_boundary=include_boundary,
        no_providers=no_providers,
        no_speeds=no_speeds,
        no_ratings=no_ratings,
        no_ias=no_ias,
        include_velocity=include_velocity,
        include_trajectory=include_trajectory,
    )
    click.echo(json.dumps(sheet.to_dict(), indent=2, default=str))


@cli.command()
def info() -> None:
    """Show resolved config and dataset paths."""
    s = get_settings()
    click.echo(f"data_dir          : {s.data_dir}")
    click.echo(f"raw_dir           : {s.raw_dir}")
    click.echo(f"processed_dir     : {s.processed_dir}")
    click.echo(f"cache_db_path     : {s.cache_db_path}")
    click.echo(f"census_api_key    : {'set' if s.census_api_key else 'NOT SET'}")
    click.echo(f"google_places_key : {'set' if s.google_places_key else 'NOT SET'}")
    click.echo(f"fcc_username      : {'set' if s.fcc_username else 'NOT SET'}")
    click.echo(f"fcc_api_token     : {'set' if s.fcc_api_token else 'NOT SET'}")


@cli.command(name="bdc-info")
def bdc_info() -> None:
    """Probe the FCC BDC API: list as-of dates and pick latest release.

    Use this to verify FCC credentials are working before running a full
    market lookup.
    """
    try:
        dates = fcc_bdc.list_as_of_dates()
        click.echo(f"FCC BDC API: returned {len(dates)} as-of date(s).")
        for d in dates[:5]:
            click.echo(f"  {d}")
        latest = fcc_bdc.latest_release()
        click.echo(f"\nLatest release resolved to: {latest}")
    except RuntimeError as exc:
        raise click.ClickException(str(exc)) from exc


if __name__ == "__main__":
    cli()
