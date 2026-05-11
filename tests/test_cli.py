"""Tests for ftth_compete.cli."""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from ftth_compete.cli import _parse_market, cli


def test_cli_help() -> None:
    result = CliRunner().invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "ftth-compete" in result.output


def test_info_command(isolated_data_dir: object) -> None:
    result = CliRunner().invoke(cli, ["info"])
    assert result.exit_code == 0
    assert "data_dir" in result.output


@pytest.mark.parametrize(
    "text,expected",
    [
        ("Evans, CO", ("Evans", "CO")),
        ("Evans,CO", ("Evans", "CO")),
        ("New York, NY", ("New York", "NY")),
        ("  Plano  ,  tx  ", ("Plano", "TX")),
    ],
)
def test_parse_market_valid(text: str, expected: tuple[str, str]) -> None:
    assert _parse_market(text) == expected


@pytest.mark.parametrize(
    "text",
    [
        "Evans CO",        # missing comma
        "Evans, ",         # missing state
        ", CO",            # missing city
        "Evans, COLO",     # state too long
        "Evans, 12",       # state not alpha
    ],
)
def test_parse_market_invalid(text: str) -> None:
    with pytest.raises(Exception):
        _parse_market(text)
