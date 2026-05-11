"""Tests for ftth_compete.format."""

from __future__ import annotations

import math

import pytest

from ftth_compete.format import (
    DASH,
    fmt_currency,
    fmt_int,
    fmt_pct,
    fmt_speed,
    fmt_speed_pair,
)


@pytest.mark.parametrize("n,expected", [
    (22324, "22,324"),
    (1, "1"),
    (0, "0"),
    (1500.7, "1,501"),
    (1_000_000, "1,000,000"),
    (None, DASH),
    (float("nan"), DASH),
])
def test_fmt_int(n: object, expected: str) -> None:
    assert fmt_int(n) == expected  # type: ignore[arg-type]


@pytest.mark.parametrize("n,expected", [
    (50000, "$50,000"),
    (74410.69, "$74,411"),
    (0, "$0"),
    (None, DASH),
])
def test_fmt_currency(n: object, expected: str) -> None:
    assert fmt_currency(n) == expected  # type: ignore[arg-type]


@pytest.mark.parametrize("n,expected", [
    (0.135, "13.5%"),
    (0.0, "0.0%"),
    (1.0, "100.0%"),
    (0.001, "0.1%"),
    (None, DASH),
])
def test_fmt_pct(n: object, expected: str) -> None:
    assert fmt_pct(n) == expected  # type: ignore[arg-type]


def test_fmt_pct_decimals() -> None:
    assert fmt_pct(0.135, decimals=2) == "13.50%"
    # Python's format spec uses banker's rounding: 13.5 -> 14 (round half to even)
    assert fmt_pct(0.135, decimals=0) == "14%"
    assert fmt_pct(0.124, decimals=0) == "12%"


@pytest.mark.parametrize("mbps,expected", [
    (500, "500 Mbps"),
    (100, "100 Mbps"),
    (1000, "1.0 Gbps"),
    (2300, "2.3 Gbps"),
    (940, "940 Mbps"),
    (None, DASH),
])
def test_fmt_speed(mbps: object, expected: str) -> None:
    assert fmt_speed(mbps) == expected  # type: ignore[arg-type]


def test_fmt_speed_pair() -> None:
    assert fmt_speed_pair(2300, 2300) == "2.3 Gbps / 2.3 Gbps"
    assert fmt_speed_pair(940, 35) == "940 Mbps / 35 Mbps"
    assert fmt_speed_pair(None, 100) == DASH
    assert fmt_speed_pair(100, None) == DASH


def test_nan_handling() -> None:
    assert fmt_int(math.nan) == DASH
    assert fmt_currency(math.nan) == DASH
    assert fmt_pct(math.nan) == DASH
    assert fmt_speed(math.nan) == DASH
