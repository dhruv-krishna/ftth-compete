"""Tests for ftth_compete.analysis.market."""

from __future__ import annotations

import polars as pl

from ftth_compete.analysis.market import aggregate

ACS_COLS = [
    "geoid", "population_total", "poverty_universe", "poverty_below",
    "median_household_income", "housing_units_total",
]


def _frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    """Build an ACS-like frame, padding missing columns with None."""
    schema = {
        "geoid": pl.Utf8,
        "population_total": pl.Float64,
        "poverty_universe": pl.Float64,
        "poverty_below": pl.Float64,
        "median_household_income": pl.Float64,
        "housing_units_total": pl.Float64,
    }
    return pl.DataFrame(rows, schema=schema)


def test_empty_frame() -> None:
    m = aggregate(_frame([]))
    assert m.n_tracts == 0
    assert m.population is None
    assert m.poverty_rate is None


def test_single_tract() -> None:
    m = aggregate(_frame([{
        "geoid": "08123001004",
        "population_total": 1000.0,
        "poverty_universe": 1000.0,
        "poverty_below": 100.0,
        "median_household_income": 50000.0,
        "housing_units_total": 400.0,
    }]))
    assert m.n_tracts == 1
    assert m.population == 1000
    assert m.poverty_rate == 0.10
    assert m.median_household_income_weighted == 50000.0
    assert m.housing_units_total == 400


def test_pop_weighted_median() -> None:
    """Higher-population tract dominates the weighted median."""
    m = aggregate(_frame([
        {"geoid": "1", "population_total": 9000.0, "poverty_universe": None,
         "poverty_below": None, "median_household_income": 30000.0, "housing_units_total": None},
        {"geoid": "2", "population_total": 1000.0, "poverty_universe": None,
         "poverty_below": None, "median_household_income": 100000.0, "housing_units_total": None},
    ]))
    # weighted = (9000*30k + 1000*100k) / 10000 = (270M + 100M) / 10k = 37000
    assert m.median_household_income_weighted == 37000.0


def test_handles_null_medians() -> None:
    m = aggregate(_frame([
        {"geoid": "1", "population_total": 1000.0, "poverty_universe": None,
         "poverty_below": None, "median_household_income": None, "housing_units_total": None},
        {"geoid": "2", "population_total": 2000.0, "poverty_universe": None,
         "poverty_below": None, "median_household_income": 50000.0, "housing_units_total": None},
    ]))
    assert m.median_household_income_weighted == 50000.0  # only tract 2 contributes


def test_poverty_rate_with_zero_universe() -> None:
    m = aggregate(_frame([{
        "geoid": "1", "population_total": 0.0, "poverty_universe": 0.0,
        "poverty_below": 0.0, "median_household_income": None, "housing_units_total": None,
    }]))
    assert m.poverty_rate is None  # avoid div-by-zero
