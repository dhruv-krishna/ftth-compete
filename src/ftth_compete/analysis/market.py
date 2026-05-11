"""Market-level demographic roll-ups from tract-level ACS data."""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True)
class MarketMetrics:
    n_tracts: int
    population: int | None
    poverty_universe: int | None
    poverty_below: int | None
    poverty_rate: float | None
    median_household_income_weighted: float | None
    housing_units_total: int | None


def _safe_sum(df: pl.DataFrame, col: str) -> float | None:
    if df.is_empty() or col not in df.columns:
        return None
    val = df[col].sum()
    return None if val is None else float(val)


def aggregate(acs: pl.DataFrame) -> MarketMetrics:
    """Aggregate tract-level ACS data to a market-level summary.

    `median_household_income_weighted` is the population-weighted mean of
    tract medians — a proxy for true market median (which would require
    raw ACS PUMS data). Surface it as "weighted" so the UI doesn't claim
    more than it knows.
    """
    if acs.is_empty():
        return MarketMetrics(
            n_tracts=0,
            population=None,
            poverty_universe=None,
            poverty_below=None,
            poverty_rate=None,
            median_household_income_weighted=None,
            housing_units_total=None,
        )

    pop = _safe_sum(acs, "population_total")
    pov_u = _safe_sum(acs, "poverty_universe")
    pov_b = _safe_sum(acs, "poverty_below")
    housing = _safe_sum(acs, "housing_units_total")
    poverty_rate = (pov_b / pov_u) if (pov_u and pov_b is not None) else None

    valid = acs.filter(
        pl.col("median_household_income").is_not_null()
        & pl.col("population_total").is_not_null()
    )
    if valid.is_empty():
        weighted_mfi: float | None = None
    else:
        w = valid["population_total"]
        m = valid["median_household_income"]
        denom = float(w.sum() or 0)
        weighted_mfi = float((m * w).sum() / denom) if denom else None

    return MarketMetrics(
        n_tracts=len(acs),
        population=int(pop) if pop is not None else None,
        poverty_universe=int(pov_u) if pov_u is not None else None,
        poverty_below=int(pov_b) if pov_b is not None else None,
        poverty_rate=poverty_rate,
        median_household_income_weighted=weighted_mfi,
        housing_units_total=int(housing) if housing is not None else None,
    )
