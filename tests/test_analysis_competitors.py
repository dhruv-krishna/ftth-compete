"""Tests for ftth_compete.analysis.competitors."""

from __future__ import annotations

import polars as pl

from ftth_compete.analysis.competitors import (
    distinct_providers,
    has_fiber_by_provider,
    score,
)


def _coverage(rows: list[dict[str, object]]) -> pl.DataFrame:
    schema = {
        "tract_geoid": pl.Utf8,
        "provider_id": pl.Int64,
        "brand_name": pl.Utf8,
        "technology": pl.Int64,
        "locations_served": pl.Int64,
        "max_down": pl.Float64,
        "max_up": pl.Float64,
    }
    return pl.DataFrame(rows, schema=schema)


def test_empty_coverage() -> None:
    assert score(_coverage([]), n_tracts=4) == []
    assert score(
        _coverage(
            [
                {
                    "tract_geoid": "T1",
                    "provider_id": 1,
                    "brand_name": "X",
                    "technology": 50,
                    "locations_served": 10,
                    "max_down": 1000.0,
                    "max_up": 1000.0,
                }
            ]
        ),
        n_tracts=0,
    ) == []


def test_per_tech_split_for_multi_tech_provider() -> None:
    """A provider serving multiple techs produces one row per tech."""
    rows = [
        # Comcast/Xfinity: cable in T1+T2, fiber in T3
        {"tract_geoid": "T1", "provider_id": 1, "brand_name": "Comcast Cable Communications, LLC",
         "technology": 40, "locations_served": 100, "max_down": 1200.0, "max_up": 35.0},
        {"tract_geoid": "T2", "provider_id": 1, "brand_name": "Comcast Cable Communications, LLC",
         "technology": 40, "locations_served": 80, "max_down": 1200.0, "max_up": 35.0},
        {"tract_geoid": "T3", "provider_id": 1, "brand_name": "Comcast Cable Communications, LLC",
         "technology": 50, "locations_served": 5, "max_down": 2000.0, "max_up": 2000.0},
        # Verizon Fios: only fiber, in T1+T2
        {"tract_geoid": "T1", "provider_id": 2, "brand_name": "Verizon Fios",
         "technology": 50, "locations_served": 90, "max_down": 940.0, "max_up": 940.0},
        {"tract_geoid": "T2", "provider_id": 2, "brand_name": "Verizon Fios",
         "technology": 50, "locations_served": 75, "max_down": 940.0, "max_up": 940.0},
        # Tiny WISP, Licensed FW
        {"tract_geoid": "T4", "provider_id": 99, "brand_name": "Local WISP Co-op",
         "technology": 71, "locations_served": 30, "max_down": 100.0, "max_up": 20.0},
    ]
    summaries = score(_coverage(rows), n_tracts=4)

    by_key = {(s.canonical_name, s.technology): s for s in summaries}

    # Comcast splits into two rows
    assert ("Xfinity", "Cable") in by_key
    assert ("Xfinity", "Fiber") in by_key

    cable = by_key[("Xfinity", "Cable")]
    assert cable.tracts_served == 2  # T1, T2
    assert cable.coverage_pct == 0.5
    assert cable.locations_served == 180  # 100 + 80
    assert cable.has_fiber is False
    assert cable.tech_code == 40
    assert cable.max_advertised_down == 1200.0

    fiber = by_key[("Xfinity", "Fiber")]
    assert fiber.tracts_served == 1  # T3 only
    assert fiber.coverage_pct == 0.25
    assert fiber.locations_served == 5
    assert fiber.has_fiber is True
    assert fiber.tech_code == 50
    assert fiber.max_advertised_down == 2000.0

    # Single-tech providers give one row each
    fios = by_key[("Verizon Fios", "Fiber")]
    assert fios.tracts_served == 2
    assert fios.has_fiber is True

    wisp = by_key[("Local WISP Co-op", "Licensed FW")]
    assert wisp.category == "unknown"
    assert wisp.has_fiber is False
    assert wisp.tech_code == 71


def test_default_sort_is_fiber_first_then_coverage() -> None:
    rows = [
        {"tract_geoid": "T1", "provider_id": 1, "brand_name": "Cable Co",
         "technology": 40, "locations_served": 1, "max_down": 1.0, "max_up": 1.0},
        {"tract_geoid": "T2", "provider_id": 1, "brand_name": "Cable Co",
         "technology": 40, "locations_served": 1, "max_down": 1.0, "max_up": 1.0},
        {"tract_geoid": "T1", "provider_id": 2, "brand_name": "Verizon Fios",
         "technology": 50, "locations_served": 1, "max_down": 1.0, "max_up": 1.0},
    ]
    s = score(_coverage(rows), n_tracts=2)
    # Fiber first even though Cable Co has higher coverage
    assert s[0].technology == "Fiber"
    assert s[0].canonical_name == "Verizon Fios"
    assert s[1].technology == "Cable"


def test_distinct_providers_and_has_fiber_helpers() -> None:
    rows = [
        # Lumen has both DSL and Fiber rows
        {"tract_geoid": "T1", "provider_id": 1, "brand_name": "CenturyLink",
         "technology": 10, "locations_served": 500, "max_down": 100.0, "max_up": 20.0},
        {"tract_geoid": "T1", "provider_id": 1, "brand_name": "Quantum Fiber",
         "technology": 50, "locations_served": 50, "max_down": 3000.0, "max_up": 3000.0},
        # Allo: fiber only
        {"tract_geoid": "T1", "provider_id": 2, "brand_name": "Allo Communications LLC",
         "technology": 50, "locations_served": 100, "max_down": 2300.0, "max_up": 2300.0},
        # WISP: no fiber
        {"tract_geoid": "T1", "provider_id": 3, "brand_name": "Some WISP",
         "technology": 70, "locations_served": 10, "max_down": 50.0, "max_up": 5.0},
    ]
    s = score(_coverage(rows), n_tracts=1)

    distinct = distinct_providers(s)
    # Lumen, Allo, Some WISP — three distinct, even though Lumen has 2 rows
    assert len(distinct) == 3
    assert "Lumen / Quantum Fiber" in distinct
    assert "Allo Communications" in distinct

    fiber_map = has_fiber_by_provider(s)
    assert fiber_map["Lumen / Quantum Fiber"] is True  # has fiber row
    assert fiber_map["Allo Communications"] is True
    assert fiber_map["Some WISP"] is False
