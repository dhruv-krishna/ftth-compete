"""Census ACS 5-Year API client.

Fetches market metrics (population, poverty, MDU/SFH split, MFI, housing units)
for a list of tract GEOIDs. Uses the 2020-2024 vintage by default.

Caches per (state, county) since the API takes state+county+tract:* and
returns all tracts for that county in a single request. Responses are
considered immutable for a given vintage so are stored without TTL.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Final

import httpx
import polars as pl

from ..config import get_settings
from . import cache

log = logging.getLogger(__name__)

ACS_VINTAGE: Final = 2024  # 2020-2024 ACS5 (latest as of May 2026)
BASE_URL: Final = f"https://api.census.gov/data/{ACS_VINTAGE}/acs/acs5"
CACHE_SOURCE: Final = "census_acs"

# Variables we fetch per tract.
# E suffix = estimate. M suffix = margin of error (we skip in v1).
# Sentinel "missing" values returned as -666666666 / -888888888 / -999999999.
ACS_VARS: Final[dict[str, str]] = {
    "B01003_001E": "population_total",
    "B17001_001E": "poverty_universe",
    "B17001_002E": "poverty_below",
    "B19013_001E": "median_household_income",
    "B25001_001E": "housing_units_total",
    "B25024_001E": "units_in_structure_total",
    "B25024_002E": "units_1_detached",
    "B25024_003E": "units_1_attached",
    "B25024_004E": "units_2",
    "B25024_005E": "units_3_4",
    "B25024_006E": "units_5_9",
    "B25024_007E": "units_10_19",
    "B25024_008E": "units_20_49",
    "B25024_009E": "units_50_plus",
    "B25024_010E": "units_mobile_home",
    "B25024_011E": "units_other",
}

_NULL_SENTINELS = frozenset({"-666666666", "-888888888", "-999999999", "-555555555", "-222222222"})


@dataclass(frozen=True)
class AcsResult:
    """ACS metrics for a list of tracts.

    `frame` columns: geoid, plus all ACS_VARS values (renamed to friendly names).
    """

    vintage: int
    frame: pl.DataFrame


def _split_geoid(geoid: str) -> tuple[str, str, str]:
    """Split an 11-char tract GEOID into (state, county, tract)."""
    if len(geoid) != 11 or not geoid.isdigit():
        raise ValueError(f"Expected 11-digit tract GEOID, got {geoid!r}")
    return geoid[0:2], geoid[2:5], geoid[5:11]


def _coerce(raw: str | None) -> float | None:
    if raw is None or raw == "" or raw in _NULL_SENTINELS:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _fetch_county(state: str, county: str, api_key: str) -> list[list[str]]:
    """Fetch all tracts for a (state, county) pair from the Census API.

    Returns the raw API response: header row + data rows.
    """
    cache_key = f"{ACS_VINTAGE}:{state}:{county}:{','.join(ACS_VARS.keys())}"
    cached = cache.get(CACHE_SOURCE, cache_key)
    if cached is not None:
        return json.loads(cached)

    params = {
        "get": ",".join(ACS_VARS.keys()),
        "for": "tract:*",
        "in": f"state:{state} county:{county}",
        "key": api_key,
    }
    log.info("ACS fetch state=%s county=%s", state, county)
    r = httpx.get(BASE_URL, params=params, timeout=30.0)
    r.raise_for_status()
    data = r.json()
    cache.put(CACHE_SOURCE, cache_key, json.dumps(data).encode("utf-8"))
    return data


def fetch_market_metrics(geoids: list[str]) -> AcsResult:
    """Fetch ACS metrics for a list of tract GEOIDs.

    Groups GEOIDs by (state, county) for efficient batched API calls,
    filters response to the requested tracts, returns a polars DataFrame.
    """
    if not geoids:
        return AcsResult(vintage=ACS_VINTAGE, frame=pl.DataFrame())

    settings = get_settings()
    if not settings.census_api_key:
        raise RuntimeError("CENSUS_API_KEY not set in .env")

    # Group GEOIDs by (state, county).
    by_county: dict[tuple[str, str], set[str]] = {}
    for g in geoids:
        state, county, _tract = _split_geoid(g)
        by_county.setdefault((state, county), set()).add(g)

    rows: list[dict[str, object]] = []
    for (state, county), wanted in by_county.items():
        data = _fetch_county(state, county, settings.census_api_key)
        if not data or len(data) < 2:
            continue
        headers = data[0]
        for record in data[1:]:
            d = dict(zip(headers, record))
            tract_geoid = d["state"] + d["county"] + d["tract"]
            if tract_geoid not in wanted:
                continue
            rec: dict[str, object] = {"geoid": tract_geoid}
            for var, name in ACS_VARS.items():
                rec[name] = _coerce(d.get(var))
            rows.append(rec)

    schema: dict[str, type[pl.DataType] | pl.DataType] = {"geoid": pl.Utf8}
    for name in ACS_VARS.values():
        schema[name] = pl.Float64

    return AcsResult(vintage=ACS_VINTAGE, frame=pl.DataFrame(rows, schema=schema))
