"""Phase 7 — batch market screener.

Lean per-market KPI scorer used by the `/screener` Reflex route. Produces
a single `MarketKpis` row per (city, state) at a fraction of the cost of
the full `run_market()` pipeline: skips Ookla, Google Places, IAS, and
all momentum/trajectory backfills. Just demographics + BDC coverage +
opportunity score.

Typical use: enumerate places in a state via `tiger.places_in_state()`,
batch-call `screen_market(city, state)` for each, collect into a sortable
table.

Disk cache: a full screener run for a state takes 5-15 min cold. Results
persist under `<processed_dir>/screener/<release>_<states>.parquet` keyed
by the BDC release date + states CSV. Subsequent runs of the same scope
load in <1s. The cache invalidates whenever the BDC release changes.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from typing import Any

from ..data import fcc_bdc, tiger
from ..data.census_acs import fetch_market_metrics
from .competitors import score as competitors_score
from .housing import split as compute_housing
from .lenses import market_opportunity
from .market import aggregate as compute_market_metrics

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class MarketKpis:
    """Compact per-market summary row for the screener results table.

    Designed to be safely serializable to dict/CSV with no nested objects.
    """

    city: str
    state: str
    market_id: str        # "city|state" key for routing back to /v2
    n_tracts: int
    population: int
    median_hh_income: int
    poverty_rate: float
    housing_units: int
    mdu_share: float
    sfh_share: float
    n_providers: int           # distinct canonical providers
    n_fiber_providers: int     # distinct providers with at least one fiber row
    fiber_avail_pct: float     # share of BSL locations with fiber from any provider
    cable_avail_pct: float     # same for cable
    opportunity_score: float   # 0-1 from lenses.market_opportunity
    opportunity_headline: str
    # Competitive snapshot — up to 8 providers (fiber-first, by locations
    # served desc). Each entry: {name, tech_label, locations, has_fiber}.
    # Surfaced inline in the screener results so the user sees who's in
    # the market without drilling into /v2.
    top_providers: list[dict[str, Any]] = field(default_factory=list)
    error: str = ""            # populated when the market failed to resolve


def screen_market(
    city: str,
    state: str,
) -> MarketKpis:
    """Run the screener-lean pipeline for one market.

    Returns a `MarketKpis` row even on partial failure — an exception
    populates `error` rather than propagating. Callers running this in
    a worker loop want every job to terminate cleanly.

    What this skips (vs `pipeline.run_market`):
      - Ookla measured speeds  (~5-15s saved)
      - Google Places ratings  (network + quota)
      - FCC IAS subscription anchor + historical trajectory (~3-15s)
      - Velocity + trajectory BDC fetches (~5-20 min saved cold)
      - Penetration estimation (heuristic only, no calibration)

    What it keeps: TIGER place→tracts resolution, ACS demographics,
    BDC coverage matrix, BSL availability, opportunity score.
    """
    market_id = f"{city.strip()}|{state.strip().upper()}"
    try:
        resolution = tiger.city_to_tracts(city, state)
    except Exception as exc:  # noqa: BLE001
        return _empty_kpis(city, state, market_id, f"city_to_tracts: {exc}")
    geoids = list(resolution.geoids)
    if not geoids:
        return _empty_kpis(city, state, market_id, "no tracts resolved")

    # Demographics
    try:
        acs = fetch_market_metrics(geoids)
    except Exception as exc:  # noqa: BLE001
        log.warning("ACS fetch failed for %s, %s: %s", city, state, exc)
        return _empty_kpis(city, state, market_id, f"acs: {exc}")
    metrics = compute_market_metrics(acs.frame)
    housing = compute_housing(acs.frame)

    # BDC coverage
    n_providers = 0
    n_fiber_providers = 0
    fiber_avail_pct = 0.0
    cable_avail_pct = 0.0
    top_providers: list[dict[str, Any]] = []
    try:
        coverage = fcc_bdc.coverage_matrix(geoids)
        if not coverage.is_empty():
            providers_block = competitors_score(coverage, n_tracts=len(geoids))
            n_providers = len({p.canonical_name for p in providers_block})
            n_fiber_providers = len({
                p.canonical_name for p in providers_block if p.has_fiber
            })
            # Top providers list — fiber-first, then by locations desc.
            # Capped at 8 so the screener UI stays compact.
            ranked = sorted(
                providers_block,
                key=lambda p: (
                    0 if p.has_fiber else 1,
                    -int(p.locations_served or 0),
                    p.canonical_name.lower(),
                ),
            )
            for p in ranked[:8]:
                top_providers.append({
                    "name": p.canonical_name,
                    "tech_label": p.technology,
                    "locations": int(p.locations_served or 0),
                    "has_fiber": bool(p.has_fiber),
                    "coverage_pct": float(p.coverage_pct or 0.0),
                })
            avail = fcc_bdc.location_availability(geoids)
            if not avail.is_empty():
                avail_rows = avail.to_dicts()
                total_locs = sum(int(r.get("total_locations") or 0) for r in avail_rows)
                fiber_locs = sum(int(r.get("fiber_locations") or 0) for r in avail_rows)
                cable_locs = sum(int(r.get("cable_locations") or 0) for r in avail_rows)
                if total_locs > 0:
                    fiber_avail_pct = fiber_locs / total_locs
                    cable_avail_pct = cable_locs / total_locs
            opp = market_opportunity(
                providers_block, rating_lookup=None,
                mdu_share=housing.mdu_share,
            )
            opp_score = float(opp.get("score") or 0.0)
            opp_headline = str(opp.get("headline") or "")
        else:
            opp_score, opp_headline = 0.0, "No BDC coverage rows"
    except Exception as exc:  # noqa: BLE001
        log.warning("BDC fetch failed for %s, %s: %s", city, state, exc)
        return _empty_kpis(city, state, market_id, f"bdc: {exc}")

    return MarketKpis(
        city=city,
        state=state.upper(),
        market_id=market_id,
        n_tracts=len(geoids),
        population=int(metrics.population or 0),
        median_hh_income=int(metrics.median_household_income_weighted or 0),
        poverty_rate=float(metrics.poverty_rate or 0.0),
        housing_units=int(housing.total or 0),
        mdu_share=float(housing.mdu_share or 0.0),
        sfh_share=float(housing.sfh_share or 0.0),
        n_providers=n_providers,
        n_fiber_providers=n_fiber_providers,
        fiber_avail_pct=fiber_avail_pct,
        cable_avail_pct=cable_avail_pct,
        opportunity_score=opp_score,
        opportunity_headline=opp_headline,
        top_providers=top_providers,
    )


def _empty_kpis(city: str, state: str, market_id: str, error: str) -> MarketKpis:
    return MarketKpis(
        city=city, state=state.upper(), market_id=market_id,
        n_tracts=0, population=0, median_hh_income=0, poverty_rate=0.0,
        housing_units=0, mdu_share=0.0, sfh_share=0.0,
        n_providers=0, n_fiber_providers=0,
        fiber_avail_pct=0.0, cable_avail_pct=0.0,
        opportunity_score=0.0, opportunity_headline="",
        error=error,
    )


# ---------------------------------------------------------------------------
# Disk cache for screener runs.

def _screener_cache_dir():
    from ..config import get_settings
    d = get_settings().processed_dir / "screener"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _cache_key(states_csv: str, release: str) -> str:
    """Stable filename component from a comma-separated states list."""
    parts = sorted(s.strip().upper() for s in states_csv.split(",") if s.strip())
    return f"{release}__{'-'.join(parts) or 'empty'}"


def load_cached_run(
    states_csv: str, release: str,
) -> tuple[list[MarketKpis], float] | None:
    """Load a previously-saved screener run from disk.

    Returns `(rows, built_at_epoch)` or None if no cache exists. The
    timestamp lets the UI render "Loaded from cache, built N hours ago."

    The `top_providers` column is JSON-encoded for storage (polars'
    nested-struct-list roundtrip through parquet is finicky); we decode
    on the way out.
    """
    import json

    import polars as pl
    path = _screener_cache_dir() / f"{_cache_key(states_csv, release)}.parquet"
    if not path.exists():
        return None
    try:
        df = pl.read_parquet(path)
    except Exception:  # noqa: BLE001
        return None
    rows: list[MarketKpis] = []
    for r in df.to_dicts():
        raw_tp = r.get("top_providers")
        if isinstance(raw_tp, str):
            try:
                r["top_providers"] = json.loads(raw_tp)
            except (json.JSONDecodeError, TypeError):
                r["top_providers"] = []
        try:
            rows.append(MarketKpis(**r))
        except (TypeError, ValueError):
            continue
    return rows, path.stat().st_mtime


def save_run(states_csv: str, release: str, rows: list[MarketKpis]) -> None:
    """Persist a completed run to disk. Best-effort; failures are silent.

    `top_providers` is serialized to a JSON string column for storage.
    """
    import json
    import logging

    import polars as pl
    if not rows:
        return
    path = _screener_cache_dir() / f"{_cache_key(states_csv, release)}.parquet"
    try:
        flat_rows: list[dict[str, Any]] = []
        for r in rows:
            d = asdict(r)
            d["top_providers"] = json.dumps(d.get("top_providers") or [])
            flat_rows.append(d)
        df = pl.DataFrame(flat_rows)
        df.write_parquet(path)
    except Exception as exc:  # noqa: BLE001
        logging.getLogger(__name__).warning(
            "Failed writing screener cache %s: %s", path, exc,
        )


def kpis_to_csv(rows: list[MarketKpis]) -> str:
    """Render screener results as a CSV string.

    Columns mirror the dataclass field order for stability across releases.
    `top_providers` collapses to a single semicolon-separated string column
    like "Lumen (Fiber, 8500 locs); Xfinity (Cable, 7200 locs)" so a flat
    CSV can include competitive context.
    """
    import csv
    import io
    if not rows:
        return ""
    base_fields = [f for f in asdict(rows[0]).keys() if f != "top_providers"]
    fieldnames = [*base_fields, "top_providers"]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    for r in rows:
        d = asdict(r)
        provs = d.pop("top_providers", []) or []
        d["top_providers"] = "; ".join(
            f"{p.get('name')} ({p.get('tech_label')}, {p.get('locations'):,} locs)"
            for p in provs
            if p.get("name")
        )
        writer.writerow(d)
    return buf.getvalue()
