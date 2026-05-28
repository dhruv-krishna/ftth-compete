"""Phase 8 — provider-centric view.

Flips the perspective from market-first to provider-first. Pick a canonical
provider (Allo / Comcast / Verizon Fios) and see:

- Portfolio footprint: every state + tract they serve across cached BDC
  parquets, with tech breakdown
- Trajectory: tracts served per BDC release (uses all cached releases)
- Head-to-head overlap: top-N other providers sharing the most tracts

All data comes from the on-disk BDC parquet cache. No new network calls.
If a provider hasn't been "loaded" via a market lookup yet (i.e. their
state's BDC parquet isn't cached), they won't appear in the directory.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from ..config import get_settings
from ..data.providers import canonical_name
from ..data.tiger import STATE_FIPS

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProviderPortfolioStat:
    """Compact summary row for the provider directory."""

    canonical: str
    n_states: int
    n_tracts: int
    n_fiber_tracts: int
    has_fiber: bool
    total_locations: int
    fiber_locations: int
    n_brands: int               # how many raw brand_names canonicalize here
    latest_release: str


@dataclass(frozen=True)
class StateBreakdown:
    state: str                  # 2-letter abbr
    state_fips: str
    n_tracts: int
    n_fiber_tracts: int
    total_locations: int
    fiber_locations: int


@dataclass(frozen=True)
class HeadToHeadEntry:
    canonical: str
    shared_tracts: int
    pct_overlap: float          # shared / our_n_tracts


@dataclass(frozen=True)
class ProviderTrajectoryPoint:
    release: str
    n_tracts: int
    n_fiber_tracts: int
    total_locations: int


@dataclass(frozen=True)
class ProviderDetail:
    """Full provider view returned by `provider_detail(canonical)`."""

    canonical: str
    summary: ProviderPortfolioStat
    states: list[StateBreakdown]
    head_to_head: list[HeadToHeadEntry] = field(default_factory=list)
    trajectory: list[ProviderTrajectoryPoint] = field(default_factory=list)
    raw_brand_names: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Cache discovery

def _bdc_processed_root() -> Path:
    return get_settings().processed_dir / "bdc"


def list_cached_releases() -> list[str]:
    """Discover BDC releases (`YYYY-MM-DD` dirs) the user has on disk.

    Sorted newest-first. Empty if no markets have been looked up yet.
    """
    root = _bdc_processed_root()
    if not root.exists():
        return []
    releases: list[str] = []
    for p in root.iterdir():
        if not p.is_dir():
            continue
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", p.name):
            releases.append(p.name)
    releases.sort(reverse=True)
    return releases


def _parquets_for_release(release: str) -> list[Path]:
    root = _bdc_processed_root() / release
    if not root.exists():
        return []
    return sorted(root.glob("state=*.parquet"))


_FIPS_TO_STATE: dict[str, str] = {v: k for k, v in STATE_FIPS.items()}

# Two-tier aggregation cache:
#   1. Process-level dict (fastest, free) keyed by (parquet_dir, max_mtime)
#   2. On-disk parquet under `<processed_dir>/provider_view/<release>.parquet`
#      that survives Reflex restarts. The disk path stores both the frame
#      AND the mtime stamp it was built from; on load we check that the
#      stored mtime is still >= the current state parquets, otherwise
#      we rebuild.
_AGG_CACHE: dict[tuple[str, float], pl.DataFrame] = {}


def _provider_view_cache_dir() -> Path:
    d = get_settings().processed_dir / "provider_view"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _disk_cache_path(parquet_dir: str) -> Path:
    """Cache file lives alongside `<processed_dir>/provider_view/<release>.parquet`.

    Release date is the trailing dir name of the BDC parquet directory.
    """
    release = Path(parquet_dir).name  # e.g. "2025-06-30"
    return _provider_view_cache_dir() / f"{release}.parquet"


def _disk_meta_path(parquet_dir: str) -> Path:
    return _provider_view_cache_dir() / f"{Path(parquet_dir).name}.meta"


def _load_disk_cache(parquet_dir: str, expected_mtime: float) -> pl.DataFrame | None:
    """Load aggregated frame from disk if its stored mtime stamp still
    matches the source parquets. Returns None on any mismatch / missing /
    read error so the caller falls through to a fresh aggregation.
    """
    cache_path = _disk_cache_path(parquet_dir)
    meta_path = _disk_meta_path(parquet_dir)
    if not cache_path.exists() or not meta_path.exists():
        return None
    try:
        stored_mtime = float(meta_path.read_text().strip())
    except (OSError, ValueError):
        return None
    if stored_mtime < expected_mtime:
        log.info(
            "Provider-view disk cache stale for %s (stored=%.0f < current=%.0f); rebuilding",
            parquet_dir, stored_mtime, expected_mtime,
        )
        return None
    try:
        df = pl.read_parquet(cache_path)
    except Exception as exc:  # noqa: BLE001
        log.warning("Failed reading provider-view cache %s: %s", cache_path, exc)
        return None
    log.info("Loaded provider-view aggregation from disk: %s", cache_path)
    return df


def _save_disk_cache(parquet_dir: str, mtime: float, frame: pl.DataFrame) -> None:
    """Persist aggregated frame + mtime stamp. Failures are logged, not raised."""
    cache_path = _disk_cache_path(parquet_dir)
    meta_path = _disk_meta_path(parquet_dir)
    try:
        frame.write_parquet(cache_path)
        meta_path.write_text(f"{mtime:.6f}")
        log.info("Persisted provider-view aggregation to %s", cache_path)
    except Exception as exc:  # noqa: BLE001
        log.warning("Failed writing provider-view cache %s: %s", cache_path, exc)


def _aggregate_parquets(parquets: list[Path]) -> pl.DataFrame:
    """Aggregate BDC location-level parquets to (state, brand, tract, tech, locs).

    BDC parquets are per-location rows — `locs` is computed here as the
    `COUNT(DISTINCT location_id)` for each group. Adds the `canonical`
    column from the provider registry via a small dedup-then-join so we
    don't pay the per-row Python call across hundreds of thousands of rows.
    """
    if not parquets:
        return pl.DataFrame()

    # Cache key: parent dir + max child mtime. Invalidates cleanly when
    # the user re-ingests a state (new parquet → new mtime).
    parent = str(parquets[0].parent)
    try:
        mtime = max(p.stat().st_mtime for p in parquets)
    except OSError:
        mtime = 0.0
    cache_key = (parent, mtime)
    if cache_key in _AGG_CACHE:
        return _AGG_CACHE[cache_key]
    # Tier 2: disk cache (survives Reflex restart).
    disk = _load_disk_cache(parent, mtime)
    if disk is not None:
        _AGG_CACHE[cache_key] = disk
        return disk

    glob = ", ".join(f"'{p.as_posix()}'" for p in parquets)
    sql = f"""
    SELECT
        state_usps,
        brand_name,
        tract_geoid,
        technology,
        COUNT(DISTINCT location_id) AS locs
    FROM read_parquet([{glob}])
    GROUP BY state_usps, brand_name, tract_geoid, technology
    """
    con = duckdb.connect(":memory:")
    try:
        raw = con.execute(sql).pl()
    finally:
        con.close()
    if raw.is_empty():
        _AGG_CACHE[cache_key] = raw
        return raw

    # Build a tiny canonical-name lookup table from the unique (brand, tech)
    # pairs in this frame — typically 50-300 entries vs hundreds of thousands
    # of rows in `raw`. Polars joins the lookup back in vectorized fashion.
    pairs = raw.select(["brand_name", "technology"]).unique()
    canon_rows: list[dict[str, Any]] = []
    for row in pairs.iter_rows(named=True):
        canon_rows.append({
            "brand_name": row["brand_name"],
            "technology": row["technology"],
            "canonical": canonical_name(
                row["brand_name"], None,
                int(row["technology"] or 0),
            ),
        })
    lookup = pl.DataFrame(canon_rows)
    aggregated = raw.join(lookup, on=["brand_name", "technology"], how="left")
    _AGG_CACHE[cache_key] = aggregated
    _save_disk_cache(parent, mtime, aggregated)
    return aggregated


# ---------------------------------------------------------------------------
# Aggregation

def provider_directory(
    release: str | None = None,
) -> list[ProviderPortfolioStat]:
    """Build the cross-market provider summary table.

    Reads all cached state parquets for `release` (defaults to newest
    cached release) and aggregates per canonical provider.

    Returns sorted by n_tracts desc — dominant providers first.
    """
    releases = list_cached_releases()
    if not releases:
        return []
    rel = release or releases[0]
    parquets = _parquets_for_release(rel)
    if not parquets:
        return []

    raw = _aggregate_parquets(parquets)
    if raw.is_empty():
        return []

    # Aggregate per canonical.
    out: dict[str, dict[str, Any]] = {}
    for row in raw.iter_rows(named=True):
        c = row["canonical"]
        rec = out.setdefault(c, {
            "states": set(), "tracts": set(), "fiber_tracts": set(),
            "brands": set(), "total_locs": 0, "fiber_locs": 0,
        })
        rec["states"].add(str(row["state_usps"]))
        rec["tracts"].add(str(row["tract_geoid"]))
        rec["brands"].add(str(row["brand_name"] or ""))
        rec["total_locs"] += int(row["locs"] or 0)
        if int(row["technology"] or 0) == 50:
            rec["fiber_tracts"].add(str(row["tract_geoid"]))
            rec["fiber_locs"] += int(row["locs"] or 0)

    result: list[ProviderPortfolioStat] = []
    for canon, rec in out.items():
        result.append(ProviderPortfolioStat(
            canonical=canon,
            n_states=len(rec["states"]),
            n_tracts=len(rec["tracts"]),
            n_fiber_tracts=len(rec["fiber_tracts"]),
            has_fiber=len(rec["fiber_tracts"]) > 0,
            total_locations=int(rec["total_locs"]),
            fiber_locations=int(rec["fiber_locs"]),
            n_brands=len([b for b in rec["brands"] if b]),
            latest_release=rel,
        ))
    result.sort(key=lambda s: (-s.n_tracts, s.canonical.lower()))
    return result


def provider_detail(
    canonical: str,
    *,
    release: str | None = None,
    head_to_head_top_n: int = 10,
) -> ProviderDetail | None:
    """Full provider portfolio view.

    Returns None if no rows in the cached release map to this canonical.
    """
    releases = list_cached_releases()
    if not releases:
        return None
    rel = release or releases[0]
    parquets = _parquets_for_release(rel)
    if not parquets:
        return None

    raw = _aggregate_parquets(parquets)
    if raw.is_empty():
        return None

    # Our rows (the target canonical) + competitor rows.
    ours = raw.filter(pl.col("canonical") == canonical)
    if ours.is_empty():
        return None
    competitors = raw.filter(pl.col("canonical") != canonical)

    # Summary
    our_tracts: set[str] = set(ours["tract_geoid"].to_list())
    our_fiber_tracts: set[str] = set(
        ours.filter(pl.col("technology") == 50)["tract_geoid"].to_list()
    )
    our_states: set[str] = set(ours["state_usps"].to_list())
    our_brands: list[str] = sorted({
        str(b) for b in ours["brand_name"].to_list() if b
    })
    total_locs = int(ours["locs"].sum())
    fiber_locs = int(
        ours.filter(pl.col("technology") == 50)["locs"].sum()
    )
    summary = ProviderPortfolioStat(
        canonical=canonical,
        n_states=len(our_states),
        n_tracts=len(our_tracts),
        n_fiber_tracts=len(our_fiber_tracts),
        has_fiber=bool(our_fiber_tracts),
        total_locations=total_locs,
        fiber_locations=fiber_locs,
        n_brands=len(our_brands),
        latest_release=rel,
    )

    # Per-state breakdown
    states: list[StateBreakdown] = []
    for st_abbr in sorted(our_states):
        sub = ours.filter(pl.col("state_usps") == st_abbr)
        st_tracts = set(sub["tract_geoid"].to_list())
        st_fiber = set(
            sub.filter(pl.col("technology") == 50)["tract_geoid"].to_list()
        )
        states.append(StateBreakdown(
            state=str(st_abbr),
            state_fips=STATE_FIPS.get(str(st_abbr), ""),
            n_tracts=len(st_tracts),
            n_fiber_tracts=len(st_fiber),
            total_locations=int(sub["locs"].sum()),
            fiber_locations=int(
                sub.filter(pl.col("technology") == 50)["locs"].sum()
            ),
        ))
    states.sort(key=lambda s: -s.n_tracts)

    # Head-to-head: which canonicals share the most tracts with us?
    h2h: list[HeadToHeadEntry] = []
    if competitors.height > 0 and our_tracts:
        comp_shared = (
            competitors
            .filter(pl.col("tract_geoid").is_in(list(our_tracts)))
            .group_by("canonical")
            .agg(pl.col("tract_geoid").n_unique().alias("shared"))
            .sort("shared", descending=True)
            .head(head_to_head_top_n)
        )
        for row in comp_shared.iter_rows(named=True):
            shared = int(row["shared"])
            h2h.append(HeadToHeadEntry(
                canonical=str(row["canonical"]),
                shared_tracts=shared,
                pct_overlap=shared / len(our_tracts) if our_tracts else 0.0,
            ))

    # Trajectory across cached releases. Two-tier check: prefer in-process
    # cache, fall back to disk cache. Only skip when neither has the
    # aggregation — re-aggregating an uncached release costs ~100s of CPU,
    # which we won't pay just to render a detail page.
    trajectory: list[ProviderTrajectoryPoint] = []
    for rel_iter in releases:
        traj_pq = _parquets_for_release(rel_iter)
        if not traj_pq:
            continue
        try:
            mtime = max(p.stat().st_mtime for p in traj_pq)
        except OSError:
            mtime = 0.0
        parent = str(traj_pq[0].parent)
        in_process = (parent, mtime) in _AGG_CACHE
        # Disk cache check is just a file-exists + mtime stamp read; cheap.
        on_disk = _disk_cache_path(parent).exists() and _disk_meta_path(parent).exists()
        if not in_process and not on_disk:
            continue
        try:
            point = _trajectory_point(canonical, rel_iter, traj_pq)
        except Exception as exc:  # noqa: BLE001
            log.warning("trajectory point %s failed: %s", rel_iter, exc)
            continue
        if point is not None:
            trajectory.append(point)
    trajectory.sort(key=lambda p: p.release)

    return ProviderDetail(
        canonical=canonical,
        summary=summary,
        states=states,
        head_to_head=h2h,
        trajectory=trajectory,
        raw_brand_names=our_brands,
    )


def _trajectory_point(
    canonical: str, release: str, parquets: list[Path],
) -> ProviderTrajectoryPoint | None:
    """Aggregate one release into a single trajectory point for `canonical`."""
    raw = _aggregate_parquets(parquets)
    if raw.is_empty():
        return None
    ours = raw.filter(pl.col("canonical") == canonical)
    if ours.is_empty():
        return None
    return ProviderTrajectoryPoint(
        release=release,
        n_tracts=int(ours["tract_geoid"].n_unique()),
        n_fiber_tracts=int(
            ours.filter(pl.col("technology") == 50)["tract_geoid"].n_unique()
        ),
        total_locations=int(ours["locs"].sum()),
    )


# ---------------------------------------------------------------------------
# URL slug helpers (for /provider/<slug> route)

def slugify(canonical: str) -> str:
    """Provider canonical → URL-safe slug. Reversible via `unslugify`."""
    s = canonical.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def find_by_slug(slug: str) -> str | None:
    """Find a canonical provider name matching the given slug.

    Slug → canonical reverse lookup is a directory scan (no canonical
    name index). Cheap because the directory caps at a few hundred.
    """
    target = slug.lower()
    for stat in provider_directory():
        if slugify(stat.canonical) == target:
            return stat.canonical
    return None
