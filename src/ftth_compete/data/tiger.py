"""TIGER/Line geographies.

Downloads census PLACE (city) and TRACT shapefiles per state from
www2.census.gov, caches them locally, and provides a city-name to
tract-GEOID resolver via point-in-polygon spatial join (tract centroid
inside city polygon).
"""

from __future__ import annotations

import io
import logging
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import geopandas as gpd
import httpx

from ..config import get_settings

log = logging.getLogger(__name__)

TIGER_YEAR: Final = 2024
TIGER_BASE: Final = f"https://www2.census.gov/geo/tiger/TIGER{TIGER_YEAR}"

# State / territory abbreviation -> 2-digit FIPS code.
STATE_FIPS: Final[dict[str, str]] = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
    "CO": "08", "CT": "09", "DE": "10", "DC": "11", "FL": "12",
    "GA": "13", "HI": "15", "ID": "16", "IL": "17", "IN": "18",
    "IA": "19", "KS": "20", "KY": "21", "LA": "22", "ME": "23",
    "MD": "24", "MA": "25", "MI": "26", "MN": "27", "MS": "28",
    "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38",
    "OH": "39", "OK": "40", "OR": "41", "PA": "42", "RI": "44",
    "SC": "45", "SD": "46", "TN": "47", "TX": "48", "UT": "49",
    "VT": "50", "VA": "51", "WA": "53", "WV": "54", "WI": "55",
    "WY": "56", "PR": "72",
}

# Aliases for places where the user-typed name doesn't match a TIGER PLACE
# but the intent is clear (NYC boroughs are the classic example — Brooklyn
# isn't a PLACE, it's the popular name for Kings County). Resolved via
# tract GEOID prefix filter on the state's tract shapefile (no extra download).
#
# Format: (city_lower, STATE) -> (display_name, county_fips_3, place_geoid_synthetic)
# place_geoid_synthetic uses 5 digits (state + county) so downstream code
# that expects a 5-7 char place GEOID can still print something meaningful.
# Cross-state metro aliases. These are markets where the popular name spans
# multiple TIGER PLACE entries in different states (e.g., "Kansas City" exists
# in both MO and KS as distinct incorporated places, and the metro is
# economically a single market). Resolved by running `_resolve_single_place`
# on each component and merging the tract lists.
#
# Format: (label_lower, primary_state) -> list[(place_name, STATE), ...]
# The primary_state is what the user types alongside the label; for KC most
# users would type "Kansas City, MO" expecting the metro.
_METRO_ALIASES: Final[dict[tuple[str, str], list[tuple[str, str]]]] = {
    # KC metro: MO side is the bigger half (~510K), KS side ("Kansas City, KS"
    # a.k.a. "Wyandotte County" colloquially) is ~150K. Together ~660K.
    ("kansas city metro", "MO"): [("Kansas City", "MO"), ("Kansas City", "KS")],
    ("kansas city metro", "KS"): [("Kansas City", "MO"), ("Kansas City", "KS")],
    ("kc metro", "MO"): [("Kansas City", "MO"), ("Kansas City", "KS")],
    # Add additional cross-state metros here as they come up.
}


_BOROUGH_ALIASES: Final[dict[tuple[str, str], tuple[str, str, str]]] = {
    ("brooklyn", "NY"): ("Brooklyn (Kings County)", "047", "36047"),
    ("manhattan", "NY"): ("Manhattan (New York County)", "061", "36061"),
    ("queens", "NY"): ("Queens (Queens County)", "081", "36081"),
    ("bronx", "NY"): ("Bronx (Bronx County)", "005", "36005"),
    ("the bronx", "NY"): ("Bronx (Bronx County)", "005", "36005"),
    ("staten island", "NY"): ("Staten Island (Richmond County)", "085", "36085"),
}


@dataclass(frozen=True)
class CityResolution:
    """Result of resolving a city name to a tract list."""

    city_name: str
    state: str
    state_fips: str
    place_geoid: str
    geoids: list[str]
    boundary_tract_geoids: list[str]  # tracts that touch the city but whose centroid is outside


def _tiger_dir() -> Path:
    return get_settings().raw_dir / "tiger" / str(TIGER_YEAR)


# ---------------------------------------------------------------------------
# Census ZCTA <-> Tract crosswalk
#
# The Census Bureau publishes a relationship file each decennial mapping
# every ZCTA (ZIP Code Tabulation Area) to the census tracts it overlaps,
# with the area + housing-unit overlap proportions. We use it to allocate
# ZIP-level data (USAC ACP enrollment, USPS counts, etc.) down to tracts.
#
# 2020 vintage URL pattern (most recent decennial as of writing):
#   https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/
#       tab20_zcta520_tract20_natl.txt
#
# Schema (pipe-delimited; documented in the README at that path):
#   OID_ZCTA5_20, GEOID_ZCTA5_20, NAMELSAD_ZCTA5_20, AREALAND_ZCTA5_20,
#   AREAWATER_ZCTA5_20, MTFCC_ZCTA5_20, FUNCSTAT_ZCTA5_20,
#   OID_TRACT_20, GEOID_TRACT_20, NAMELSAD_TRACT_20, STATE_TRACT_20,
#   COUNTY_TRACT_20, AREALAND_TRACT_20, AREAWATER_TRACT_20, MTFCC_TRACT_20,
#   FUNCSTAT_TRACT_20, AREALAND_PART, AREAWATER_PART
#
# The `AREALAND_PART` field is the land area (m²) of the overlap between
# the ZCTA and the tract. To compute each tract's fraction of the ZCTA:
#   fraction = AREALAND_PART / sum(AREALAND_PART) per ZCTA
#
# Some downstream allocations (ACP enrollment, USPS counts) are better
# weighted by housing units than by land area, but the housing-unit
# crosswalk lives in a separate ACS table; for now area-weighting is the
# reasonable default and matches HUD's standard practice.

_ZCTA_TRACT_URL: Final = (
    "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/"
    "tab20_zcta520_tract20_natl.txt"
)


def get_zcta_tract_crosswalk(*, force_refresh: bool = False) -> Path:
    """Return path to the cached ZCTA <-> tract relationship file.

    Downloads from Census on first call (~30 MB), then reads from cache.
    """
    dest_dir = _tiger_dir() / "ZCTA_TRACT_REL"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / "tab20_zcta520_tract20_natl.txt"
    if dest.exists() and not force_refresh:
        return dest

    log.info("Downloading ZCTA<->tract crosswalk: %s", _ZCTA_TRACT_URL)
    with httpx.stream(
        "GET", _ZCTA_TRACT_URL, timeout=300.0, follow_redirects=True
    ) as r:
        r.raise_for_status()
        with dest.open("wb") as f:
            for chunk in r.iter_bytes(chunk_size=1 << 20):
                f.write(chunk)
    return dest


def load_zcta_tract_crosswalk(*, force_refresh: bool = False):
    """Return a Polars DataFrame mapping ZCTA -> tract with area weights.

    Output columns:
        zip5    : 5-char ZCTA code (str, zero-padded)
        tract_geoid : 11-char tract GEOID (str)
        area_part   : land area of overlap (m²)
        area_weight : overlap area / total ZCTA land area (0..1)

    The `area_weight` column sums to ~1.0 per ZCTA across all tracts that
    overlap it (water-only ZCTAs may sum to 0 — they're filtered out by
    callers that need a non-zero divisor).
    """
    import polars as pl

    path = get_zcta_tract_crosswalk(force_refresh=force_refresh)
    df = pl.read_csv(
        path,
        separator="|",
        schema_overrides={
            "GEOID_ZCTA5_20": pl.Utf8,
            "GEOID_TRACT_20": pl.Utf8,
        },
    )
    df = df.select([
        pl.col("GEOID_ZCTA5_20").alias("zip5"),
        pl.col("GEOID_TRACT_20").alias("tract_geoid"),
        pl.col("AREALAND_PART").alias("area_part"),
    ])
    totals = df.group_by("zip5").agg(pl.col("area_part").sum().alias("zip_total"))
    joined = df.join(totals, on="zip5")
    return joined.with_columns(
        (pl.col("area_part") / pl.col("zip_total").clip(lower_bound=1)).alias("area_weight"),
    ).select(["zip5", "tract_geoid", "area_part", "area_weight"])


def _download_zip(url: str, target_dir: Path) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    log.info("Downloading %s", url)
    with httpx.stream("GET", url, timeout=120.0, follow_redirects=True) as r:
        r.raise_for_status()
        buf = io.BytesIO()
        for chunk in r.iter_bytes(chunk_size=1 << 20):
            buf.write(chunk)
    with zipfile.ZipFile(buf) as zf:
        zf.extractall(target_dir)


def _state_fips(state: str) -> str:
    s = state.upper().strip()
    if s not in STATE_FIPS:
        raise ValueError(f"Unknown state abbreviation: {state!r}")
    return STATE_FIPS[s]


def get_place_shapefile(state: str) -> Path:
    """Return path to TIGER PLACE shapefile for `state`, downloading if missing."""
    fips = _state_fips(state)
    dest = _tiger_dir() / f"PLACE_{fips}"
    shp = dest / f"tl_{TIGER_YEAR}_{fips}_place.shp"
    if not shp.exists():
        url = f"{TIGER_BASE}/PLACE/tl_{TIGER_YEAR}_{fips}_place.zip"
        _download_zip(url, dest)
    return shp


def get_tract_shapefile(state: str) -> Path:
    """Return path to TIGER TRACT shapefile for `state`, downloading if missing."""
    fips = _state_fips(state)
    dest = _tiger_dir() / f"TRACT_{fips}"
    shp = dest / f"tl_{TIGER_YEAR}_{fips}_tract.shp"
    if not shp.exists():
        url = f"{TIGER_BASE}/TRACT/tl_{TIGER_YEAR}_{fips}_tract.zip"
        _download_zip(url, dest)
    return shp


def _find_place(places: gpd.GeoDataFrame, city: str) -> gpd.GeoDataFrame:
    """Locate the PLACE row(s) matching `city`. Tries exact-name first, then
    case-insensitive substring. Excludes census-designated places (MTFCC G4210)
    when an incorporated place (G4110) match exists.
    """
    name_lower = places["NAME"].str.lower()
    target = city.lower().strip()

    exact = places[name_lower == target]
    if not exact.empty:
        # Prefer incorporated place over CDP if both exist.
        incorporated = exact[exact["MTFCC"] == "G4110"]
        if not incorporated.empty:
            return incorporated
        return exact

    contains = places[name_lower.str.contains(target, na=False)]
    if contains.empty:
        raise ValueError(f"City {city!r} not found in TIGER PLACE layer")
    incorporated = contains[contains["MTFCC"] == "G4110"]
    if not incorporated.empty:
        return incorporated
    return contains


def places_in_state(
    state: str,
    *,
    incorporated_only: bool = True,
) -> list[dict[str, str]]:
    """List all TIGER places (cities) in a state.

    Used by the Phase 7 batch screener to enumerate candidate markets.
    Reads the cached PLACE shapefile (downloads on first call ~5MB) and
    returns a sorted, deduplicated list of dicts:
      {"name": "Boulder", "fips": "08123", "mtfcc": "G4110", "ns": "..."}

    When `incorporated_only=True` (default), filters to MTFCC G4110
    (incorporated municipalities) and drops G4210 census-designated
    places — keeps the candidate list focused on real cities the user
    might want to compete in.
    """
    shp = get_place_shapefile(state)
    df = gpd.read_file(shp)
    if incorporated_only:
        df = df[df["MTFCC"] == "G4110"]
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for _, row in df.iterrows():
        name = str(row.get("NAME") or "").strip()
        if not name or name.lower() in seen:
            continue
        seen.add(name.lower())
        out.append({
            "name": name,
            "fips": str(row.get("PLACEFP") or row.get("GEOID") or ""),
            "mtfcc": str(row.get("MTFCC") or ""),
            "ns": str(row.get("PLACENS") or ""),
        })
    out.sort(key=lambda p: p["name"].lower())
    return out


def tract_polygons(geoids: list[str], state: str | None = None) -> gpd.GeoDataFrame:
    """Return TIGER tract polygons for the given GEOIDs as a GeoDataFrame.

    Reads from the cached state-level TIGER shapefile(s) (downloads if missing).
    If `state` is None, infers the state(s) from the GEOID prefixes — required
    for cross-state markets (e.g., KC metro across MO+KS).

    Output CRS is EPSG:4326 (WGS84 lat/lon) — what Folium / Leaflet expect.
    Columns include `GEOID`, `NAME`, `INTPTLAT`, `INTPTLON`, `geometry`.
    """
    if not geoids:
        return gpd.GeoDataFrame(columns=["GEOID", "geometry"], geometry="geometry", crs="EPSG:4326")

    # Infer state set from GEOID prefixes. Falls back to the explicit `state`
    # arg for backwards compatibility; if both are present, the inferred set
    # supersedes (a single-state caller passing multi-state GEOIDs is the
    # interesting case — we want all polygons regardless of what they passed).
    fips_to_state = {v: k for k, v in STATE_FIPS.items()}
    state_fips_set = {g[:2] for g in geoids if len(g) >= 2}
    states = [fips_to_state[f] for f in state_fips_set if f in fips_to_state]
    if not states and state is not None:
        states = [state.upper()]

    frames: list[gpd.GeoDataFrame] = []
    for st_abbr in states:
        shp = get_tract_shapefile(st_abbr)
        tracts = gpd.read_file(shp)
        tracts = tracts[tracts["GEOID"].isin(geoids)].copy()
        if tracts.crs and str(tracts.crs).upper() != "EPSG:4326":
            tracts = tracts.to_crs(epsg=4326)
        if not tracts.empty:
            frames.append(tracts)

    if not frames:
        return gpd.GeoDataFrame(columns=["GEOID", "geometry"], geometry="geometry", crs="EPSG:4326")
    if len(frames) == 1:
        return frames[0]
    import pandas as pd
    return gpd.GeoDataFrame(
        pd.concat(frames, ignore_index=True), geometry="geometry", crs="EPSG:4326"
    )


def _resolve_via_alias(city: str, state: str) -> CityResolution | None:
    """If `(city, state)` is a known borough/county alias, resolve via tract
    GEOID prefix filter on the state tract shapefile.

    Returns None if no alias match.
    """
    key = (city.lower().strip(), state.upper())
    if key not in _BOROUGH_ALIASES:
        return None

    display, county_fips, place_geoid = _BOROUGH_ALIASES[key]
    state_fips = _state_fips(state)
    tract_shp = get_tract_shapefile(state)
    tracts = gpd.read_file(tract_shp)
    prefix = f"{state_fips}{county_fips}"
    matching = tracts[tracts["GEOID"].str.startswith(prefix)]
    geoids = sorted(matching["GEOID"].tolist())
    log.info(
        "Resolved %r via alias to county FIPS %s%s: %d tracts",
        city, state_fips, county_fips, len(geoids),
    )
    return CityResolution(
        city_name=display,
        state=state.upper(),
        state_fips=state_fips,
        place_geoid=place_geoid,
        geoids=geoids,
        boundary_tract_geoids=[],  # county boundaries ARE tract boundaries — no straddle
    )


def _resolve_via_metro(city: str, state: str) -> CityResolution | None:
    """If `(city, state)` is a known cross-state metro alias, resolve each
    component place independently and merge the tract lists.

    Each component is resolved through the normal `city_to_tracts` path
    (PLACE match for that state). Boundary tracts are merged across
    components, with cross-component overlaps removed.
    """
    key = (city.lower().strip(), state.upper())
    if key not in _METRO_ALIASES:
        return None

    components = _METRO_ALIASES[key]
    all_geoids: list[str] = []
    all_boundary: list[str] = []
    display_parts: list[str] = []
    primary_state_fips = _state_fips(state)
    primary_place_geoid = ""

    for comp_city, comp_state in components:
        try:
            sub = city_to_tracts(comp_city, comp_state)
        except ValueError as exc:
            log.warning("Metro component %s, %s failed: %s", comp_city, comp_state, exc)
            continue
        all_geoids.extend(sub.geoids)
        all_boundary.extend(sub.boundary_tract_geoids)
        display_parts.append(f"{sub.city_name}, {sub.state}")
        if comp_state.upper() == state.upper() and not primary_place_geoid:
            primary_place_geoid = sub.place_geoid

    if not all_geoids:
        return None

    # Dedupe — overlapping boundary tracts shouldn't be double-counted.
    seen: set[str] = set()
    deduped_geoids: list[str] = []
    for g in all_geoids:
        if g not in seen:
            seen.add(g)
            deduped_geoids.append(g)
    deduped_boundary = sorted(set(all_boundary) - seen)

    log.info(
        "Resolved %r as cross-state metro across %s: %d tracts",
        city, [f"{c}, {s}" for c, s in components], len(deduped_geoids),
    )
    return CityResolution(
        city_name=f"{city.title()} Metro ({' + '.join(display_parts)})",
        state=state.upper(),
        state_fips=primary_state_fips,
        place_geoid=primary_place_geoid or f"METRO-{state.upper()}",
        geoids=sorted(deduped_geoids),
        boundary_tract_geoids=deduped_boundary,
    )


def city_to_tracts(city: str, state: str) -> CityResolution:
    """Resolve a city name to its census tract GEOIDs.

    Resolution order:
      1. Cross-state metro alias — for labels like "Kansas City Metro, MO"
         that span PLACEs in multiple states. Resolves each component and
         merges.
      2. TIGER PLACE match (incorporated place or CDP) — the typical path for
         most cities. Returns tracts whose centroid is inside the city polygon
         (primary), plus boundary tracts that touch but whose centroid is outside.
      3. Borough/county alias fallback — for inputs like "Brooklyn, NY" that
         don't have a TIGER PLACE entry but unambiguously map to a county.
         Returns all tracts in that county; no boundary tracts (county
         boundaries are tract boundaries).
    """
    metro = _resolve_via_metro(city, state)
    if metro is not None:
        return metro

    fips = _state_fips(state)
    place_shp = get_place_shapefile(state)
    tract_shp = get_tract_shapefile(state)

    places = gpd.read_file(place_shp)
    tracts = gpd.read_file(tract_shp)

    try:
        matches = _find_place(places, city)
    except ValueError:
        # Not in PLACE — try a borough/county alias before giving up.
        aliased = _resolve_via_alias(city, state)
        if aliased is not None:
            return aliased
        raise
    if len(matches) > 1:
        log.warning(
            "Multiple PLACE matches for %r in %s: %s; using first (%s)",
            city,
            state,
            matches["NAME"].tolist(),
            matches.iloc[0]["NAME"],
        )
    place_row = matches.iloc[0]
    city_poly = matches.geometry.union_all()

    # Project both layers to a planar CRS for stable centroid math.
    # NAD83 / Conus Albers (EPSG:5070) is fine for CONUS; adequate elsewhere
    # for this resolution. Suppress geopandas's centroid-on-geographic warning.
    tracts_proj = tracts.to_crs(epsg=5070)
    city_proj = gpd.GeoSeries([city_poly], crs=tracts.crs).to_crs(epsg=5070).iloc[0]

    centroids = tracts_proj.geometry.centroid
    centroid_in_city = centroids.within(city_proj)
    intersects_city = tracts_proj.geometry.intersects(city_proj)

    in_geoids = sorted(tracts_proj.loc[centroid_in_city, "GEOID"].tolist())
    boundary_geoids = sorted(
        tracts_proj.loc[intersects_city & ~centroid_in_city, "GEOID"].tolist()
    )

    return CityResolution(
        city_name=str(place_row["NAME"]),
        state=state.upper(),
        state_fips=fips,
        place_geoid=str(place_row["GEOID"]),
        geoids=in_geoids,
        boundary_tract_geoids=boundary_geoids,
    )
