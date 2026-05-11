"""Ookla Open Speedtest Data integration.

LICENSE: Ookla open data is distributed under CC BY-NC-SA 4.0. This project
is personal/non-commercial only. Required attribution string (rendered in
the Streamlit footer):

    "Speed test data (c) Ookla, 2019-present, distributed under CC BY-NC-SA 4.0."

Data source: https://registry.opendata.aws/speedtest-global-performance/
Github: https://github.com/teamookla/ookla-open-data

Per-tile schema (Ookla parquet, verified May 2026):
    quadkey: str (z=16 hex, ~610m)
    tile: str (WKT polygon, edge boundary)
    tile_x: float (centroid lon — pre-computed, used for bbox filter)
    tile_y: float (centroid lat)
    avg_d_kbps, avg_u_kbps, avg_lat_ms: int
    tests: int (count of speed tests aggregated into this tile)
    devices: int (distinct devices)
    year, quarter, type: partitioning fields

Strategy:
    1. Use DuckDB's `httpfs` extension to query the S3 parquet remotely.
    2. Push down a bbox filter on `tile_x` / `tile_y` so only tiles within
       our market polygon are pulled (cuts ~6M global tiles -> hundreds).
    3. Spatial-join tile centroids -> tract polygons via GeoPandas.
    4. Aggregate per tract: median down/up/latency, sum of tests, tile count.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Final

import duckdb
import geopandas as gpd
import polars as pl
from shapely.geometry import Point

log = logging.getLogger(__name__)

S3_BASE: Final = "s3://ookla-open-data/parquet/performance/type=fixed"
ATTRIBUTION: Final = (
    "Speed test data (c) Ookla, 2019-present, "
    "distributed under CC BY-NC-SA 4.0."
)

# Minimum sample size before we trust a tract-level aggregate. Tiles are
# user-submitted; thin samples produce noisy medians.
MIN_TESTS_PER_TRACT: Final = 30


def _release_url(year: int, quarter: int) -> str:
    """Build the S3 URL for a fixed-broadband tile parquet release.

    Ookla's filename uses the first month of the quarter (01/04/07/10).
    """
    month = (quarter - 1) * 3 + 1
    return (
        f"{S3_BASE}/year={year}/quarter={quarter}/"
        f"{year:04d}-{month:02d}-01_performance_fixed_tiles.parquet"
    )


def _candidate_releases(today: date | None = None) -> list[tuple[int, int]]:
    """Return (year, quarter) candidates from newest to ~2 years back."""
    today = today or date.today()
    current_q = (today.month - 1) // 3 + 1
    out: list[tuple[int, int]] = []
    yr, q = today.year, current_q
    for _ in range(8):
        out.append((yr, q))
        q -= 1
        if q == 0:
            q = 4
            yr -= 1
    return out


def latest_release(*, probe: bool = True) -> tuple[int, int]:
    """Return (year, quarter) for the latest published Ookla release.

    Walks newest-first and verifies the parquet exists via DuckDB. Caches
    nothing; callers should cache the result themselves.
    """
    if not probe:
        return _candidate_releases()[1]  # one quarter back, decent guess
    con = duckdb.connect(":memory:")
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("SET s3_region='us-east-1';")
        for year, quarter in _candidate_releases():
            url = _release_url(year, quarter)
            try:
                con.execute(f"SELECT COUNT(*) FROM read_parquet('{url}') LIMIT 1").fetchone()
                log.info("Ookla latest release: %d Q%d", year, quarter)
                return year, quarter
            except duckdb.Error:
                continue
        raise RuntimeError("No published Ookla release found in the last 2 years")
    finally:
        con.close()


def fetch_tract_speeds(
    tract_polys: gpd.GeoDataFrame,
    *,
    year: int | None = None,
    quarter: int | None = None,
    min_tests: int = MIN_TESTS_PER_TRACT,
) -> pl.DataFrame:
    """Aggregate Ookla speed test tiles to per-tract medians.

    Args:
        tract_polys: GeoDataFrame with `GEOID` + `geometry` columns in
            EPSG:4326. Tracts you want metrics for.
        year, quarter: Specific Ookla release. Defaults to latest published.
        min_tests: Tracts with fewer aggregated tests than this are
            included but flagged with `low_sample=True`.

    Returns:
        Polars DataFrame:
            tract_geoid, median_down_mbps, median_up_mbps, median_lat_ms,
            n_tests, n_tiles, low_sample
        Tracts with zero tiles are omitted (caller can left-join if needed).
    """
    if tract_polys.empty:
        return _empty_frame()

    if year is None or quarter is None:
        year, quarter = latest_release()
    url = _release_url(year, quarter)

    minx, miny, maxx, maxy = tract_polys.total_bounds
    log.info(
        "Ookla query: bbox=(%.4f,%.4f,%.4f,%.4f) release=%dQ%d",
        minx, miny, maxx, maxy, year, quarter,
    )

    con = duckdb.connect(":memory:")
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("SET s3_region='us-east-1';")

        sql = f"""
        SELECT
            tile_x, tile_y,
            avg_d_kbps, avg_u_kbps, avg_lat_ms,
            tests, devices
        FROM read_parquet('{url}')
        WHERE tile_x BETWEEN {minx} AND {maxx}
          AND tile_y BETWEEN {miny} AND {maxy}
        """
        tiles_pl = con.execute(sql).pl()
    finally:
        con.close()

    if tiles_pl.is_empty():
        log.warning("No Ookla tiles in bbox; possibly low-sample area")
        return _empty_frame()

    # GeoPandas spatial join: tile centroid -> tract polygon
    tiles = tiles_pl.to_pandas()
    tiles_gdf = gpd.GeoDataFrame(
        tiles,
        geometry=[Point(x, y) for x, y in zip(tiles["tile_x"], tiles["tile_y"])],
        crs="EPSG:4326",
    )
    joined = gpd.sjoin(
        tiles_gdf,
        tract_polys[["GEOID", "geometry"]],
        how="inner",
        predicate="within",
    )

    if joined.empty:
        return _empty_frame()

    agg = (
        joined.groupby("GEOID")
        .agg(
            median_down_kbps=("avg_d_kbps", "median"),
            median_up_kbps=("avg_u_kbps", "median"),
            median_lat_ms=("avg_lat_ms", "median"),
            n_tests=("tests", "sum"),
            n_tiles=("avg_d_kbps", "count"),
        )
        .reset_index()
    )

    out = pl.from_pandas(agg).rename({"GEOID": "tract_geoid"})
    out = out.with_columns(
        [
            (pl.col("median_down_kbps") / 1000).alias("median_down_mbps"),
            (pl.col("median_up_kbps") / 1000).alias("median_up_mbps"),
            (pl.col("n_tests") < min_tests).alias("low_sample"),
        ]
    ).select(
        "tract_geoid",
        "median_down_mbps",
        "median_up_mbps",
        "median_lat_ms",
        "n_tests",
        "n_tiles",
        "low_sample",
    )
    return out


def _empty_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "tract_geoid": pl.Utf8,
            "median_down_mbps": pl.Float64,
            "median_up_mbps": pl.Float64,
            "median_lat_ms": pl.Float64,
            "n_tests": pl.Int64,
            "n_tiles": pl.Int64,
            "low_sample": pl.Boolean,
        }
    )
