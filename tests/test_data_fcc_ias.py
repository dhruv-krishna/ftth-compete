"""Tests for ftth_compete.data.fcc_ias.

Network-touching paths (downloads) are not exercised here — those run only in
real pipeline runs. We test bucket math + DataFrame schema normalization.
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

import polars as pl

from ftth_compete.data.fcc_ias import (
    _BUCKETS,
    _read_csv_from_zip,
    bucket_midpoint,
)


def test_bucket_midpoint_known_codes() -> None:
    assert bucket_midpoint(0) == (0.0, 0.0, 0.0)
    assert bucket_midpoint(1) == (1.0, 100.0, 200.0)
    assert bucket_midpoint(2) == (200.0, 300.0, 400.0)
    assert bucket_midpoint(3) == (400.0, 500.0, 600.0)
    assert bucket_midpoint(4) == (600.0, 700.0, 800.0)
    assert bucket_midpoint(5) == (800.0, 900.0, 1000.0)


def test_bucket_midpoint_handles_unknown() -> None:
    assert bucket_midpoint(None) == (0.0, 0.0, 0.0)
    assert bucket_midpoint(99) == (0.0, 0.0, 0.0)


def test_bucket_midpoints_form_monotone_ranges() -> None:
    """For each code, low <= mid <= high; codes increase in midpoint."""
    last_high = -1.0
    for code in sorted(_BUCKETS.keys()):
        low, mid, high = _BUCKETS[code]
        assert low <= mid <= high, f"Bucket {code} has non-monotone range"
        if code > 0:
            assert mid >= last_high, f"Bucket {code} mid drops from prev high"
        last_high = high


def test_read_csv_from_zip_normalizes_columns(tmp_path: Path) -> None:
    """Build a fake FCC ZIP and confirm read_csv_from_zip normalizes correctly."""
    csv_text = (
        "tractcode,pcat_all,pcat_25x0\n"
        "01001020100,5,5\n"
        "08123001005,4,3\n"
        "08123001405,3,2\n"
    )
    zp = tmp_path / "tract_map_jun_2022.zip"
    with zipfile.ZipFile(zp, "w") as zf:
        zf.writestr("tract_map_jun_2022/tract_map_jun_2022.csv", csv_text)

    df = _read_csv_from_zip(zp)
    assert set(df.columns) == {"tract_geoid", "bucket_all", "bucket_25"}
    assert df.shape == (3, 3)
    # GEOIDs must stay strings (no leading-zero loss)
    assert df["tract_geoid"][0] == "01001020100"
    # Buckets are int8
    assert df["bucket_all"].dtype == pl.Int8
    # Spot-check Evans CO tract 005 = (4, 3) per fixture
    row = df.filter(pl.col("tract_geoid") == "08123001005")
    assert row["bucket_all"][0] == 4
    assert row["bucket_25"][0] == 3


def test_read_csv_from_zip_handles_25x3_legacy_name(tmp_path: Path) -> None:
    csv_text = "tractcode,pcat_all,pcat_25x3\n12345678901,5,5\n"
    zp = tmp_path / "tract_map_dec_2018.zip"
    with zipfile.ZipFile(zp, "w") as zf:
        zf.writestr("tract_map_dec_2018/tract_map_dec_2018.csv", csv_text)

    df = _read_csv_from_zip(zp)
    assert "bucket_25" in df.columns  # legacy 25x3 also normalized


# ---------------------------------------------------------------------------
# Phase 6e — Historical trajectory tests

from ftth_compete.data.fcc_ias import (
    DIRECT_RELEASE_URLS,
    IasRelease,
    MarketSubscriptionPoint,
    _tract_mean_density,
    _url_for,
    market_subscription_history,
)


def test_direct_release_url_pattern() -> None:
    """`_url_for` should produce the conventional FCC tract_map filename."""
    assert _url_for("2022-06-30").endswith("tract_map_jun_2022.zip")
    assert _url_for("2018-12-31").endswith("tract_map_dec_2018.zip")
    assert _url_for("2014-06-30").endswith("tract_map_jun_2014.zip")


def test_release_registry_spans_2014_to_2022() -> None:
    """Phase 6e expects historical releases back to 2014. If this fails,
    someone trimmed the registry — likely accidentally."""
    keys = sorted(DIRECT_RELEASE_URLS.keys())
    assert keys[0].startswith("2014")
    assert keys[-1].startswith("2022")
    # 17 known semi-annual releases
    assert len(keys) >= 17


def test_tract_mean_density_zero_when_no_overlap() -> None:
    """If none of the requested GEOIDs appear in the frame, density is 0."""
    df = pl.DataFrame({
        "tract_geoid": ["99999999999"],
        "bucket_all": [5],
        "bucket_25": [5],
    })
    lo, mi, hi = _tract_mean_density(df, ["08123001005"], "bucket_all")
    assert (lo, mi, hi) == (0.0, 0.0, 0.0)


def test_tract_mean_density_averages_across_geoids() -> None:
    """Mean across a 3-tract market: 0+5+3 → buckets (0, ..., 0) (800, 900, 1000) (400, 500, 600).
    Mean low = (0 + 800 + 400)/3 = 400."""
    df = pl.DataFrame({
        "tract_geoid": ["a", "b", "c"],
        "bucket_all": [0, 5, 3],
    })
    lo, mi, hi = _tract_mean_density(df, ["a", "b", "c"], "bucket_all")
    assert lo == 400.0      # (0 + 800 + 400) / 3
    assert mi == (0 + 900 + 500) / 3
    assert hi == (0 + 1000 + 600) / 3


def test_tract_mean_density_missing_column_returns_zero() -> None:
    """Old pre-2016 releases lack the 25/3 bucket column. Should return zero
    rather than throw."""
    df = pl.DataFrame({"tract_geoid": ["a"], "bucket_all": [5]})
    out = _tract_mean_density(df, ["a"], "bucket_25")
    assert out == (0.0, 0.0, 0.0)


def test_market_subscription_history_builds_points(monkeypatch) -> None:
    """E2E: feed a fake 2-release history → expect 2 MarketSubscriptionPoints
    sorted newest-first, with proper take-rate fractions."""

    def fake_historical(*, auto_download: bool, since: str | None):
        df_old = pl.DataFrame({
            "tract_geoid": ["08123001005"],
            "bucket_all": [3],
            "bucket_25": [2],
        })
        df_new = pl.DataFrame({
            "tract_geoid": ["08123001005"],
            "bucket_all": [5],
            "bucket_25": [4],
        })
        return [
            IasRelease(as_of="2022-06-30", source="x", frame=df_new),
            IasRelease(as_of="2015-12-31", source="x", frame=df_old),
        ]

    monkeypatch.setattr(
        "ftth_compete.data.fcc_ias.historical_releases", fake_historical,
    )
    pts = market_subscription_history(["08123001005"])
    assert len(pts) == 2
    # Newest first
    assert pts[0].as_of == "2022-06-30"
    assert pts[1].as_of == "2015-12-31"
    # 2022 bucket_25 = 4 → mid = 700 → take_rate_25_mid = 0.70
    assert pts[0].take_rate_25_mid == 0.7
    # 2015 bucket_25 = 2 → mid = 300 → take_rate_25_mid = 0.30
    assert pts[1].take_rate_25_mid == 0.3
    # Direction-of-travel: take rate grew
    assert pts[0].take_rate_25_mid > pts[1].take_rate_25_mid


def test_market_subscription_history_empty_when_no_releases(monkeypatch) -> None:
    monkeypatch.setattr(
        "ftth_compete.data.fcc_ias.historical_releases", lambda **kw: [],
    )
    assert market_subscription_history(["08123001005"]) == []


def test_market_subscription_point_is_frozen() -> None:
    """Pipeline serializes via asdict() — confirm the dataclass is frozen so
    it can't be mutated mid-flight."""
    pt = MarketSubscriptionPoint(
        as_of="2022-06-30", n_tracts=1,
        density_all_per_1k_low=0.0, density_all_per_1k_mid=0.0,
        density_all_per_1k_high=0.0,
        density_25_per_1k_low=0.0, density_25_per_1k_mid=0.0,
        density_25_per_1k_high=0.0,
        take_rate_all_mid=0.0, take_rate_25_mid=0.0,
    )
    try:
        pt.as_of = "wrong"  # type: ignore[misc]
    except Exception:
        return
    raise AssertionError("MarketSubscriptionPoint should be frozen")
