"""Tests for ftth_compete.analysis.provider_view.

Heavy SQL paths (`_aggregate_parquets` against real BDC parquets) are
exercised at runtime via the dashboard, not here — those need cached
parquets we don't want as test fixtures. We test the pure helpers:
slugify, find_by_slug, disk-cache stamp logic, list_cached_releases.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl
import pytest

from ftth_compete.analysis import provider_view as pv
from ftth_compete.analysis.provider_view import (
    _AGG_CACHE,
    _aggregate_parquets,
    _disk_cache_path,
    _disk_meta_path,
    _load_disk_cache,
    _save_disk_cache,
    find_by_slug,
    list_cached_releases,
    slugify,
)


def test_slugify_basic() -> None:
    assert slugify("Allo") == "allo"
    assert slugify("AT&T Fiber") == "at-t-fiber"
    assert slugify("Lumen / Quantum Fiber") == "lumen-quantum-fiber"
    assert slugify("Verizon Fios") == "verizon-fios"


def test_slugify_collapses_runs_of_separators() -> None:
    assert slugify("A   B___C") == "a-b-c"
    assert slugify("---x---") == "x"


def test_slugify_handles_empty() -> None:
    assert slugify("") == ""
    assert slugify("!!!") == ""


def test_list_cached_releases_filters_to_iso_dates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only `YYYY-MM-DD` named dirs count as releases."""
    bdc = tmp_path / "processed" / "bdc"
    (bdc / "2025-06-30").mkdir(parents=True)
    (bdc / "2024-12-31").mkdir(parents=True)
    (bdc / "stale-data").mkdir(parents=True)    # ignored
    (bdc / "_tmp").mkdir(parents=True)          # ignored
    # Point provider_view at this synthetic root.
    monkeypatch.setattr(
        pv, "_bdc_processed_root", lambda: bdc,
    )
    out = list_cached_releases()
    assert out == ["2025-06-30", "2024-12-31"]  # newest-first


def test_disk_cache_roundtrip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Write a frame to disk cache, read it back; mtime stamp respected."""
    monkeypatch.setattr(pv, "_provider_view_cache_dir", lambda: tmp_path)
    parent = str(tmp_path / "bdc" / "2025-06-30")
    df = pl.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})

    _save_disk_cache(parent, mtime=1000.0, frame=df)
    # Files were created.
    assert _disk_cache_path(parent).exists()
    assert _disk_meta_path(parent).exists()

    # Equal-or-newer expected mtime → hit.
    loaded = _load_disk_cache(parent, expected_mtime=1000.0)
    assert loaded is not None
    assert loaded.shape == df.shape
    assert loaded.equals(df)


def test_disk_cache_invalidates_when_stale(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Source parquets newer than the stored mtime → miss (rebuild required)."""
    monkeypatch.setattr(pv, "_provider_view_cache_dir", lambda: tmp_path)
    parent = str(tmp_path / "bdc" / "2025-06-30")
    df = pl.DataFrame({"x": [1]})
    _save_disk_cache(parent, mtime=1000.0, frame=df)

    # Caller's expected mtime is newer than the stored stamp.
    loaded = _load_disk_cache(parent, expected_mtime=2000.0)
    assert loaded is None


def test_disk_cache_returns_none_when_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pv, "_provider_view_cache_dir", lambda: tmp_path)
    out = _load_disk_cache(str(tmp_path / "nope"), expected_mtime=0.0)
    assert out is None


def test_disk_cache_handles_corrupt_meta(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A .meta file with junk content should be treated as a miss, not crash."""
    monkeypatch.setattr(pv, "_provider_view_cache_dir", lambda: tmp_path)
    parent = str(tmp_path / "bdc" / "2025-06-30")
    df = pl.DataFrame({"x": [1]})
    _save_disk_cache(parent, mtime=1000.0, frame=df)
    # Corrupt the meta stamp.
    _disk_meta_path(parent).write_text("not-a-float")
    assert _load_disk_cache(parent, expected_mtime=0.0) is None


def test_aggregate_parquets_empty_input() -> None:
    """Empty parquet list short-circuits to empty frame without crashing."""
    out = _aggregate_parquets([])
    assert isinstance(out, pl.DataFrame)
    assert out.is_empty()


def test_find_by_slug_returns_none_when_no_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If no BDC parquets are cached, find_by_slug returns None cleanly."""
    monkeypatch.setattr(
        pv, "_bdc_processed_root", lambda: tmp_path / "empty",
    )
    # Also clear the in-process aggregation cache so we don't accidentally
    # hit a value cached from a real prior run.
    _AGG_CACHE.clear()
    assert find_by_slug("any-slug") is None
