"""Tests for ftth_compete.analysis.screener cache helpers.

The full `screen_market()` path requires real BDC + Census fixtures —
that's exercised through the `/screener` route at runtime, not here.
We test the cache key composition + roundtrip serialization.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ftth_compete.analysis import screener as scr
from dataclasses import asdict

from ftth_compete.analysis.screener import (
    MarketKpis,
    _cache_key,
    kpis_to_csv,
    load_cached_run,
    save_run,
)


def _sample_row(city: str, state: str, score: float = 0.5) -> MarketKpis:
    return MarketKpis(
        city=city, state=state, market_id=f"{city}|{state}",
        n_tracts=10, population=50000, median_hh_income=70000,
        poverty_rate=0.10, housing_units=20000, mdu_share=0.25,
        sfh_share=0.70, n_providers=4, n_fiber_providers=2,
        fiber_avail_pct=0.55, cable_avail_pct=0.90,
        opportunity_score=score, opportunity_headline="Moderate",
    )


def test_cache_key_is_stable_under_state_reordering() -> None:
    """`CO,WY` and `WY,CO` should hit the same cache row."""
    assert _cache_key("CO,WY", "2025-06-30") == _cache_key("WY,CO", "2025-06-30")
    assert _cache_key("CO,WY", "2025-06-30") == _cache_key("co, wy", "2025-06-30")


def test_cache_key_includes_release() -> None:
    a = _cache_key("CO", "2024-12-31")
    b = _cache_key("CO", "2025-06-30")
    assert a != b


def test_cache_key_handles_empty_states_list() -> None:
    """Empty CSV shouldn't crash — used by the 'no states picked' edge case."""
    assert _cache_key("", "2025-06-30") == "2025-06-30__empty"
    assert _cache_key("   ,  ", "2025-06-30") == "2025-06-30__empty"


def test_save_run_then_load_roundtrip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(scr, "_screener_cache_dir", lambda: tmp_path)
    rows = [_sample_row("Evans", "CO", 0.62), _sample_row("Greeley", "CO", 0.41)]
    save_run("CO", "2025-06-30", rows)

    loaded = load_cached_run("CO", "2025-06-30")
    assert loaded is not None
    loaded_rows, built_at = loaded
    assert len(loaded_rows) == 2
    assert {r.city for r in loaded_rows} == {"Evans", "Greeley"}
    assert built_at > 0


def test_load_cached_run_misses_when_not_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(scr, "_screener_cache_dir", lambda: tmp_path)
    assert load_cached_run("CO", "2025-06-30") is None


def test_save_run_skips_empty_input(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No rows → no file written (don't pollute cache with empties)."""
    monkeypatch.setattr(scr, "_screener_cache_dir", lambda: tmp_path)
    save_run("CO", "2025-06-30", [])
    assert not list(tmp_path.iterdir())


def test_load_cached_run_different_release_misses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cache from an older release shouldn't be served for a newer one."""
    monkeypatch.setattr(scr, "_screener_cache_dir", lambda: tmp_path)
    save_run("CO", "2024-12-31", [_sample_row("Evans", "CO")])
    assert load_cached_run("CO", "2025-06-30") is None
    assert load_cached_run("CO", "2024-12-31") is not None


def test_kpis_to_csv_emits_header_and_row() -> None:
    csv = kpis_to_csv([_sample_row("Evans", "CO", 0.62)])
    assert "city,state" in csv
    assert "Evans,CO" in csv
    # Has 18 fields (17 base + top_providers)
    header = csv.splitlines()[0]
    assert header.count(",") == 17
    assert "top_providers" in header


def test_kpis_to_csv_flattens_top_providers_to_string() -> None:
    """top_providers list → human-readable single-column string."""
    row = _sample_row("Evans", "CO", 0.5)
    row = MarketKpis(
        **{**asdict(row), "top_providers": [
            {"name": "Lumen", "tech_label": "Fiber", "locations": 8500},
            {"name": "Xfinity", "tech_label": "Cable", "locations": 7200},
        ]},
    )
    csv = kpis_to_csv([row])
    assert "Lumen (Fiber, 8,500 locs)" in csv
    assert "Xfinity (Cable, 7,200 locs)" in csv


def test_kpis_to_csv_empty_input() -> None:
    assert kpis_to_csv([]) == ""
