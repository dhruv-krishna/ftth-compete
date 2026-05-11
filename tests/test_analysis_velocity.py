"""Tests for ftth_compete.analysis.velocity."""

from __future__ import annotations

from ftth_compete.analysis.competitors import ProviderSummary
from ftth_compete.analysis.velocity import compute


def _provider(
    name: str,
    *,
    tech: str = "Fiber",
    tech_code: int = 50,
    locations: int = 1000,
) -> ProviderSummary:
    return ProviderSummary(
        canonical_name=name,
        holding_company=name,
        category="national_fiber",
        technology=tech,
        tech_code=tech_code,
        tracts_served=4,
        coverage_pct=1.0,
        locations_served=locations,
        has_fiber=tech_code == 50,
        max_advertised_down=1000.0,
        max_advertised_up=1000.0,
        raw_brand_names=[name],
    )


def test_existing_provider_growing() -> None:
    cur = [_provider("Allo", locations=3500)]
    prev = [_provider("Allo", locations=2500)]
    result = compute(cur, prev, current_release="2025-06-30", prev_release="2024-06-30")
    assert len(result) == 1
    v = result[0]
    assert v.canonical_name == "Allo"
    assert v.delta_abs == 1000
    assert v.delta_pct == 0.4
    assert not v.new_offering
    assert not v.discontinued


def test_new_offering() -> None:
    cur = [_provider("Allo", locations=500)]
    prev: list[ProviderSummary] = []
    result = compute(cur, prev, current_release="2025-06-30", prev_release="2024-06-30")
    assert len(result) == 1
    v = result[0]
    assert v.new_offering is True
    assert v.delta_abs == 500
    assert v.delta_pct is None
    assert v.prev_locations == 0


def test_discontinued() -> None:
    cur: list[ProviderSummary] = []
    prev = [_provider("Old DSL Co", tech="DSL", tech_code=10, locations=200)]
    result = compute(cur, prev, current_release="2025-06-30", prev_release="2024-06-30")
    assert len(result) == 1
    v = result[0]
    assert v.discontinued is True
    assert v.current_locations == 0
    assert v.delta_abs == -200
    assert v.delta_pct == -1.0


def test_sort_order_biggest_growth_first() -> None:
    cur = [
        _provider("Big Growth", locations=5000),
        _provider("Small Growth", locations=1100),
        _provider("Shrinking", locations=400),
    ]
    prev = [
        _provider("Big Growth", locations=2000),
        _provider("Small Growth", locations=1000),
        _provider("Shrinking", locations=900),
    ]
    result = compute(cur, prev, current_release="2025-06-30", prev_release="2024-06-30")
    assert [v.canonical_name for v in result] == ["Big Growth", "Small Growth", "Shrinking"]
    assert result[2].delta_abs == -500


def test_per_tech_split_in_velocity() -> None:
    """Provider with both fiber and DSL gets two velocity rows."""
    cur = [
        _provider("Lumen", tech="Fiber", tech_code=50, locations=200),
        _provider("Lumen", tech="DSL", tech_code=10, locations=4500),  # shrinking
    ]
    prev = [
        _provider("Lumen", tech="Fiber", tech_code=50, locations=50),  # growing fast
        _provider("Lumen", tech="DSL", tech_code=10, locations=5000),
    ]
    result = compute(cur, prev, current_release="2025-06-30", prev_release="2024-06-30")
    by_tech = {v.technology: v for v in result}
    assert by_tech["Fiber"].delta_abs == 150
    assert by_tech["Fiber"].delta_pct == 3.0  # 200% growth
    assert by_tech["DSL"].delta_abs == -500
    assert by_tech["DSL"].delta_pct == -0.1
