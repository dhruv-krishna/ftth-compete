"""Tests for ftth_compete.analysis.penetration."""

from __future__ import annotations

from ftth_compete.analysis.competitors import ProviderSummary
from ftth_compete.analysis.penetration import (
    MARKET_LEVEL_ANCHORS,
    NATIONAL_TAKE_RATES,
    MarketContext,
    calibrate_with_ias,
    estimate_all,
    estimate_market_subs,
    find_anchor,
    market_subscription_anchor,
    market_total_subs,
)


def _provider(
    name: str,
    *,
    category: str = "national_fiber",
    technology: str = "Fiber",
    tech_code: int = 50,
    locations: int = 1000,
    has_fiber: bool = True,
) -> ProviderSummary:
    return ProviderSummary(
        canonical_name=name,
        holding_company=name,
        category=category,
        technology=technology,
        tech_code=tech_code,
        tracts_served=4,
        coverage_pct=1.0,
        locations_served=locations,
        has_fiber=has_fiber,
        max_advertised_down=1000.0,
        max_advertised_up=1000.0,
        raw_brand_names=[name],
    )


def test_known_provider_uses_10k_anchor() -> None:
    p = _provider("Xfinity", category="cable", technology="Cable", tech_code=40, locations=1000, has_fiber=False)
    e = estimate_market_subs(p)
    assert e.canonical_name == "Xfinity"
    assert e.confidence == "medium"
    assert e.take_rate == NATIONAL_TAKE_RATES["Xfinity"].take_rate
    assert e.estimate_mid == int(round(1000 * 0.55))
    assert e.estimate_low < e.estimate_mid < e.estimate_high


def test_anchor_registry_shape() -> None:
    """Every seed anchor should have plausible field shapes."""
    seen_keys: set[tuple[str, str, str]] = set()
    for a in MARKET_LEVEL_ANCHORS:
        assert a.geo_kind in {"city", "metro", "state"}
        assert a.subscribers > 0
        if a.locations_passed is not None:
            assert a.locations_passed >= a.subscribers, (
                f"Anchor for {a.canonical_name} @ {a.geo_key}: "
                f"locations_passed ({a.locations_passed:,}) must be >= subscribers "
                f"({a.subscribers:,})"
            )
        assert a.source, f"Anchor {a.canonical_name} @ {a.geo_key} missing source"
        key = (a.canonical_name, a.geo_kind, a.geo_key.lower())
        assert key not in seen_keys, f"Duplicate anchor key {key}"
        seen_keys.add(key)


def test_find_anchor_city_first() -> None:
    """City anchors beat state anchors for the same provider."""
    ctx = MarketContext(city_state="Chattanooga, TN", state="TN")
    a = find_anchor("EPB Chattanooga", ctx)
    assert a is not None
    assert a.geo_kind == "city"
    assert a.geo_key == "Chattanooga, TN"


def test_find_anchor_state_fallback() -> None:
    """When no city match, falls through to state."""
    ctx = MarketContext(city_state="Brooklyn, NY", state="NY")
    a = find_anchor("Verizon Fios", ctx)
    assert a is not None
    assert a.geo_kind == "state"
    assert a.geo_key == "NY"


def test_find_anchor_no_match_returns_none() -> None:
    ctx = MarketContext(city_state="Somewhere Else, XX", state="XX")
    assert find_anchor("Xfinity", ctx) is None
    assert find_anchor("Nonexistent Provider", ctx) is None


def test_anchored_estimate_uses_high_confidence() -> None:
    """Anchored estimates should be marked high-confidence and have a tighter range."""
    p = _provider("EPB Chattanooga", category="muni", locations=180_000)
    ctx = MarketContext(city_state="Chattanooga, TN", state="TN")
    e = estimate_market_subs(p, market_context=ctx)
    assert e.confidence == "high"
    assert "EPB" in e.source or "anchor" in e.source.lower()
    # Tighter range than the heuristic (±12.5% vs ±25%)
    spread = (e.estimate_high - e.estimate_low) / max(e.estimate_mid, 1)
    assert spread < 0.30


def test_anchor_caps_at_disclosed_total() -> None:
    """Anchor's disclosed total should clamp the estimate from above —
    a single in-state market can't exceed the state's disclosed subscribers."""
    # Use Verizon Fios in NY but with a huge fake location count.
    p = _provider("Verizon Fios", locations=20_000_000)
    ctx = MarketContext(city_state="Brooklyn, NY", state="NY")
    e = estimate_market_subs(p, market_context=ctx)
    # NY state anchor is 2.4M subs.
    assert e.estimate_mid <= 2_400_000
    assert e.confidence == "high"


def test_unknown_provider_falls_back_to_category() -> None:
    p = _provider("Mystery WISP", category="fixed_wireless", technology="Licensed FW",
                   tech_code=71, locations=200, has_fiber=False)
    e = estimate_market_subs(p)
    assert e.confidence == "low"
    # fixed_wireless default is 0.05 -> 200 * 0.05 = 10
    assert e.estimate_mid == 10
    assert "category default" in e.source


def test_satellite_takes_very_low() -> None:
    p = _provider("HughesNet", category="satellite", technology="GSO Satellite",
                   tech_code=60, locations=10_000, has_fiber=False)
    e = estimate_market_subs(p)
    assert e.confidence == "medium"
    # HughesNet is 0.008 -> 80 subs from 10K locations
    assert 75 <= e.estimate_mid <= 85


def test_estimate_all_returns_one_per_input() -> None:
    providers = [
        _provider("Xfinity", category="cable", technology="Cable", tech_code=40, locations=500, has_fiber=False),
        _provider("Allo Communications", category="regional_fiber", locations=300),
        _provider("New WISP", category="unknown", technology="Licensed FW", tech_code=71, locations=50, has_fiber=False),
    ]
    est = estimate_all(providers)
    assert len(est) == 3
    assert est[0].confidence == "medium"
    assert est[2].confidence == "low"


def test_market_total_subs_sums_ranges() -> None:
    providers = [
        _provider("Xfinity", category="cable", technology="Cable", tech_code=40, locations=1000, has_fiber=False),
        _provider("Allo Communications", category="regional_fiber", locations=400),
    ]
    est = estimate_all(providers)
    total = market_total_subs(est)
    assert total is not None
    # Xfinity: 1000 * 0.55 = 550 mid;  Allo: 400 * 0.45 = 180 mid -> 730 total
    assert total["mid"] == est[0].estimate_mid + est[1].estimate_mid
    assert total["low"] < total["mid"] < total["high"]


def test_market_total_subs_empty() -> None:
    assert market_total_subs([]) is None


def test_lumen_dsl_and_fiber_have_separate_estimates() -> None:
    """Per-tech split means Lumen Fiber and Lumen DSL get separate estimates."""
    fiber = _provider("Lumen / Quantum Fiber", category="national_fiber",
                       technology="Fiber", tech_code=50, locations=100, has_fiber=True)
    dsl = _provider("Lumen / Quantum Fiber", category="national_fiber",
                     technology="DSL", tech_code=10, locations=5000, has_fiber=False)
    e_fiber = estimate_market_subs(fiber)
    e_dsl = estimate_market_subs(dsl)
    # Both use the same canonical's 10-K take rate but different location counts
    assert e_fiber.take_rate == e_dsl.take_rate
    assert e_fiber.estimate_mid < e_dsl.estimate_mid  # fewer fiber locations
    assert e_fiber.technology == "Fiber"
    assert e_dsl.technology == "DSL"


# ---------------------------------------------------------------------------
# IAS calibration

def test_market_subscription_anchor_basic() -> None:
    """Two tracts with bucket 5 (800-1000 per 1k HH) and 1000 HH each."""
    tract_subs = [
        {"tract_geoid": "T1", "bucket_all": 5, "bucket_25": 5},
        {"tract_geoid": "T2", "bucket_all": 5, "bucket_25": 5},
    ]
    tract_acs = [
        {"geoid": "T1", "housing_units_total": 1000.0},
        {"geoid": "T2", "housing_units_total": 1000.0},
    ]
    anchor = market_subscription_anchor(
        tract_subs, tract_acs, ias_release="2022-06-30"
    )
    assert anchor is not None
    # 2000 HH x 800-1000 per 1k = 1600 to 2000 subs
    assert anchor.market_subs_low == 1600
    assert anchor.market_subs_mid == 1800
    assert anchor.market_subs_high == 2000
    assert anchor.total_housing_units == 2000
    assert anchor.n_tracts_with_data == 2
    assert anchor.take_rate_mid == 0.9
    assert anchor.ias_release == "2022-06-30"


def test_market_subscription_anchor_skips_missing_data() -> None:
    """Tracts with no IAS or ACS housing data should be skipped, not crash."""
    tract_subs = [
        {"tract_geoid": "T1", "bucket_all": 5, "bucket_25": 5},
        {"tract_geoid": "T2", "bucket_all": None, "bucket_25": None},
    ]
    tract_acs = [
        {"geoid": "T1", "housing_units_total": 500.0},
        {"geoid": "T3", "housing_units_total": 200.0},  # geoid mismatch
    ]
    anchor = market_subscription_anchor(
        tract_subs, tract_acs, ias_release="2022-06-30"
    )
    assert anchor is not None
    assert anchor.n_tracts_with_data == 1  # only T1 had both
    assert anchor.total_housing_units == 500


def test_market_subscription_anchor_empty_returns_none() -> None:
    assert market_subscription_anchor([], [], ias_release="2022-06-30") is None
    assert market_subscription_anchor(
        [{"tract_geoid": "T1", "bucket_25": 5}], [], ias_release="x"
    ) is None


def test_calibrate_with_ias_scales_estimates_to_anchor() -> None:
    providers = [
        _provider("Xfinity", category="cable", technology="Cable", tech_code=40,
                   locations=1000, has_fiber=False),
        _provider("Allo Communications", locations=600),
    ]
    estimates = estimate_all(providers)
    heuristic_mid_sum = sum(e.estimate_mid for e in estimates)
    # Xfinity 0.55*1000=550, Allo 0.45*600=270 -> 820 total

    # Anchor at half the heuristic — implies overcounting
    from ftth_compete.analysis.penetration import MarketSubscriptionAnchor
    anchor = MarketSubscriptionAnchor(
        market_subs_low=300, market_subs_mid=410, market_subs_high=500,
        take_rate_low=0.3, take_rate_mid=0.4, take_rate_high=0.5,
        ias_release="2022-06-30",
        n_tracts_with_data=2, total_housing_units=1000,
    )
    calibrated = calibrate_with_ias(estimates, anchor)
    new_sum = sum(e.estimate_mid for e in calibrated)
    # Sum should now be approximately equal to anchor mid (allowing rounding)
    assert abs(new_sum - 410) <= 2
    # Per-provider proportions preserved
    xf_idx = next(i for i, e in enumerate(estimates) if e.canonical_name == "Xfinity")
    orig_ratio = estimates[xf_idx].estimate_mid / heuristic_mid_sum
    new_ratio = calibrated[xf_idx].estimate_mid / new_sum
    assert abs(orig_ratio - new_ratio) < 0.02
    # Confidence bumped
    assert all(e.confidence == "high" for e in calibrated)
    assert "IAS-calibrated" in calibrated[0].source


def test_calibrate_with_ias_caps_extreme_scales() -> None:
    """If heuristic is wildly off (>2x or <0.3x of anchor), scale is capped."""
    providers = [_provider("X", locations=100, has_fiber=False, category="cable")]
    estimates = estimate_all(providers)
    # Heuristic mid: 100 * 0.45 = 45
    # Anchor mid is 1000 → would scale by 22x; capped at 2.0
    from ftth_compete.analysis.penetration import MarketSubscriptionAnchor
    anchor = MarketSubscriptionAnchor(
        market_subs_low=900, market_subs_mid=1000, market_subs_high=1100,
        take_rate_low=0.9, take_rate_mid=1.0, take_rate_high=1.1,
        ias_release="2022-06-30", n_tracts_with_data=1, total_housing_units=1000,
    )
    calibrated = calibrate_with_ias(estimates, anchor)
    # Cap at 2.0 means new mid <= 2 * 45 = 90, NOT 1000
    assert calibrated[0].estimate_mid <= 90
    assert calibrated[0].estimate_mid >= 45  # still scaled up some


def test_calibrate_with_ias_no_anchor_returns_unchanged() -> None:
    providers = [_provider("X", locations=100)]
    estimates = estimate_all(providers)
    # When anchor sums to 0 (no data), original estimates returned
    from ftth_compete.analysis.penetration import MarketSubscriptionAnchor
    anchor = MarketSubscriptionAnchor(
        market_subs_low=0, market_subs_mid=0, market_subs_high=0,
        take_rate_low=0, take_rate_mid=0, take_rate_high=0,
        ias_release="x", n_tracts_with_data=0, total_housing_units=0,
    )
    out = calibrate_with_ias(estimates, anchor)
    assert out == estimates
