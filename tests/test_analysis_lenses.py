"""Tests for ftth_compete.analysis.lenses."""

from __future__ import annotations

import pytest

from ftth_compete.analysis.competitors import ProviderSummary
from ftth_compete.analysis.lenses import Lens, apply, market_opportunity


def _provider(
    name: str,
    *,
    category: str = "national_fiber",
    coverage_pct: float = 1.0,
    has_fiber: bool = True,
    max_down: float = 1000.0,
    locations: int = 100,
    technology: str | None = None,
    tech_code: int | None = None,
) -> ProviderSummary:
    if technology is None:
        technology = "Fiber" if has_fiber else "Cable"
    if tech_code is None:
        tech_code = 50 if has_fiber else 40
    return ProviderSummary(
        canonical_name=name,
        holding_company=name,
        category=category,
        technology=technology,
        tech_code=tech_code,
        tracts_served=4,
        coverage_pct=coverage_pct,
        locations_served=locations,
        has_fiber=has_fiber,
        max_advertised_down=max_down,
        max_advertised_up=max_down,
        raw_brand_names=[name],
    )


# ---------------------------------------------------------------------------
# Neutral lens

def test_neutral_returns_all_with_no_scores() -> None:
    providers = [_provider("A"), _provider("B")]
    out = apply(providers, Lens.NEUTRAL)
    assert len(out) == 2
    assert all(s.score is None for s in out)
    assert all(s.score_label is None for s in out)
    assert [s.provider.canonical_name for s in out] == ["A", "B"]  # original order


def test_unknown_lens_falls_back_to_neutral() -> None:
    providers = [_provider("A")]
    out = apply(providers, "totally_made_up")
    assert len(out) == 1
    assert out[0].score is None


# ---------------------------------------------------------------------------
# Defensive lens

def test_defensive_no_incumbent_falls_back_with_label() -> None:
    out = apply([_provider("A")], Lens.DEFENSIVE)
    assert out[0].score is None
    assert "Pick an incumbent" in (out[0].score_label or "")


def test_defensive_unknown_incumbent_labels_each_row() -> None:
    out = apply([_provider("A")], Lens.DEFENSIVE, incumbent="NotInMarket")
    assert "not in this market" in (out[0].score_label or "").lower()


def test_defensive_pins_incumbent_first_and_scores_threats() -> None:
    providers = [
        _provider("Xfinity", category="cable", has_fiber=False),  # incumbent
        _provider("Verizon Fios", category="national_fiber", has_fiber=True, coverage_pct=1.0),
        _provider("Cox", category="cable", has_fiber=False, coverage_pct=0.3),
    ]
    out = apply(providers, Lens.DEFENSIVE, incumbent="Xfinity")

    # Incumbent first
    assert out[0].provider.canonical_name == "Xfinity"
    assert out[0].is_incumbent
    assert out[0].score is None
    assert out[0].score_label == "Incumbent"

    # Verizon Fios is a fiber competitor against a cable-only incumbent => high threat
    fios = next(s for s in out if s.provider.canonical_name == "Verizon Fios")
    cox = next(s for s in out if s.provider.canonical_name == "Cox")
    assert fios.score is not None and cox.score is not None
    assert fios.score > cox.score
    assert "Threat" in (fios.score_label or "")


def test_defensive_rating_advantage_matters() -> None:
    providers = [
        _provider("Inc", has_fiber=True, category="cable"),
        _provider("Loved", has_fiber=True, category="national_fiber"),
        _provider("Hated", has_fiber=True, category="national_fiber"),
    ]
    rating_lookup = {
        "Inc": {"rating": 3.0},
        "Loved": {"rating": 4.5},
        "Hated": {"rating": 2.0},
    }
    out = apply(
        providers, Lens.DEFENSIVE, incumbent="Inc", rating_lookup=rating_lookup
    )
    by_name = {s.provider.canonical_name: s for s in out if s.score is not None}
    # Loved threatens incumbent more than Hated does (rating advantage)
    assert by_name["Loved"].score > by_name["Hated"].score


# ---------------------------------------------------------------------------
# Offensive lens

def test_offensive_ranks_vulnerable_first() -> None:
    providers = [
        _provider("StrongFiber", category="national_fiber", has_fiber=True),
        _provider("BadCableOnly", category="cable", has_fiber=False),
        _provider("CableWithFiber", category="cable", has_fiber=True),
    ]
    out = apply(providers, Lens.OFFENSIVE)
    # Cable-only with no fiber = highest vulnerability
    assert out[0].provider.canonical_name == "BadCableOnly"
    # Strong fiber = lowest vulnerability
    assert out[-1].provider.canonical_name == "StrongFiber"
    assert all(s.score is not None for s in out)
    assert "Vulnerability" in (out[0].score_label or "")


def test_offensive_uses_low_ratings_to_increase_score() -> None:
    a = _provider("A", category="cable", has_fiber=False)
    b = _provider("B", category="cable", has_fiber=False)
    out = apply(
        [a, b],
        Lens.OFFENSIVE,
        rating_lookup={"A": {"rating": 1.5}, "B": {"rating": 4.5}},
    )
    by_name = {s.provider.canonical_name: s for s in out}
    # A has worse rating = more vulnerable
    assert by_name["A"].score > by_name["B"].score


# ---------------------------------------------------------------------------
# Market-level offensive

def test_market_opportunity_strong_target_when_underserved_cable_only() -> None:
    providers = [
        _provider("Cable Only A", category="cable", has_fiber=False),
        _provider("Cable Only B", category="cable", has_fiber=False),
        _provider("Tiny WISP", category="fixed_wireless", has_fiber=False),
    ]
    result = market_opportunity(providers, mdu_share=0.4)
    assert result["score"] is not None
    assert result["score"] >= 0.55
    assert "Strong" in result["headline"]


def test_market_opportunity_weak_target_when_saturated() -> None:
    providers = [
        _provider("Fios", category="national_fiber"),
        _provider("AT&T Fiber", category="national_fiber"),
        _provider("Allo", category="regional_fiber"),
        _provider("Xfinity", category="cable", has_fiber=True),
        _provider("Lumen", category="national_fiber"),
    ]
    rating_lookup = {p.canonical_name: {"rating": 4.2} for p in providers}
    result = market_opportunity(providers, rating_lookup=rating_lookup, mdu_share=0.1)
    assert result["score"] is not None
    assert result["score"] < 0.35


def test_market_opportunity_handles_empty() -> None:
    result = market_opportunity([])
    assert result["score"] is None
    assert "No providers" in result["headline"]
