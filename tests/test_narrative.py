"""Tests for ftth_compete.ui.narrative."""

from __future__ import annotations

from ftth_compete.analysis.competitors import ProviderSummary
from ftth_compete.analysis.housing import HousingSplit
from ftth_compete.analysis.market import MarketMetrics
from ftth_compete.pipeline import TearSheet
from ftth_compete.ui.narrative import fiber_share, market_narrative


def _sheet(
    *,
    pop: int | None = 22324,
    n_tracts: int = 4,
    poverty_rate: float | None = 0.135,
    mfi: float | None = 74410.7,
    sfh_share: float = 0.67,
    mdu_share: float = 0.21,
    other_share: float = 0.12,
    total: int = 7681,
    providers: list[ProviderSummary] | None = None,
    providers_note: str | None = None,
) -> TearSheet:
    return TearSheet(
        market={"city": "Evans", "state": "CO", "place_geoid": "0825280", "state_fips": "08"},
        tracts={"inside_city": ["X"] * n_tracts, "boundary": [], "included_in_analysis": ["X"] * n_tracts},
        demographics=MarketMetrics(
            n_tracts=n_tracts,
            population=pop,
            poverty_universe=22033,
            poverty_below=2967,
            poverty_rate=poverty_rate,
            median_household_income_weighted=mfi,
            housing_units_total=total,
        ),
        housing=HousingSplit(
            sfh=int(total * sfh_share),
            mdu_small=100, mdu_mid=500, mdu_large=1000,
            mobile_home=800, other=80,
            total=total,
            sfh_share=sfh_share,
            mdu_share=mdu_share,
            other_share=other_share,
        ),
        tract_acs=[],
        coverage_matrix=[],
        location_availability=[],
        providers=providers,
        providers_note=providers_note,
        provider_subs=[],
        market_subs_anchor=None,
        tract_subs=[],
        ias_note=None,
        provider_velocity=[],
        velocity_note=None,
        provider_trajectory=[],
        trajectory_note=None,
        tract_speeds=[],
        speeds_note=None,
        provider_ratings={},
        ratings_note=None,
        data_versions={"tiger": 2024, "acs5": 2024, "bdc": "2025-06-30"},
    )


def test_full_narrative_evans_co_shape() -> None:
    providers = [
        ProviderSummary(
            canonical_name="Xfinity",
            holding_company="Comcast",
            category="cable",
            technology="Cable",
            tech_code=40,
            tracts_served=4,
            coverage_pct=1.0,
            locations_served=6750,
            has_fiber=False,
            max_advertised_down=1200.0,
            max_advertised_up=35.0,
            raw_brand_names=["Xfinity"],
        ),
        ProviderSummary(
            canonical_name="Allo Communications",
            holding_company="Allo Communications",
            category="regional_fiber",
            technology="Fiber",
            tech_code=50,
            tracts_served=4,
            coverage_pct=1.0,
            locations_served=3328,
            has_fiber=True,
            max_advertised_down=2300.0,
            max_advertised_up=2300.0,
            raw_brand_names=["Allo Communications LLC"],
        ),
    ]
    text = market_narrative(_sheet(providers=providers))
    assert "Evans, CO" in text
    assert "22,324" in text
    assert "$74,411" in text
    assert "13.5%" in text
    assert "single-family" in text
    assert "broadband providers" in text
    # 2 distinct providers, 1 with fiber -> "1 with fiber, led by Allo"
    assert "fiber, led by Allo Communications" in text
    assert "cable from Xfinity" in text


def test_singular_tract_phrasing() -> None:
    text = market_narrative(_sheet(n_tracts=1, providers=None))
    assert "1 census tract." in text
    assert "tracts" not in text.split("census ")[1][:10]  # no plural in census tract phrase


def test_missing_providers_note_surfaces() -> None:
    text = market_narrative(_sheet(providers=None, providers_note="FCC creds not set"))
    assert "Provider data unavailable" in text
    assert "FCC creds not set" in text


def test_handles_missing_demographics() -> None:
    text = market_narrative(_sheet(poverty_rate=None, mfi=None, providers=None))
    # Should still produce SOME text without crashing
    assert "Evans, CO" in text


def test_zero_providers() -> None:
    text = market_narrative(_sheet(providers=[]))
    assert "No providers found" in text


def test_fiber_share() -> None:
    providers = [
        ProviderSummary(
            canonical_name="A", holding_company="A", category="cable",
            tracts_served=1, coverage_pct=1.0, locations_served=1,
            technology="Cable", tech_code=40, has_fiber=False,
            max_advertised_down=100.0, max_advertised_up=10.0, raw_brand_names=[],
        ),
        ProviderSummary(
            canonical_name="B", holding_company="B", category="national_fiber",
            tracts_served=1, coverage_pct=1.0, locations_served=1,
            technology="Fiber", tech_code=50, has_fiber=True,
            max_advertised_down=1000.0, max_advertised_up=1000.0, raw_brand_names=[],
        ),
    ]
    # 1 of 2 distinct providers has fiber
    assert fiber_share(providers) == 0.5
    assert fiber_share([]) is None
    assert fiber_share(None) is None


def test_fiber_share_dedupes_by_canonical() -> None:
    """A provider with both fiber and DSL rows should count once for fiber share."""
    rows = [
        ProviderSummary(
            canonical_name="Lumen", holding_company="Lumen", category="national_fiber",
            tracts_served=1, coverage_pct=1.0, locations_served=500,
            technology="DSL", tech_code=10, has_fiber=False,
            max_advertised_down=100.0, max_advertised_up=20.0, raw_brand_names=[],
        ),
        ProviderSummary(
            canonical_name="Lumen", holding_company="Lumen", category="national_fiber",
            tracts_served=1, coverage_pct=1.0, locations_served=50,
            technology="Fiber", tech_code=50, has_fiber=True,
            max_advertised_down=3000.0, max_advertised_up=3000.0, raw_brand_names=[],
        ),
        ProviderSummary(
            canonical_name="Cable Co", holding_company="Cable Co", category="cable",
            tracts_served=1, coverage_pct=1.0, locations_served=100,
            technology="Cable", tech_code=40, has_fiber=False,
            max_advertised_down=1200.0, max_advertised_up=35.0, raw_brand_names=[],
        ),
    ]
    # 2 distinct providers, 1 (Lumen) has any fiber row -> 0.5
    assert fiber_share(rows) == 0.5
