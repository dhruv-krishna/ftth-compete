"""Smoke test: every planned module imports cleanly."""

from __future__ import annotations

import importlib

import pytest

MODULES = [
    "ftth_compete",
    "ftth_compete.config",
    "ftth_compete.cli",
    "ftth_compete.data",
    "ftth_compete.data.fcc_bdc",
    "ftth_compete.data.fcc_ias",
    "ftth_compete.data.census_acs",
    "ftth_compete.data.tiger",
    "ftth_compete.data.ookla",
    "ftth_compete.data.google_places",
    "ftth_compete.data.providers",
    "ftth_compete.data.cache",
    "ftth_compete.data.acp",
    "ftth_compete.data.mlab",
    "ftth_compete.analysis",
    "ftth_compete.analysis.market",
    "ftth_compete.analysis.housing",
    "ftth_compete.analysis.competitors",
    "ftth_compete.analysis.speeds",
    "ftth_compete.analysis.penetration",
    "ftth_compete.analysis.lenses",
    "ftth_compete.analysis.trajectory",
    "ftth_compete.analysis.velocity",
    "ftth_compete.ui",
    "ftth_compete.ui.tabs.overview",
    "ftth_compete.ui.tabs.competitors",
    "ftth_compete.ui.tabs.map",
    "ftth_compete.ui.tabs.housing",
    "ftth_compete.ui.tabs.compare",
    "ftth_compete.ui.tabs.methodology",
    "ftth_compete.ui.components.kpi_cards",
    "ftth_compete.ui.components.provider_card",
    "ftth_compete.ui.components.choropleth",
    "ftth_compete.pipelines",
    "ftth_compete.pipelines.refresh_bdc",
    "ftth_compete.pipelines.refresh_ias",
    "ftth_compete.pipelines.refresh_acs",
    "ftth_compete.pipelines.refresh_all",
]


@pytest.mark.parametrize("name", MODULES)
def test_module_imports(name: str) -> None:
    importlib.import_module(name)


def test_metro_aliases_table_shape() -> None:
    """Catch typos in the cross-state metro alias table without hitting the network."""
    from ftth_compete.data.tiger import _METRO_ALIASES, STATE_FIPS

    for (label, primary_state), components in _METRO_ALIASES.items():
        assert label == label.lower(), f"Metro key {label!r} must be lowercased"
        assert primary_state in STATE_FIPS, f"Unknown primary state {primary_state!r}"
        assert len(components) >= 1, f"Metro {label!r} has no components"
        states_in_metro = {s for _, s in components}
        assert len(states_in_metro) >= 2, (
            f"Metro {label!r} should span 2+ states; only {states_in_metro} found"
        )
        for comp_city, comp_state in components:
            assert comp_state.upper() in STATE_FIPS, (
                f"Bad component state {comp_state!r} for metro {label!r}"
            )
            assert comp_city, f"Empty component city in metro {label!r}"


def test_borough_aliases_table_shape() -> None:
    """Catch typos in the NYC borough alias table without hitting the network."""
    from ftth_compete.data.tiger import _BOROUGH_ALIASES, STATE_FIPS

    for (city, state), (display, county_fips, place_geoid) in _BOROUGH_ALIASES.items():
        assert state in STATE_FIPS, f"Unknown state {state!r} in alias table"
        assert city == city.lower(), f"Alias key {city!r} should be lowercased"
        assert len(county_fips) == 3, f"County FIPS for {city} must be 3 digits, got {county_fips!r}"
        assert place_geoid == STATE_FIPS[state] + county_fips, (
            f"place_geoid {place_geoid!r} should equal "
            f"state {STATE_FIPS[state]} + county {county_fips}"
        )
        assert display, f"Empty display name for {city}"
