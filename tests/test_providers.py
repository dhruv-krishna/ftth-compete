"""Tests for ftth_compete.data.providers."""

from __future__ import annotations

import pytest

from ftth_compete.data.fcc_bdc import (
    TECH_CABLE,
    TECH_FIBER,
    TECH_FW_LICENSED,
    TECH_NGSO_SAT,
)
from ftth_compete.data.providers import canonical_name, canonicalize


@pytest.mark.parametrize(
    "brand,holding,tech,expected",
    [
        ("Comcast Cable Communications, LLC", "Comcast Corporation", TECH_CABLE, "Xfinity"),
        ("Charter Communications Operating, LLC", "Charter Communications", TECH_CABLE, "Spectrum"),
        ("CSC Holdings, LLC", "Altice USA, Inc.", TECH_CABLE, "Optimum"),
        ("Spectrum", "Charter Communications", TECH_FIBER, "Spectrum"),
        ("Verizon Fios", "Verizon Communications Inc.", TECH_FIBER, "Verizon Fios"),
        ("Verizon 5G Home", "Verizon Communications Inc.", TECH_FW_LICENSED, "Verizon 5G Home"),
        ("AT&T Fiber", "AT&T Inc.", TECH_FIBER, "AT&T Fiber"),
        ("Google Fiber Inc.", "Alphabet Inc.", TECH_FIBER, "Google Fiber"),
        ("Quantum Fiber", "Lumen Technologies, Inc.", TECH_FIBER, "Lumen / Quantum Fiber"),
        ("CenturyLink", "Lumen Technologies, Inc.", TECH_FIBER, "Lumen / Quantum Fiber"),
        ("SpaceX Services, Inc.", "SpaceX", TECH_NGSO_SAT, "Starlink"),
        ("Allo Communications LLC", "Allo Communications", TECH_FIBER, "Allo Communications"),
    ],
)
def test_canonical_name_matches(brand: str, holding: str, tech: int, expected: str) -> None:
    assert canonical_name(brand, holding, tech) == expected


def test_unknown_provider_passes_through() -> None:
    assert canonical_name("Tiny Co-Op WISP", "Some Holding LLC", TECH_FIBER) == "Tiny Co-Op WISP"


def test_empty_input_returns_unknown() -> None:
    assert canonical_name(None) == "Unknown"
    assert canonical_name("", "") == "Unknown"


def test_returns_provider_object() -> None:
    p = canonicalize("Xfinity Internet", "Comcast Corporation", TECH_CABLE)
    assert p is not None
    assert p.canonical == "Xfinity"
    assert p.category == "cable"


def test_tech_disambiguation_verizon() -> None:
    """Verizon 5G Home and Verizon Fios share holding company but differ by tech."""
    fios = canonicalize("Verizon Fios", "Verizon Communications Inc.", TECH_FIBER)
    assert fios is not None and fios.canonical == "Verizon Fios"

    fwa = canonicalize("Verizon 5G Home", "Verizon Communications Inc.", TECH_FW_LICENSED)
    assert fwa is not None and fwa.canonical == "Verizon 5G Home"
