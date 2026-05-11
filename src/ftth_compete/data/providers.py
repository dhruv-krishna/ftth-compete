"""Canonical provider registry.

Maps raw FCC BDC brand_name + holding_company strings to canonical brand
entries. The registry is the single source of truth — `.claude/providers.md`
mirrors it for human reference.

Canonicalization is intentionally simple: case-insensitive substring match
against `raw_aliases`, with `holding_company_aliases` as a tiebreaker for
providers that share a brand prefix. Unknown providers pass through as-is
so they're surfaced in the UI rather than silently dropped.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Final

from .fcc_bdc import (
    TECH_CABLE,
    TECH_DSL,
    TECH_FIBER,
    TECH_FW_LBR,
    TECH_FW_LICENSED,
    TECH_FW_UNLICENSED,
    TECH_GSO_SAT,
    TECH_NGSO_SAT,
)


@dataclass(frozen=True)
class Provider:
    canonical: str
    holding_company: str
    category: str  # "national_fiber" | "cable" | "fixed_wireless" | "satellite" | "regional_fiber" | "muni"
    raw_aliases: tuple[str, ...]  # case-insensitive substring patterns matched against brand_name
    holding_aliases: tuple[str, ...] = ()  # patterns matched against holding_company
    primary_techs: frozenset[int] = field(default_factory=frozenset)  # for tech-aware tiebreak


# v1 covers the providers in `.claude/providers.md`. Extend over time.
_REGISTRY: Final[tuple[Provider, ...]] = (
    # National fiber majors
    Provider(
        canonical="Verizon Fios",
        holding_company="Verizon Communications",
        category="national_fiber",
        raw_aliases=("fios", "verizon online"),
        holding_aliases=("verizon",),
        primary_techs=frozenset({TECH_FIBER}),
    ),
    Provider(
        canonical="Verizon 5G Home",
        holding_company="Verizon Communications",
        category="fixed_wireless",
        raw_aliases=("verizon 5g", "5g home", "verizon"),
        holding_aliases=("verizon",),
        primary_techs=frozenset({TECH_FW_LICENSED}),
    ),
    Provider(
        canonical="AT&T Fiber",
        holding_company="AT&T",
        category="national_fiber",
        raw_aliases=("at&t fiber", "att fiber"),
        holding_aliases=("at&t", "at and t"),
        primary_techs=frozenset({TECH_FIBER}),
    ),
    Provider(
        canonical="AT&T Internet Air",
        holding_company="AT&T",
        category="fixed_wireless",
        raw_aliases=("at&t internet air", "at&t",),
        holding_aliases=("at&t",),
        primary_techs=frozenset({TECH_FW_LICENSED}),
    ),
    Provider(
        canonical="AT&T Internet",
        holding_company="AT&T",
        category="national_fiber",
        raw_aliases=("at&t",),
        holding_aliases=("at&t",),
        primary_techs=frozenset({TECH_DSL, TECH_FIBER}),
    ),
    Provider(
        canonical="Frontier Fiber",
        holding_company="Frontier Communications",
        category="national_fiber",
        raw_aliases=("frontier",),
        holding_aliases=("frontier",),
        primary_techs=frozenset({TECH_FIBER, TECH_DSL}),
    ),
    Provider(
        canonical="Google Fiber",
        holding_company="Alphabet",
        category="national_fiber",
        raw_aliases=("google fiber",),
        holding_aliases=("google", "alphabet"),
        primary_techs=frozenset({TECH_FIBER}),
    ),
    Provider(
        canonical="Lumen / Quantum Fiber",
        holding_company="Lumen Technologies",
        category="national_fiber",
        raw_aliases=("quantum fiber", "centurylink", "lumen", "embarq"),
        holding_aliases=("lumen", "centurylink", "qwest"),
        primary_techs=frozenset({TECH_FIBER, TECH_DSL}),
    ),
    Provider(
        canonical="Ziply Fiber",
        holding_company="Ziply Fiber",
        category="national_fiber",
        raw_aliases=("ziply",),
        holding_aliases=("ziply",),
    ),
    # Cable incumbents
    Provider(
        canonical="Xfinity",
        holding_company="Comcast",
        category="cable",
        raw_aliases=("xfinity", "comcast"),
        holding_aliases=("comcast",),
        primary_techs=frozenset({TECH_CABLE, TECH_FIBER}),
    ),
    Provider(
        canonical="Spectrum",
        holding_company="Charter Communications",
        category="cable",
        raw_aliases=("spectrum", "charter"),
        holding_aliases=("charter",),
        primary_techs=frozenset({TECH_CABLE, TECH_FIBER}),
    ),
    Provider(
        canonical="Cox",
        holding_company="Cox Communications",
        category="cable",
        raw_aliases=("cox",),
        holding_aliases=("cox",),
        primary_techs=frozenset({TECH_CABLE}),
    ),
    Provider(
        canonical="Optimum",
        holding_company="Altice USA",
        category="cable",
        raw_aliases=("optimum", "altice", "csc holdings", "suddenlink"),
        holding_aliases=("altice", "csc"),
        primary_techs=frozenset({TECH_CABLE, TECH_FIBER}),
    ),
    Provider(
        canonical="Mediacom",
        holding_company="Mediacom",
        category="cable",
        raw_aliases=("mediacom",),
        holding_aliases=("mediacom",),
    ),
    Provider(
        canonical="WOW!",
        holding_company="WideOpenWest",
        category="cable",
        raw_aliases=("wow!", "wideopenwest", "wow internet"),
        holding_aliases=("wideopenwest", "wow!"),
    ),
    # Fixed wireless / 5G home
    Provider(
        canonical="T-Mobile Home Internet",
        holding_company="T-Mobile US",
        category="fixed_wireless",
        raw_aliases=("t-mobile home", "t-mobile internet", "tmobile", "t-mobile"),
        holding_aliases=("t-mobile",),
        primary_techs=frozenset({TECH_FW_LICENSED}),
    ),
    Provider(
        canonical="Starlink",
        holding_company="SpaceX",
        category="satellite",
        raw_aliases=("starlink", "spacex"),
        holding_aliases=("spacex",),
        primary_techs=frozenset({TECH_NGSO_SAT}),
    ),
    Provider(
        canonical="HughesNet",
        holding_company="Hughes Network Systems",
        category="satellite",
        raw_aliases=("hughesnet", "hughes"),
        holding_aliases=("hughes",),
        primary_techs=frozenset({TECH_GSO_SAT}),
    ),
    Provider(
        canonical="Viasat",
        holding_company="Viasat",
        category="satellite",
        raw_aliases=("viasat", "exede"),
        holding_aliases=("viasat",),
        primary_techs=frozenset({TECH_GSO_SAT}),
    ),
    # Regional fiber overbuilders / muni
    Provider(
        canonical="Allo Communications",
        holding_company="Allo Communications",
        category="regional_fiber",
        raw_aliases=("allo",),
        holding_aliases=("allo",),
        primary_techs=frozenset({TECH_FIBER}),
    ),
    Provider(
        canonical="Tillman Fiber",
        holding_company="Tillman FiberCo",
        category="regional_fiber",
        raw_aliases=("tillman fiber", "tillman fiberco"),
        holding_aliases=("tillman",),
    ),
    Provider(
        canonical="GoNetspeed",
        holding_company="GoNetspeed",
        category="regional_fiber",
        raw_aliases=("gonetspeed",),
        holding_aliases=("gonetspeed",),
    ),
    Provider(
        canonical="Brightspeed",
        holding_company="Connect Holding II",
        category="regional_fiber",
        raw_aliases=("brightspeed",),
        holding_aliases=("brightspeed",),
    ),
    Provider(
        canonical="EPB Chattanooga",
        holding_company="EPB",
        category="muni",
        raw_aliases=("epb",),
        holding_aliases=("epb",),
    ),
    Provider(
        canonical="MetroNet",
        holding_company="MetroNet",
        category="regional_fiber",
        raw_aliases=("metronet",),
        holding_aliases=("metronet",),
        primary_techs=frozenset({TECH_FIBER}),
    ),
    Provider(
        canonical="Hotwire Communications",
        holding_company="Hotwire Communications",
        category="regional_fiber",
        raw_aliases=("hotwire",),
        holding_aliases=("hotwire",),
    ),
    Provider(
        canonical="Rise Broadband",
        holding_company="JAB Wireless",
        category="fixed_wireless",
        raw_aliases=("rise broadband", "rise"),
        holding_aliases=("jab wireless",),
        primary_techs=frozenset({TECH_FW_UNLICENSED, TECH_FW_LICENSED}),
    ),
    Provider(
        canonical="Vero Fiber",
        holding_company="Vero Fiber",
        category="regional_fiber",
        raw_aliases=("vero fiber", "vero",),
        holding_aliases=("vero",),
        primary_techs=frozenset({TECH_FIBER}),
    ),
    Provider(
        canonical="Zayo",
        holding_company="Zayo Group",
        category="regional_fiber",
        raw_aliases=("zayo",),
        holding_aliases=("zayo",),
    ),
    Provider(
        canonical="Conexon Connect",
        holding_company="Conexon",
        category="regional_fiber",
        raw_aliases=("conexon connect", "conexon"),
        holding_aliases=("conexon",),
        primary_techs=frozenset({TECH_FIBER}),
    ),
    # Astound Broadband — formed via the 2021 merger of RCN, Wave Broadband,
    # Grande Communications, and enTouch Systems under Stonepeak ownership.
    # All four sub-brand names need to canonicalize to "Astound Broadband"
    # so anchor lookups + market-level metrics roll up correctly.
    Provider(
        canonical="Astound Broadband",
        holding_company="Stonepeak",
        category="cable",
        raw_aliases=("astound", "rcn", "wave broadband", "grande", "entouch"),
        holding_aliases=("stonepeak", "astound"),
    ),
    # Cable One operates under the "Sparklight" consumer brand in most
    # markets but still files as Cable One Inc. The 2024 10-K names both.
    Provider(
        canonical="Cable One",
        holding_company="Cable One",
        category="cable",
        raw_aliases=("cable one", "sparklight"),
        holding_aliases=("cable one",),
    ),
)


def _matches(text: str, patterns: tuple[str, ...]) -> bool:
    if not text:
        return False
    lower = text.lower()
    return any(p in lower for p in patterns)


def canonicalize(
    brand_name: str | None,
    holding_company: str | None = None,
    technology: int | None = None,
) -> Provider | None:
    """Resolve a raw (brand_name, holding_company, technology) to a Provider.

    Returns None if no canonical match. Match priority:
      1. brand_name substring match, with technology in `primary_techs` if provided.
      2. brand_name substring match (any tech).
      3. holding_company match, with tech filter.
      4. holding_company match (any tech).
    """
    bn = (brand_name or "").lower()
    hc = (holding_company or "").lower()

    # Priority 1: brand match + tech compatible
    if technology is not None:
        for p in _REGISTRY:
            if _matches(bn, p.raw_aliases) and (
                not p.primary_techs or technology in p.primary_techs
            ):
                return p

    # Priority 2: brand match (any tech)
    for p in _REGISTRY:
        if _matches(bn, p.raw_aliases):
            return p

    # Priority 3: holding match + tech compatible
    if technology is not None:
        for p in _REGISTRY:
            if _matches(hc, p.holding_aliases) and (
                not p.primary_techs or technology in p.primary_techs
            ):
                return p

    # Priority 4: holding match (any tech)
    for p in _REGISTRY:
        if _matches(hc, p.holding_aliases):
            return p

    return None


def canonical_name(
    brand_name: str | None,
    holding_company: str | None = None,
    technology: int | None = None,
) -> str:
    """Convenience: return canonical name or original brand_name as fallback."""
    p = canonicalize(brand_name, holding_company, technology)
    if p is not None:
        return p.canonical
    return (brand_name or "Unknown").strip()


def all_providers() -> tuple[Provider, ...]:
    """Return the full canonical registry (immutable)."""
    return _REGISTRY
