"""Deterministic narrative generator for the Overview tab.

Produces 2-4 short sentences summarizing the market. Built from templates,
not an LLM, so output is reproducible and we control the voice. Drops
sentences gracefully when their underlying data is missing.
"""

from __future__ import annotations

from .analysis.competitors import ProviderSummary
from .format import fmt_currency, fmt_int, fmt_pct, fmt_speed
from .pipeline import TearSheet


def market_narrative(sheet: TearSheet) -> str:
    """Return a 3-6 sentence summary of the market."""
    parts: list[str] = []

    parts.append(_sentence_pop_geo(sheet))
    inc_pov = _sentence_income_poverty(sheet)
    if inc_pov:
        parts.append(inc_pov)
    housing = _sentence_housing(sheet)
    if housing:
        parts.append(housing)
    providers = _sentence_providers(sheet)
    if providers:
        parts.append(providers)
    take_rate = _sentence_take_rate(sheet)
    if take_rate:
        parts.append(take_rate)
    velocity = _sentence_velocity(sheet)
    if velocity:
        parts.append(velocity)

    return " ".join(p for p in parts if p)


def _sentence_pop_geo(sheet: TearSheet) -> str:
    pop = sheet.demographics.population
    n_tracts = sheet.demographics.n_tracts
    market = sheet.market.get("city", ""), sheet.market.get("state", "")

    if pop and n_tracts:
        tract_word = "tract" if n_tracts == 1 else "tracts"
        return (
            f"{market[0]}, {market[1]} is a {fmt_int(pop)}-population market "
            f"across {n_tracts} census {tract_word}."
        )
    return f"{market[0]}, {market[1]} market summary."


def _sentence_income_poverty(sheet: TearSheet) -> str:
    mfi = sheet.demographics.median_household_income_weighted
    pov = sheet.demographics.poverty_rate
    if mfi is None and pov is None:
        return ""
    fragments: list[str] = []
    if mfi is not None:
        fragments.append(
            f"median household income (population-weighted) is {fmt_currency(mfi)}"
        )
    if pov is not None:
        fragments.append(f"{fmt_pct(pov)} of residents are below the poverty line")
    return _capitalize_first(", ".join(fragments) + ".")


def _sentence_housing(sheet: TearSheet) -> str:
    h = sheet.housing
    if h.total <= 0:
        return ""
    return (
        f"Housing stock is {fmt_pct(h.sfh_share)} single-family, "
        f"{fmt_pct(h.mdu_share)} multi-dwelling, and "
        f"{fmt_pct(h.other_share)} mobile/other."
    )


def _sentence_providers(sheet: TearSheet) -> str:
    if sheet.providers is None:
        if sheet.providers_note:
            return f"Provider data unavailable: {sheet.providers_note}"
        return ""

    if not sheet.providers:
        return "No providers found in the FCC BDC data for this market."

    # `sheet.providers` is per-(provider, tech). Reduce to distinct provider
    # counts for the headline narrative — "12 providers" reads better than
    # "17 provider-tech offerings."
    distinct_names = {p.canonical_name for p in sheet.providers}
    n = len(distinct_names)
    fiber_offerings = [p for p in sheet.providers if p.has_fiber]
    fiber_provider_names = {p.canonical_name for p in fiber_offerings}
    cable_offerings = [p for p in sheet.providers if p.technology == "Cable"]

    base = (
        f"{n} broadband providers serve the market"
        if n != 1
        else "1 broadband provider serves the market"
    )

    detail_parts: list[str] = []
    # If we have location_availability data, lead with the household metric;
    # otherwise fall back to provider-count framing.
    fiber_avail = fiber_availability_share(sheet.location_availability)
    if fiber_offerings:
        top_fiber = max(
            fiber_offerings,
            key=lambda p: (p.coverage_pct or 0, p.locations_served),
        )
        if fiber_avail is not None:
            detail_parts.append(
                f"fiber is available at {fmt_pct(fiber_avail)} of locations, "
                f"led by {top_fiber.canonical_name} "
                f"(max {fmt_speed(top_fiber.max_advertised_down)} down)"
            )
        else:
            detail_parts.append(
                f"{len(fiber_provider_names)} with fiber, led by {top_fiber.canonical_name} "
                f"(max {fmt_speed(top_fiber.max_advertised_down)} down)"
            )
    if cable_offerings:
        # Distinct cable provider names, ordered by coverage of their cable rows.
        seen: list[str] = []
        for p in sorted(
            cable_offerings,
            key=lambda p: (-p.coverage_pct, -p.locations_served),
        ):
            if p.canonical_name not in seen:
                seen.append(p.canonical_name)
        cable_names = ", ".join(seen[:2])
        detail_parts.append(f"cable from {cable_names}")

    if detail_parts:
        return f"{base}: {'; '.join(detail_parts)}."
    return f"{base}."


def _sentence_take_rate(sheet: TearSheet) -> str:
    """One-sentence broadband take-rate summary from FCC IAS, if loaded."""
    anchor = sheet.market_subs_anchor
    if not anchor:
        return ""
    take_mid = anchor.get("take_rate_mid")
    if take_mid is None:
        return ""
    market_subs_mid = anchor.get("market_subs_mid")
    release = anchor.get("ias_release", "")
    return (
        f"FCC IAS data ({release}) puts broadband take rate at "
        f"{fmt_pct(take_mid)} of households (~{fmt_int(market_subs_mid)} "
        f"estimated subscribers)."
    )


def _sentence_velocity(sheet: TearSheet) -> str:
    """One-sentence velocity highlight: top expander + top contractor (if any).

    Only included when `provider_velocity` is loaded (opt-in).
    """
    if not sheet.provider_velocity:
        return ""
    fiber_velos = [v for v in sheet.provider_velocity if v.get("tech_code") == 50]
    if not fiber_velos:
        return ""

    growers = sorted(
        (v for v in fiber_velos if (v.get("delta_abs") or 0) > 0),
        key=lambda v: -v["delta_abs"],
    )
    if not growers:
        return ""

    top = growers[0]
    cur = sheet.market.get("city", "")
    delta = int(top["delta_abs"])
    if top.get("new_offering"):
        descriptor = (
            f"new fiber offering since {top['prev_release']} "
            f"({delta:,} locations)"
        )
    else:
        pct = top.get("delta_pct")
        if pct is not None and abs(pct) < 10:
            descriptor = (
                f"expanded fiber by {fmt_pct(pct)} ({delta:+,} locations) "
                f"vs {top['prev_release']}"
            )
        else:
            descriptor = f"added {delta:,} fiber locations vs {top['prev_release']}"
    return f"Year-over-year, {top['canonical_name']} {descriptor} in {cur}."


def _capitalize_first(s: str) -> str:
    return s[:1].upper() + s[1:] if s else s


def fiber_share(providers: list[ProviderSummary] | None) -> float | None:
    """Share of distinct canonical providers offering fiber (tech 50).

    `providers` is per-(provider, tech) — dedupe by canonical name first.
    Note: this answers "how many providers offer fiber?" — NOT
    "what share of homes can get fiber?" (use `fiber_availability_share`).
    """
    if not providers:
        return None
    has_fiber: dict[str, bool] = {}
    for p in providers:
        has_fiber[p.canonical_name] = has_fiber.get(p.canonical_name, False) or p.has_fiber
    if not has_fiber:
        return None
    return sum(1 for v in has_fiber.values() if v) / len(has_fiber)


def fiber_availability_share(
    location_availability: list[dict[str, object]] | None,
) -> float | None:
    """Share of LOCATIONS (BSLs) where fiber is available — the household metric.

    A location is "fiber available" if at least one provider offers tech 50
    at that BSL, regardless of what the resident is currently subscribed to.
    Counts unique location_ids — NOT providers, NOT subscribers.
    """
    if not location_availability:
        return None
    total = 0
    fiber = 0
    for row in location_availability:
        total += int(row.get("total_locations") or 0)
        fiber += int(row.get("fiber_locations") or 0)
    return (fiber / total) if total > 0 else None


def availability_share(
    location_availability: list[dict[str, object]] | None,
    *,
    tech_key: str,
) -> float | None:
    """Generic household-availability share for any tech.

    `tech_key` is one of 'fiber', 'cable', 'dsl', 'fw', 'sat'.
    """
    if not location_availability:
        return None
    col = f"{tech_key}_locations"
    total = 0
    matched = 0
    for row in location_availability:
        total += int(row.get("total_locations") or 0)
        matched += int(row.get(col) or 0)
    return (matched / total) if total > 0 else None
