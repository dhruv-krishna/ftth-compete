"""Overview tab: KPI cards + generated narrative + tract details."""

from __future__ import annotations

import streamlit as st

from ftth_compete.analysis.lenses import Lens, market_opportunity
from ftth_compete.format import fmt_currency, fmt_int, fmt_pct, fmt_speed
from ftth_compete.pipeline import TearSheet
from ftth_compete.ui.narrative import (
    availability_share,
    fiber_availability_share,
    fiber_share,
    market_narrative,
)


def render_overview(sheet: TearSheet, *, lens: Lens = Lens.NEUTRAL) -> None:
    """Render the Overview tab content for a TearSheet.

    When `lens=Lens.OFFENSIVE`, an additional market-opportunity panel
    surfaces the composite "is this a good entry target?" score with
    factor breakdown.
    """

    # Top: market title and one-liner
    n_tracts = sheet.demographics.n_tracts
    n_boundary = len(sheet.tracts.get("boundary", []))
    sub = f"{n_tracts} census tract{'s' if n_tracts != 1 else ''} analyzed"
    if n_boundary:
        sub += f" · {n_boundary} boundary tract{'s' if n_boundary != 1 else ''} excluded"
    sub += " · see **📖 Methodology** tab for sources & caveats"
    st.caption(sub)

    st.markdown("##### Market scale and demographics")
    # KPI cards row 1: market scale + economics
    cols = st.columns(4)
    cols[0].metric("Population", fmt_int(sheet.demographics.population))
    cols[1].metric(
        "Median HH income",
        fmt_currency(sheet.demographics.median_household_income_weighted),
        help=(
            "Population-weighted mean of tract medians (ACS B19013).\n\n"
            "**Caveat:** this is a proxy for the true market median; computing "
            "the exact market median requires raw ACS PUMS data."
        ),
    )
    cols[2].metric(
        "Poverty rate",
        fmt_pct(sheet.demographics.poverty_rate),
        help=(
            "Share of poverty-universe population below the federal poverty "
            "line (ACS B17001).\n\nNational average is ~12.4%."
        ),
    )
    cols[3].metric(
        "Housing units",
        fmt_int(sheet.demographics.housing_units_total),
        help="Total housing units across analyzed tracts (ACS B25001).",
    )

    st.markdown("")  # vertical spacer
    st.markdown("##### Competitive landscape")
    # KPI cards row 2: housing mix + competitive landscape
    cols = st.columns(4)
    cols[0].metric(
        "MDU share",
        fmt_pct(sheet.housing.mdu_share),
        help=(
            f"Share of housing units in 2+ unit structures (ACS B25024). "
            f"{fmt_int(sheet.housing.mdu_small + sheet.housing.mdu_mid + sheet.housing.mdu_large)} MDU units "
            f"of {fmt_int(sheet.housing.total)} total.\n\n"
            f"SFH share = {fmt_pct(sheet.housing.sfh_share)}, "
            f"Mobile/other = {fmt_pct(sheet.housing.other_share)}.\n\n"
            f"National MDU share is ~27%."
        ),
    )
    # Distinct canonical providers (sheet.providers is per-(provider, tech))
    n_providers = (
        len({p.canonical_name for p in sheet.providers}) if sheet.providers else 0
    )
    cols[1].metric(
        "Providers",
        str(n_providers),
        help="Distinct canonical providers serving any location in the analysis tracts (FCC BDC). Multi-tech providers (e.g. Lumen offering both DSL and Fiber) count once.",
    )
    # Household-availability metric — the share of locations where fiber is
    # offered by AT LEAST ONE provider. Distinct from "% of providers offering
    # fiber" which is a provider-count metric.
    fiber_avail = fiber_availability_share(sheet.location_availability)
    fiber_prov_share = fiber_share(sheet.providers)
    fiber_prov_count = (
        sum(1 for n in {p.canonical_name for p in sheet.providers} if any(
            q.canonical_name == n and q.has_fiber for q in sheet.providers
        )) if sheet.providers else 0
    )
    cols[2].metric(
        "Fiber available",
        fmt_pct(fiber_avail) if fiber_avail is not None else "—",
        help=(
            "Share of locations (FCC BSLs) where fiber is offered by at least one "
            "provider — i.e. where homes COULD get fiber. Includes addresses currently "
            "subscribed to other tech.\n\n"
            f"Reference: {fiber_prov_count} of {n_providers} providers offer fiber "
            f"({fmt_pct(fiber_prov_share)} of providers). The household-availability "
            "and provider-count metrics often diverge — a few fiber providers can cover "
            "most of a market."
        ),
    )
    cols[3].metric(
        "Boundary tracts",
        fmt_int(n_boundary),
        help=(
            "Tracts whose polygon intersects the city but whose centroid "
            "falls outside. Excluded from analysis by default.\n\n"
            "Toggle **Include boundary tracts** in the sidebar to include "
            "them in all metrics."
        ),
    )

    # KPI cards row 3: full availability breakdown by tech, when we have BSL data
    if sheet.location_availability:
        st.divider()
        st.markdown("##### Location-level technology availability")
        st.caption(
            "Share of FCC BSLs (Broadband Serviceable Locations) where at least "
            "one provider offers each technology. Computed from BDC location_id-level data."
        )
        avail_cols = st.columns(5)
        for i, (label, key, helptext) in enumerate(
            [
                ("Fiber", "fiber", "Locations where any provider offers FTTP (tech 50)."),
                ("Cable", "cable", "Locations where any cable provider serves (HFC, tech 40)."),
                ("DSL", "dsl", "Locations where any copper/DSL provider serves (tech 10)."),
                ("Fixed wireless", "fw", "Locations where any fixed wireless provider serves (tech 70/71/72)."),
                ("Satellite", "sat", "Locations where any satellite provider serves (tech 60/61). Usually ~100%."),
            ]
        ):
            share = availability_share(sheet.location_availability, tech_key=key)
            avail_cols[i].metric(
                label,
                fmt_pct(share) if share is not None else "—",
                help=helptext,
            )

    # IAS subscription anchor — separates "homes with broadband available" from
    # "homes that subscribe." When loaded, surfaces the market-level take rate
    # and confirms the per-provider subs estimates are IAS-calibrated.
    if sheet.market_subs_anchor:
        a = sheet.market_subs_anchor
        st.divider()
        st.markdown("##### Broadband subscription density (FCC IAS)")
        st.caption(
            "Aggregate broadband adoption from FCC IAS tract-level subscription "
            "buckets. **All-tech and all-provider** — not a per-fiber-provider "
            "take rate. See Methodology tab for bucket-math caveats."
        )
        s_cols = st.columns(4)
        s_cols[0].metric(
            "Take rate (≥25/3 Mbps)",
            fmt_pct(a.get("take_rate_mid")),
            help=(
                f"Share of housing units in this market with an active "
                f"≥25/3 Mbps broadband connection. From FCC IAS tract-level "
                f"data, release {a.get('ias_release')}.\n\n"
                f"Range (low-high): "
                f"{fmt_pct(a.get('take_rate_low'))} - {fmt_pct(a.get('take_rate_high'))} "
                f"(FCC publishes bucketed ratios, not raw counts)."
            ),
        )
        s_cols[1].metric(
            "Estimated total subs",
            fmt_int(a.get("market_subs_mid")),
            help=(
                f"Mid estimate of total broadband subscribers across the market. "
                f"Range: {fmt_int(a.get('market_subs_low'))} - "
                f"{fmt_int(a.get('market_subs_high'))}."
            ),
        )
        s_cols[2].metric(
            "Housing units (denominator)",
            fmt_int(a.get("total_housing_units")),
        )
        s_cols[3].metric(
            "IAS data as-of",
            str(a.get("ias_release") or "—"),
            help=(
                "FCC IAS publishes tract-level subscription data with a 1-2yr lag. "
                "Drop a newer release into data/raw/ias/ to upgrade automatically. "
                "Newest auto-downloadable release is Jun 2022; newer releases are "
                "Box-hosted at us-fcc.box.com (manual download)."
            ),
        )
    elif sheet.ias_note:
        st.caption(f"IAS subscription anchor: {sheet.ias_note}")

    # Measured-speed strip (Ookla) - only if data is loaded
    if sheet.tract_speeds:
        st.divider()
        downs = [t.get("median_down_mbps") for t in sheet.tract_speeds if t.get("median_down_mbps")]
        ups = [t.get("median_up_mbps") for t in sheet.tract_speeds if t.get("median_up_mbps")]
        lats = [t.get("median_lat_ms") for t in sheet.tract_speeds if t.get("median_lat_ms") is not None]
        total_tests = sum(int(t.get("n_tests") or 0) for t in sheet.tract_speeds)
        st.markdown("### Measured network reality (Ookla speedtest)")
        s_cols = st.columns(4)
        s_cols[0].metric(
            "Median measured down (across tracts)",
            fmt_speed(sum(downs) / len(downs)) if downs else "—",
            help="Average of tract-level medians. What users actually get, not what's advertised.",
        )
        s_cols[1].metric(
            "Median measured up",
            fmt_speed(sum(ups) / len(ups)) if ups else "—",
        )
        s_cols[2].metric(
            "Median latency",
            f"{int(sum(lats) / len(lats))} ms" if lats else "—",
        )
        s_cols[3].metric(
            "Speedtest sample",
            f"{total_tests:,} tests",
            help="Total Ookla tests aggregated across all tracts (~1 quarter of data).",
        )
    elif sheet.speeds_note:
        st.caption(f"Speed data: {sheet.speeds_note}")

    # 12-month velocity highlights: top fiber growers / decliners.
    # Shown when velocity data is loaded (sidebar opt-in). Surfaces the
    # year-over-year story prominently on the Overview tab.
    if sheet.provider_velocity:
        st.divider()
        _render_velocity_highlights(sheet)

    # Lens-specific panel: market-level opportunity score (offensive lens only)
    if lens == Lens.OFFENSIVE and sheet.providers:
        st.divider()
        result = market_opportunity(
            sheet.providers,
            rating_lookup=sheet.provider_ratings or {},
            mdu_share=sheet.housing.mdu_share,
        )
        if result.get("score") is not None:
            st.markdown("### 🚀 Market opportunity (entrant view)")
            score = float(result["score"])
            cols = st.columns([1, 2])
            with cols[0]:
                color = "green" if score >= 0.55 else ("orange" if score >= 0.35 else "red")
                st.markdown(
                    f"**{result['headline']}**  \n"
                    f":{color}-badge[Score {score:.2f} / 1.00]"
                )
            with cols[1]:
                factors = result.get("factors") or {}
                st.markdown(
                    f"- No-fiber providers: {fmt_pct(factors.get('no_fiber_share'))}\n"
                    f"- Cable-only providers: {fmt_pct(factors.get('cable_only_share'))}\n"
                    f"- Average rating weakness: {fmt_pct(factors.get('rating_weakness'))}\n"
                    f"- MDU build economics: {fmt_pct(factors.get('mdu_score'))}"
                )

    # Generated narrative
    st.divider()
    st.markdown("##### Snapshot")
    st.write(market_narrative(sheet))

    # Tract details expander
    with st.expander(f"Tract details ({n_tracts} inside-city, {n_boundary} boundary)"):
        if sheet.tracts.get("inside_city"):
            st.write("**Inside city (analyzed):**")
            st.code(", ".join(sheet.tracts["inside_city"]), language="text")
        if sheet.tracts.get("boundary"):
            st.write("**Boundary (intersects city, centroid outside):**")
            st.code(", ".join(sheet.tracts["boundary"]), language="text")

    # Provider note (e.g., FCC creds missing)
    if sheet.providers_note:
        st.warning(sheet.providers_note)


def _render_velocity_highlights(sheet: TearSheet) -> None:
    """Top fiber growers + decliners from 12-month BDC velocity data."""
    st.markdown("##### 12-month fiber footprint change")
    fiber_velos = [
        v for v in (sheet.provider_velocity or [])
        if v.get("tech_code") == 50
    ]
    if not fiber_velos:
        return

    # Top 3 expanders + bottom 3 contractors (excluding zero-delta rows)
    growers = sorted(
        (v for v in fiber_velos if (v.get("delta_abs") or 0) > 0),
        key=lambda v: -v["delta_abs"],
    )[:3]
    decliners = sorted(
        (v for v in fiber_velos if (v.get("delta_abs") or 0) < 0),
        key=lambda v: v["delta_abs"],
    )[:3]

    if not growers and not decliners:
        return

    prev_release = (
        growers[0]["prev_release"] if growers
        else decliners[0]["prev_release"]
    )
    cur_release = (
        growers[0]["current_release"] if growers
        else decliners[0]["current_release"]
    )
    st.caption(f"BDC release {prev_release} → {cur_release}")

    cols = st.columns(2)
    with cols[0]:
        st.markdown("**Top expanders**")
        if not growers:
            st.caption("No fiber providers expanded year-over-year.")
        for v in growers:
            delta = int(v["delta_abs"])
            pct = v.get("delta_pct")
            new = v.get("new_offering")
            if new:
                line = (
                    f":green-badge[NEW]  **{v['canonical_name']}**  "
                    f"+{delta:,} locations (no prior fiber)"
                )
            else:
                pct_str = f"{pct:+.0%}" if pct is not None else "—"
                line = (
                    f":green-badge[+{delta:,}]  **{v['canonical_name']}**  "
                    f"{pct_str} YoY  ({fmt_int(v['prev_locations'])} → "
                    f"{fmt_int(v['current_locations'])})"
                )
            st.markdown(line)
    with cols[1]:
        st.markdown("**Top decliners**")
        if not decliners:
            st.caption("No fiber providers contracted year-over-year.")
        for v in decliners:
            delta = int(v["delta_abs"])
            pct = v.get("delta_pct")
            disc = v.get("discontinued")
            if disc:
                line = (
                    f":red-badge[Discontinued]  **{v['canonical_name']}**  "
                    f"-{abs(delta):,} locations (no longer offering)"
                )
            else:
                pct_str = f"{pct:+.0%}" if pct is not None else "—"
                line = (
                    f":orange-badge[{delta:,}]  **{v['canonical_name']}**  "
                    f"{pct_str} YoY  ({fmt_int(v['prev_locations'])} → "
                    f"{fmt_int(v['current_locations'])})"
                )
            st.markdown(line)
