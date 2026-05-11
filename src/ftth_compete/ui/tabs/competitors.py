"""Competitors tab: provider card grid + sortable table view.

The tab people will spend the most time on. Two views:

- **Cards** (default): one bordered card per canonical provider, sorted by
  the user's chosen metric. Card shows category badge, tech tags, coverage %
  with a progress bar, max advertised speeds, locations served, holding
  company, and raw brand names.
- **Table**: same data flattened into a sortable dataframe.

Top controls let the user sort, filter by category, restrict to fiber-only,
and swap views.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import polars as pl
import streamlit as st

from ftth_compete.analysis.competitors import ProviderSummary
from ftth_compete.analysis.lenses import Lens, ScoredProvider
from ftth_compete.analysis.lenses import apply as apply_lens
from ftth_compete.format import fmt_int, fmt_pct, fmt_speed
from ftth_compete.pipeline import TearSheet

# Category presentation. Streamlit's :badge[text] markdown supports a fixed
# palette: blue, green, orange, red, violet, gray, rainbow, primary.
_CATEGORY_DISPLAY: Final[dict[str, tuple[str, str]]] = {
    "national_fiber": ("National Fiber", "green"),
    "regional_fiber": ("Regional Fiber", "green"),
    "cable": ("Cable", "orange"),
    "fixed_wireless": ("Fixed Wireless", "blue"),
    "satellite": ("Satellite", "violet"),
    "muni": ("Municipal", "gray"),
    "unknown": ("Unknown", "gray"),
}

_TECH_COLOR: Final[dict[str, str]] = {
    "Fiber": "green",
    "Cable": "orange",
    "DSL": "gray",
    "Licensed FW": "blue",
    "Unlicensed FW": "blue",
    "Licensed-by-Rule FW": "blue",
    "GSO Satellite": "violet",
    "Non-GSO Satellite": "violet",
}

_SORT_OPTIONS: Final[dict[str, str]] = {
    "Coverage % (desc)": "coverage_desc",
    "Locations served (desc)": "locations_desc",
    "Max advertised down (desc)": "speed_desc",
    "Fiber-first, then coverage": "fiber_first",
    "Name (A-Z)": "name_asc",
}


@dataclass(frozen=True)
class _Filters:
    sort_key: str
    categories: list[str]
    fiber_only: bool
    view: str


def render_competitors(
    sheet: TearSheet,
    *,
    lens: Lens = Lens.NEUTRAL,
    incumbent: str | None = None,
) -> None:
    """Render the Competitors tab content for a TearSheet.

    `lens` re-ranks the provider list through a strategic perspective.
    `incumbent` is required when lens=Lens.DEFENSIVE.
    """
    # Build a per-(provider, tech) coverage_matrix lookup so each card's
    # drill-down section can render per-tract presence without re-querying.
    cov_by_key: dict[tuple[str, int], list[dict]] = {}
    raw_to_canonical: dict[str, str] = {}
    if sheet.providers:
        for p in sheet.providers:
            for raw in p.raw_brand_names:
                raw_to_canonical[raw] = p.canonical_name
    for row in sheet.coverage_matrix:
        raw = row.get("brand_name")
        canonical = raw_to_canonical.get(str(raw)) if raw else None
        if canonical is None:
            continue
        try:
            tech_code = int(row.get("technology"))
        except (TypeError, ValueError):
            continue
        cov_by_key.setdefault((canonical, tech_code), []).append(row)

    # Velocity (12-month coverage delta) lookup, opt-in.
    velocity_by_key: dict[tuple[str, int], dict] = {
        (v["canonical_name"], v["tech_code"]): v
        for v in (sheet.provider_velocity or [])
    }

    # Multi-release trajectory lookup, opt-in.
    trajectory_by_key: dict[tuple[str, int], dict] = {
        (t["canonical_name"], t["tech_code"]): t
        for t in (sheet.provider_trajectory or [])
    }
    if sheet.providers is None:
        if sheet.providers_note:
            st.warning(sheet.providers_note)
        else:
            st.info("Provider data not loaded.")
        return

    if not sheet.providers:
        st.warning("No providers found in FCC BDC for this market.")
        return

    ratings = sheet.provider_ratings or {}
    _lens_banner(lens, incumbent)

    # Build a (canonical_name, technology) -> subs estimate lookup so each
    # card / table row can render its specific tech's penetration estimate.
    subs_by_key: dict[tuple[str, str], dict] = {
        (s["canonical_name"], s["technology"]): s for s in (sheet.provider_subs or [])
    }

    # Apply lens scoring up front. Score-based ordering supersedes the
    # user's "Sort by" choice when an active lens is in use.
    scored = apply_lens(sheet.providers, lens, incumbent=incumbent, rating_lookup=ratings)
    score_by_name = {s.provider.canonical_name: s for s in scored}

    filters = _controls(sheet.providers, lens_active=(lens != Lens.NEUTRAL))
    visible = _apply_filters(sheet.providers, filters)
    if lens != Lens.NEUTRAL:
        # Re-sort the user's filtered list to match lens order, keeping the
        # filtering they chose.
        order = {s.provider.canonical_name: i for i, s in enumerate(scored)}
        visible.sort(key=lambda p: order.get(p.canonical_name, len(order)))

    _summary_strip(visible, sheet.providers)

    if sheet.ratings_note:
        st.caption(f"Ratings: {sheet.ratings_note}")

    if filters.view == "Table":
        _render_table(visible, ratings, score_by_name, subs_by_key, velocity_by_key)
    else:
        _render_cards(
            visible, ratings, score_by_name, subs_by_key, cov_by_key,
            velocity_by_key, trajectory_by_key,
        )


def _lens_banner(lens: Lens, incumbent: str | None) -> None:
    if lens == Lens.NEUTRAL:
        return
    if lens == Lens.DEFENSIVE:
        if incumbent:
            st.info(
                f"⚔️ **Incumbent-defensive view** — defending **{incumbent}**. "
                "Other providers ranked by *threat* (fiber attack potential + "
                "coverage + Google rating advantage)."
            )
        else:
            st.warning(
                "⚔️ Incumbent-defensive lens active but no incumbent picked. "
                "Pick one in the sidebar to see threat scores."
            )
    elif lens == Lens.OFFENSIVE:
        st.info(
            "🚀 **New-entrant-offensive view** — providers ranked by "
            "*vulnerability to disruption* (no-fiber, cable-only, low ratings). "
            "Top-of-list = easiest entry target."
        )


# ---------------------------------------------------------------------------
# Controls

def _controls(providers: list[ProviderSummary], *, lens_active: bool = False) -> _Filters:
    cols = st.columns([2, 3, 1, 1])
    with cols[0]:
        if lens_active:
            st.caption("Sort by")
            st.markdown("_Lens score (active)_")
            sort_label = "Fiber-first, then coverage"  # ignored when lens active
        else:
            sort_label = st.selectbox(
                "Sort by",
                list(_SORT_OPTIONS.keys()),
                index=3,  # default: fiber-first
            )
    with cols[1]:
        all_cats = sorted({p.category for p in providers})
        category_labels = [_CATEGORY_DISPLAY.get(c, (c, "gray"))[0] for c in all_cats]
        label_to_cat = dict(zip(category_labels, all_cats))
        chosen_labels = st.multiselect(
            "Filter by category",
            category_labels,
            default=category_labels,
            help="Hide categories you don't care about.",
        )
        chosen_cats = [label_to_cat[c] for c in chosen_labels]
    with cols[2]:
        fiber_only = st.checkbox("Fiber only", value=False)
    with cols[3]:
        view = st.radio("View", ["Cards", "Table"], horizontal=False, label_visibility="visible")

    return _Filters(
        sort_key=_SORT_OPTIONS[sort_label],
        categories=chosen_cats,
        fiber_only=fiber_only,
        view=view,
    )


def _apply_filters(
    providers: list[ProviderSummary], filters: _Filters
) -> list[ProviderSummary]:
    out = [
        p
        for p in providers
        if p.category in filters.categories and (not filters.fiber_only or p.has_fiber)
    ]
    return _sort(out, filters.sort_key)


def _sort(providers: list[ProviderSummary], key: str) -> list[ProviderSummary]:
    if key == "coverage_desc":
        return sorted(
            providers,
            key=lambda p: (-p.coverage_pct, -p.locations_served, p.canonical_name),
        )
    if key == "locations_desc":
        return sorted(providers, key=lambda p: (-p.locations_served, p.canonical_name))
    if key == "speed_desc":
        return sorted(
            providers,
            key=lambda p: (-(p.max_advertised_down or 0), p.canonical_name),
        )
    if key == "fiber_first":
        return sorted(
            providers,
            key=lambda p: (
                not p.has_fiber,
                -p.coverage_pct,
                -p.locations_served,
                p.canonical_name,
            ),
        )
    if key == "name_asc":
        return sorted(providers, key=lambda p: p.canonical_name)
    return providers


def _summary_strip(visible: list[ProviderSummary], all_providers: list[ProviderSummary]) -> None:
    """Small stat strip showing what's currently shown."""
    cols = st.columns(4)
    cols[0].metric(
        "Showing",
        f"{len(visible)} of {len(all_providers)}",
        help="Provider×technology rows matching the current filter / view.",
    )
    fiber_rows = sum(1 for p in visible if p.has_fiber)
    cols[1].metric(
        "Fiber rows",
        str(fiber_rows),
        help="Count of (provider, tech) rows where tech is Fiber (50). One provider can have multiple fiber rows if they appear under different brand names; usually equals distinct fiber providers.",
    )
    full_coverage = sum(1 for p in visible if p.coverage_pct >= 0.99)
    cols[2].metric(
        "Full coverage",
        str(full_coverage),
        help="Providers serving every analysis tract at this tech.",
    )
    if visible:
        max_down = max((p.max_advertised_down or 0) for p in visible)
        cols[3].metric(
            "Top advertised",
            fmt_speed(max_down),
            help="Max advertised download across all visible rows. Marketing claim — see Map tab's measured-speed layers for what users actually get.",
        )
    else:
        cols[3].metric("Top advertised", "—")
    st.divider()


# ---------------------------------------------------------------------------
# Card view

def _render_cards(
    providers: list[ProviderSummary],
    ratings: dict[str, dict | None],
    score_by_name: dict[str, ScoredProvider],
    subs_by_key: dict[tuple[str, str], dict],
    cov_by_key: dict[tuple[str, int], list[dict]],
    velocity_by_key: dict[tuple[str, int], dict],
    trajectory_by_key: dict[tuple[str, int], dict],
) -> None:
    if not providers:
        st.info("No providers match the current filters.")
        return

    # Lens scoring keys by canonical_name (rating-aware), so when a provider
    # appears as multiple tech rows (e.g. Lumen Fiber + Lumen DSL) both cards
    # show the same lens badge.
    cols_per_row = 3
    rows = [providers[i : i + cols_per_row] for i in range(0, len(providers), cols_per_row)]
    for row in rows:
        cols = st.columns(cols_per_row)
        for i, provider in enumerate(row):
            with cols[i]:
                _render_card(
                    provider,
                    ratings.get(provider.canonical_name),
                    score_by_name.get(provider.canonical_name),
                    subs_by_key.get((provider.canonical_name, provider.technology)),
                    cov_by_key.get((provider.canonical_name, provider.tech_code), []),
                    velocity_by_key.get((provider.canonical_name, provider.tech_code)),
                    trajectory_by_key.get((provider.canonical_name, provider.tech_code)),
                )


def _render_card(
    p: ProviderSummary,
    rating: dict | None,
    scored: ScoredProvider | None,
    subs: dict | None,
    coverage_rows: list[dict],
    velocity: dict | None,
    trajectory: dict | None = None,
) -> None:
    cat_label, cat_color = _CATEGORY_DISPLAY.get(p.category, (p.category, "gray"))
    tech_color = _TECH_COLOR.get(p.technology, "gray")

    with st.container(border=True):
        # Header: "Provider — Technology" so multi-tech providers split cleanly
        # into distinct cards (e.g. "Lumen / Quantum Fiber — Fiber" vs DSL).
        # Smaller header (####) keeps cards from overwhelming the grid.
        score_badge = _score_badge(scored)
        header_extras = (
            f":{cat_color}-badge[{cat_label}] · "
            f":{tech_color}-badge[{p.technology}]"
        )
        if score_badge:
            header_extras = f"{score_badge} · {header_extras}"
        st.markdown(
            f"#### {p.canonical_name} — {p.technology}\n"
            f"{header_extras}  \n"
            f"<span style='color:#6B7280;font-size:0.85em'>{p.holding_company}</span>",
            unsafe_allow_html=True,
        )

        # Coverage bar
        st.markdown(f"**Coverage** &nbsp; {fmt_pct(p.coverage_pct)} of tracts")
        st.progress(min(max(p.coverage_pct, 0.0), 1.0))

        # Speed + locations metrics — scoped to THIS tech only
        m_cols = st.columns(2)
        m_cols[0].metric(
            "Max down (advertised)",
            fmt_speed(p.max_advertised_down),
            help=(
                f"FCC BDC `max_advertised_download_speed` for {p.canonical_name} "
                f"at {p.technology.lower()} tech. **Marketing claim, not measured** — "
                "see Map tab for Ookla-measured speeds."
            ),
        )
        m_cols[1].metric(
            "Locations served",
            fmt_int(p.locations_served),
            help=(
                f"Distinct BSL (FCC location_id) count across analysis tracts "
                f"where {p.canonical_name} offers {p.technology.lower()} service.\n\n"
                "Multi-tech providers (e.g. Lumen offering both DSL and Fiber) "
                "appear as separate cards with their own location counts."
            ),
        )

        # Speed tier breakdown (gig+ / 100Mbps+ / <100Mbps)
        _render_speed_tiers(p)

        # Google rating
        _render_rating_line(rating)

        # Estimated subscribers (heuristic from national take rate)
        _render_subs_line(subs)

        # 12-month coverage velocity (opt-in)
        _render_velocity_line(velocity)

        # Multi-release sparkline (opt-in)
        _render_trajectory_sparkline(trajectory)

        # Raw brand names (only if interestingly different from canonical)
        if p.raw_brand_names and not (
            len(p.raw_brand_names) == 1 and p.raw_brand_names[0] == p.canonical_name
        ):
            st.caption(f"Raw BDC brand names: {', '.join(p.raw_brand_names)}")

        # Per-tract drill-down (collapsed by default to keep cards scannable)
        if coverage_rows:
            _render_drilldown(p, coverage_rows)


def _render_drilldown(p: ProviderSummary, coverage_rows: list[dict]) -> None:
    """Per-tract detail for a single (provider, tech): tract list, locations,
    max speeds, plus per-tract speed variance for diagnostic purposes.
    """
    with st.expander(f"Per-tract detail ({p.tracts_served} tract{'s' if p.tracts_served != 1 else ''})"):
        rows = []
        for r in coverage_rows:
            rows.append(
                {
                    "Tract GEOID": r.get("tract_geoid"),
                    "Locations": int(r.get("locations_served") or 0),
                    "Max down (Mbps)": (
                        float(r.get("max_down")) if r.get("max_down") is not None else None
                    ),
                    "Max up (Mbps)": (
                        float(r.get("max_up")) if r.get("max_up") is not None else None
                    ),
                    "Low latency": bool(r.get("any_low_latency")),
                }
            )
        # Sort by locations served desc — biggest tracts first
        rows.sort(key=lambda x: -(int(x.get("Locations") or 0)))

        if not rows:
            st.caption("No per-tract rows in BDC for this provider×tech.")
            return

        df = pl.DataFrame(rows)
        st.dataframe(
            df,
            width="stretch",
            hide_index=True,
            column_config={
                "Locations": st.column_config.NumberColumn(format="%d"),
                "Max down (Mbps)": st.column_config.NumberColumn(format="%d"),
                "Max up (Mbps)": st.column_config.NumberColumn(format="%d"),
            },
        )

        # Quick variance check: do all tracts get the same speed?
        downs = [
            r["Max down (Mbps)"] for r in rows if r["Max down (Mbps)"] is not None
        ]
        if downs:
            uniq = sorted(set(downs))
            if len(uniq) > 1:
                st.caption(
                    f"Speed varies across tracts: {len(uniq)} distinct max-down values "
                    f"({fmt_speed(min(uniq))} → {fmt_speed(max(uniq))})."
                )


def _render_speed_tiers(p: ProviderSummary) -> None:
    """Render a 3-segment tier breakdown bar: gig+ / 100Mbps+ / <100Mbps.

    Each segment is a colored stripe sized by its share of THIS provider's
    locations at this technology. Skipped if all tier counts are zero (e.g.,
    older fixtures or BDC queries that didn't populate them).
    """
    gig = p.gig_locations
    hund = p.hundred_locations
    slow = p.sub_hundred_locations
    total = gig + hund + slow
    if total <= 0:
        return
    gig_pct = gig / total
    hund_pct = hund / total
    slow_pct = slow / total
    # Stacked colored bar via inline HTML — Streamlit doesn't have a native
    # stacked progress component. Heights stay short to keep cards compact.
    bar = (
        "<div style='display:flex;height:10px;width:100%;border-radius:3px;"
        "overflow:hidden;margin:4px 0 2px 0;'>"
        f"<div style='width:{gig_pct * 100:.2f}%;background:#2e8b57' title='Gig+'></div>"
        f"<div style='width:{hund_pct * 100:.2f}%;background:#4F8FBA' title='100Mbps+'></div>"
        f"<div style='width:{slow_pct * 100:.2f}%;background:#B0B0B0' title='<100Mbps'></div>"
        "</div>"
    )
    legend = (
        f"<span style='color:#6B7280;font-size:0.8em'>"
        f"<span style='color:#2e8b57'>■</span> Gig+ {fmt_pct(gig_pct, decimals=0)} · "
        f"<span style='color:#4F8FBA'>■</span> 100Mbps+ {fmt_pct(hund_pct, decimals=0)} · "
        f"<span style='color:#B0B0B0'>■</span> &lt;100 {fmt_pct(slow_pct, decimals=0)}"
        "</span>"
    )
    st.markdown(bar + legend, unsafe_allow_html=True)


def _render_rating_line(rating: dict | None) -> None:
    """Render a single line for the Google rating, if present."""
    if not rating or rating.get("rating") is None:
        return
    val = float(rating["rating"])
    count = rating.get("user_rating_count") or 0
    color = "green" if val >= 4.0 else ("orange" if val >= 3.0 else "red")
    stars = "★" * int(round(val)) + "☆" * (5 - int(round(val)))
    url = rating.get("place_url")
    line = f":{color}-badge[Google {val:.1f} {stars}] · {count:,} reviews"
    if url:
        line += f" · [view on Maps]({url})"
    st.markdown(line)


def _render_velocity_line(velocity: dict | None) -> None:
    """Render a 12-month coverage-delta line, if velocity data was loaded."""
    if not velocity:
        return
    delta_abs = int(velocity.get("delta_abs") or 0)
    delta_pct = velocity.get("delta_pct")
    new_offering = velocity.get("new_offering")
    discontinued = velocity.get("discontinued")
    prev_release = velocity.get("prev_release", "")
    cur_release = velocity.get("current_release", "")

    if new_offering:
        st.markdown(f":green-badge[NEW since {prev_release}]  +{delta_abs:,} locations")
    elif discontinued:
        st.markdown(f":red-badge[Discontinued since {prev_release}]  -{abs(delta_abs):,} locations")
    elif delta_abs > 0:
        pct_str = f"+{delta_pct:.0%}" if delta_pct is not None else f"+{delta_abs:,}"
        st.markdown(
            f":green-badge[+{delta_abs:,} locations / {pct_str} YoY]  "
            f"_{prev_release} → {cur_release}_"
        )
    elif delta_abs < 0:
        pct_str = f"{delta_pct:.0%}" if delta_pct is not None else f"{delta_abs:,}"
        st.markdown(
            f":orange-badge[{delta_abs:,} locations / {pct_str} YoY]  "
            f"_{prev_release} → {cur_release}_"
        )
    else:
        st.caption(f"Coverage flat vs {prev_release}.")


def _render_trajectory_sparkline(trajectory: dict | None) -> None:
    """Render a small sparkline showing per-release location count.

    Uses an inline SVG polyline so we don't pay a Plotly render cost per
    card. Trajectory data is `{series: [{release, locations}, ...]}` sorted
    chronologically ASC by the producer.
    """
    if not trajectory:
        return
    series = trajectory.get("series") or []
    if len(series) < 2:
        return
    locs = [int(s.get("locations") or 0) for s in series]
    labels = [s.get("release", "?") for s in series]
    if max(locs) <= 0:
        return

    w, h = 160, 32
    pad = 2
    vmax = max(locs)
    n = len(locs)
    xs = [pad + (w - 2 * pad) * i / (n - 1) for i in range(n)]
    ys = [h - pad - (h - 2 * pad) * (v / vmax) for v in locs]
    points = " ".join(f"{x:.1f},{y:.1f}" for x, y in zip(xs, ys))
    # Per-point dots so endpoints + intermediate releases are visible.
    dots = "".join(
        f"<circle cx='{x:.1f}' cy='{y:.1f}' r='2' fill='#2e8b57'/>"
        for x, y in zip(xs, ys)
    )
    svg = (
        f"<svg width='{w}' height='{h}' style='vertical-align:middle'>"
        f"<polyline points='{points}' fill='none' stroke='#2e8b57' stroke-width='1.5'/>"
        f"{dots}"
        "</svg>"
    )
    range_caption = (
        f"<span style='color:#6B7280;font-size:0.8em;margin-left:6px'>"
        f"{labels[0]} → {labels[-1]} · {locs[0]:,} → {locs[-1]:,} locations"
        "</span>"
    )
    st.markdown(
        f"<div style='display:flex;align-items:center'>{svg}{range_caption}</div>",
        unsafe_allow_html=True,
    )


def _render_subs_line(subs: dict | None) -> None:
    """Render an 'Estimated subscribers' line with confidence label."""
    if not subs:
        return
    mid = subs.get("estimate_mid")
    low = subs.get("estimate_low")
    high = subs.get("estimate_high")
    confidence = subs.get("confidence", "")
    take_rate = subs.get("take_rate", 0.0)
    source = subs.get("source", "")
    if mid is None:
        return
    color = {"medium": "blue", "low": "gray", "high": "green"}.get(confidence, "gray")
    range_str = f"~{int(mid):,}  (range {int(low):,}–{int(high):,})"
    st.markdown(
        f":{color}-badge[Est. subs · {confidence} confidence]  **{range_str}**",
        help=(
            f"Heuristic estimate: locations × national take rate ({take_rate:.0%}) ±25%.\n\n"
            f"Source: {source}\n\n"
            f"Per-market deviation from national rate is the main source of error. "
            f"For ground-truth subscriber counts we'd need IAS data ingestion (deferred)."
        ),
    )


def _score_badge(scored: ScoredProvider | None) -> str | None:
    """Return a markdown badge for a lens score, or None if not applicable."""
    if scored is None:
        return None
    if scored.is_incumbent:
        return ":blue-badge[Incumbent]"
    if scored.score is None or scored.score_label is None:
        return None
    # Pick badge color by score band: high score = red (defensive: big threat;
    # offensive: prime target).
    s = scored.score
    if s >= 0.7:
        color = "red"
    elif s >= 0.4:
        color = "orange"
    else:
        color = "gray"
    return f":{color}-badge[{scored.score_label}]"


# ---------------------------------------------------------------------------
# Table view

def _render_table(
    providers: list[ProviderSummary],
    ratings: dict[str, dict | None],
    score_by_name: dict[str, ScoredProvider],
    subs_by_key: dict[tuple[str, str], dict],
    velocity_by_key: dict[tuple[str, int], dict],
) -> None:
    if not providers:
        st.info("No providers match the current filters.")
        return

    show_score_col = any(
        s.score is not None or s.is_incumbent for s in score_by_name.values()
    )

    show_velocity_col = bool(velocity_by_key)

    rows = []
    for p in providers:
        r = ratings.get(p.canonical_name) or {}
        scored = score_by_name.get(p.canonical_name)
        subs = subs_by_key.get((p.canonical_name, p.technology)) or {}
        vel = velocity_by_key.get((p.canonical_name, p.tech_code)) or {}
        row: dict[str, object] = {
            "Provider": p.canonical_name,
            "Technology": p.technology,
            "Category": _CATEGORY_DISPLAY.get(p.category, (p.category, ""))[0],
            "Holding company": p.holding_company,
            "Coverage %": p.coverage_pct,
            "Locations": p.locations_served,
            "Max down (Mbps)": p.max_advertised_down,
            "Max up (Mbps)": p.max_advertised_up,
            "Gig+ %": (
                p.gig_locations / p.locations_served
                if p.locations_served else None
            ),
            "100Mbps+ %": (
                p.hundred_locations / p.locations_served
                if p.locations_served else None
            ),
            "Est. subs (mid)": subs.get("estimate_mid"),
            "Est. subs range": (
                f"{int(subs['estimate_low']):,}–{int(subs['estimate_high']):,}"
                if subs.get("estimate_low") is not None else None
            ),
            "Confidence": subs.get("confidence"),
            "Google rating": r.get("rating"),
            "Reviews": r.get("user_rating_count"),
            "Raw brand names": " | ".join(p.raw_brand_names),
        }
        if show_velocity_col:
            row["12mo Δ locations"] = vel.get("delta_abs")
            row["12mo Δ %"] = vel.get("delta_pct")
        if show_score_col:
            if scored and scored.is_incumbent:
                row["Lens score"] = None
                row["Lens label"] = "Incumbent"
            elif scored and scored.score is not None:
                row["Lens score"] = scored.score
                row["Lens label"] = scored.score_label
            else:
                row["Lens score"] = None
                row["Lens label"] = None
        rows.append(row)

    df = pl.DataFrame(rows)
    column_config: dict[str, object] = {
        "Coverage %": st.column_config.ProgressColumn(
            "Coverage %", format="%.0f%%", min_value=0.0, max_value=1.0,
        ),
        "Locations": st.column_config.NumberColumn(format="%d"),
        "Max down (Mbps)": st.column_config.NumberColumn(format="%d"),
        "Max up (Mbps)": st.column_config.NumberColumn(format="%d"),
        "Gig+ %": st.column_config.ProgressColumn(
            "Gig+ %", format="%.0f%%", min_value=0.0, max_value=1.0,
            help="Share of this provider's locations at >=1 Gbps advertised down.",
        ),
        "100Mbps+ %": st.column_config.ProgressColumn(
            "100Mbps+ %", format="%.0f%%", min_value=0.0, max_value=1.0,
            help="Share at 100-999 Mbps advertised down (excludes gig+).",
        ),
        "Est. subs (mid)": st.column_config.NumberColumn(
            format="%d",
            help="Estimated subscribers (mid). Heuristic: locations × national take rate from 10-K.",
        ),
        "Google rating": st.column_config.NumberColumn(format="%.1f ★"),
        "Reviews": st.column_config.NumberColumn(format="%d"),
    }
    if show_velocity_col:
        column_config["12mo Δ locations"] = st.column_config.NumberColumn(
            format="%+d",
            help="Change in locations served vs ~12 months ago (prior BDC release).",
        )
        column_config["12mo Δ %"] = st.column_config.NumberColumn(
            format="%+.0f%%",
            help="Percent change vs 12 months ago.",
        )
    if show_score_col:
        column_config["Lens score"] = st.column_config.ProgressColumn(
            "Lens score", format="%.2f", min_value=0.0, max_value=1.0,
        )
    st.dataframe(
        df,
        width="stretch",
        hide_index=True,
        column_config=column_config,
    )
