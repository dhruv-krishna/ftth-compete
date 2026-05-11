"""Compare tab: side-by-side view of multiple saved markets.

Markets get saved into the comparison set via a sidebar button. This tab
shows them aligned for direct strategic comparison:

- KPI grid (one row per market, one column per metric)
- Top providers per market (horizontal scrolling alignment)
- Lens-aware: when offensive lens is active, ranks markets by
  market-opportunity score
- Plotly comparison chart of key metrics across markets

The whole tab is read-only — it consumes saved TearSheets, doesn't
re-run any pipeline.
"""

from __future__ import annotations

import plotly.graph_objects as go
import polars as pl
import streamlit as st

from ftth_compete.analysis.lenses import Lens, market_opportunity
from ftth_compete.format import fmt_currency, fmt_int, fmt_pct, fmt_speed
from ftth_compete.pipeline import TearSheet
from ftth_compete.ui.narrative import fiber_availability_share, fiber_share


def render_compare(
    sheets: list[TearSheet],
    *,
    lens: Lens = Lens.NEUTRAL,
) -> None:
    """Render the Compare tab content."""
    if not sheets:
        st.info(
            "No markets saved for comparison yet.\n\n"
            "Look up a market, then click **Save to comparison** in the sidebar. "
            "Add 2 or more to see side-by-side analysis here."
        )
        return

    if len(sheets) == 1:
        st.info(
            f"Only 1 market saved (**{sheets[0].market['city']}, {sheets[0].market['state']}**). "
            "Save another market to see them compared side by side."
        )

    # Optional: sort markets by lens-relevant score
    if lens == Lens.OFFENSIVE:
        sheets = sorted(
            sheets,
            key=lambda s: -(_market_score(s) or 0.0),
        )
        st.caption("🚀 Markets ranked by entrant-opportunity score (offensive lens active).")

    _kpi_table(sheets)
    st.markdown("### Metric comparison")
    _comparison_chart(sheets)
    st.markdown("### Top providers per market")
    _providers_alignment(sheets)
    st.markdown("### Provider overlap")
    _provider_overlap(sheets)


def _market_score(sheet: TearSheet) -> float | None:
    if not sheet.providers:
        return None
    res = market_opportunity(
        sheet.providers,
        rating_lookup=sheet.provider_ratings or {},
        mdu_share=sheet.housing.mdu_share,
    )
    score = res.get("score")
    return float(score) if score is not None else None


def _kpi_table(sheets: list[TearSheet]) -> None:
    """One row per market, columns are the headline KPIs."""
    rows: list[dict[str, object]] = []
    for s in sheets:
        h = s.housing
        d = s.demographics
        # Distinct canonical providers (s.providers is per-(provider, tech))
        n_prov = (
            len({p.canonical_name for p in s.providers}) if s.providers else 0
        )
        downs = [
            t.get("median_down_mbps")
            for t in s.tract_speeds
            if t.get("median_down_mbps")
        ]
        avg_meas = sum(downs) / len(downs) if downs else None
        rows.append(
            {
                "Market": f"{s.market['city']}, {s.market['state']}",
                "Tracts": d.n_tracts,
                "Population": d.population,
                "Median HH income": d.median_household_income_weighted,
                "Poverty rate": d.poverty_rate,
                "MDU share": h.mdu_share,
                "Providers": n_prov,
                "Fiber available (locs)": fiber_availability_share(s.location_availability),
                "Fiber providers (share)": fiber_share(s.providers),
                "Median measured down (Mbps)": avg_meas,
                "Opportunity score": _market_score(s),
            }
        )

    df = pl.DataFrame(rows)
    st.dataframe(
        df,
        width="stretch",
        hide_index=True,
        column_config={
            "Tracts": st.column_config.NumberColumn(format="%d"),
            "Population": st.column_config.NumberColumn(format="%,d"),
            "Median HH income": st.column_config.NumberColumn(format="$%,d"),
            "Poverty rate": st.column_config.ProgressColumn(
                "Poverty rate", format="%.1f%%", min_value=0.0, max_value=0.5,
            ),
            "MDU share": st.column_config.ProgressColumn(
                "MDU share", format="%.0f%%", min_value=0.0, max_value=1.0,
            ),
            "Providers": st.column_config.NumberColumn(format="%d"),
            "Fiber available (locs)": st.column_config.ProgressColumn(
                "Fiber available (locs)",
                format="%.0f%%",
                min_value=0.0,
                max_value=1.0,
                help="% of locations where fiber is offered by at least one provider.",
            ),
            "Fiber providers (share)": st.column_config.ProgressColumn(
                "Fiber providers (share)",
                format="%.0f%%",
                min_value=0.0,
                max_value=1.0,
                help="% of canonical providers offering fiber.",
            ),
            "Median measured down (Mbps)": st.column_config.NumberColumn(format="%.0f"),
            "Opportunity score": st.column_config.ProgressColumn(
                "Opportunity score",
                format="%.2f",
                min_value=0.0,
                max_value=1.0,
                help="Composite entrant-attractiveness (offensive lens). Higher = better target.",
            ),
        },
    )


def _comparison_chart(sheets: list[TearSheet]) -> None:
    """Grouped bar chart of normalized KPIs for direct visual comparison."""
    if len(sheets) < 2:
        st.caption("Save at least 2 markets to see the comparison chart.")
        return

    labels = [f"{s.market['city']}, {s.market['state']}" for s in sheets]

    def _fiber_share_or_zero(s: TearSheet) -> float:
        v = fiber_share(s.providers)
        return float(v) if v is not None else 0.0

    metrics = [
        ("Poverty rate", lambda s: s.demographics.poverty_rate or 0.0),
        ("MDU share", lambda s: s.housing.mdu_share),
        ("% providers w/ fiber", _fiber_share_or_zero),
        ("Opportunity score", lambda s: _market_score(s) or 0.0),
    ]

    fig = go.Figure()
    for metric_name, fn in metrics:
        values = [float(fn(s)) for s in sheets]
        fig.add_trace(go.Bar(name=metric_name, x=labels, y=values))
    fig.update_layout(
        barmode="group",
        height=380,
        margin=dict(l=10, r=10, t=10, b=10),
        yaxis=dict(tickformat=".0%", range=[0, 1]),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    st.plotly_chart(fig, width="stretch")


def _providers_alignment(sheets: list[TearSheet]) -> None:
    """Side-by-side top providers per market."""
    cols = st.columns(len(sheets))
    for i, s in enumerate(sheets):
        with cols[i]:
            st.markdown(f"**{s.market['city']}, {s.market['state']}**")
            if not s.providers:
                st.caption("No provider data.")
                continue
            subs_by_key = {
                (e["canonical_name"], e["technology"]): e
                for e in (s.provider_subs or [])
            }
            top = sorted(
                s.providers,
                key=lambda p: (not p.has_fiber, -(p.coverage_pct or 0.0), -p.locations_served),
            )[:8]
            for p in top:
                fiber_dot = "🟢 " if p.has_fiber else "⚪ "
                est = subs_by_key.get((p.canonical_name, p.technology))
                subs_part = ""
                if est and est.get("estimate_mid") is not None:
                    subs_part = f" · ~{int(est['estimate_mid']):,} subs"
                st.markdown(
                    f"{fiber_dot}**{p.canonical_name}** "
                    f"<span style='color:#6B7280'>· {p.technology} · {fmt_pct(p.coverage_pct)} · "
                    f"{fmt_speed(p.max_advertised_down)}{subs_part}</span>",
                    unsafe_allow_html=True,
                )


def _provider_overlap(sheets: list[TearSheet]) -> None:
    """Show which providers are present in which markets."""
    if len(sheets) < 2:
        st.caption("Provider-overlap table needs at least 2 saved markets.")
        return

    # Build {provider_name: {market_label: True/False}}
    provider_set: dict[str, dict[str, bool]] = {}
    market_labels = []
    for s in sheets:
        label = f"{s.market['city']}, {s.market['state']}"
        market_labels.append(label)
        if not s.providers:
            continue
        for p in s.providers:
            entry = provider_set.setdefault(p.canonical_name, {})
            entry[label] = True

    if not provider_set:
        st.caption("No providers in any saved market.")
        return

    # Sort: present in most markets first, then alphabetical
    rows = []
    for name in sorted(
        provider_set.keys(),
        key=lambda n: (-sum(provider_set[n].values()), n),
    ):
        row: dict[str, object] = {"Provider": name}
        for label in market_labels:
            row[label] = "✓" if provider_set[name].get(label) else ""
        row["# markets"] = sum(1 for label in market_labels if provider_set[name].get(label))
        rows.append(row)

    df = pl.DataFrame(rows)
    st.dataframe(df, width="stretch", hide_index=True)
