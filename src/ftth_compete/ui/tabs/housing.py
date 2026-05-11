"""Housing tab: B25024 unit-type breakdown + per-tract MDU detail.

Three sections:

1. Top-line summary metrics: SFH / MDU / mobile+other shares (with national
   ACS averages as a context point).
2. Detailed unit-type breakdown — horizontal bar chart of all 8 B25024
   buckets (1-detached, 1-attached, 2u, 3-4u, 5-9u, 10-19u, 20-49u, 50+u)
   plus mobile and other.
3. Per-tract drilldown table — MDU concentration and unit counts per tract.
   Useful for spotting "MDU pockets" within an otherwise SFH-heavy market.
"""

from __future__ import annotations

from typing import Final

import plotly.graph_objects as go
import polars as pl
import streamlit as st

from ftth_compete.format import fmt_int, fmt_pct
from ftth_compete.pipeline import TearSheet

# Reference figures (Census ACS 5-Year national averages, approximate).
# Used as context labels on summary metrics, not authoritative.
_NATIONAL_SFH_SHARE: Final = 0.66
_NATIONAL_MDU_SHARE: Final = 0.27

# B25024 column -> human label, in display order.
_UNIT_BUCKETS: Final[list[tuple[str, str, str]]] = [
    # (acs_col, label, group)  group in {"sfh","mdu","other"}
    ("units_1_detached", "1 unit, detached", "sfh"),
    ("units_1_attached", "1 unit, attached", "sfh"),
    ("units_2", "2 units", "mdu"),
    ("units_3_4", "3-4 units", "mdu"),
    ("units_5_9", "5-9 units", "mdu"),
    ("units_10_19", "10-19 units", "mdu"),
    ("units_20_49", "20-49 units", "mdu"),
    ("units_50_plus", "50+ units", "mdu"),
    ("units_mobile_home", "Mobile home", "other"),
    ("units_other", "Boat/RV/van/other", "other"),
]

_GROUP_COLOR: Final[dict[str, str]] = {
    "sfh": "#4F8FBA",
    "mdu": "#E08A3C",
    "other": "#9B9B9B",
}


def render_housing(sheet: TearSheet) -> None:
    """Render the Housing tab content for a TearSheet."""
    h = sheet.housing
    if h.total <= 0:
        st.warning("No housing-stock data for this market.")
        return

    _summary_metrics(sheet)
    st.divider()
    st.markdown("##### Unit-type breakdown")
    st.caption(
        "ACS 5-Year B25024 (Units in Structure). "
        "Counts are housing units, not occupied housing units."
    )
    _unit_type_bar(sheet)

    if sheet.tract_acs:
        st.divider()
        st.markdown("##### Per-tract detail")
        st.caption("Sortable. Click a column to sort; toggle MDU share progress.")
        _per_tract_table(sheet)


def _summary_metrics(sheet: TearSheet) -> None:
    h = sheet.housing
    cols = st.columns(4)
    cols[0].metric(
        "Single-family share",
        fmt_pct(h.sfh_share),
        delta=f"{(h.sfh_share - _NATIONAL_SFH_SHARE) * 100:+.1f} pp vs national",
        delta_color="off",
        help=f"1-unit detached + 1-unit attached. National SFH share ~{_NATIONAL_SFH_SHARE * 100:.0f}%.",
    )
    cols[1].metric(
        "MDU share",
        fmt_pct(h.mdu_share),
        delta=f"{(h.mdu_share - _NATIONAL_MDU_SHARE) * 100:+.1f} pp vs national",
        delta_color="off",
        help=f"All 2+ unit structures. National MDU share ~{_NATIONAL_MDU_SHARE * 100:.0f}%.",
    )
    cols[2].metric(
        "Mobile/other share",
        fmt_pct(h.other_share),
        help="Mobile homes, boats, RVs, vans.",
    )
    cols[3].metric("Total housing units", fmt_int(h.total))

    # MDU breakdown sub-row
    sub = st.columns(3)
    mdu_total = h.mdu_small + h.mdu_mid + h.mdu_large
    sub[0].metric(
        "Small MDU (2-4u)",
        fmt_int(h.mdu_small),
        delta=fmt_pct(h.mdu_small / mdu_total) + " of MDU" if mdu_total else None,
        delta_color="off",
    )
    sub[1].metric(
        "Mid MDU (5-19u)",
        fmt_int(h.mdu_mid),
        delta=fmt_pct(h.mdu_mid / mdu_total) + " of MDU" if mdu_total else None,
        delta_color="off",
    )
    sub[2].metric(
        "Large MDU (20+u)",
        fmt_int(h.mdu_large),
        delta=fmt_pct(h.mdu_large / mdu_total) + " of MDU" if mdu_total else None,
        delta_color="off",
    )


def _unit_type_bar(sheet: TearSheet) -> None:
    """Horizontal bar chart of detailed unit-type counts."""
    rows = _aggregate_unit_buckets(sheet.tract_acs)
    if not rows:
        # Fall back: only have aggregated HousingSplit, not per-bucket. Show
        # a simpler 3-segment stacked bar of SFH/MDU/Other.
        _render_grouped_only(sheet)
        return

    # Sort by group then count desc within group.
    group_order = {"sfh": 0, "mdu": 1, "other": 2}
    rows.sort(key=lambda r: (group_order[r["group"]], -r["count"]))

    fig = go.Figure(
        data=[
            go.Bar(
                y=[r["label"] for r in rows],
                x=[r["count"] for r in rows],
                orientation="h",
                marker_color=[_GROUP_COLOR[r["group"]] for r in rows],
                text=[fmt_int(r["count"]) for r in rows],
                textposition="outside",
                hovertemplate="<b>%{y}</b><br>%{x:,} units<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        height=420,
        margin=dict(l=0, r=20, t=10, b=10),
        xaxis_title="Housing units",
        yaxis=dict(autorange="reversed"),  # SFH at top, then MDU, then Other
        showlegend=False,
    )
    st.plotly_chart(fig, width="stretch")


def _aggregate_unit_buckets(tract_acs: list[dict[str, object]]) -> list[dict[str, object]]:
    """Sum each B25024 bucket across all tracts. Returns list of dicts ordered
    by display order with `label`, `count`, `group`."""
    if not tract_acs:
        return []
    rows: list[dict[str, object]] = []
    for col, label, group in _UNIT_BUCKETS:
        total = 0
        for t in tract_acs:
            v = t.get(col)
            if v is not None:
                try:
                    total += int(float(v))  # type: ignore[arg-type]
                except (ValueError, TypeError):
                    continue
        rows.append({"label": label, "count": total, "group": group})
    return rows


def _render_grouped_only(sheet: TearSheet) -> None:
    """Fallback when we don't have per-bucket data — show SFH/MDU/Other only."""
    h = sheet.housing
    fig = go.Figure(
        data=[
            go.Bar(
                y=["Single-family", "MDU (2+)", "Mobile/other"],
                x=[h.sfh, h.mdu_small + h.mdu_mid + h.mdu_large, h.mobile_home + h.other],
                orientation="h",
                marker_color=[_GROUP_COLOR["sfh"], _GROUP_COLOR["mdu"], _GROUP_COLOR["other"]],
                text=[fmt_int(h.sfh), fmt_int(h.mdu_small + h.mdu_mid + h.mdu_large),
                      fmt_int(h.mobile_home + h.other)],
                textposition="outside",
            )
        ]
    )
    fig.update_layout(height=300, margin=dict(l=0, r=20, t=10, b=10))
    st.plotly_chart(fig, width="stretch")


def _per_tract_table(sheet: TearSheet) -> None:
    """Per-tract MDU and SFH counts/shares, sortable."""
    rows = []
    for t in sheet.tract_acs:
        sfh = (_safe_int(t.get("units_1_detached")) + _safe_int(t.get("units_1_attached")))
        mdu = sum(
            _safe_int(t.get(c))
            for c in (
                "units_2", "units_3_4", "units_5_9",
                "units_10_19", "units_20_49", "units_50_plus",
            )
        )
        other = _safe_int(t.get("units_mobile_home")) + _safe_int(t.get("units_other"))
        total = sfh + mdu + other
        rows.append(
            {
                "Tract GEOID": t.get("geoid"),
                "Population": _safe_int(t.get("population_total")) or None,
                "Housing units": total or None,
                "SFH": sfh,
                "MDU": mdu,
                "Other": other,
                "MDU share": (mdu / total) if total else None,
            }
        )
    df = pl.DataFrame(rows)
    st.dataframe(
        df,
        width="stretch",
        hide_index=True,
        column_config={
            "Population": st.column_config.NumberColumn(format="%d"),
            "Housing units": st.column_config.NumberColumn(format="%d"),
            "SFH": st.column_config.NumberColumn(format="%d"),
            "MDU": st.column_config.NumberColumn(format="%d"),
            "Other": st.column_config.NumberColumn(format="%d"),
            "MDU share": st.column_config.ProgressColumn(
                "MDU share",
                format="%.0f%%",
                min_value=0.0,
                max_value=1.0,
            ),
        },
    )


def _safe_int(v: object) -> int:
    if v is None:
        return 0
    try:
        return int(float(v))  # type: ignore[arg-type]
    except (ValueError, TypeError):
        return 0
