"""Map tab: Folium choropleth of analyzed tracts.

A layer selector lets the user switch what's colored:
  - Static layers: fiber/cable availability %, provider counts, measured
    speeds, poverty, MDU share, median income.
  - Dynamic per-fiber-provider layers: one entry per canonical fiber
    provider in this market — "Fiber footprint: Allo Communications" etc.
    Renders a binary 0/1 mask showing which tracts that provider serves at
    fiber tech (50). Toggle different providers to compare footprints.

Each tract is also clickable: a popup shows GEOID, population, MDU share,
and the list of providers (canonical name + max advertised down speed).

Tract polygons come from the local TIGER shapefile cache (already downloaded
during the Phase 1 pipeline). Polygons are reprojected to EPSG:4326 (WGS84)
in `tiger.tract_polygons()` so Folium / Leaflet renders them correctly.
"""

from __future__ import annotations

import json
from typing import Any, Final

import folium
import streamlit as st
from streamlit_folium import st_folium

from ftth_compete.data import tiger
from ftth_compete.data.fcc_bdc import TECH_FIBER
from ftth_compete.format import fmt_currency, fmt_int, fmt_pct, fmt_speed
from ftth_compete.pipeline import TearSheet
from ftth_compete.ui.tabs.competitors import _CATEGORY_DISPLAY  # reuse palette
from ftth_compete.analysis.competitors import ProviderSummary

# (label, metric_key, color_scale_name, help_text)
_LAYERS: Final[list[tuple[str, str, str, str]]] = [
    ("Fiber availability % (locations)", "fiber_availability_pct", "YlGn", "Share of locations in this tract where fiber is offered by AT LEAST one provider — the household-availability metric. Different from 'fiber providers' (provider count)."),
    ("Cable availability % (locations)", "cable_availability_pct", "Oranges", "Share of locations where any cable provider serves (HFC, tech 40)."),
    ("Fiber providers per tract", "fiber_providers", "YlGn", "How many distinct fiber providers (BDC tech 50) serve any location in this tract."),
    ("All providers per tract", "all_providers", "YlGnBu", "How many distinct providers (any tech) serve any location in this tract."),
    ("Measured median down (Mbps)", "measured_down_mbps", "Greens", "Ookla speedtest median download per tract (latest release). Real user-measured throughput."),
    ("Measured median up (Mbps)", "measured_up_mbps", "Blues", "Ookla speedtest median upload per tract."),
    ("Median latency (ms)", "measured_lat_ms", "OrRd", "Ookla speedtest median round-trip latency. Lower is better."),
    ("MDU share (housing)", "mdu_share", "Oranges", "Share of housing units in 2+ unit structures (ACS B25024)."),
    ("Poverty rate", "poverty_rate", "RdPu", "Share of population below the federal poverty line (ACS B17001)."),
    ("Median household income", "median_income", "BuPu", "Population-weighted median household income (ACS B19013)."),
]


def render_map(sheet: TearSheet) -> None:
    """Render the Map tab content for a TearSheet."""
    geoids = sheet.tracts.get("included_in_analysis", [])
    if not geoids:
        st.info("No tracts to map.")
        return

    state = sheet.market.get("state")
    if not state:
        st.error("Market is missing a state code; cannot resolve tract polygons.")
        return

    # Build the layer list: static layers + one entry per fiber provider in
    # this market. The per-provider layers render binary 0/1 masks so the
    # user can flip between them to see overlap/non-overlap.
    layer_options: list[tuple[str, str, str, str]] = list(_LAYERS)
    fiber_providers_in_market = sorted(
        {
            p.canonical_name
            for p in (sheet.providers or [])
            if p.has_fiber and p.canonical_name and p.canonical_name != "Unknown"
        }
    )
    for name in fiber_providers_in_market:
        metric_key = _fiber_provider_metric_key(name)
        layer_options.append(
            (
                f"Fiber footprint: {name}",
                metric_key,
                "Greens",
                f"Tracts where {name} offers fiber service (BDC tech 50). "
                "Binary 0/1 mask — lit tracts are served, dim tracts are not.",
            )
        )

    # Controls
    cols = st.columns([3, 1])
    with cols[0]:
        layer_label = st.selectbox(
            "Color tracts by",
            [layer[0] for layer in layer_options],
            index=0,
            help=(
                "Choose what metric drives tract coloring. "
                "Per-fiber-provider layers (`Fiber footprint: X`) render a "
                "binary mask — lit tracts are served by that provider at fiber."
            ),
        )
    layer_meta = next(layer for layer in layer_options if layer[0] == layer_label)
    _, metric_key, color_scale, help_text = layer_meta
    with cols[1]:
        show_boundary = st.checkbox(
            "Show boundary tracts",
            value=False,
            help=(
                "Draw tracts that touch the city polygon but whose centroid is "
                "outside (excluded from analysis). Dashed gray outline, not "
                "colored."
            ),
        )
    st.caption(help_text + " · See **📖 Methodology** tab for source details.")

    # Compute per-tract metric values
    tract_metrics = _compute_tract_metrics(sheet)

    # Load polygons
    try:
        polys = tiger.tract_polygons(geoids, state)
    except Exception as exc:  # noqa: BLE001
        st.error(f"Could not load tract polygons: {exc}")
        return

    if polys.empty:
        st.warning("No tract polygons matched. May be a TIGER cache miss.")
        return

    # Center the map on the bbox of polygons
    bounds = polys.total_bounds  # (minx, miny, maxx, maxy)
    center_lat = (bounds[1] + bounds[3]) / 2
    center_lon = (bounds[0] + bounds[2]) / 2

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=12,
        tiles="cartodbpositron",
    )

    # Build the choropleth data
    # Folium's Choropleth wants a dict {GEOID: value}
    metric_values = {
        geoid: (tract_metrics.get(geoid) or {}).get(metric_key) for geoid in geoids
    }
    # Filter out tracts with missing values for the choropleth (Folium can't color None)
    valid = {k: v for k, v in metric_values.items() if v is not None}

    if valid:
        # Convert polys -> GeoJSON for Folium
        gj = json.loads(polys.to_json())

        folium.Choropleth(
            geo_data=gj,
            data=valid,
            columns=["GEOID", metric_key],
            key_on="feature.properties.GEOID",
            fill_color=color_scale,
            fill_opacity=0.75,
            line_opacity=0.4,
            line_color="white",
            line_weight=1,
            legend_name=layer_label,
            highlight=True,
        ).add_to(m)

    # Click-popup layer (transparent, just for tooltips/popups on click)
    folium.GeoJson(
        json.loads(polys.to_json()),
        name="Tracts (click for detail)",
        style_function=lambda _f: {"fillOpacity": 0, "color": "transparent"},
        highlight_function=lambda _f: {
            "fillOpacity": 0.15,
            "color": "#000",
            "weight": 2,
        },
        tooltip=folium.GeoJsonTooltip(
            fields=["GEOID"],
            aliases=["Tract:"],
            sticky=False,
        ),
        popup=folium.GeoJsonPopup(
            fields=["GEOID"],
            aliases=["Tract:"],
            labels=True,
            max_width=400,
        ),
    ).add_to(m)

    # Optional boundary tract outlines
    if show_boundary and sheet.tracts.get("boundary"):
        try:
            boundary_polys = tiger.tract_polygons(sheet.tracts["boundary"], state)
            if not boundary_polys.empty:
                folium.GeoJson(
                    json.loads(boundary_polys.to_json()),
                    name="Boundary tracts",
                    style_function=lambda _f: {
                        "fillOpacity": 0,
                        "color": "#888",
                        "weight": 1,
                        "dashArray": "5,5",
                    },
                ).add_to(m)
        except Exception:  # noqa: BLE001
            pass  # boundary is decorative

    # Render
    map_state = st_folium(
        m,
        width="stretch",
        height=560,
        returned_objects=["last_object_clicked", "last_object_clicked_tooltip"],
    )

    # Side panel: clicked-tract detail. Streamlit's st_folium returns the
    # last-clicked feature dict (with the GEOID) — render details below.
    clicked = (map_state or {}).get("last_object_clicked_tooltip")
    if clicked and "Tract:" in clicked:
        clicked_geoid = clicked.split("Tract:")[1].strip().split()[0]
        _render_tract_detail(sheet, clicked_geoid, tract_metrics)
    else:
        with st.expander("How to use the map"):
            st.markdown(
                "- **Color** is controlled by the dropdown above.\n"
                "- **Click a tract** to see its detail panel below the map.\n"
                "- **Boundary tracts** (toggle in the sidebar) are dashed-gray outlines, "
                "drawn for context but not colored or counted in the analysis."
            )


# ---------------------------------------------------------------------------
# Per-tract metric computation

def _fiber_provider_metric_key(canonical_name: str) -> str:
    """Stable internal key for a per-fiber-provider tract metric."""
    return f"fiber_provider:{canonical_name}"


def _compute_tract_metrics(sheet: TearSheet) -> dict[str, dict[str, Any]]:
    """Build a {tract_geoid: {fiber_providers, all_providers, mdu_share,
    poverty_rate, median_income, providers_list, fiber_provider:NAME...}} dict.
    """
    result: dict[str, dict[str, Any]] = {}

    # Map raw BDC brand_name -> canonical_name for the per-provider layer
    # metrics. Initialize each tract×provider pair to 0 so non-served tracts
    # render as the "low" color in the binary choropleth.
    raw_to_canonical: dict[str, str] = {}
    fiber_provider_names: set[str] = set()
    if sheet.providers:
        for p in sheet.providers:
            for raw in p.raw_brand_names:
                raw_to_canonical[str(raw)] = p.canonical_name
            if p.has_fiber:
                fiber_provider_names.add(p.canonical_name)
    geoid_universe = sheet.tracts.get("included_in_analysis", [])
    for g in geoid_universe:
        entry = result.setdefault(g, {})
        for name in fiber_provider_names:
            entry.setdefault(_fiber_provider_metric_key(name), 0)

    # ACS-derived metrics, per tract
    for t in sheet.tract_acs:
        geoid = str(t.get("geoid") or "")
        if not geoid:
            continue
        sfh = _safe_int(t.get("units_1_detached")) + _safe_int(t.get("units_1_attached"))
        mdu = sum(
            _safe_int(t.get(c))
            for c in (
                "units_2", "units_3_4", "units_5_9",
                "units_10_19", "units_20_49", "units_50_plus",
            )
        )
        other = _safe_int(t.get("units_mobile_home")) + _safe_int(t.get("units_other"))
        total = sfh + mdu + other
        pov_u = _safe_int(t.get("poverty_universe"))
        pov_b = _safe_int(t.get("poverty_below"))
        result.setdefault(geoid, {}).update(
            {
                "population": _safe_int(t.get("population_total")) or None,
                "mdu_share": (mdu / total) if total else None,
                "sfh_share": (sfh / total) if total else None,
                "poverty_rate": (pov_b / pov_u) if pov_u else None,
                "median_income": _safe_float(t.get("median_household_income")),
            }
        )

    # BDC-derived per-tract metrics
    fiber_set: dict[str, set[str]] = {}
    all_set: dict[str, set[str]] = {}
    by_tract_provs: dict[str, list[dict[str, Any]]] = {}

    for row in sheet.coverage_matrix:
        geoid = str(row.get("tract_geoid") or "")
        if not geoid:
            continue
        brand = str(row.get("brand_name") or "Unknown")
        tech = row.get("technology")
        try:
            tech_int = int(tech) if tech is not None else None
        except (ValueError, TypeError):
            tech_int = None
        all_set.setdefault(geoid, set()).add(brand)
        if tech_int == TECH_FIBER:
            fiber_set.setdefault(geoid, set()).add(brand)
            # Per-canonical-provider fiber-presence flag (binary 1)
            canonical = raw_to_canonical.get(brand)
            if canonical:
                result.setdefault(geoid, {})[_fiber_provider_metric_key(canonical)] = 1
        by_tract_provs.setdefault(geoid, []).append(
            {
                "brand": brand,
                "tech": tech_int,
                "max_down": _safe_float(row.get("max_down")),
                "max_up": _safe_float(row.get("max_up")),
                "locations": _safe_int(row.get("locations_served")),
            }
        )

    for geoid, provs in by_tract_provs.items():
        result.setdefault(geoid, {})["providers_detail"] = provs

    for geoid, brands in fiber_set.items():
        result.setdefault(geoid, {})["fiber_providers"] = len(brands)
    for geoid in by_tract_provs:
        result.setdefault(geoid, {}).setdefault("fiber_providers", 0)
    for geoid, brands in all_set.items():
        result.setdefault(geoid, {})["all_providers"] = len(brands)

    # Ookla measured-speed metrics (optional)
    for row in sheet.tract_speeds:
        geoid = str(row.get("tract_geoid") or "")
        if not geoid:
            continue
        result.setdefault(geoid, {}).update(
            {
                "measured_down_mbps": _safe_float(row.get("median_down_mbps")),
                "measured_up_mbps": _safe_float(row.get("median_up_mbps")),
                "measured_lat_ms": _safe_float(row.get("median_lat_ms")),
                "measured_n_tests": _safe_int(row.get("n_tests")),
                "measured_low_sample": bool(row.get("low_sample")),
            }
        )

    # Per-tract location availability (% of BSLs with each tech available)
    for row in sheet.location_availability:
        geoid = str(row.get("tract_geoid") or "")
        if not geoid:
            continue
        total = _safe_int(row.get("total_locations"))
        entry: dict[str, Any] = {
            "total_locations": total,
            "fiber_locations": _safe_int(row.get("fiber_locations")),
            "cable_locations": _safe_int(row.get("cable_locations")),
            "dsl_locations": _safe_int(row.get("dsl_locations")),
            "fw_locations": _safe_int(row.get("fw_locations")),
            "sat_locations": _safe_int(row.get("sat_locations")),
        }
        if total > 0:
            entry["fiber_availability_pct"] = entry["fiber_locations"] / total
            entry["cable_availability_pct"] = entry["cable_locations"] / total
            entry["dsl_availability_pct"] = entry["dsl_locations"] / total
            entry["fw_availability_pct"] = entry["fw_locations"] / total
            entry["sat_availability_pct"] = entry["sat_locations"] / total
        result.setdefault(geoid, {}).update(entry)

    return result


def _render_tract_detail(
    sheet: TearSheet,
    geoid: str,
    tract_metrics: dict[str, dict[str, Any]],
) -> None:
    """Detail card under the map for a clicked tract."""
    m = tract_metrics.get(geoid)
    if not m:
        st.info(f"No detail available for tract {geoid}.")
        return

    st.markdown(f"### Tract `{geoid}`")
    cols = st.columns(5)
    cols[0].metric("Population", fmt_int(m.get("population")))
    cols[1].metric("MDU share", fmt_pct(m.get("mdu_share")))
    cols[2].metric("Poverty rate", fmt_pct(m.get("poverty_rate")))
    cols[3].metric("Median HH income", fmt_currency(m.get("median_income")))
    cols[4].metric(
        "Fiber available",
        fmt_pct(m.get("fiber_availability_pct")),
        help=f"{fmt_int(m.get('fiber_locations'))} of {fmt_int(m.get('total_locations'))} locations have fiber offered by at least one provider.",
    )

    # Measured speeds row (if Ookla data available)
    if m.get("measured_down_mbps") is not None:
        m_cols = st.columns(4)
        m_cols[0].metric(
            "Measured median down",
            fmt_speed(m.get("measured_down_mbps")),
            help=f"Ookla speedtest median, {m.get('measured_n_tests') or 0} tests aggregated.",
        )
        m_cols[1].metric("Measured median up", fmt_speed(m.get("measured_up_mbps")))
        m_cols[2].metric("Median latency", f"{int(m.get('measured_lat_ms') or 0)} ms")
        sample_label = "low sample" if m.get("measured_low_sample") else "good sample"
        m_cols[3].metric("Sample size", f"{m.get('measured_n_tests') or 0} tests", help=sample_label)

    provs = m.get("providers_detail") or []
    if not provs:
        return

    st.markdown("**Providers serving this tract**")

    # Roll up per-provider (drop tech splits) for cleaner display
    by_brand: dict[str, dict[str, Any]] = {}
    for p in provs:
        brand = p["brand"]
        agg = by_brand.setdefault(
            brand,
            {"techs": set(), "max_down": 0.0, "max_up": 0.0, "locations": 0},
        )
        if p["tech"] is not None:
            agg["techs"].add(p["tech"])
        agg["max_down"] = max(agg["max_down"], p["max_down"] or 0)
        agg["max_up"] = max(agg["max_up"], p["max_up"] or 0)
        agg["locations"] += p["locations"]

    # Sort by max_down desc
    rows = sorted(by_brand.items(), key=lambda kv: -kv[1]["max_down"])
    table_rows = []
    for brand, agg in rows:
        techs = ", ".join(_tech_label(t) for t in sorted(agg["techs"]))
        table_rows.append(
            {
                "Provider (raw BDC name)": brand,
                "Tech": techs,
                "Max down": fmt_speed(agg["max_down"] or None),
                "Max up": fmt_speed(agg["max_up"] or None),
                "Locations": fmt_int(agg["locations"]),
            }
        )
    st.dataframe(table_rows, width="stretch", hide_index=True)


def _tech_label(code: int) -> str:
    from ftth_compete.data.fcc_bdc import TECH_LABEL
    return TECH_LABEL.get(code, f"Tech {code}")


def _safe_int(v: object) -> int:
    if v is None:
        return 0
    try:
        return int(float(v))  # type: ignore[arg-type]
    except (ValueError, TypeError):
        return 0


def _safe_float(v: object) -> float | None:
    if v is None:
        return None
    try:
        return float(v)  # type: ignore[arg-type]
    except (ValueError, TypeError):
        return None
