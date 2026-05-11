"""Streamlit app entry point.

Run via:
    uv run streamlit run src/ftth_compete/ui/app.py
"""

from __future__ import annotations

import streamlit as st

# NOTE: absolute imports are required here. Streamlit runs this file as
# `__main__` (not as part of the ftth_compete.ui package), so relative
# imports like `from ..config` fail with "no known parent package."
# Modules imported below import each other relatively, which works
# because they're loaded via the installed package path.
from ftth_compete import pipeline as pipeline_mod
from ftth_compete.analysis.lenses import Lens
from ftth_compete.config import get_settings
from ftth_compete.data.tiger import STATE_FIPS
from ftth_compete.export import build_tearsheet_pdf
from ftth_compete.pipeline import TearSheet, run_market
from ftth_compete.ui.tabs.compare import render_compare
from ftth_compete.ui.tabs.competitors import render_competitors
from ftth_compete.ui.tabs.housing import render_housing
from ftth_compete.ui.tabs.map import render_map
from ftth_compete.ui.tabs.methodology import render_methodology
from ftth_compete.ui.tabs.overview import render_overview

# Sorted state list for the dropdown (50 states + DC + PR).
_STATE_LIST = sorted(STATE_FIPS.keys())

# Test-market presets, used in the sidebar quick-pick.
_PRESETS: dict[str, tuple[str, str]] = {
    "(custom)": ("", ""),
    "Evans, CO": ("Evans", "CO"),
    "Plano, TX": ("Plano", "TX"),
    "Brooklyn, NY": ("Brooklyn", "NY"),
    "Manhattan, NY": ("Manhattan", "NY"),
    "Queens, NY": ("Queens", "NY"),
    "Mountain View, CA": ("Mountain View", "CA"),
    "Kansas City Metro, MO": ("Kansas City Metro", "MO"),
}

# Lens display labels -> internal Lens enum value. The emoji is for sidebar
# display only; we strip it before passing the choice to analysis/lenses.
_LENS_OPTIONS: dict[str, Lens] = {
    "⚖️ Neutral": Lens.NEUTRAL,
    "⚔️ Incumbent-defensive": Lens.DEFENSIVE,
    "🚀 New-entrant-offensive": Lens.OFFENSIVE,
}


@st.cache_data(ttl=3600, show_spinner=False)
def _cached_run_market(
    city: str,
    state: str,
    include_boundary: bool,
    no_speeds: bool = False,
    no_ratings: bool = False,
    include_velocity: bool = False,
    include_trajectory: bool = False,
) -> TearSheet:
    """Memoized wrapper around `run_market`. Cache key = the args.

    TTL is 1 hour so re-runs within a session are instant but stale data
    eventually clears. The underlying SQLite + parquet caches persist longer
    on disk regardless.
    """
    return run_market(
        city,
        state,
        include_boundary=include_boundary,
        no_speeds=no_speeds,
        no_ratings=no_ratings,
        include_velocity=include_velocity,
        include_trajectory=include_trajectory,
    )


def _sidebar() -> tuple[str, str, bool, bool, bool, bool, bool, Lens, str | None]:
    """Render sidebar.

    Returns: (city, state, include_boundary, no_speeds, no_ratings, submitted,
              lens, incumbent).

    Lens + incumbent are reactive (no submit needed) and don't trigger a data
    refetch — they only re-render the existing TearSheet through a different
    perspective.
    """
    with st.sidebar:
        st.markdown("# ftth-compete")
        st.caption("FTTH market competitive intelligence")

        preset_label = st.selectbox("Quick-pick", list(_PRESETS.keys()), index=1)
        preset_city, preset_state = _PRESETS[preset_label]

        with st.form("market_form", clear_on_submit=False):
            city = st.text_input("City", value=preset_city or "")
            state = st.selectbox(
                "State",
                _STATE_LIST,
                index=_STATE_LIST.index(preset_state) if preset_state else _STATE_LIST.index("CO"),
            )
            include_boundary = st.checkbox(
                "Include boundary tracts",
                value=False,
                help=(
                    "Tracts that touch the city polygon but whose centroid is outside. "
                    "Excluded by default; included markets show extra population on the edges."
                ),
            )
            with st.expander("Advanced options", expanded=False):
                st.caption("Skip slow data fetches when you don't need them.")
                no_speeds = st.checkbox(
                    "Skip Ookla measured speeds",
                    value=False,
                    help=(
                        "Faster lookup (~8s saved). Without Ookla you lose the "
                        "advertised-vs-measured speed story."
                    ),
                )
                no_ratings = st.checkbox(
                    "Skip Google ratings",
                    value=False,
                    help=(
                        "Google Places `rating` field is Enterprise-tier billing "
                        "(~1K free events/month). Skip to conserve quota or if "
                        "`GOOGLE_PLACES_KEY` isn't set."
                    ),
                )
                st.caption("Slower opt-in features:")
                include_velocity = st.checkbox(
                    "Include 12-month coverage change",
                    value=False,
                    help=(
                        "Fetches a prior BDC release for per-provider expansion "
                        "velocity. Triggers an extra state-level BDC ingest "
                        "(~5 min cold first time)."
                    ),
                )
                include_trajectory = st.checkbox(
                    "Include multi-release trajectory (sparklines)",
                    value=False,
                    help=(
                        "Fetches 4 BDC releases (~6mo apart, ~2 years) for a "
                        "per-provider footprint sparkline. Triggers 3 additional "
                        "state-level BDC ingests (~15-20 min cold first time)."
                    ),
                )
            submitted = st.form_submit_button("Look up", type="primary", width="stretch")

        # Strategic lens (reactive — no fetch needed). Only meaningful when a
        # sheet with providers is loaded.
        sheet_for_lens: TearSheet | None = st.session_state.get("sheet")
        lens: Lens = Lens.NEUTRAL
        incumbent: str | None = None
        if sheet_for_lens is not None and sheet_for_lens.providers:
            st.divider()
            st.markdown("**Strategic lens**")
            lens_label = st.radio(
                "Lens",
                options=list(_LENS_OPTIONS.keys()),
                index=0,
                label_visibility="collapsed",
                help=(
                    "Re-rank the same data through a strategic perspective. "
                    "Defensive = threats to a chosen incumbent. "
                    "Offensive = vulnerability to disruption (entry targets)."
                ),
            )
            lens = _LENS_OPTIONS[lens_label]
            if lens == Lens.DEFENSIVE:
                names = sorted({p.canonical_name for p in sheet_for_lens.providers})
                # Default to the cable incumbent if there is one, else first name.
                cable_options = [
                    p.canonical_name for p in sheet_for_lens.providers
                    if p.category == "cable"
                ]
                default_idx = (
                    names.index(cable_options[0]) if cable_options and cable_options[0] in names
                    else 0
                )
                incumbent = st.selectbox(
                    "Incumbent to defend",
                    names,
                    index=default_idx,
                    help=(
                        "Pick the provider you're analyzing the market FROM. "
                        "Threat scores rank everyone else by how much they "
                        "endanger this incumbent."
                    ),
                )

        # Save / clear comparison set
        if sheet_for_lens is not None:
            saved: dict[str, TearSheet] = st.session_state.setdefault("saved_sheets", {})
            current_key = (
                f"{sheet_for_lens.market['city']}, {sheet_for_lens.market['state']}"
            )
            already_saved = current_key in saved
            st.divider()
            st.markdown("**Comparison set**")
            cols = st.columns([3, 1])
            with cols[0]:
                if st.button(
                    "✓ Saved" if already_saved else "Save to comparison",
                    width="stretch",
                    disabled=already_saved,
                ):
                    saved[current_key] = sheet_for_lens
                    st.rerun()
            with cols[1]:
                if saved and st.button(
                    "Clear",
                    width="stretch",
                    help="Remove all saved markets from the comparison set.",
                ):
                    st.session_state["saved_sheets"] = {}
                    st.rerun()
            if saved:
                st.caption(f"{len(saved)} saved: " + " · ".join(sorted(saved.keys())))

        # PDF export (only when a market is loaded)
        sheet_for_export: TearSheet | None = st.session_state.get("sheet")
        if sheet_for_export is not None:
            st.divider()
            st.markdown("**Export**")
            try:
                pdf_bytes = build_tearsheet_pdf(sheet_for_export)
                st.download_button(
                    "Download PDF tear-sheet",
                    data=pdf_bytes,
                    file_name=(
                        f"{sheet_for_export.market['city'].replace(' ', '_')}"
                        f"_{sheet_for_export.market['state']}_tearsheet.pdf"
                    ),
                    mime="application/pdf",
                    width="stretch",
                )
            except Exception as exc:  # noqa: BLE001
                st.caption(f"PDF export error: {exc}")

        # Config / data status
        st.divider()
        with st.expander("ⓘ Configuration", expanded=False):
            s = get_settings()
            st.write(f"`data_dir` = `{s.data_dir}`")
            st.write(f"Census API key: {'✓ set' if s.census_api_key else '✗ NOT SET'}")
            st.write(f"FCC API token: {'✓ set' if s.fcc_api_token else '✗ NOT SET'}")
            st.write(f"Google Places: {'✓ set' if s.google_places_key else '— not set'}")
            st.caption(
                "Missing keys are gracefully skipped — see methodology tab for setup."
            )

    return (
        city, state, include_boundary, no_speeds, no_ratings,
        include_velocity, include_trajectory, submitted, lens, incumbent,
    )


def _footer(sheet: TearSheet | None) -> None:
    st.markdown("---")
    if sheet:
        v = sheet.data_versions
        st.caption(
            f"Data versions: TIGER {v.get('tiger', '?')} · "
            f"ACS5 5-Year {v.get('acs5', '?')} · "
            f"FCC BDC {v.get('bdc', '—')}"
        )
    st.caption(
        "Speed test data © Ookla, 2019-present, distributed under "
        "[CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/). "
        "Personal/non-commercial use only."
    )


def main() -> None:
    st.set_page_config(
        page_title="ftth-compete",
        page_icon="📡",  # actual emoji — `:shortcode:` form is parsed as a static-file path on Windows
        layout="wide",
        initial_sidebar_state="expanded",
    )

    (
        city, state, include_boundary, no_speeds, no_ratings,
        include_velocity, include_trajectory, submitted, lens, incumbent,
    ) = _sidebar()

    # Trigger a fetch on submit; otherwise reuse the last result.
    if submitted:
        if not city.strip():
            st.error("Enter a city name in the sidebar.")
            st.stop()
        # Per-phase progress via st.status. Pipeline emits phase strings
        # through `set_progress_callback`; cache hits short-circuit and the
        # status block updates only once.
        with st.status(
            f"Looking up {city.strip()}, {state}...", expanded=True
        ) as status:
            pipeline_mod.set_progress_callback(
                lambda label: status.update(label=label)
            )
            try:
                sheet = _cached_run_market(
                    city.strip(), state, include_boundary,
                    no_speeds=no_speeds, no_ratings=no_ratings,
                    include_velocity=include_velocity,
                    include_trajectory=include_trajectory,
                )
                st.session_state["sheet"] = sheet
                status.update(
                    label=f"{sheet.market['city']}, {sheet.market['state']} ready",
                    state="complete",
                    expanded=False,
                )
            except ValueError as exc:
                # Bad city/state — friendly message instead of red exception.
                status.update(label=f"Couldn't resolve market: {exc}", state="error")
                st.error(
                    f"Could not resolve **{city.strip()}, {state}**.\n\n"
                    f"`{exc}`\n\n"
                    "Try a different spelling or pick from the **Quick-pick** dropdown. "
                    "Multi-borough cities (e.g. Brooklyn, Queens) and multi-state metros "
                    "(NYC, KC) aren't fully supported yet — see roadmap."
                )
                st.stop()
            except Exception as exc:  # noqa: BLE001
                status.update(label=f"Lookup failed: {exc}", state="error")
                st.exception(exc)
                st.stop()
            finally:
                pipeline_mod.set_progress_callback(None)

    sheet: TearSheet | None = st.session_state.get("sheet")

    if sheet is None:
        st.title("ftth-compete")
        st.markdown(
            "Pick a market in the sidebar and click **Look up** to begin. "
            "First market per state takes ~90 seconds (downloads FCC BDC bulk data); "
            "subsequent markets in the same state are nearly instant."
        )
        st.caption("Try the **Evans, CO** preset for a quick demo.")
        _footer(None)
        return

    # Header
    st.title(f"{sheet.market['city']}, {sheet.market['state']}")

    saved_sheets: dict[str, TearSheet] = st.session_state.get("saved_sheets", {})
    compare_label = (
        f"📊 Compare ({len(saved_sheets)})" if saved_sheets else "📊 Compare"
    )

    (
        tab_overview, tab_competitors, tab_housing, tab_map, tab_compare,
        tab_methodology,
    ) = st.tabs(
        [
            "📊 Overview", "🏢 Competitors", "🏘️ Housing", "🗺️ Map",
            compare_label, "📖 Methodology",
        ]
    )

    with tab_overview:
        render_overview(sheet, lens=lens)

    with tab_competitors:
        render_competitors(sheet, lens=lens, incumbent=incumbent)

    with tab_housing:
        render_housing(sheet)

    with tab_map:
        render_map(sheet)

    with tab_compare:
        render_compare(list(saved_sheets.values()), lens=lens)

    with tab_methodology:
        render_methodology()

    _footer(sheet)


if __name__ == "__main__":
    main()
