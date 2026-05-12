"""Reflex app for ftth-compete.

Phase 2 (this file): `LookupState.run_lookup` wired to `pipeline.run_market()`
via a background async event. Overview tab fully ported — KPI cards,
availability strip, IAS subscription anchor, measured-speeds strip,
12-month velocity highlights, opportunity panel (offensive lens), narrative,
tract details. Remaining tabs (Competitors / Housing / Map / Compare /
Methodology) remain placeholders for later phases.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, ClassVar

import plotly.graph_objects as go
import reflex as rx

from ftth_compete.analysis.competitors import ProviderSummary
from ftth_compete.analysis.lenses import Lens, market_opportunity
from ftth_compete.analysis.lenses import apply as apply_lens
from ftth_compete.data.tiger import STATE_FIPS
from ftth_compete.format import fmt_currency, fmt_int, fmt_pct, fmt_speed
from ftth_compete.pipeline import run_market
from ftth_compete.narrative import (
    availability_share,
    fiber_availability_share,
    fiber_share,
    market_narrative,
)
from ftth_compete_web import analytics

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants

STATE_LIST: list[str] = sorted(STATE_FIPS.keys())

PRESETS: list[tuple[str, str, str]] = [
    ("(custom)", "", ""),
    ("Evans, CO", "Evans", "CO"),
    ("Plano, TX", "Plano", "TX"),
    ("Brooklyn, NY", "Brooklyn", "NY"),
    ("Manhattan, NY", "Manhattan", "NY"),
    ("Queens, NY", "Queens", "NY"),
    ("Mountain View, CA", "Mountain View", "CA"),
    ("Kansas City Metro, MO", "Kansas City Metro", "MO"),
]

PRESET_LABELS: list[str] = [p[0] for p in PRESETS]

LENS_OPTIONS: list[tuple[str, str]] = [
    ("Neutral", "neutral"),
    ("Incumbent-defensive", "defensive"),
    ("New-entrant-offensive", "offensive"),
]

TABS: list[tuple[str, str]] = [
    ("overview", "Overview"),
    ("competitors", "Competitors"),
    ("housing", "Housing"),
    ("map", "Map"),
    ("compare", "Compare"),
    ("methodology", "Methodology"),
]


# ---------------------------------------------------------------------------
# State

class LookupState(rx.State):
    """All reactive state for the app.

    Form fields drive `run_lookup`, which executes `pipeline.run_market()`
    in a background thread (so the UI stays responsive during the ~90s
    cold-state lookups). Result fields are populated when the lookup
    completes; each tab renders by reading whichever fields it cares about.
    """

    # Form fields ---------------------------------------------------------
    city: str = "Evans"
    state: str = "CO"
    include_boundary: bool = False
    no_speeds: bool = False
    no_ratings: bool = False
    include_velocity: bool = True
    include_trajectory: bool = True
    preset_label: str = "Evans, CO"

    # Lookup status -------------------------------------------------------
    is_loading: bool = False
    has_result: bool = False
    market_title: str = ""
    lookup_error: str = ""
    progress_label: str = ""
    # Momentum (velocity + trajectory) is loaded as a follow-up backfill
    # AFTER the primary lookup paints, because those need 1-4 extra BDC
    # release parquets (each ~5min cold per state). True while that
    # background fetch is in flight; the UI shows a subtle "Loading
    # momentum data..." indicator near velocity / sparklines.
    momentum_loading: bool = False
    momentum_note: str = ""
    # Enrichment (IAS take-rate anchor, Ookla measured speeds, Google
    # ratings) is loaded as a follow-up after the fast base lookup so
    # KPIs + provider list paint immediately. True while that fetch is
    # in flight; nav-bar shows a "Loading data..." spinner.
    enrich_loading: bool = False

    # Strategic lens / UI state ------------------------------------------
    lens: str = "neutral"
    incumbent: str = ""
    active_tab: str = "overview"
    saved_market_keys: list[str] = []

    # Result fields — Overview KPIs --------------------------------------
    # Display strings (computed via fmt_* helpers in _populate_from_sheet)
    # are used by the UI; raw numeric fields are kept for conditional logic.
    n_tracts: int = 0
    n_boundary: int = 0
    population_display: str = "—"
    mfi_display: str = "—"
    poverty_rate_display: str = "—"
    housing_units_display: str = "—"
    mdu_share_display: str = "—"
    sfh_share_display: str = "—"
    other_share_display: str = "—"

    # Result fields — Competitive landscape ------------------------------
    n_distinct_providers: int = 0
    n_distinct_providers_display: str = "—"
    fiber_avail_has: bool = False
    fiber_avail_display: str = "—"
    fiber_prov_count: int = 0
    fiber_prov_share: float = 0.0
    n_boundary_display: str = "—"

    # Per-tech BSL availability (display strings; "—" when missing)
    availability_fiber_display: str = "—"
    availability_cable_display: str = "—"
    availability_dsl_display: str = "—"
    availability_fw_display: str = "—"
    availability_sat_display: str = "—"
    has_availability: bool = False

    # IAS market-subs anchor ---------------------------------------------
    has_ias: bool = False
    ias_note: str = ""
    ias_take_rate_display: str = "—"
    ias_subs_display: str = "—"
    ias_housing_display: str = "—"
    ias_release: str = ""

    # Ookla measured-speeds strip ----------------------------------------
    has_speeds: bool = False
    speeds_note: str = ""
    speed_down_display: str = "—"
    speed_up_display: str = "—"
    speed_lat_display: str = "—"
    speed_tests_display: str = "—"

    # 12-month velocity highlights ---------------------------------------
    velocity_growers: list[dict[str, Any]] = []  # list of {name, delta, pct, prev, cur, new}
    velocity_decliners: list[dict[str, Any]] = []
    velocity_release_label: str = ""

    # Market-opportunity (offensive lens) --------------------------------
    opp_headline: str = ""
    opp_score: float = 0.0
    opp_score_display: str = "—"
    opp_no_fiber_display: str = "—"
    opp_cable_only_display: str = "—"
    opp_rating_weak_display: str = "—"
    opp_mdu_score_display: str = "—"
    has_opportunity: bool = False

    # Narrative + tracts --------------------------------------------------
    narrative_text: str = ""
    inside_city_tracts: list[str] = []
    boundary_tracts: list[str] = []

    # Notes (for tabs not yet ported, kept for completeness) -------------
    providers_note: str = ""

    # Housing tab --------------------------------------------------------
    # Unit-type breakdown bars (B25024) + per-tract MDU rows.
    has_housing: bool = False
    housing_total_display: str = "—"
    sfh_delta_pp_display: str = ""  # "+1.0 pp vs national"
    mdu_delta_pp_display: str = ""
    mdu_small_display: str = "—"
    mdu_mid_display: str = "—"
    mdu_large_display: str = "—"
    mdu_small_share_display: str = ""
    mdu_mid_share_display: str = ""
    mdu_large_share_display: str = ""
    unit_buckets: list[dict[str, Any]] = []  # bars for the chart
    unit_buckets_max: int = 1  # for scaling bar widths
    tract_housing_rows: list[dict[str, Any]] = []

    # v2 page — spatial selection state for click-to-drill UX -----------
    # When the user clicks a tract on the v2 map, `selected_tract` is set
    # and the right rail switches from "market summary" to "tract detail".
    # When they click a provider in the right rail, `selected_provider` is
    # set and the map highlights that provider's footprint. Setting one
    # clears the other so the rail is never ambiguous.
    selected_tract: str = ""
    selected_provider: str = ""
    v2_map_layer: str = "Fiber providers per tract"
    # Tract polygon GeoJSON for the v2 Plotly map. Built once at lookup
    # time so layer-switches don't refetch from TIGER.
    v2_tract_geojson: dict[str, Any] = {}
    # Provider-footprint search: when non-empty, overrides `v2_map_layer`
    # and paints just the network of the named provider on the map.
    # Empty string = use the base `v2_map_layer` choice.
    footprint_provider: str = ""
    # Searchable list of fiber providers in the current market (sorted
    # by tract count desc). Populated during `_populate_from_sheet`.
    footprint_provider_options: list[str] = []
    # Free-text search query that filters `footprint_provider_options` in
    # the left-rail combobox. Case-insensitive substring match.
    footprint_search: str = ""

    # ACP enrollment density (Phase 6c) ---------------------------------
    # Phase 6e — historical take-rate trajectory ------------------------
    has_subs_history: bool = False
    # Captions: "Take rate 61% → 74% over 8 years (Jun 2015 → Jun 2024)"
    subs_history_summary: str = ""
    # Pre-rendered SVG <path d="..."/> for the sparkline. Built once at
    # populate time; the UI just drops it into an <svg>.
    subs_history_sparkline_d: str = ""
    subs_history_dot_x: float = 0.0
    subs_history_dot_y: float = 0.0
    subs_history_first_label: str = ""
    subs_history_last_label: str = ""
    subs_history_first_pct: str = ""
    subs_history_last_pct: str = ""
    subs_history_note: str = ""

    has_acp: bool = False
    market_acp_density: float = 0.0
    market_acp_density_display: str = "—"

    # Map tab ------------------------------------------------------------
    map_html: str = ""
    map_layer: str = "Fiber providers per tract"
    map_layer_options: list[str] = []
    map_loading: bool = False
    map_note: str = ""
    # Per-tract scalar values for every map layer, keyed by GEOID.
    # Built during _populate_from_sheet so the map renderer can look up
    # values without going back to the TearSheet.
    tract_values: dict[str, dict[str, float]] = {}

    # Per-tract (canonical, tech_code) provider set. Used by
    # `selected_tract_providers` to filter the right-rail list to just
    # the providers serving the clicked tract.
    tract_providers_map: dict[str, list[list[Any]]] = {}

    # Compare tab --------------------------------------------------------
    saved_markets: list[dict[str, Any]] = []

    # Competitors tab — raw data ----------------------------------------
    # `providers_data` is the per-(provider, tech) list with display strings
    # pre-baked. `ratings_data` mirrors `sheet.provider_ratings` so lens
    # scoring can be re-run without re-fetching.
    providers_data: list[dict[str, Any]] = []
    ratings_data: dict[str, dict[str, Any]] = {}
    # Competitors — filter state
    cp_sort_key: str = "fiber_first"
    cp_categories_csv: str = ""  # empty == all
    cp_fiber_only: bool = False
    cp_view: str = "Cards"
    cp_category_options: list[str] = []
    # Competitors — derived view (recomputed on filter / lens change)
    visible_providers: list[dict[str, Any]] = []
    # Summary strip
    cp_total_rows: int = 0
    cp_visible_rows: int = 0
    cp_fiber_rows: int = 0
    cp_full_coverage_rows: int = 0
    cp_top_speed_display: str = "—"
    # Lens UI
    incumbent_options: list[str] = []
    lens_banner_kind: str = ""  # "" / "defensive" / "offensive" / "defensive_missing"

    # Class-level helpers (not reactive) ---------------------------------
    _PRESET_LOOKUP: ClassVar[dict[str, tuple[str, str]]] = {
        label: (c, s) for label, c, s in PRESETS
    }

    # --- Setters (Reflex 0.9 dropped auto-generated setters) -----------
    @rx.event
    def set_preset(self, label: str) -> None:
        self.preset_label = label
        if label in self._PRESET_LOOKUP:
            c, s = self._PRESET_LOOKUP[label]
            if c:
                self.city = c
            if s:
                self.state = s

    # Analytics helper — best-effort, swallows errors. Records the
    # caller's event with stable session token + hashed visitor IP (from
    # X-Forwarded-For when behind Caddy/HF). Never raises so a logger
    # outage can't break a state event.
    def _track(self, kind: str, payload: dict | None = None) -> None:
        try:
            session_id = None
            ip = None
            ua = None
            try:
                sess = getattr(self.router, "session", None)
                if sess is not None:
                    tok = getattr(sess, "client_token", None) or getattr(sess, "session_id", None)
                    if tok:
                        session_id = str(tok)
            except Exception:
                pass
            try:
                headers = getattr(self.router, "headers", None)
                if headers is not None:
                    xff = (
                        getattr(headers, "x_forwarded_for", None)
                        or getattr(headers, "host", None)
                    )
                    if xff:
                        # X-Forwarded-For may be comma-separated; first IP is client.
                        ip = str(xff).split(",")[0].strip()
                    ua = getattr(headers, "user_agent", None) or ""
            except Exception:
                pass
            analytics.record(
                kind, payload or {},
                session_id=session_id, ip=ip, ua=ua,
            )
        except Exception:
            pass  # analytics never breaks the app

    @rx.event
    def set_active_tab(self, tab: str) -> None:
        self.active_tab = tab
        self._track("tab", {"tab": tab})

    @rx.event
    def set_city(self, value: str) -> None:
        self.city = value

    @rx.event
    def set_state(self, value: str) -> None:
        self.state = value

    @rx.event
    def set_include_boundary(self, value: bool) -> None:
        self.include_boundary = bool(value)

    @rx.event
    def set_no_speeds(self, value: bool) -> None:
        self.no_speeds = bool(value)

    @rx.event
    def set_no_ratings(self, value: bool) -> None:
        self.no_ratings = bool(value)

    @rx.event
    def set_include_velocity(self, value: bool) -> None:
        self.include_velocity = bool(value)

    @rx.event
    def set_include_trajectory(self, value: bool) -> None:
        self.include_trajectory = bool(value)

    @rx.event
    def set_lens(self, value: str) -> None:
        label_to_key = {label: key for label, key in LENS_OPTIONS}
        self.lens = label_to_key.get(value, "neutral")
        self._recompute_visible_providers()
        self._track("lens", {"lens": self.lens})

    @rx.event
    def set_incumbent(self, value: str) -> None:
        self.incumbent = value
        self._recompute_visible_providers()
        self._track("incumbent", {"incumbent": value})

    # Competitors filter setters --------------------------------------
    @rx.event
    def set_cp_sort(self, value: str) -> None:
        self.cp_sort_key = value
        self._recompute_visible_providers()

    @rx.event
    def set_cp_fiber_only(self, value: bool) -> None:
        self.cp_fiber_only = bool(value)
        self._recompute_visible_providers()

    @rx.event
    def set_cp_view(self, value: str | list[str]) -> None:
        # rx.segmented_control's on_change is typed `str | list[str]` because
        # the same component supports both single- and multi-select modes.
        # We only use single-select, so we collapse a list back to its first item.
        if isinstance(value, list):
            self.cp_view = value[0] if value else "Cards"
        else:
            self.cp_view = value

    # v2 page — selection handlers ------------------------------------
    # The v2 map is reactive: clicking a tract or provider repaints the
    # right rail. Setting one selection clears the other so the rail
    # always shows exactly one focus.
    @rx.event
    def select_tract(self, geoid: str) -> None:
        self.selected_tract = geoid
        self.selected_provider = ""
        self._track("tract_click", {"geoid": geoid, "market": self.market_title})

    @rx.event
    def select_provider(self, name: str) -> None:
        """Click a provider card or right-rail row → focus that provider.

        Side effect: when the provider has a fiber footprint in this
        market, also flip the map to their footprint layer so the click
        actually paints their network on the canvas.
        """
        self.selected_provider = name
        self.selected_tract = ""
        if name in (self.footprint_provider_options or []):
            self.footprint_provider = name
            self.footprint_search = ""
        self._track("provider_click", {"name": name, "market": self.market_title})

    @rx.event
    def clear_selection(self) -> None:
        """Right-rail back-arrow / Escape. Wipes both selections AND
        clears any provider-footprint overlay the selection had painted,
        so the map reverts to the base layer the user picked in the radio.
        """
        self.selected_tract = ""
        self.selected_provider = ""
        self.footprint_provider = ""
        self.footprint_search = ""

    @rx.event
    def set_v2_map_layer(self, value: str) -> None:
        self.v2_map_layer = value
        # Picking a base layer clears the footprint override so the choice
        # behaves the way the user expects (radio takes back over).
        self.footprint_provider = ""

    @rx.event
    def set_footprint_provider(self, value: str) -> None:
        """Search dropdown changed. Empty / 'All providers' clears the
        override; anything else paints that provider's footprint on the
        map (overrides the base layer until cleared)."""
        if value == "All providers" or not value:
            self.footprint_provider = ""
        else:
            self.footprint_provider = value
        # Drop the search query so the picked provider is visible at the
        # top of the (now-unfiltered) list next time the user re-opens.
        self.footprint_search = ""

    @rx.event
    def set_footprint_search(self, value: str) -> None:
        self.footprint_search = value

    @rx.event
    def clear_footprint(self) -> None:
        self.footprint_provider = ""
        self.footprint_search = ""

    @rx.var(cache=True)
    def filtered_footprint_options(self) -> list[str]:
        """`footprint_provider_options` filtered by the search query.
        Case-insensitive substring match; empty query returns all (capped
        at 30 to keep the rail manageable on very wide markets like NYC)."""
        q = (self.footprint_search or "").strip().lower()
        opts = list(self.footprint_provider_options or [])
        if q:
            opts = [name for name in opts if q in name.lower()]
        return opts[:30]

    @rx.var(cache=True)
    def selected_tract_rows(self) -> list[tuple[str, str]]:
        """Per-tract layer values formatted as (label, display_string) tuples.

        Reads from `tract_values[selected_tract]` and formats each value
        using the layer's display kind. Empty list when no tract selected
        or that tract has no recorded values.
        """
        gid = self.selected_tract
        if not gid:
            return []
        tract_data = self.tract_values.get(gid, {})
        out: list[tuple[str, str]] = []
        for layer in self.map_layer_options:
            v = tract_data.get(layer)
            if v is None:
                out.append((layer, "—"))
                continue
            _, kind = _layer_style_for(layer)
            out.append((layer, _format_value(float(v), kind)))
        return out

    @rx.var(cache=True)
    def selected_tract_providers(self) -> list[dict[str, Any]]:
        """Providers serving the selected tract, filtered from
        `providers_data` against `tract_providers_map[selected_tract]`.

        Fiber rows surface first, then everything else by max-down desc
        so the user sees the most material competitor in the tract on top.
        """
        if not self.selected_tract:
            return []
        pairs = self.tract_providers_map.get(self.selected_tract) or []
        if not pairs:
            return []
        # tract_providers_map is `list[list[canonical, tech_code]]` — rebuild
        # the set of `(canonical, tech_int)` tuples for membership tests.
        wanted: set[tuple[str, int]] = set()
        for p in pairs:
            try:
                wanted.add((str(p[0]), int(p[1])))
            except (IndexError, TypeError, ValueError):
                continue
        out: list[dict[str, Any]] = []
        for row in self.providers_data:
            try:
                key = (str(row.get("name", "")), int(row.get("tech_code", 0)))
            except (TypeError, ValueError):
                continue
            if key in wanted:
                out.append(dict(row))
        # Fiber first, then by advertised max-down desc (best speed first).
        def _sort_key(r: dict[str, Any]) -> tuple[int, int]:
            try:
                tech = int(r.get("tech_code") or 0)
            except (TypeError, ValueError):
                tech = 0
            try:
                spd = int(r.get("_max_down_raw") or 0)
            except (TypeError, ValueError):
                spd = 0
            return (0 if tech == 50 else 1, -spd)
        out.sort(key=_sort_key)
        return out

    @rx.var(cache=True)
    def selected_provider_row(self) -> dict[str, Any]:
        """The full row dict for the currently-selected provider, or empty."""
        if not self.selected_provider:
            return {}
        for r in self.providers_data:
            if r.get("name") == self.selected_provider:
                return dict(r)
        return {}

    @rx.var
    def v2_map_iframe_url(self) -> str:
        """URL for the v2 map iframe. Includes city/state/layer in query
        string so the FastAPI endpoint can serve the right HTML.

        Backend port is hardcoded to 8000 (Reflex default). Frontend at
        3000 cross-origin-fetches via the rewrite Reflex sets up — at
        runtime the iframe can use a relative URL.
        """
        if not self.has_result:
            return ""
        import urllib.parse
        # Footprint search overrides the radio: paint the named provider's
        # network rather than the base metric layer.
        effective_layer = (
            f"Footprint: {self.footprint_provider}"
            if self.footprint_provider
            else self.v2_map_layer
        )
        params = urllib.parse.urlencode({
            "city": self.city,
            "state": self.state,
            "layer": effective_layer,
        })
        # Relative URL — Caddy / the dev backend serves /v2_map_html on
        # the same origin as the frontend, so a relative path Just Works
        # locally (Reflex's dev frontend on :3000 proxies /v2_map_html
        # to :8000) AND in cloud (Caddy on :7860 routes /v2_map_html*
        # to :8000). Hardcoding localhost broke the cloud deploy.
        return f"/v2_map_html?{params}"

    @rx.var(cache=True)
    def v2_figure(self) -> go.Figure:
        """Computed var: reactive Plotly figure for the v2 map.

        Reflex 0.9 + Plotly 6 + plotly.js 3 had a subtle serialization
        issue where `rx.plotly(data=Figure)` rendered blank. Kept for
        completeness — current v2 page uses `v2_figure_html` (iframe
        fallback) instead, which is known to work.
        """
        return build_v2_plotly_figure(
            dict(self.v2_tract_geojson) if self.v2_tract_geojson else {},
            dict(self.tract_values) if self.tract_values else {},
            str(self.v2_map_layer),
            str(self.selected_tract),
        )

    @rx.var(cache=True)
    def v2_figure_html(self) -> str:
        """Plotly figure embedded as a self-contained HTML fragment.

        Returns a `<div>...</div><script>Plotly.newPlot(...)</script>`
        snippet that we drop directly into the parent page via
        `rx.html`. Avoids both:
        1. `rx.plotly`'s Var-serialization quirks with Figure.
        2. iframe `src_doc` origin issues (OSM tile fetches were silently
           failing from `about:srcdoc`).

        Plotly.js is loaded once from CDN (the first call's `include_plotlyjs`
        injects a `<script src=...>`; subsequent calls reuse it).
        """
        fig = build_v2_plotly_figure(
            dict(self.v2_tract_geojson) if self.v2_tract_geojson else {},
            dict(self.tract_values) if self.tract_values else {},
            str(self.v2_map_layer),
            str(self.selected_tract),
        )
        # `include_plotlyjs='cdn'` + `full_html=False` emits a `<div>` plus
        # a `<script>` block that loads plotly.js from CDN (once) then
        # calls `Plotly.newPlot`. Lives in the same origin as our app, so
        # tile fetches behave like the standalone HTML test.
        return fig.to_html(
            full_html=False,
            include_plotlyjs="cdn",
            config={"displayModeBar": False, "responsive": True},
            default_height="100%",
            default_width="100%",
        )

    @rx.event
    def on_v2_map_click(self, points) -> None:
        """Plotly on_click hands back a dict like
        `{"points": [{"location": "<geoid>", ...}, ...]}`. Pull the first
        point's `location` (the tract GEOID, since we set that as the
        feature key) and route to `select_tract`.

        Defensive about shape — Plotly's click payloads have shifted a
        few times across versions, so we guard every access.
        """
        if not points:
            return
        if isinstance(points, dict):
            pts = points.get("points") or []
        elif isinstance(points, list):
            pts = points
        else:
            return
        if not pts:
            return
        first = pts[0]
        geoid = first.get("location") or first.get("customdata") or ""
        if isinstance(geoid, list):
            geoid = geoid[0] if geoid else ""
        if geoid:
            self.select_tract(str(geoid))

    # Compare tab handlers --------------------------------------------
    @rx.event
    def save_current_market(self) -> None:
        """Snapshot the headline KPIs of the current market into `saved_markets`."""
        if not self.has_result:
            return
        title = self.market_title
        # Replace any existing entry for this market title so re-saving updates.
        existing = [m for m in self.saved_markets if m.get("title") != title]
        existing.append({
            "title": title,
            "population_display": self.population_display,
            "mfi_display": self.mfi_display,
            "mdu_share_display": self.mdu_share_display,
            "providers_display": self.n_distinct_providers_display,
            "fiber_avail_display": self.fiber_avail_display,
            "ias_take_rate_display": self.ias_take_rate_display if self.has_ias else "—",
        })
        self.saved_markets = existing

    @rx.event
    def remove_saved_market(self, title: str) -> None:
        self.saved_markets = [m for m in self.saved_markets if m.get("title") != title]

    @rx.event
    def clear_saved_markets(self) -> None:
        self.saved_markets = []

    # Map tab handlers --------------------------------------------------
    @rx.event
    def set_map_layer(self, value: str) -> None:
        self.map_layer = value
        self.map_html = ""  # invalidate; next click rebuilds

    @rx.event(background=True)
    async def render_map(self):
        """Build a Folium choropleth for the current market off-thread.

        Picks per-tract values for the chosen layer from `self.tract_values`
        (pre-built in `_populate_from_sheet`). Renders a colormap-driven
        choropleth with auto-legend via branca + a title overlay.
        """
        async with self:
            if not self.has_result:
                self.map_note = "Load a market first."
                return
            self.map_loading = True
            self.map_html = ""
            tract_geoids_inside = list(self.inside_city_tracts)
            layer = self.map_layer
            tract_values_snapshot = dict(self.tract_values)
            market_title = self.market_title

        try:
            html = await asyncio.to_thread(
                _render_folium_map,
                tract_geoids_inside,
                layer,
                tract_values_snapshot,
                market_title,
            )
        except Exception as exc:  # noqa: BLE001
            log.exception("Map render failed")
            async with self:
                self.map_loading = False
                self.map_note = f"Map render failed: {exc}"
            return

        async with self:
            self.map_html = html
            self.map_loading = False
            self.map_note = ""

    @rx.event
    def toggle_cp_category(self, cat: str) -> None:
        """Add/remove a category from the comma-separated filter set."""
        current = set(c for c in self.cp_categories_csv.split(",") if c)
        if cat in current:
            current.discard(cat)
        else:
            current.add(cat)
        self.cp_categories_csv = ",".join(sorted(current))
        self._recompute_visible_providers()

    def _recompute_visible_providers(self) -> None:
        """Re-derive `visible_providers` + summary strip from `providers_data`
        applying the current sort / filter / lens state.

        Called whenever the user changes a filter, the lens, or the
        incumbent. Pure transform — does no I/O, runs on the backend
        synchronously (sub-millisecond on typical 50-row inputs).
        """
        if not self.providers_data:
            self.visible_providers = []
            self.cp_total_rows = 0
            self.cp_visible_rows = 0
            self.cp_fiber_rows = 0
            self.cp_full_coverage_rows = 0
            self.cp_top_speed_display = "—"
            return

        rows = list(self.providers_data)

        # Lens re-scoring. Reconstruct minimal ProviderSummary instances
        # from stored dicts so we can call apply_lens(); cheaper than
        # serializing the original objects through Reflex state.
        if self.lens != "neutral":
            stubs = [
                ProviderSummary(
                    canonical_name=r["name"],
                    holding_company=r["holding"],
                    category=r["category_key"],
                    technology=r["tech_label"],
                    tech_code=int(r["tech_code"]),
                    tracts_served=int(r.get("tracts_served") or 0),
                    coverage_pct=float(r.get("coverage_pct") or 0.0),
                    locations_served=int(r.get("n_locations") or 0),
                    has_fiber=bool(r.get("has_fiber")),
                    max_advertised_down=r.get("_max_down_raw"),
                    max_advertised_up=r.get("_max_up_raw"),
                    raw_brand_names=list(r.get("_raw_brands") or []),
                )
                for r in rows
            ]
            scored = apply_lens(
                stubs,
                self.lens,
                incumbent=(self.incumbent or None),
                rating_lookup=self.ratings_data or {},
            )
            score_by_name: dict[str, float | None] = {}
            label_by_name: dict[str, str] = {}
            incumbent_set: set[str] = set()
            for sp in scored:
                score_by_name[sp.provider.canonical_name] = sp.score
                if sp.score_label:
                    label_by_name[sp.provider.canonical_name] = sp.score_label
                if sp.is_incumbent:
                    incumbent_set.add(sp.provider.canonical_name)
            # Re-order rows to match lens output.
            order = {
                sp.provider.canonical_name: i for i, sp in enumerate(scored)
            }
            rows.sort(key=lambda r: order.get(r["name"], len(order)))
            for r in rows:
                s = score_by_name.get(r["name"])
                r["lens_score"] = float(s) if s is not None else -1.0
                r["lens_score_display"] = (
                    f"{float(s):.2f}" if s is not None else ""
                )
                r["lens_label"] = label_by_name.get(r["name"], "")
                r["is_incumbent"] = r["name"] in incumbent_set
                # Color band for badge: high score = red (defensive: threat,
                # offensive: target), mid = orange, low = gray.
                if s is None:
                    r["lens_color"] = "gray"
                elif s >= 0.7:
                    r["lens_color"] = "red"
                elif s >= 0.4:
                    r["lens_color"] = "orange"
                else:
                    r["lens_color"] = "gray"
        else:
            for r in rows:
                r["lens_score"] = -1.0
                r["lens_score_display"] = ""
                r["lens_label"] = ""
                r["is_incumbent"] = False
                r["lens_color"] = "gray"

        # Category filter (multi-select stored as comma-separated). Empty
        # means "all" — no filtering.
        active_cats = set(c for c in self.cp_categories_csv.split(",") if c)
        if active_cats:
            rows = [r for r in rows if r["category_key"] in active_cats]
        if self.cp_fiber_only:
            rows = [r for r in rows if r.get("has_fiber")]

        # Sort. When a non-neutral lens is active the lens order wins.
        if self.lens == "neutral":
            key = self.cp_sort_key
            if key == "coverage_desc":
                rows.sort(key=lambda r: (-float(r["coverage_pct"]), r["name"]))
            elif key == "locations_desc":
                rows.sort(key=lambda r: (-int(r["n_locations"]), r["name"]))
            elif key == "speed_desc":
                rows.sort(key=lambda r: (-float(r.get("_max_down_raw") or 0), r["name"]))
            elif key == "name_asc":
                rows.sort(key=lambda r: r["name"])
            else:  # fiber_first (default)
                rows.sort(
                    key=lambda r: (
                        not r.get("has_fiber"),
                        -float(r["coverage_pct"]),
                        -int(r["n_locations"]),
                        r["name"],
                    )
                )

        # Lens banner kind
        if self.lens == "defensive":
            self.lens_banner_kind = (
                "defensive" if self.incumbent else "defensive_missing"
            )
        elif self.lens == "offensive":
            self.lens_banner_kind = "offensive"
        else:
            self.lens_banner_kind = ""

        # Summary strip
        self.cp_total_rows = len(self.providers_data)
        self.cp_visible_rows = len(rows)
        self.cp_fiber_rows = sum(1 for r in rows if r.get("has_fiber"))
        self.cp_full_coverage_rows = sum(
            1 for r in rows if float(r["coverage_pct"]) >= 0.99
        )
        top_speed = max((r.get("_max_down_raw") or 0) for r in rows) if rows else 0
        from ftth_compete.format import fmt_speed
        self.cp_top_speed_display = fmt_speed(top_speed) if top_speed else "—"

        self.visible_providers = rows

    # --- Deep-link autorun: /v2?city=...&state=...&autorun=1 -----------
    # Used by the screener's Open button to jump straight into a market.
    @rx.event
    def maybe_autorun(self):
        """Read URL query params and trigger run_lookup when autorun=1.

        Bound to v2_page's on_load. Reflex 0.9 exposes the parsed query
        dict at `self.router.page.params` (synchronous; safe to call from
        a regular event handler).
        """
        params = getattr(self.router, "page", None)
        if params is None:
            return
        qp = getattr(params, "params", {}) or {}
        city = (qp.get("city") or "").strip()
        state = (qp.get("state") or "").strip().upper()
        autorun = (qp.get("autorun") or "").strip()
        if not city or not state:
            return
        # Don't clobber an existing lookup the user already kicked off.
        if self.has_result and self.city == city and self.state == state:
            return
        self.city = city
        self.state = state
        self.preset_label = ""
        if autorun in {"1", "true", "yes"}:
            return LookupState.run_lookup

    # --- Main event: run the pipeline -----------------------------------
    @rx.event(background=True)
    async def run_lookup(self):
        """Run `pipeline.run_market()` off-thread; populate result fields.

        Three-phase progressive load:

        **A1 — fast base** (~30-50s cold): TIGER + ACS + BDC providers +
        housing + heuristic penetration. Skips IAS / Ookla / Places.
        Paints KPIs, provider list, fiber availability.

        **A2 — enrichment** (~10-20s after A1, since BDC/ACS are warm):
        IAS take-rate anchor + Ookla measured speeds + Google ratings.
        Fills the take-rate panel, speeds strip, rating badges.

        **B — momentum** (~5-10min cold, near-instant warm): velocity +
        trajectory + subscription history. Fills sparklines + 12-month
        delta highlights.

        Each phase paints incrementally so the user sees something
        useful within ~30-50s instead of waiting ~60-90s for the lot.
        """
        # Snapshot the form inputs and clear prior state.
        async with self:
            city = self.city.strip()
            state = self.state.strip().upper()
            if not city:
                self.lookup_error = "Enter a city name."
                return
            self.lookup_error = ""
            self.is_loading = True
            self.has_result = False
            self.progress_label = "Resolving market..."
            self.momentum_loading = False
            self.momentum_note = ""
            self.enrich_loading = False
            include_boundary = self.include_boundary
            no_speeds = self.no_speeds
            no_ratings = self.no_ratings
            wants_momentum = self.include_velocity or self.include_trajectory
            self._track("market_lookup", {
                "city": city, "state": state, "lens": self.lens,
            })

        try:
            # Phase A1: fast base — skip IAS / speeds / ratings.
            sheet_fast = await asyncio.to_thread(
                run_market,
                city,
                state,
                include_boundary=include_boundary,
                no_speeds=True,
                no_ratings=True,
                no_ias=True,
                include_velocity=False,
                include_trajectory=False,
            )
        except ValueError as exc:
            async with self:
                self.is_loading = False
                self.lookup_error = str(exc)
            return
        except Exception as exc:  # noqa: BLE001
            log.exception("Lookup failed")
            async with self:
                self.is_loading = False
                self.lookup_error = f"Lookup failed: {exc}"
            return

        # Paint Phase A1: KPIs + providers + housing now visible.
        async with self:
            _populate_from_sheet(self, sheet_fast)
            self.is_loading = False
            self.has_result = True
            self.progress_label = ""
            # Enrichment fires next; momentum waits until enrichment ends
            # so the two progress indicators don't pile on top of each other.
            self.enrich_loading = True

        # Phase A2: enrichment. Re-runs run_market with IAS / speeds /
        # ratings turned on. BDC/ACS/TIGER are disk-warm so this stage
        # only pays for the new I/O.
        try:
            sheet_full = await asyncio.to_thread(
                run_market,
                city,
                state,
                include_boundary=include_boundary,
                no_speeds=no_speeds,
                no_ratings=no_ratings,
                include_velocity=False,
                include_trajectory=False,
            )
        except Exception as exc:  # noqa: BLE001
            log.exception("Enrichment lookup failed")
            async with self:
                self.enrich_loading = False
            # Non-fatal: leave A1 results in place; A2-only fields stay "—".
        else:
            async with self:
                _populate_from_sheet(self, sheet_full)
                self.enrich_loading = False

        async with self:
            self.momentum_loading = wants_momentum
            self.momentum_note = (
                "Fetching take-rate trajectory..."
                if wants_momentum
                else ""
            )

        # Phase B1: subs-history (take-rate trajectory) is fast (~30s
        # warm, ~2min cold over 17 IAS releases). Running it before the
        # ~5min velocity + trajectory fetch in B2 means the Overview
        # sparkline paints minutes sooner.
        if wants_momentum:
            yield LookupState.backfill_subs_history

    # --- Follow-up: backfill historical IAS take-rate trajectory (B1) -
    @rx.event(background=True)
    async def backfill_subs_history(self):
        """Fetch the multi-release IAS subscription history and paint
        the trendline sparkline on the Overview tab.

        Light vs B2: needs ~17 small IAS release parquets (~200-450KB
        each). On a warm cache: sub-second. Cold: ~1-2min total.
        The 14-release seed bundle keeps cold containers near-warm.
        """
        async with self:
            if not self.has_result:
                return
            city = self.city.strip()
            state = self.state.strip().upper()
            include_boundary = self.include_boundary
            no_speeds = self.no_speeds
            no_ratings = self.no_ratings

        try:
            sheet = await asyncio.to_thread(
                run_market,
                city,
                state,
                include_boundary=include_boundary,
                no_speeds=no_speeds,
                no_ratings=no_ratings,
                include_velocity=False,
                include_trajectory=False,
                include_subs_history=True,
            )
        except Exception as exc:  # noqa: BLE001
            log.exception("Subs-history backfill failed")
            async with self:
                self.momentum_note = f"Trajectory unavailable: {exc}"
            # Non-fatal: continue to B2 anyway so velocity still loads.
        else:
            async with self:
                _populate_subs_history(self, sheet)
                self.momentum_note = "Fetching prior BDC releases..."

        # Phase B2: velocity + trajectory.
        yield LookupState.backfill_momentum

    # --- Follow-up: backfill velocity + trajectory (B2) ----------------
    @rx.event(background=True)
    async def backfill_momentum(self):
        """Re-run `run_market` with velocity + trajectory enabled and
        merge the momentum-only fields into the already-populated rows.

        Heavy: needs 1-4 extra BDC release parquets (~5min each cold).
        The base data is already warm-cached so this is the only added
        wait. Failures surface as `momentum_note` text — non-fatal.
        """
        async with self:
            if not self.has_result:
                return
            city = self.city.strip()
            state = self.state.strip().upper()
            include_boundary = self.include_boundary
            no_speeds = self.no_speeds
            no_ratings = self.no_ratings

        try:
            sheet = await asyncio.to_thread(
                run_market,
                city,
                state,
                include_boundary=include_boundary,
                no_speeds=no_speeds,
                no_ratings=no_ratings,
                include_velocity=True,
                include_trajectory=True,
                include_subs_history=True,
            )
        except Exception as exc:  # noqa: BLE001
            log.exception("Momentum backfill failed")
            async with self:
                self.momentum_loading = False
                self.momentum_note = f"Momentum data unavailable: {exc}"
            return

        async with self:
            # Re-run the full populate — cheap relative to BDC fetch — so
            # velocity badges + trajectory sparklines + the Overview
            # "12-month fiber footprint change" panel all refresh in one
            # shot. Safe because Phase A's sheet had the same base data.
            _populate_from_sheet(self, sheet)
            self.momentum_loading = False
            self.momentum_note = ""


def _populate_from_sheet(s: LookupState, sheet) -> None:
    """Copy the TearSheet's renderable bits into typed state fields.

    Defensive about Nones — Reflex state defaults to 0 / 0.0 / "" so we
    coerce missing values to those rather than store None (which would
    require Optional types and rx.cond gating everywhere).
    """
    s.market_title = f"{sheet.market['city']}, {sheet.market['state']}"
    d = sheet.demographics
    h = sheet.housing

    # Pre-formatted display strings — Reflex Vars can't run fmt_*() at
    # render time, so we bake every "displayed number" here.
    s.population_display = fmt_int(d.population)
    s.mfi_display = fmt_currency(d.median_household_income_weighted)
    s.poverty_rate_display = fmt_pct(d.poverty_rate)
    s.housing_units_display = fmt_int(d.housing_units_total)
    s.mdu_share_display = fmt_pct(h.mdu_share)
    s.sfh_share_display = fmt_pct(h.sfh_share)
    s.other_share_display = fmt_pct(h.other_share)
    s.n_tracts = int(d.n_tracts or 0)
    s.n_boundary = len(sheet.tracts.get("boundary", []) or [])
    s.n_boundary_display = fmt_int(s.n_boundary)
    s.inside_city_tracts = list(sheet.tracts.get("inside_city", []) or [])
    s.boundary_tracts = list(sheet.tracts.get("boundary", []) or [])
    s.providers_note = sheet.providers_note or ""

    # Distinct provider count + fiber stats
    if sheet.providers:
        names = {p.canonical_name for p in sheet.providers}
        s.n_distinct_providers = len(names)
        fiber_names = {
            p.canonical_name for p in sheet.providers if p.has_fiber
        }
        s.fiber_prov_count = len(fiber_names)
        fps = fiber_share(sheet.providers)
        s.fiber_prov_share = float(fps or 0.0)
    else:
        s.n_distinct_providers = 0
        s.fiber_prov_count = 0
        s.fiber_prov_share = 0.0
    s.n_distinct_providers_display = str(s.n_distinct_providers)

    fa = fiber_availability_share(sheet.location_availability)
    if fa is not None:
        s.fiber_avail_has = True
        s.fiber_avail_display = fmt_pct(fa)
    else:
        s.fiber_avail_has = False
        s.fiber_avail_display = "—"

    # Full BSL availability breakdown — pre-formatted percentages.
    s.has_availability = bool(sheet.location_availability)
    if sheet.location_availability:
        for key, attr in [
            ("fiber", "availability_fiber_display"),
            ("cable", "availability_cable_display"),
            ("dsl", "availability_dsl_display"),
            ("fw", "availability_fw_display"),
            ("sat", "availability_sat_display"),
        ]:
            v = availability_share(sheet.location_availability, tech_key=key)
            setattr(s, attr, fmt_pct(v) if v is not None else "—")

    # IAS anchor
    a = sheet.market_subs_anchor
    s.has_ias = a is not None
    s.ias_note = sheet.ias_note or ""
    if a:
        s.ias_take_rate_display = fmt_pct(a.get("take_rate_mid"))
        s.ias_subs_display = fmt_int(a.get("market_subs_mid"))
        s.ias_housing_display = fmt_int(a.get("total_housing_units"))
        s.ias_release = str(a.get("ias_release") or "")

    # Ookla measured speeds — pre-format speed values.
    s.speeds_note = sheet.speeds_note or ""
    s.has_speeds = bool(sheet.tract_speeds)
    if sheet.tract_speeds:
        downs = [t.get("median_down_mbps") for t in sheet.tract_speeds if t.get("median_down_mbps")]
        ups = [t.get("median_up_mbps") for t in sheet.tract_speeds if t.get("median_up_mbps")]
        lats = [t.get("median_lat_ms") for t in sheet.tract_speeds if t.get("median_lat_ms") is not None]
        avg_down = (sum(downs) / len(downs)) if downs else None
        avg_up = (sum(ups) / len(ups)) if ups else None
        avg_lat = int(sum(lats) / len(lats)) if lats else None
        n_tests = sum(int(t.get("n_tests") or 0) for t in sheet.tract_speeds)
        s.speed_down_display = fmt_speed(avg_down)
        s.speed_up_display = fmt_speed(avg_up)
        s.speed_lat_display = f"{avg_lat} ms" if avg_lat is not None else "—"
        s.speed_tests_display = f"{n_tests:,} tests"

    # 12-month velocity highlights (fiber only)
    fiber_velos = [
        v for v in (sheet.provider_velocity or []) if v.get("tech_code") == 50
    ]
    growers = sorted(
        (v for v in fiber_velos if (v.get("delta_abs") or 0) > 0),
        key=lambda v: -v["delta_abs"],
    )[:3]
    decliners = sorted(
        (v for v in fiber_velos if (v.get("delta_abs") or 0) < 0),
        key=lambda v: v["delta_abs"],
    )[:3]

    def _velo_row(v: dict) -> dict[str, Any]:
        name = str(v.get("canonical_name") or "")
        delta = int(v.get("delta_abs") or 0)
        pct = v.get("delta_pct")
        prev_locs = int(v.get("prev_locations") or 0)
        cur_locs = int(v.get("current_locations") or 0)
        is_new = bool(v.get("new_offering"))
        is_disc = bool(v.get("discontinued"))
        pct_str = f"{pct:+.0%}" if pct is not None else "—"

        # Pre-format the badge label and detail line so Reflex doesn't have
        # to concatenate dict-indexed Vars at render time (which trips its
        # `ObjectItemOperation + str` type checking).
        if is_new:
            badge_label = "NEW"
            badge_color = "green"
        elif is_disc:
            badge_label = "Discontinued"
            badge_color = "red"
        elif delta > 0:
            badge_label = f"+{delta:,}"
            badge_color = "green"
        else:
            badge_label = f"{delta:,}"
            badge_color = "orange"

        detail = f"{pct_str}  ({prev_locs:,} → {cur_locs:,} locations)"
        return {
            "name": name,
            "badge_label": badge_label,
            "badge_color": badge_color,
            "detail": detail,
        }

    s.velocity_growers = [_velo_row(v) for v in growers]
    s.velocity_decliners = [_velo_row(v) for v in decliners]
    if growers or decliners:
        ref = growers[0] if growers else decliners[0]
        s.velocity_release_label = (
            f"BDC {ref.get('prev_release', '?')} → {ref.get('current_release', '?')}"
        )
    else:
        s.velocity_release_label = ""

    # Opportunity panel (offensive lens only; always compute, gate rendering)
    if sheet.providers:
        result = market_opportunity(
            sheet.providers,
            rating_lookup=sheet.provider_ratings or {},
            mdu_share=sheet.housing.mdu_share,
        )
        if result.get("score") is not None:
            factors = result.get("factors") or {}
            s.opp_headline = str(result.get("headline") or "")
            s.opp_score = float(result["score"])
            s.opp_score_display = f"{s.opp_score:.2f} / 1.00"
            s.opp_no_fiber_display = fmt_pct(factors.get("no_fiber_share"))
            s.opp_cable_only_display = fmt_pct(factors.get("cable_only_share"))
            s.opp_rating_weak_display = fmt_pct(factors.get("rating_weakness"))
            s.opp_mdu_score_display = fmt_pct(factors.get("mdu_score"))
            s.has_opportunity = True

    # Competitors tab — build the per-(provider, tech) row list with all
    # display strings pre-baked. This is the canonical data source for
    # both the cards and the sortable table view.
    s.ratings_data = {
        k: dict(v) for k, v in (sheet.provider_ratings or {}).items() if v
    }
    s.providers_data = (
        _build_providers_data(sheet) if sheet.providers else []
    )

    # Velocity + trajectory lookup-maps so we can attach them per row.
    velocity_by_key = {
        (v.get("canonical_name"), int(v.get("tech_code") or 0)): v
        for v in (sheet.provider_velocity or [])
    }
    trajectory_by_key = {
        (t.get("canonical_name"), int(t.get("tech_code") or 0)): t
        for t in (sheet.provider_trajectory or [])
    }
    for r in s.providers_data:
        key = (r["name"], int(r["tech_code"]))
        v = velocity_by_key.get(key)
        if v:
            r.update(_format_velocity(v))
        t = trajectory_by_key.get(key)
        if t:
            r.update(_format_trajectory(t))

    # Available categories (for the multi-select filter)
    s.cp_category_options = sorted({r["category_key"] for r in s.providers_data})
    s.cp_categories_csv = ""  # default: show all

    # Available incumbents (sorted), auto-pick the cable provider as default.
    incumbent_names = sorted({r["name"] for r in s.providers_data})
    s.incumbent_options = incumbent_names
    if not s.incumbent and incumbent_names:
        cable_incumbents = [
            r["name"] for r in s.providers_data if r["category_key"] == "cable"
        ]
        s.incumbent = cable_incumbents[0] if cable_incumbents else incumbent_names[0]

    # First pass at visible_providers (neutral lens by default)
    s._recompute_visible_providers()

    # Housing tab data
    _populate_housing(s, sheet)

    # Map tab — initialize layer choices, defer rendering until user clicks.
    # The base metric layers go in the radio. Per-provider footprint layers
    # are accessible via the searchable dropdown (`footprint_provider_options`)
    # so a 5-fiber market doesn't dump 5 extra radio rows on the user, and
    # bigger markets like Brooklyn (15+ fiber providers) stay scannable.
    s.map_layer_options = [
        "Fiber providers per tract",
        "Total providers per tract",
        "Fiber availability %",
        "Cable availability %",
        "Median measured down (Mbps)",
        "Median measured up (Mbps)",
        "Median latency (ms)",
        "MDU share %",
        "Poverty rate %",
        "Median HH income",
        "ACP enrollment density %",
    ]
    # Build the per-provider footprint search list (fiber providers sorted
    # by tract count desc — dominant overbuilders first).
    from ftth_compete.data.providers import canonical_name
    fiber_prov_tract_counts: dict[str, int] = {}
    for r in sheet.coverage_matrix or []:
        try:
            if int(r.get("technology") or 0) != 50:
                continue
        except (TypeError, ValueError):
            continue
        canon = canonical_name(r.get("brand_name"), None, 50)
        fiber_prov_tract_counts[canon] = fiber_prov_tract_counts.get(canon, 0) + 1
    s.footprint_provider_options = [
        name for name, _ in sorted(
            fiber_prov_tract_counts.items(),
            key=lambda kv: (-kv[1], kv[0].lower()),
        )
    ]
    # New market → clear any prior footprint pick.
    s.footprint_provider = ""
    if not s.map_layer or s.map_layer not in s.map_layer_options:
        s.map_layer = s.map_layer_options[0]
    s.map_html = ""  # forces re-render when user opens Map tab

    # ACP enrollment density (Phase 6c)
    sheet_acp = getattr(sheet, "market_acp_density", None)
    if sheet_acp is not None and sheet_acp > 0:
        s.has_acp = True
        s.market_acp_density = float(sheet_acp)
        s.market_acp_density_display = fmt_pct(sheet_acp)
    else:
        s.has_acp = False
        s.market_acp_density = 0.0
        s.market_acp_density_display = "—"

    # Phase 6e — historical take-rate trajectory.
    _populate_subs_history(s, sheet)

    # Per-tract value lookup for every map layer.
    s.tract_values = _build_tract_values(sheet)
    # Per-tract provider sets — for the right-rail "Providers in this tract"
    # list. Stored as `dict[geoid, list[[canonical, tech_code]]]`.
    from ftth_compete.data.providers import canonical_name as _cn
    tract_provs: dict[str, set[tuple[str, int]]] = {}
    for r in sheet.coverage_matrix or []:
        gid = str(r.get("tract_geoid") or "")
        if not gid:
            continue
        try:
            tech = int(r.get("technology") or 0)
        except (TypeError, ValueError):
            tech = 0
        canon = _cn(r.get("brand_name"), None, tech)
        tract_provs.setdefault(gid, set()).add((canon, tech))
    s.tract_providers_map = {
        gid: [list(pair) for pair in sorted(provs)]
        for gid, provs in tract_provs.items()
    }
    # Drop cached v2 map HTML for any previous version of this market
    # so the next iframe load regenerates with fresh data.
    _v2_html_cache.clear()

    # v2 map — pre-build the GeoJSON of tract polygons once at lookup time
    # (Plotly choropleth needs `geojson=` once; cheaper than rebuilding on
    # every layer-switch). Store as a state dict.
    try:
        s.v2_tract_geojson = _build_tract_geojson(s.inside_city_tracts)
    except Exception as exc:  # noqa: BLE001
        log.exception("v2 tract geojson build failed")
        s.v2_tract_geojson = {}

    # Narrative
    try:
        s.narrative_text = market_narrative(sheet)
    except Exception:  # noqa: BLE001
        s.narrative_text = ""


# ---------------------------------------------------------------------------
# Per-provider data flattening helpers

_CATEGORY_DISPLAY: dict[str, tuple[str, str]] = {
    "national_fiber": ("National Fiber", "green"),
    "regional_fiber": ("Regional Fiber", "green"),
    "cable": ("Cable", "orange"),
    "fixed_wireless": ("Fixed Wireless", "blue"),
    "satellite": ("Satellite", "violet"),
    "muni": ("Municipal", "gray"),
    "unknown": ("Unknown", "gray"),
}

_TECH_COLOR: dict[str, str] = {
    "Fiber": "green", "Cable": "orange", "DSL": "gray",
    "Licensed FW": "blue", "Unlicensed FW": "blue", "Licensed-by-Rule FW": "blue",
    "GSO Satellite": "violet", "Non-GSO Satellite": "violet",
}


def _build_providers_data(sheet) -> list[dict[str, Any]]:
    """Flatten `sheet.providers` into JSON-friendly dicts with display strings
    pre-computed. Each row is one (canonical_name, technology) entry.

    Includes the raw numerical fields needed for sort + lens scoring under
    `_max_down_raw`, `_raw_brands`, etc. (the underscore-prefix convention
    signals "internal — don't render directly").
    """
    subs_by_key = {
        (s2["canonical_name"], s2["technology"]): s2
        for s2 in (sheet.provider_subs or [])
    }
    ratings = sheet.provider_ratings or {}
    rows: list[dict[str, Any]] = []
    for p in sheet.providers:
        cat_label, cat_color = _CATEGORY_DISPLAY.get(
            p.category, (p.category.title(), "gray"),
        )
        tech_color = _TECH_COLOR.get(p.technology, "gray")
        n_locs = int(p.locations_served or 0)
        gig = int(p.gig_locations or 0)
        hund = int(p.hundred_locations or 0)
        slow = int(p.sub_hundred_locations or 0)
        tier_total = gig + hund + slow
        has_tiers = tier_total > 0
        gig_pct = (gig / tier_total) if has_tiers else 0.0
        hund_pct = (hund / tier_total) if has_tiers else 0.0
        slow_pct = (slow / tier_total) if has_tiers else 0.0

        # Rating
        r = ratings.get(p.canonical_name) or {}
        rating_val = r.get("rating")
        rating_count = r.get("user_rating_count")
        if rating_val is not None:
            rv = float(rating_val)
            rating_color = "green" if rv >= 4.0 else ("orange" if rv >= 3.0 else "red")
            stars = "★" * int(round(rv)) + "☆" * (5 - int(round(rv)))
            rating_display = f"{rv:.1f}"
            rating_count_display = f"{int(rating_count or 0):,} reviews"
            has_rating = True
        else:
            rating_color = "gray"
            stars = ""
            rating_display = ""
            rating_count_display = ""
            has_rating = False

        # Subs
        sk = subs_by_key.get((p.canonical_name, p.technology))
        if sk and sk.get("estimate_mid") is not None:
            sub_mid = int(sk["estimate_mid"])
            sub_low = int(sk.get("estimate_low") or 0)
            sub_high = int(sk.get("estimate_high") or 0)
            sub_conf = str(sk.get("confidence") or "")
            subs_display = f"~{sub_mid:,}  ({sub_low:,}-{sub_high:,})"
            subs_conf_color = {"high": "green", "medium": "blue", "low": "gray"}.get(
                sub_conf, "gray"
            )
            has_subs = True
        else:
            sub_conf = ""
            subs_display = ""
            subs_conf_color = "gray"
            has_subs = False

        rows.append({
            # Identity
            "name": p.canonical_name,
            "holding": p.holding_company,
            "category_key": p.category,
            "category_label": cat_label,
            "category_color": cat_color,
            "tech_label": p.technology,
            "tech_color": tech_color,
            "tech_code": int(p.tech_code),
            "has_fiber": bool(p.has_fiber),
            # Coverage
            "tracts_served": int(p.tracts_served or 0),
            "coverage_pct": float(p.coverage_pct or 0.0),
            "coverage_display": fmt_pct(p.coverage_pct),
            # Locations
            "n_locations": n_locs,
            "locations_display": fmt_int(n_locs),
            # Speed (advertised)
            "max_down_display": fmt_speed(p.max_advertised_down),
            "_max_down_raw": p.max_advertised_down,
            "_max_up_raw": p.max_advertised_up,
            # Speed tier breakdown
            "has_tiers": has_tiers,
            "gig_pct_str": f"{gig_pct * 100:.2f}",  # width % for bar segment
            "hundred_pct_str": f"{hund_pct * 100:.2f}",
            "slow_pct_str": f"{slow_pct * 100:.2f}",
            "tier_caption": (
                f"Gig {gig_pct * 100:.0f}%  ·  100Mbps+ {hund_pct * 100:.0f}%  ·  <100  {slow_pct * 100:.0f}%"
            ) if has_tiers else "",
            # Rating
            "has_rating": has_rating,
            "rating_display": rating_display,
            "rating_count_display": rating_count_display,
            "rating_color": rating_color,
            "stars": stars,
            "rating_url": str(r.get("place_url") or ""),
            # Subs
            "has_subs": has_subs,
            "subs_display": subs_display,
            "subs_confidence": sub_conf,
            "subs_color": subs_conf_color,
            # Velocity placeholders (filled below if data is available)
            "has_velocity": False,
            "velocity_badge": "",
            "velocity_color": "gray",
            "velocity_caption": "",
            # Trajectory placeholders
            "has_trajectory": False,
            "trajectory_svg": "",
            "trajectory_caption": "",
            # Internals (used by lens scoring + sort)
            "_raw_brands": list(p.raw_brand_names or []),
            "raw_brands_str": (
                "Raw BDC names: " + ", ".join(p.raw_brand_names)
                if p.raw_brand_names and not (
                    len(p.raw_brand_names) == 1
                    and p.raw_brand_names[0] == p.canonical_name
                )
                else ""
            ),
            # Lens fields populated by _recompute_visible_providers
            "lens_score": -1.0,
            "lens_score_display": "",
            "lens_label": "",
            "is_incumbent": False,
            "lens_color": "gray",
        })
    return rows


def _format_velocity(v: dict[str, Any]) -> dict[str, Any]:
    """Build velocity display fields for a provider row."""
    delta = int(v.get("delta_abs") or 0)
    pct = v.get("delta_pct")
    new = bool(v.get("new_offering"))
    disc = bool(v.get("discontinued"))
    prev_rel = str(v.get("prev_release", ""))
    if new:
        return {
            "has_velocity": True,
            "velocity_badge": f"NEW since {prev_rel}",
            "velocity_color": "green",
            "velocity_caption": f"+{delta:,} locations (no prior fiber)",
        }
    if disc:
        return {
            "has_velocity": True,
            "velocity_badge": f"Discontinued since {prev_rel}",
            "velocity_color": "red",
            "velocity_caption": f"-{abs(delta):,} locations",
        }
    if delta == 0:
        return {
            "has_velocity": True,
            "velocity_badge": "Flat YoY",
            "velocity_color": "gray",
            "velocity_caption": "",
        }
    pct_str = f"{pct:+.0%}" if pct is not None else ""
    color = "green" if delta > 0 else "orange"
    sign = "+" if delta > 0 else ""
    return {
        "has_velocity": True,
        "velocity_badge": f"{sign}{delta:,} locations · {pct_str} YoY",
        "velocity_color": color,
        "velocity_caption": f"vs {prev_rel}",
    }


# ---------------------------------------------------------------------------
# Map data helpers

# Per-layer presentation rules: (colormap palette, value formatter, units).
# Palettes are branca-compatible color lists; the colormap is built fresh
# per render so vmin/vmax track the current market's values.
_LAYER_STYLE: dict[str, tuple[list[str], str]] = {
    # (palette, format-suffix). Higher is rendered in the rightmost color.
    "Fiber providers per tract":     (["#f7fbff", "#08519c"], "count"),
    "Total providers per tract":     (["#f7fbff", "#08519c"], "count"),
    "Fiber availability %":          (["#f7fcf5", "#00441b"], "pct"),
    "Cable availability %":          (["#fff5eb", "#7f2704"], "pct"),
    "Median measured down (Mbps)":   (["#f7fcf5", "#00441b"], "mbps"),
    "Median measured up (Mbps)":     (["#f7fcf5", "#00441b"], "mbps"),
    # Latency: reverse — high latency is bad, so paint it red.
    "Median latency (ms)":           (["#fff5f0", "#67000d"], "ms"),
    "MDU share %":                   (["#fcfbfd", "#3f007d"], "pct"),
    "Poverty rate %":                (["#fff5f0", "#67000d"], "pct"),
    "Median HH income":              (["#f7fcf5", "#00441b"], "currency"),
    # ACP density: high enrollment = low-income covariate, render in amber.
    "ACP enrollment density %":      (["#fff5eb", "#7f2704"], "pct"),
}


def _format_value(v: float, kind: str) -> str:
    if kind == "pct":
        return f"{v * 100:.1f}%"
    if kind == "mbps":
        return f"{v:.0f} Mbps"
    if kind == "ms":
        return f"{v:.0f} ms"
    if kind == "currency":
        return f"${v:,.0f}"
    if kind == "binary":
        return "Served" if v >= 0.5 else "Not served"
    return f"{v:.0f}"  # count default


def _layer_style_for(layer: str) -> tuple[list[str], str]:
    """Style lookup with per-provider footprint fallback.

    Footprint layers get a teal binary palette; everything else reads from
    the static `_LAYER_STYLE` table.
    """
    if layer.startswith("Footprint: "):
        return (["#f0fdfa", "#0f766e"], "binary")
    return _LAYER_STYLE.get(layer, (["#f7fbff", "#08519c"], "count"))


def _build_tract_provider_hover(
    sheet,
) -> dict[str, list[tuple[str, bool]]]:
    """Per-tract `[(canonical_name, has_fiber), ...]` lookup from the sheet.

    `has_fiber` is True if the provider has any tech=50 row in this tract,
    used to surface a teal dot next to fiber providers in the hover.
    Deduped per (tract, canonical) so multi-tech providers (e.g. Lumen
    DSL + Fiber, Comcast Cable + Fiber) only show once per tract — with
    the fiber flag set if any of their rows is fiber.
    """
    from ftth_compete.data.providers import canonical_name as _cn
    per_tract: dict[str, dict[str, bool]] = {}
    for r in sheet.coverage_matrix or []:
        gid = str(r.get("tract_geoid") or "")
        if not gid:
            continue
        try:
            tech = int(r.get("technology") or 0)
        except (TypeError, ValueError):
            tech = 0
        name = _cn(r.get("brand_name"), None, tech)
        bucket = per_tract.setdefault(gid, {})
        bucket[name] = bucket.get(name, False) or (tech == 50)
    return {gid: list(provs.items()) for gid, provs in per_tract.items()}


def _hover_provider_line(
    tract_providers: dict[str, list[tuple[str, bool]]] | None,
    geoid: str,
    max_show: int = 8,
) -> str:
    """Build the per-tract provider line for the map hover.

    Fiber providers come first (with a thin teal dot), then non-fiber.
    Caps at `max_show`; extras roll up to "+N more". Returns empty string
    when there's nothing to show so the hover stays compact.
    """
    if not tract_providers:
        return ""
    rows = tract_providers.get(geoid) or []
    if not rows:
        return ""
    # rows: list of (canonical_name, has_fiber). Sort fiber-first by name.
    rows_sorted = sorted(rows, key=lambda r: (not r[1], r[0].lower()))
    visible = rows_sorted[:max_show]
    overflow = len(rows_sorted) - len(visible)
    parts: list[str] = []
    for name, has_fiber in visible:
        dot = (
            "<span style='color:#14b8a6'>&#9679;</span> "
            if has_fiber else ""
        )
        parts.append(f"{dot}{name}")
    line = "<br>".join(parts)
    if overflow > 0:
        line += (
            f"<br><span style='color:#94a3b8'>+{overflow} more</span>"
        )
    return (
        "<br><span style='color:#94a3b8;font-size:10px'>Providers</span>"
        f"<br>{line}"
    )


def _build_tract_values(sheet) -> dict[str, dict[str, float]]:
    """Flatten the per-tract data on a TearSheet into one lookup keyed
    by GEOID, with one scalar per supported map layer.

    Layers without source data (e.g. "Median measured down" when Ookla
    wasn't loaded) simply don't appear in the per-tract dict — the
    renderer's "no data for this layer" path kicks in.
    """
    tv: dict[str, dict[str, float]] = {}

    # ACS-derived layers
    for t in sheet.tract_acs or []:
        geoid = str(t.get("geoid") or "")
        if not geoid:
            continue
        v = tv.setdefault(geoid, {})
        sfh = (_to_int(t.get("units_1_detached"))
               + _to_int(t.get("units_1_attached")))
        mdu = sum(
            _to_int(t.get(c))
            for c in (
                "units_2", "units_3_4", "units_5_9",
                "units_10_19", "units_20_49", "units_50_plus",
            )
        )
        other = _to_int(t.get("units_mobile_home")) + _to_int(t.get("units_other"))
        total = sfh + mdu + other
        if total > 0:
            v["MDU share %"] = mdu / total
        pov_universe = _to_float(t.get("poverty_universe"))
        pov_below = _to_float(t.get("poverty_below"))
        if pov_universe > 0:
            v["Poverty rate %"] = pov_below / pov_universe
        mfi = _to_float(t.get("median_household_income"))
        if mfi > 0:
            v["Median HH income"] = mfi

    # Ookla measured speeds
    for t in sheet.tract_speeds or []:
        geoid = str(t.get("geoid") or "")
        if not geoid:
            continue
        v = tv.setdefault(geoid, {})
        if t.get("median_down_mbps"):
            v["Median measured down (Mbps)"] = float(t["median_down_mbps"])
        if t.get("median_up_mbps"):
            v["Median measured up (Mbps)"] = float(t["median_up_mbps"])
        if t.get("median_lat_ms") is not None:
            v["Median latency (ms)"] = float(t["median_lat_ms"])

    # BSL availability
    for t in sheet.location_availability or []:
        geoid = str(t.get("tract_geoid") or "")
        if not geoid:
            continue
        v = tv.setdefault(geoid, {})
        total = _to_int(t.get("total_locations"))
        if total > 0:
            v["Fiber availability %"] = _to_int(t.get("fiber_locations")) / total
            v["Cable availability %"] = _to_int(t.get("cable_locations")) / total

    # BDC coverage matrix → fiber-providers and total-providers per tract
    fiber_provs: dict[str, set[str]] = {}
    total_provs: dict[str, set[str]] = {}
    for r in sheet.coverage_matrix or []:
        geoid = str(r.get("tract_geoid") or "")
        provider_id = str(r.get("provider_id") or "")
        if not geoid or not provider_id:
            continue
        total_provs.setdefault(geoid, set()).add(provider_id)
        try:
            if int(r.get("technology") or 0) == 50:
                fiber_provs.setdefault(geoid, set()).add(provider_id)
        except (TypeError, ValueError):
            pass
    for geoid, provs in total_provs.items():
        tv.setdefault(geoid, {})["Total providers per tract"] = float(len(provs))
    for geoid, provs in fiber_provs.items():
        tv.setdefault(geoid, {})["Fiber providers per tract"] = float(len(provs))
    # Tracts with BDC coverage but zero fiber providers should still show 0
    # (not "no data") so they render as the lowest-color band.
    for geoid in total_provs:
        tv.setdefault(geoid, {}).setdefault("Fiber providers per tract", 0.0)

    # ACP enrollment density (Phase 6c)
    for row in getattr(sheet, "acp_density", []) or []:
        geoid = str(row.get("tract_geoid") or "")
        if not geoid:
            continue
        density = row.get("density")
        if density is not None:
            tv.setdefault(geoid, {})["ACP enrollment density %"] = float(density)

    # Per-provider fiber footprint layers ("Footprint: <Provider>"). 1.0 if
    # the provider serves that tract with fiber (tech=50), 0.0 otherwise.
    # Lets a user flip from "all fiber" to per-provider footprints.
    from ftth_compete.data.providers import canonical_name
    fiber_footprints: dict[str, set[str]] = {}
    fiber_tracts_all: set[str] = set()
    for r in sheet.coverage_matrix or []:
        try:
            if int(r.get("technology") or 0) != 50:
                continue
        except (TypeError, ValueError):
            continue
        geoid = str(r.get("tract_geoid") or "")
        if not geoid:
            continue
        fiber_tracts_all.add(geoid)
        canon = canonical_name(
            r.get("brand_name"), None, 50,
        )
        fiber_footprints.setdefault(canon, set()).add(geoid)
    for prov, served_tracts in fiber_footprints.items():
        layer_name = f"Footprint: {prov}"
        for geoid in fiber_tracts_all | set(total_provs.keys()):
            tv.setdefault(geoid, {})[layer_name] = (
                1.0 if geoid in served_tracts else 0.0
            )

    return tv


def _to_int(x: object) -> int:
    if x is None:
        return 0
    try:
        return int(float(x))  # type: ignore[arg-type]
    except (ValueError, TypeError):
        return 0


def _to_float(x: object) -> float:
    if x is None:
        return 0.0
    try:
        return float(x)  # type: ignore[arg-type]
    except (ValueError, TypeError):
        return 0.0


def _render_folium_map(
    geoids: list[str],
    layer: str,
    tract_values: dict[str, dict[str, float]],
    market_title: str,
) -> str:
    """Build a Folium choropleth + legend as a complete HTML document.

    Runs on a worker thread (`asyncio.to_thread` from `render_map`).
    Returns rendered HTML for `rx.el.iframe(src_doc=...)`.

    Renders the value from `tract_values[geoid][layer]` per tract. Tracts
    without a value (e.g., Ookla speed layers on a market that didn't load
    Ookla) are rendered as muted gray.

    A branca colormap is added as a legend (auto-positioned bottom-right
    by Folium) with the layer name as caption and value-formatted ticks.
    """
    import branca.colormap as bcm
    import folium
    import geopandas as gpd

    from ftth_compete.data import tiger

    if not geoids:
        return "<html><body>No tracts to render.</body></html>"

    polys = tiger.tract_polygons(geoids)
    if polys.empty:
        return "<html><body>No tract polygons found.</body></html>"

    # Center the map on the union centroid (planar CRS for stable math,
    # then projected back to lat/lon for Folium).
    polys_proj = polys.to_crs(epsg=5070)
    center_proj = polys_proj.geometry.union_all().centroid
    center_ll = (
        gpd.GeoSeries([center_proj], crs="EPSG:5070")
        .to_crs(epsg=4326)
        .iloc[0]
    )

    # Per-tract value extraction
    values: dict[str, float] = {}
    for gid in geoids:
        v = tract_values.get(gid, {}).get(layer)
        if v is not None:
            values[gid] = float(v)

    if not values:
        return (
            "<html><body style='font-family:sans-serif;padding:20px;color:#444'>"
            f"<h3>No data available for layer:<br><i>{layer}</i></h3>"
            "<p>This layer needs data that wasn't loaded for the current market. "
            "For measured-speed layers, ensure Ookla was loaded (uncheck "
            "'Skip Ookla measured speeds' in the sidebar).</p>"
            "</body></html>"
        )

    vmin, vmax = min(values.values()), max(values.values())
    if vmin == vmax:
        vmax = vmin + 1.0  # avoid zero-range colormap

    palette, kind = _layer_style_for(layer)
    colormap = bcm.LinearColormap(
        colors=palette,
        vmin=vmin,
        vmax=vmax,
        caption=layer,
    )

    # Add value + formatted display to the GeoDataFrame so the GeoJsonTooltip
    # can read them directly. Pre-format the display so the tooltip shows
    # "82.3%" not "0.823".
    polys = polys.copy()
    polys["value"] = polys["GEOID"].map(lambda g: values.get(str(g)))
    polys["value_display"] = polys["value"].apply(
        lambda v: _format_value(v, kind) if v is not None else "no data"
    )

    m = folium.Map(
        location=[center_ll.y, center_ll.x],
        zoom_start=11,
        tiles="CartoDB positron",
        control_scale=True,
    )

    def _style(feature):
        v = feature["properties"].get("value")
        if v is None:
            return {
                "fillColor": "#e0e0e0",
                "color": "#999",
                "weight": 1,
                "fillOpacity": 0.4,
            }
        return {
            "fillColor": colormap(v),
            "color": "#666",
            "weight": 1,
            "fillOpacity": 0.75,
        }

    folium.GeoJson(
        polys.to_json(),
        style_function=_style,
        tooltip=folium.GeoJsonTooltip(
            fields=["GEOID", "value_display"],
            aliases=["Tract:", layer + ":"],
            sticky=True,
            style="font-family: sans-serif; font-size: 12px;",
        ),
    ).add_to(m)

    # Legend (branca auto-attaches to the map).
    colormap.add_to(m)

    # Subtle title overlay top-left so screenshots are self-explanatory.
    title_html = (
        '<div style="position: fixed; top: 12px; left: 60px; z-index: 9999; '
        'background: white; padding: 8px 12px; border-radius: 6px; '
        'box-shadow: 0 2px 8px rgba(0,0,0,0.15); font-family: sans-serif; '
        'font-size: 13px; line-height: 1.3;">'
        f'<div style="font-weight:600;color:#111">{market_title}</div>'
        f'<div style="color:#666;font-size:11px;margin-top:2px">{layer}</div>'
        "</div>"
    )
    m.get_root().html.add_child(folium.Element(title_html))

    return m.get_root().render()


# ---------------------------------------------------------------------------
# v2 map — Plotly choropleth (real React component, click-to-drill)

def _build_tract_geojson(geoids: list[str]) -> dict[str, Any]:
    """Build a GeoJSON FeatureCollection for the market's tract polygons.

    Plotly's `Choroplethmapbox.geojson=` expects this shape. Each feature's
    `id` is the tract GEOID — that's how we wire click events back to a
    specific tract via `points[0].location`.

    Returns an empty dict if tract resolution fails (the v2 map then renders
    a "no polygons" placeholder).
    """
    if not geoids:
        return {}
    import json

    from ftth_compete.data import tiger

    polys = tiger.tract_polygons(geoids)
    if polys.empty:
        return {}
    # GeoJSON: features keyed by GEOID for Plotly's `featureidkey`.
    gjson_str = polys.to_json()
    gjson = json.loads(gjson_str)
    for feat in gjson.get("features", []):
        feat["id"] = feat.get("properties", {}).get("GEOID", "")
    return gjson


def build_provider_footprint_figure(states_data: list[dict[str, Any]]):
    """National state-level choropleth painting a provider's footprint.

    Input is `provider_detail.states` (already aggregated per-state).
    Returns a Plotly Figure ready for `.to_html()` injection into the
    iframe served by `/provider_map_html`.

    Color encodes `n_tracts` served per state — gives an immediate read
    on where a provider is concentrated. Hover reveals fiber-tract count
    + total locations.
    """
    import plotly.graph_objects as go
    if not states_data:
        fig = go.Figure()
        fig.update_layout(
            geo=dict(scope="usa"),
            margin={"r": 0, "t": 0, "l": 0, "b": 0},
            paper_bgcolor="rgba(0,0,0,0)",
        )
        return fig

    states = [str(s.get("state") or "") for s in states_data]
    n_tracts = [int(s.get("n_tracts") or 0) for s in states_data]
    n_fiber = [int(s.get("n_fiber_tracts") or 0) for s in states_data]
    locs = [int(s.get("total_locations") or 0) for s in states_data]
    hover = [
        f"<b>{st}</b><br>"
        f"<span style='color:#94a3b8;font-size:10px'>Tracts served</span><br>"
        f"<b style='font-size:14px'>{nt:,}</b><br>"
        f"<span style='color:#94a3b8;font-size:10px'>Fiber tracts</span> {nf:,}<br>"
        f"<span style='color:#94a3b8;font-size:10px'>Locations</span> {lc:,}"
        for st, nt, nf, lc in zip(states, n_tracts, n_fiber, locs)
    ]

    fig = go.Figure(go.Choropleth(
        locations=states,
        z=n_tracts,
        locationmode="USA-states",
        colorscale=[[0.0, "#f0fdfa"], [1.0, "#0f766e"]],
        marker_line_color="rgba(15, 23, 42, 0.4)",
        marker_line_width=0.6,
        text=hover,
        hovertemplate="%{text}<extra></extra>",
        colorbar=dict(
            title=dict(text="Tracts served", side="top", font=dict(size=11)),
            thickness=10,
            len=0.45,
            x=0.99,
            xanchor="right",
            y=0.02,
            yanchor="bottom",
            bgcolor="rgba(255,255,255,0.85)",
            bordercolor="rgba(15,23,42,0.15)",
            borderwidth=1,
            tickfont=dict(size=10),
            outlinewidth=0,
        ),
    ))
    fig.update_layout(
        geo=dict(
            scope="usa",
            projection=dict(type="albers usa"),
            showlakes=False,
            bgcolor="rgba(0,0,0,0)",
        ),
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(
            family="Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
            size=11,
            color="#0f172a",
        ),
        hoverlabel=dict(
            bgcolor="rgba(15, 23, 42, 0.92)",
            bordercolor="rgba(15, 23, 42, 0.92)",
            font=dict(color="#fff", size=12, family="Inter, sans-serif"),
        ),
    )
    return fig


def build_v2_plotly_figure(
    geojson: dict[str, Any],
    tract_values: dict[str, dict[str, float]],
    layer: str,
    selected_tract: str = "",
    tract_providers: dict[str, list[tuple[str, bool]]] | None = None,
):
    """Return a Plotly figure for the v2 page's interactive map.

    Returns `plotly.graph_objects.Figure` (NOT a dict — Reflex's
    `rx.plotly(data=...)` expects the Figure type and serializes it
    internally).

    **IMPORTANT** — uses `Choroplethmap` (NOT the older `Choroplethmapbox`).
    Reflex bundles `plotly.js@3.5.0` which deprecated `Choroplethmapbox`
    in favor of `Choroplethmap` (backed by MapLibre instead of Mapbox).
    The new API uses `map_style` / `map_zoom` / `map_center` props on
    the layout, and `carto-positron` is still a valid style — but the
    trace type and layout-prop names had to change.
    """
    import plotly.graph_objects as go

    if not geojson or not geojson.get("features"):
        # Empty figure with a centered message; Plotly handles the placeholder.
        empty = go.Figure()
        empty.update_layout(
            map_style="carto-positron",
            map_zoom=3.5,
            map_center={"lat": 39.5, "lon": -98.0},
            margin={"r": 0, "t": 0, "l": 0, "b": 0},
            paper_bgcolor="rgba(0,0,0,0)",
        )
        return empty

    # Per-tract values for the chosen layer
    locations: list[str] = []
    values: list[float] = []
    hover_text: list[str] = []
    palette, kind = _layer_style_for(layer)
    for feat in geojson["features"]:
        gid = str(feat.get("id") or "")
        if not gid:
            continue
        v = tract_values.get(gid, {}).get(layer)
        if v is None:
            continue
        locations.append(gid)
        values.append(float(v))
        # Show last 6 digits of the GEOID (tract + suffix) for readability;
        # full ID is in the right-rail detail panel when the user clicks.
        short_id = gid[-6:] if len(gid) > 6 else gid
        provider_line = _hover_provider_line(tract_providers, gid)
        hover_text.append(
            f"<b>Tract {short_id}</b><br>"
            f"<span style='color:#94a3b8;font-size:10px'>{layer}</span><br>"
            f"<b style='font-size:14px'>{_format_value(float(v), kind)}</b>"
            + provider_line
        )

    if not locations:
        # No data for this layer — render an empty choropleth so the map
        # still shows base tiles + tract outlines. Hover still shows the
        # provider list so the user can read it off any tract.
        locations = [str(f.get("id") or "") for f in geojson["features"]]
        values = [0.0] * len(locations)
        hover_text = [
            f"Tract {g[-6:] if len(g) > 6 else g}<br>"
            f"<span style='color:#94a3b8;font-size:10px'>No data for {layer}</span>"
            + _hover_provider_line(tract_providers, g)
            for g in locations
        ]

    # Center map on the union centroid (compute from feature bounds).
    lats: list[float] = []
    lons: list[float] = []
    for feat in geojson["features"]:
        coords = feat.get("geometry", {}).get("coordinates")
        if not coords:
            continue
        # Polygon: coords = [[[lon, lat], ...]]; MultiPolygon: deeper
        # Flatten generally.
        for ring in _iter_rings(coords):
            for pt in ring:
                if isinstance(pt, list | tuple) and len(pt) >= 2:
                    lons.append(float(pt[0]))
                    lats.append(float(pt[1]))
    center_lat = (min(lats) + max(lats)) / 2 if lats else 39.5
    center_lon = (min(lons) + max(lons)) / 2 if lons else -98.0
    # Rough zoom from bounding-box span. Empirical mapping.
    span = max((max(lats) - min(lats)) if lats else 1.0,
               (max(lons) - min(lons)) if lons else 1.0)
    if span < 0.1:
        zoom = 12
    elif span < 0.3:
        zoom = 11
    elif span < 0.7:
        zoom = 10
    elif span < 1.5:
        zoom = 9
    else:
        zoom = 8

    fig = go.Figure(go.Choroplethmap(
        geojson=geojson,
        locations=locations,
        z=values,
        featureidkey="id",
        colorscale=[[0.0, palette[0]], [1.0, palette[1]]],
        marker_opacity=0.72,
        marker_line_width=0.6,
        marker_line_color="rgba(15, 23, 42, 0.55)",
        text=hover_text,
        hovertemplate="%{text}<extra></extra>",
        colorbar=dict(
            title=dict(text=layer, side="top", font=dict(size=11)),
            thickness=10,
            len=0.45,
            x=0.99,
            xanchor="right",
            y=0.02,
            yanchor="bottom",
            bgcolor="rgba(255,255,255,0.85)",
            bordercolor="rgba(15,23,42,0.15)",
            borderwidth=1,
            tickfont=dict(size=10),
            outlinewidth=0,
        ),
    ))

    # Selected tract overlay — thicker outline + accent color.
    if selected_tract:
        sel_feat = next(
            (f for f in geojson["features"] if f.get("id") == selected_tract),
            None,
        )
        if sel_feat:
            fig.add_trace(go.Choroplethmap(
                geojson={"type": "FeatureCollection", "features": [sel_feat]},
                locations=[selected_tract],
                z=[1.0],
                featureidkey="id",
                colorscale=[[0.0, "rgba(0,0,0,0)"], [1.0, "rgba(0,0,0,0)"]],
                marker_opacity=0.0,
                marker_line_width=3.0,
                marker_line_color="#3b82f6",
                showscale=False,
                hoverinfo="skip",
            ))

    fig.update_layout(
        map_style="carto-positron",
        map_zoom=zoom,
        map_center={"lat": center_lat, "lon": center_lon},
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(
            family="Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
            size=11,
            color="#0f172a",
        ),
        hoverlabel=dict(
            bgcolor="rgba(15, 23, 42, 0.92)",
            bordercolor="rgba(15, 23, 42, 0.92)",
            font=dict(color="#fff", size=12, family="Inter, sans-serif"),
        ),
    )
    return fig


def _iter_rings(coords):
    """Flatten Polygon / MultiPolygon coordinate arrays to flat ring iterator."""
    if not coords:
        return
    # Heuristic: a ring is a list of [lon, lat] pairs.
    if isinstance(coords[0], (int, float)):
        return  # not a ring
    if isinstance(coords[0][0], (int, float)):
        yield coords  # single ring
        return
    for sub in coords:
        yield from _iter_rings(sub)


_HOUSING_UNIT_BUCKETS: list[tuple[str, str, str]] = [
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

_HOUSING_GROUP_BG = {"sfh": "var(--blue-9)", "mdu": "var(--orange-9)", "other": "var(--gray-8)"}
_NATIONAL_SFH_SHARE: float = 0.66
_NATIONAL_MDU_SHARE: float = 0.27


def _safe_int(v: object) -> int:
    if v is None:
        return 0
    try:
        return int(float(v))  # type: ignore[arg-type]
    except (ValueError, TypeError):
        return 0


def _populate_subs_history(s: LookupState, sheet) -> None:
    """Build the take-rate trajectory state from sheet.market_subscription_history.

    Trajectory points come newest-first from the pipeline; we reverse to
    chronological for the sparkline. Pre-renders the SVG path-d string
    instead of building it in JSX so the UI just drops it into <svg>.
    """
    points = list(getattr(sheet, "market_subscription_history", []) or [])
    note = getattr(sheet, "subs_history_note", None) or ""
    s.subs_history_note = note
    if not points:
        s.has_subs_history = False
        s.subs_history_sparkline_d = ""
        s.subs_history_summary = ""
        return
    # Chronological for the sparkline.
    points.sort(key=lambda p: p.get("as_of") or "")
    # Filter to points with usable take_rate_25_mid (older releases may
    # have only `bucket_all` data so the 25/3 metric is 0). Fall back to
    # take_rate_all_mid (any-speed) when the 25/3 metric is zero across
    # the whole series.
    xs_25 = [float(p.get("take_rate_25_mid") or 0.0) for p in points]
    xs_all = [float(p.get("take_rate_all_mid") or 0.0) for p in points]
    use_25 = max(xs_25) > 0.0
    values = xs_25 if use_25 else xs_all
    labels = [str(p.get("as_of") or "") for p in points]
    # Clamp values to a reasonable display range (0–1.5; multi-line HH can
    # push connections-per-1k above 1000 ~ 100% take rate).
    vmin = 0.0
    vmax = max(max(values), 0.001) * 1.05  # 5% headroom above peak
    # Sparkline in a 0..100 × 0..30 viewBox so the rendered <svg> can
    # scale to any size.
    W, H = 100.0, 30.0
    n = len(values)
    coords: list[tuple[float, float]] = []
    for i, v in enumerate(values):
        x = (i / max(n - 1, 1)) * W
        y = H - ((v - vmin) / (vmax - vmin) * H if vmax > vmin else H / 2)
        coords.append((x, y))
    s.subs_history_sparkline_d = "M " + " L ".join(
        f"{x:.2f},{y:.2f}" for x, y in coords
    )
    if coords:
        s.subs_history_dot_x = float(coords[-1][0])
        s.subs_history_dot_y = float(coords[-1][1])
    s.has_subs_history = True
    s.subs_history_first_label = _humanize_release_label(labels[0])
    s.subs_history_last_label = _humanize_release_label(labels[-1])
    s.subs_history_first_pct = f"{values[0] * 100:.0f}%"
    s.subs_history_last_pct = f"{values[-1] * 100:.0f}%"
    span_years = max(1, (int(labels[-1][:4]) - int(labels[0][:4])))
    metric_label = "≥25/3 take rate" if use_25 else "broadband take rate"
    s.subs_history_summary = (
        f"{metric_label}: {s.subs_history_first_pct} → {s.subs_history_last_pct} "
        f"over {span_years} years"
    )


def _humanize_release_label(as_of: str) -> str:
    """`2022-06-30` → `Jun 2022`."""
    if not as_of or len(as_of) < 7:
        return as_of or ""
    yyyy, mm = as_of[:4], as_of[5:7]
    mon = {"06": "Jun", "12": "Dec"}.get(mm, mm)
    return f"{mon} {yyyy}"


def _populate_housing(s: LookupState, sheet) -> None:
    """Build Housing-tab state from sheet.housing + sheet.tract_acs.

    Aggregates the 10 B25024 buckets, computes share-of-MDU breakdowns,
    and assembles a per-tract table with MDU/SFH counts + MDU share.
    """
    h = sheet.housing
    if h.total <= 0:
        s.has_housing = False
        return
    s.has_housing = True

    s.housing_total_display = fmt_int(h.total)
    s.sfh_delta_pp_display = (
        f"{(h.sfh_share - _NATIONAL_SFH_SHARE) * 100:+.1f} pp vs national"
    )
    s.mdu_delta_pp_display = (
        f"{(h.mdu_share - _NATIONAL_MDU_SHARE) * 100:+.1f} pp vs national"
    )

    mdu_total = h.mdu_small + h.mdu_mid + h.mdu_large
    s.mdu_small_display = fmt_int(h.mdu_small)
    s.mdu_mid_display = fmt_int(h.mdu_mid)
    s.mdu_large_display = fmt_int(h.mdu_large)
    if mdu_total:
        s.mdu_small_share_display = fmt_pct(h.mdu_small / mdu_total) + " of MDU"
        s.mdu_mid_share_display = fmt_pct(h.mdu_mid / mdu_total) + " of MDU"
        s.mdu_large_share_display = fmt_pct(h.mdu_large / mdu_total) + " of MDU"

    # Aggregate B25024 buckets across tracts.
    if sheet.tract_acs:
        bucket_totals: dict[str, int] = {col: 0 for col, _, _ in _HOUSING_UNIT_BUCKETS}
        for t in sheet.tract_acs:
            for col, _, _ in _HOUSING_UNIT_BUCKETS:
                bucket_totals[col] += _safe_int(t.get(col))
        buckets = [
            {
                "label": label,
                "count": bucket_totals[col],
                "count_display": fmt_int(bucket_totals[col]),
                "group": group,
                "bar_color": _HOUSING_GROUP_BG[group],
            }
            for col, label, group in _HOUSING_UNIT_BUCKETS
        ]
        # Sort by group order then count desc within group.
        order_key = {"sfh": 0, "mdu": 1, "other": 2}
        buckets.sort(key=lambda r: (order_key[r["group"]], -r["count"]))
        s.unit_buckets = buckets
        s.unit_buckets_max = max((b["count"] for b in buckets), default=1) or 1
    else:
        s.unit_buckets = []
        s.unit_buckets_max = 1

    # Per-tract table.
    tract_rows: list[dict[str, Any]] = []
    for t in sheet.tract_acs or []:
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
        mdu_share = (mdu / total) if total else 0.0
        tract_rows.append({
            "geoid": str(t.get("geoid") or ""),
            "population_display": fmt_int(t.get("population_total")),
            "housing_display": fmt_int(total) if total else "—",
            "sfh_display": fmt_int(sfh),
            "mdu_display": fmt_int(mdu),
            "other_display": fmt_int(other),
            "mdu_share": mdu_share,
            "mdu_share_display": fmt_pct(mdu_share) if total else "—",
            "mdu_pct_str": f"{mdu_share * 100:.1f}",  # for inline bar width
        })
    # Sort by MDU share desc to spot MDU pockets quickly.
    tract_rows.sort(key=lambda r: -r["mdu_share"])
    s.tract_housing_rows = tract_rows


def _format_trajectory(t: dict[str, Any]) -> dict[str, Any]:
    """Build inline SVG sparkline for a provider trajectory."""
    series = t.get("series") or []
    if len(series) < 2:
        return {"has_trajectory": False, "trajectory_svg": "", "trajectory_caption": ""}
    locs = [int(s.get("locations") or 0) for s in series]
    labels = [str(s.get("release", "?")) for s in series]
    vmax = max(locs)
    if vmax <= 0:
        return {"has_trajectory": False, "trajectory_svg": "", "trajectory_caption": ""}
    w, h, pad = 180, 32, 3
    n = len(locs)
    xs = [pad + (w - 2 * pad) * i / (n - 1) for i in range(n)]
    ys = [h - pad - (h - 2 * pad) * (v / vmax) for v in locs]
    points = " ".join(f"{x:.1f},{y:.1f}" for x, y in zip(xs, ys))
    dots = "".join(
        f"<circle cx='{x:.1f}' cy='{y:.1f}' r='2' fill='var(--green-9)'/>"
        for x, y in zip(xs, ys)
    )
    svg = (
        f"<svg width='{w}' height='{h}'>"
        f"<polyline points='{points}' fill='none' stroke='var(--green-9)' stroke-width='1.5'/>"
        f"{dots}</svg>"
    )
    cap = f"{labels[0]} → {labels[-1]}  ·  {locs[0]:,} → {locs[-1]:,} locations"
    return {"has_trajectory": True, "trajectory_svg": svg, "trajectory_caption": cap}


# ---------------------------------------------------------------------------
# UI helpers

def _section_title(text: str) -> rx.Component:
    """Small uppercase section header with consistent vertical rhythm."""
    return rx.text(
        text,
        size="1",
        weight="bold",
        color="var(--gray-11)",
        letter_spacing="0.05em",
        class_name="uppercase",
        margin_top="5",
        margin_bottom="2",
    )


def _kpi_card(
    label: str,
    value: rx.Var | str,
    *,
    help_text: str = "",
    delta: rx.Var | str | None = None,
) -> rx.Component:
    """A polished KPI card.

    Visual rhythm: tiny uppercase-ish label, large bold value, optional
    helper line. Subtle hover lift via accent border. Help text shows
    as a tooltip when present (info icon hint to make it discoverable).
    """
    label_line = rx.hstack(
        rx.text(
            label,
            size="1",
            color="var(--gray-11)",
            weight="medium",
        ),
        rx.cond(
            help_text != "",
            rx.icon("info", size=12, color="var(--gray-9)"),
            rx.fragment(),
        ),
        spacing="1",
        align="center",
    )
    body = [
        label_line,
        rx.heading(value, size="6", weight="bold", color="var(--gray-12)"),
    ]
    if delta is not None:
        body.append(rx.text(delta, size="1", color_scheme="gray"))
    inner = rx.vstack(*body, spacing="2", align="start", width="100%")
    card = rx.card(
        inner,
        size="2",
        width="100%",
        height="100%",
        _hover={"border_color": "var(--accent-7)"},
        transition="border-color 0.15s ease",
    )
    if help_text:
        return rx.tooltip(card, content=help_text)
    return card


# ---------------------------------------------------------------------------
# Sidebar

def _sidebar_section(title: str, *children: rx.Component) -> rx.Component:
    return rx.vstack(
        rx.text(
            title,
            size="1",
            weight="medium",
            color_scheme="gray",
            class_name="uppercase tracking-wider",
        ),
        *children,
        spacing="2",
        align="stretch",
        width="100%",
    )


def _sidebar_form() -> rx.Component:
    return _sidebar_section(
        "Market",
        rx.select(
            PRESET_LABELS,
            value=LookupState.preset_label,
            on_change=LookupState.set_preset,
            placeholder="Quick-pick",
            width="100%",
        ),
        rx.input(
            placeholder="City (e.g. Evans)",
            value=LookupState.city,
            on_change=LookupState.set_city,
            width="100%",
        ),
        rx.select(
            STATE_LIST,
            value=LookupState.state,
            on_change=LookupState.set_state,
            width="100%",
        ),
        rx.accordion.root(
            rx.accordion.item(
                header="Advanced options",
                content=rx.vstack(
                    rx.checkbox(
                        "Include boundary tracts",
                        checked=LookupState.include_boundary,
                        on_change=LookupState.set_include_boundary,
                    ),
                    rx.checkbox(
                        "Skip Ookla measured speeds",
                        checked=LookupState.no_speeds,
                        on_change=LookupState.set_no_speeds,
                    ),
                    rx.checkbox(
                        "Skip Google ratings",
                        checked=LookupState.no_ratings,
                        on_change=LookupState.set_no_ratings,
                    ),
                    rx.checkbox(
                        "Include 12-month velocity",
                        checked=LookupState.include_velocity,
                        on_change=LookupState.set_include_velocity,
                    ),
                    rx.checkbox(
                        "Include multi-release trajectory",
                        checked=LookupState.include_trajectory,
                        on_change=LookupState.set_include_trajectory,
                    ),
                    spacing="2",
                    align="start",
                    padding_y="2",
                ),
                value="adv",
            ),
            collapsible=True,
            type="single",
            width="100%",
            variant="ghost",
        ),
        rx.button(
            rx.cond(LookupState.is_loading, "Looking up...", "Look up"),
            on_click=LookupState.run_lookup,
            loading=LookupState.is_loading,
            width="100%",
            size="2",
        ),
        rx.cond(
            LookupState.lookup_error != "",
            rx.callout(
                LookupState.lookup_error,
                icon="triangle_alert",
                color_scheme="red",
                size="1",
            ),
        ),
    )


def _sidebar_lens() -> rx.Component:
    return rx.cond(
        LookupState.has_result,
        _sidebar_section(
            "Strategic lens",
            rx.radio(
                [label for label, _ in LENS_OPTIONS],
                value=rx.cond(
                    LookupState.lens == "neutral",
                    LENS_OPTIONS[0][0],
                    rx.cond(
                        LookupState.lens == "defensive",
                        LENS_OPTIONS[1][0],
                        LENS_OPTIONS[2][0],
                    ),
                ),
                on_change=LookupState.set_lens,
                direction="column",
                spacing="1",
            ),
            rx.cond(
                LookupState.lens == "defensive",
                rx.vstack(
                    rx.text(
                        "Incumbent to defend",
                        size="1",
                        color_scheme="gray",
                        weight="medium",
                    ),
                    rx.select(
                        LookupState.incumbent_options,
                        value=LookupState.incumbent,
                        on_change=LookupState.set_incumbent,
                        width="100%",
                    ),
                    spacing="1",
                    width="100%",
                    margin_top="2",
                ),
            ),
        ),
    )


def _sidebar() -> rx.Component:
    return rx.vstack(
        rx.hstack(
            rx.icon("radio_tower", size=22, color="var(--accent-9)"),
            rx.vstack(
                rx.heading("ftth-compete", size="4", weight="bold"),
                rx.text("FTTH market intel", size="1", color_scheme="gray"),
                spacing="0",
                align="start",
            ),
            spacing="2",
            align="center",
            width="100%",
        ),
        rx.divider(),
        _sidebar_form(),
        rx.divider(),
        _sidebar_lens(),
        spacing="4",
        align="stretch",
        width="320px",
        height="100vh",
        padding="5",
        border_right="1px solid var(--gray-a4)",
        background_color="var(--gray-1)",
        overflow_y="auto",
    )


# ---------------------------------------------------------------------------
# Overview tab

def _overview_subtitle() -> rx.Component:
    """Subtitle showing tracts analyzed + boundary count.

    f-strings don't work with Reflex Vars (Python evaluates them at module
    load time, producing the literal var-name text), so we compose the
    string with `+` operators that the Var system intercepts.
    """
    return rx.text(
        rx.cond(
            LookupState.n_boundary > 0,
            LookupState.n_tracts.to_string() + " census tracts analyzed · "
            + LookupState.n_boundary.to_string() + " boundary tracts excluded",
            LookupState.n_tracts.to_string() + " census tracts analyzed",
        ),
        size="2",
        color_scheme="gray",
    )


def _kpi_grid(*cards: rx.Component) -> rx.Component:
    return rx.grid(
        *cards,
        columns="4",
        spacing="3",
        width="100%",
    )


def _overview_demographics() -> rx.Component:
    return rx.vstack(
        _section_title("Market scale and demographics"),
        _kpi_grid(
            _kpi_card("Population", LookupState.population_display),
            _kpi_card(
                "Median HH income",
                LookupState.mfi_display,
                help_text="Population-weighted mean of tract medians (ACS B19013).",
            ),
            _kpi_card(
                "Poverty rate",
                LookupState.poverty_rate_display,
                help_text="Share below the federal poverty line (ACS B17001). National ~12.4%.",
            ),
            _kpi_card(
                "Housing units",
                LookupState.housing_units_display,
                help_text="Total housing units across analyzed tracts (ACS B25001).",
            ),
        ),
        spacing="0",
        align="stretch",
        width="100%",
    )


def _overview_landscape() -> rx.Component:
    return rx.vstack(
        _section_title("Competitive landscape"),
        _kpi_grid(
            _kpi_card(
                "MDU share",
                LookupState.mdu_share_display,
                help_text="Share of housing units in 2+ unit structures (ACS B25024). National ~27%.",
            ),
            _kpi_card(
                "Providers",
                LookupState.n_distinct_providers_display,
                help_text="Distinct canonical providers serving any location (FCC BDC).",
            ),
            _kpi_card(
                "Fiber available",
                LookupState.fiber_avail_display,
                help_text="Share of locations where any provider offers fiber.",
            ),
            _kpi_card(
                "Boundary tracts",
                LookupState.n_boundary_display,
                help_text="Tracts touching the city polygon but with centroid outside.",
            ),
        ),
        spacing="0",
        align="stretch",
        width="100%",
    )


def _availability_card(label: str, share_display: rx.Var, helptext: str) -> rx.Component:
    return _kpi_card(label, share_display, help_text=helptext)


def _overview_availability() -> rx.Component:
    return rx.cond(
        LookupState.has_availability,
        rx.vstack(
            _section_title("Location-level technology availability"),
            rx.text(
                "Share of FCC BSLs where at least one provider offers each technology.",
                size="2",
                color_scheme="gray",
                margin_bottom="2",
            ),
            rx.grid(
                _availability_card("Fiber", LookupState.availability_fiber_display,
                                   "Locations where any provider offers FTTP (tech 50)."),
                _availability_card("Cable", LookupState.availability_cable_display,
                                   "Locations where any cable provider serves (HFC, tech 40)."),
                _availability_card("DSL", LookupState.availability_dsl_display,
                                   "Locations where any DSL provider serves (tech 10)."),
                _availability_card("Fixed wireless", LookupState.availability_fw_display,
                                   "Tech 70/71/72."),
                _availability_card("Satellite", LookupState.availability_sat_display,
                                   "Tech 60/61. Usually ~100%."),
                columns="5",
                spacing="3",
                width="100%",
            ),
            spacing="0",
            align="stretch",
            width="100%",
        ),
    )


def _overview_ias() -> rx.Component:
    return rx.cond(
        LookupState.has_ias,
        rx.vstack(
            _section_title("Broadband subscription density (FCC IAS)"),
            rx.text(
                "All-tech aggregate adoption from FCC IAS tract buckets. Not a per-fiber-provider take rate.",
                size="2",
                color_scheme="gray",
                margin_bottom="2",
            ),
            _kpi_grid(
                _kpi_card(
                    "Take rate (≥25/3)",
                    LookupState.ias_take_rate_display,
                    help_text="Share of housing units with active ≥25/3 broadband.",
                ),
                _kpi_card(
                    "Estimated subs",
                    LookupState.ias_subs_display,
                    help_text="Mid estimate of total broadband subscribers.",
                ),
                _kpi_card("Housing units", LookupState.ias_housing_display),
                _kpi_card(
                    "IAS as-of",
                    LookupState.ias_release,
                    help_text="FCC IAS publishes with a 1-2yr lag.",
                ),
            ),
            spacing="0",
            align="stretch",
            width="100%",
        ),
        rx.cond(
            LookupState.ias_note != "",
            rx.text(
                "IAS subscription anchor: " + LookupState.ias_note,
                size="1",
                color_scheme="gray",
                margin_top="2",
            ),
        ),
    )


def _overview_acp() -> rx.Component:
    """ACP enrollment density panel — peak EBB/ACP household share per market.

    Only renders when an ACP file has been loaded for the lookup. The
    market-weighted density is also embedded as a covariate in the
    penetration estimator (see analysis/penetration.py).
    """
    return rx.cond(
        LookupState.has_acp,
        rx.vstack(
            _section_title("ACP / EBB enrollment density"),
            rx.text(
                "Share of housing units that were enrolled in the ACP (or its "
                "EBB predecessor) at the snapshot date. Used as a low-income "
                "broadband covariate in the penetration estimator — providers "
                "with strong ACP plans (Comcast/Spectrum/AT&T) get a "
                "take-rate boost in high-ACP-density markets.",
                size="2",
                color_scheme="gray",
                margin_bottom="2",
            ),
            _kpi_grid(
                _kpi_card(
                    "ACP density (market avg)",
                    LookupState.market_acp_density_display,
                    help_text=(
                        "Allocated ACP households / total housing units. "
                        "US national peak baseline was ~18%."
                    ),
                ),
            ),
            spacing="0",
            align="stretch",
            width="100%",
        ),
    )


def _overview_speeds() -> rx.Component:
    return rx.cond(
        LookupState.has_speeds,
        rx.vstack(
            _section_title("Measured network reality (Ookla speedtest)"),
            _kpi_grid(
                _kpi_card(
                    "Median down (measured)",
                    LookupState.speed_down_display,
                    help_text="Average of tract-level medians.",
                ),
                _kpi_card("Median up", LookupState.speed_up_display),
                _kpi_card("Median latency", LookupState.speed_lat_display),
                _kpi_card(
                    "Speedtest sample",
                    LookupState.speed_tests_display,
                    help_text="Total Ookla tests aggregated across tracts.",
                ),
            ),
            spacing="0",
            align="stretch",
            width="100%",
        ),
        rx.cond(
            LookupState.speeds_note != "",
            rx.text(
                "Speed data: " + LookupState.speeds_note,
                size="1",
                color_scheme="gray",
                margin_top="2",
            ),
        ),
    )


def _velo_row(v: rx.Var) -> rx.Component:
    """One velocity row — all formatting was done in Python during
    `_populate_from_sheet`, so we just read pre-baked string fields."""
    return rx.hstack(
        rx.badge(
            v["badge_label"].to(str),
            color_scheme=v["badge_color"].to(str),
            variant="soft",
        ),
        rx.text(v["name"].to(str), weight="medium"),
        rx.text(v["detail"].to(str), size="1", color_scheme="gray"),
        spacing="2",
        align="center",
    )


def _overview_velocity() -> rx.Component:
    has_velocity = (LookupState.velocity_growers.length() > 0) | (
        LookupState.velocity_decliners.length() > 0
    )
    return rx.cond(
        has_velocity,
        rx.vstack(
            _section_title("12-month fiber footprint change"),
            rx.text(LookupState.velocity_release_label, size="2", color_scheme="gray"),
            rx.grid(
                rx.vstack(
                    rx.text("Top expanders", weight="bold", size="2"),
                    rx.foreach(LookupState.velocity_growers, _velo_row),
                    rx.cond(
                        LookupState.velocity_growers.length() == 0,
                        rx.text(
                            "No fiber providers expanded year-over-year.",
                            size="1", color_scheme="gray",
                        ),
                    ),
                    spacing="2",
                    align="start",
                ),
                rx.vstack(
                    rx.text("Top decliners", weight="bold", size="2"),
                    rx.foreach(LookupState.velocity_decliners, _velo_row),
                    rx.cond(
                        LookupState.velocity_decliners.length() == 0,
                        rx.text(
                            "No fiber providers contracted year-over-year.",
                            size="1", color_scheme="gray",
                        ),
                    ),
                    spacing="2",
                    align="start",
                ),
                columns="2",
                spacing="6",
                width="100%",
            ),
            spacing="2",
            align="stretch",
            width="100%",
        ),
    )


def _overview_opportunity() -> rx.Component:
    # Only render when offensive lens is active AND we have opportunity data.
    show = (LookupState.lens == "offensive") & LookupState.has_opportunity
    return rx.cond(
        show,
        rx.vstack(
            _section_title("Market opportunity (entrant view)"),
            rx.card(
                rx.hstack(
                    rx.vstack(
                        rx.text(LookupState.opp_headline, weight="bold", size="4"),
                        rx.badge(
                            "Score " + LookupState.opp_score_display,
                            color_scheme=rx.cond(
                                LookupState.opp_score >= 0.55, "green",
                                rx.cond(LookupState.opp_score >= 0.35, "orange", "red"),
                            ),
                            size="2",
                        ),
                        spacing="2",
                        align="start",
                    ),
                    rx.divider(orientation="vertical"),
                    rx.vstack(
                        rx.text("No-fiber providers: " + LookupState.opp_no_fiber_display, size="2"),
                        rx.text("Cable-only providers: " + LookupState.opp_cable_only_display, size="2"),
                        rx.text("Rating weakness: " + LookupState.opp_rating_weak_display, size="2"),
                        rx.text("MDU build economics: " + LookupState.opp_mdu_score_display, size="2"),
                        spacing="1",
                        align="start",
                    ),
                    spacing="5",
                    align="start",
                ),
                size="3",
                width="100%",
            ),
            spacing="2",
            width="100%",
        ),
    )


def _overview_snapshot() -> rx.Component:
    return rx.vstack(
        _section_title("Snapshot"),
        rx.cond(
            LookupState.narrative_text != "",
            rx.card(rx.text(LookupState.narrative_text, size="3"), size="2"),
            rx.text("Narrative unavailable.", color_scheme="gray", size="2"),
        ),
        spacing="2",
        width="100%",
    )


def _overview_tracts() -> rx.Component:
    return rx.accordion.root(
        rx.accordion.item(
            header="Tract details",
            content=rx.vstack(
                rx.text("Inside city (analyzed):", weight="medium", size="2"),
                rx.code(
                    LookupState.inside_city_tracts.join(", "),
                    class_name="break-all",
                ),
                rx.cond(
                    LookupState.boundary_tracts.length() > 0,
                    rx.vstack(
                        rx.text("Boundary (excluded by default):", weight="medium", size="2"),
                        rx.code(
                            LookupState.boundary_tracts.join(", "),
                            class_name="break-all",
                        ),
                        spacing="1",
                        margin_top="3",
                        align="start",
                    ),
                ),
                padding_y="2",
                spacing="2",
                align="start",
            ),
            value="tracts",
        ),
        collapsible=True,
        type="single",
        variant="ghost",
        width="100%",
    )


def _overview_tab() -> rx.Component:
    return rx.vstack(
        _overview_subtitle(),
        _overview_demographics(),
        _overview_landscape(),
        _overview_availability(),
        _overview_ias(),
        _overview_acp(),
        _overview_speeds(),
        _overview_velocity(),
        _overview_opportunity(),
        _overview_snapshot(),
        _overview_tracts(),
        rx.cond(
            LookupState.providers_note != "",
            rx.callout(LookupState.providers_note, icon="info", color_scheme="amber"),
        ),
        spacing="5",
        width="100%",
        padding="6",
        align="stretch",
    )


# ---------------------------------------------------------------------------
# Competitors tab

_SORT_OPTIONS_CP: list[tuple[str, str]] = [
    ("fiber_first", "Fiber-first, then coverage"),
    ("coverage_desc", "Coverage % (desc)"),
    ("locations_desc", "Locations served (desc)"),
    ("speed_desc", "Max advertised down (desc)"),
    ("name_asc", "Name (A-Z)"),
]
_SORT_LABEL_TO_KEY: dict[str, str] = {label: key for key, label in _SORT_OPTIONS_CP}
_SORT_KEY_TO_LABEL: dict[str, str] = {key: label for key, label in _SORT_OPTIONS_CP}


def _lens_banner() -> rx.Component:
    """Top-of-tab strip explaining the active lens, color-coded.

    Hidden under the neutral lens. Defensive variant calls out the
    chosen incumbent; offensive variant frames the page as an entry-target
    view. Tracks `lens_banner_kind` (recomputed in `_recompute_visible_providers`).
    """
    return rx.match(
        LookupState.lens_banner_kind,
        ("defensive", rx.callout(
            "Incumbent-defensive view — defending " + LookupState.incumbent
            + ". Other providers are ranked by threat (fiber attack potential, "
            "coverage, rating advantage).",
            icon="shield",
            color_scheme="blue",
        )),
        ("defensive_missing", rx.callout(
            "Defensive lens active but no incumbent picked. Choose one in the sidebar to see threat scores.",
            icon="triangle_alert",
            color_scheme="amber",
        )),
        ("offensive", rx.callout(
            "New-entrant-offensive view — providers ranked by vulnerability to disruption. "
            "Top-of-list = easiest entry target.",
            icon="zap",
            color_scheme="violet",
        )),
        rx.fragment(),
    )


def _competitors_controls() -> rx.Component:
    """Sort, category filter, fiber-only toggle, view-mode."""
    sort_labels = [label for _, label in _SORT_OPTIONS_CP]
    return rx.hstack(
        rx.vstack(
            rx.text("Sort by", size="1", color_scheme="gray", weight="medium"),
            rx.select(
                sort_labels,
                value=rx.cond(
                    LookupState.cp_sort_key == "fiber_first", _SORT_KEY_TO_LABEL["fiber_first"],
                    rx.cond(
                        LookupState.cp_sort_key == "coverage_desc", _SORT_KEY_TO_LABEL["coverage_desc"],
                        rx.cond(
                            LookupState.cp_sort_key == "locations_desc", _SORT_KEY_TO_LABEL["locations_desc"],
                            rx.cond(
                                LookupState.cp_sort_key == "speed_desc", _SORT_KEY_TO_LABEL["speed_desc"],
                                _SORT_KEY_TO_LABEL["name_asc"],
                            ),
                        ),
                    ),
                ),
                on_change=lambda v: LookupState.set_cp_sort(
                    # Convert clicked label back to key via Reflex JS bridge.
                    rx.match(
                        v,
                        (_SORT_KEY_TO_LABEL["fiber_first"], "fiber_first"),
                        (_SORT_KEY_TO_LABEL["coverage_desc"], "coverage_desc"),
                        (_SORT_KEY_TO_LABEL["locations_desc"], "locations_desc"),
                        (_SORT_KEY_TO_LABEL["speed_desc"], "speed_desc"),
                        (_SORT_KEY_TO_LABEL["name_asc"], "name_asc"),
                        "fiber_first",
                    )
                ),
                disabled=LookupState.lens != "neutral",
                width="240px",
            ),
            spacing="1",
            align="start",
        ),
        rx.vstack(
            rx.text("Category", size="1", color_scheme="gray", weight="medium"),
            rx.foreach(
                LookupState.cp_category_options,
                lambda cat: _category_chip(cat),
            ),
            spacing="1",
            align="start",
            class_name="flex flex-row flex-wrap gap-2",
        ),
        rx.spacer(),
        rx.vstack(
            rx.text(" ", size="1"),
            rx.checkbox(
                "Fiber only",
                checked=LookupState.cp_fiber_only,
                on_change=LookupState.set_cp_fiber_only,
            ),
            spacing="1",
            align="start",
        ),
        spacing="6",
        width="100%",
        align="end",
        wrap="wrap",
    )


def _category_chip(cat: rx.Var) -> rx.Component:
    """A clickable category chip. Visually active when in the CSV filter."""
    active = LookupState.cp_categories_csv.contains(cat)
    return rx.badge(
        cat.to(str),
        on_click=lambda: LookupState.toggle_cp_category(cat),
        variant=rx.cond(active, "solid", "soft"),
        color_scheme=rx.cond(active, "blue", "gray"),
        cursor="pointer",
        size="2",
    )


def _summary_strip() -> rx.Component:
    """Compact KPI row above the cards: showing / fiber rows / full coverage / top speed."""
    return rx.grid(
        _kpi_card(
            "Showing",
            LookupState.cp_visible_rows.to_string() + " of " + LookupState.cp_total_rows.to_string(),
            help_text="Provider × technology rows after filters.",
        ),
        _kpi_card(
            "Fiber rows",
            LookupState.cp_fiber_rows.to_string(),
            help_text="Provider rows where technology = Fiber (tech code 50).",
        ),
        _kpi_card(
            "Full coverage",
            LookupState.cp_full_coverage_rows.to_string(),
            help_text="Providers serving 99%+ of analysis tracts at this tech.",
        ),
        _kpi_card(
            "Top advertised",
            LookupState.cp_top_speed_display,
            help_text="Highest BDC max-advertised down across visible rows. Marketing claim — see Map tab for measured speeds.",
        ),
        columns="4",
        spacing="3",
        width="100%",
    )


def _coverage_bar(pct_var: rx.Var) -> rx.Component:
    """Inline coverage progress bar. Renders width via inline style.

    `pct_var` is a dict-indexed Var (i.e., `row["coverage_pct"]`), which
    Reflex treats as untyped `ObjectItemOperation` — arithmetic ops fail
    unless we cast to float first.
    """
    width_expr = (pct_var.to(float) * 100).to_string() + "%"
    return rx.box(
        rx.box(
            background_color="var(--accent-9)",
            height="100%",
            width=width_expr,
            transition="width 0.3s ease",
        ),
        height="6px",
        width="100%",
        background_color="var(--gray-a4)",
        border_radius="3px",
        overflow="hidden",
    )


def _speed_tier_bar(row: rx.Var) -> rx.Component:
    """Three-segment stacked bar — gig+ / 100Mbps+ / <100 — sized by share.

    Reuses pre-computed `gig_pct_str` / etc. (CSS width strings) so we
    don't fight Var arithmetic at render time.
    """
    return rx.cond(
        row["has_tiers"],
        rx.vstack(
            rx.box(
                rx.box(
                    height="100%",
                    background="var(--green-9)",
                    width=row["gig_pct_str"].to(str) + "%",
                ),
                rx.box(
                    height="100%",
                    background="var(--blue-9)",
                    width=row["hundred_pct_str"].to(str) + "%",
                ),
                rx.box(
                    height="100%",
                    background="var(--gray-7)",
                    width=row["slow_pct_str"].to(str) + "%",
                ),
                display="flex",
                height="6px",
                width="100%",
                border_radius="3px",
                overflow="hidden",
            ),
            rx.text(
                row["tier_caption"].to(str),
                size="1",
                color_scheme="gray",
            ),
            spacing="1",
            width="100%",
            align="start",
        ),
    )


def _lens_badge(row: rx.Var) -> rx.Component:
    """Lens score badge ("Threat 0.81" etc.) or Incumbent pill, or nothing."""
    return rx.cond(
        row["is_incumbent"],
        rx.badge("Incumbent", color_scheme="blue", variant="soft"),
        rx.cond(
            row["lens_label"] != "",
            rx.badge(
                row["lens_label"].to(str),
                color_scheme=row["lens_color"].to(str),
                variant="soft",
            ),
            rx.fragment(),
        ),
    )


def _rating_block(row: rx.Var) -> rx.Component:
    """Compact rating line: stars + numeric + review count."""
    return rx.cond(
        row["has_rating"],
        rx.hstack(
            rx.text(
                row["stars"].to(str),
                color=rx.cond(
                    row["rating_color"] == "green", "var(--green-10)",
                    rx.cond(row["rating_color"] == "orange", "var(--orange-10)", "var(--red-10)"),
                ),
                size="3",
            ),
            rx.text(
                row["rating_display"].to(str),
                size="2",
                weight="medium",
            ),
            rx.text(
                "·  " + row["rating_count_display"].to(str),
                size="1",
                color_scheme="gray",
            ),
            spacing="2",
            align="center",
        ),
    )


def _subs_block(row: rx.Var) -> rx.Component:
    return rx.cond(
        row["has_subs"],
        rx.hstack(
            rx.badge(
                row["subs_confidence"].to(str),
                color_scheme=row["subs_color"].to(str),
                variant="soft",
                size="1",
            ),
            rx.text("Est. subs " + row["subs_display"].to(str), size="2"),
            spacing="2",
            align="center",
        ),
    )


def _velocity_block(row: rx.Var) -> rx.Component:
    return rx.cond(
        row["has_velocity"],
        rx.hstack(
            rx.badge(
                row["velocity_badge"].to(str),
                color_scheme=row["velocity_color"].to(str),
                variant="soft",
            ),
            rx.text(row["velocity_caption"].to(str), size="1", color_scheme="gray"),
            spacing="2",
            align="center",
            wrap="wrap",
        ),
    )


def _trajectory_block(row: rx.Var) -> rx.Component:
    return rx.cond(
        row["has_trajectory"],
        rx.hstack(
            rx.html(row["trajectory_svg"].to(str)),
            rx.text(row["trajectory_caption"].to(str), size="1", color_scheme="gray"),
            spacing="3",
            align="center",
        ),
    )


def _provider_card(row: rx.Var) -> rx.Component:
    """One polished provider card."""
    return rx.card(
        rx.vstack(
            # Header
            rx.hstack(
                rx.vstack(
                    rx.heading(
                        row["name"].to(str) + " — " + row["tech_label"].to(str),
                        size="3",
                        weight="bold",
                    ),
                    rx.text(
                        row["holding"].to(str),
                        size="1",
                        color_scheme="gray",
                    ),
                    spacing="0",
                    align="start",
                ),
                rx.spacer(),
                _lens_badge(row),
                width="100%",
                align="start",
            ),
            rx.hstack(
                rx.badge(
                    row["category_label"].to(str),
                    color_scheme=row["category_color"].to(str),
                    variant="soft",
                ),
                rx.badge(
                    row["tech_label"].to(str),
                    color_scheme=row["tech_color"].to(str),
                    variant="soft",
                ),
                spacing="2",
            ),
            rx.divider(margin_y="2"),
            # Coverage
            rx.vstack(
                rx.hstack(
                    rx.text("Coverage", size="1", color_scheme="gray", weight="medium"),
                    rx.spacer(),
                    rx.text(
                        row["coverage_display"].to(str) + " of tracts",
                        size="2",
                        weight="medium",
                    ),
                    width="100%",
                ),
                _coverage_bar(row["coverage_pct"]),
                spacing="1",
                width="100%",
                align="stretch",
            ),
            # Speed + Locations row
            rx.grid(
                rx.vstack(
                    rx.text("Max down (advertised)", size="1", color_scheme="gray"),
                    rx.text(
                        row["max_down_display"].to(str),
                        size="4",
                        weight="bold",
                    ),
                    spacing="1",
                    align="start",
                ),
                rx.vstack(
                    rx.text("Locations served", size="1", color_scheme="gray"),
                    rx.text(
                        row["locations_display"].to(str),
                        size="4",
                        weight="bold",
                    ),
                    spacing="1",
                    align="start",
                ),
                columns="2",
                spacing="3",
                width="100%",
            ),
            # Speed tier mini-bar
            _speed_tier_bar(row),
            # Rating
            _rating_block(row),
            # Subs estimate
            _subs_block(row),
            # Velocity
            _velocity_block(row),
            # Trajectory sparkline
            _trajectory_block(row),
            # Raw brand names (only when interesting)
            rx.cond(
                row["raw_brands_str"] != "",
                rx.text(
                    row["raw_brands_str"].to(str),
                    size="1",
                    color_scheme="gray",
                    class_name="italic",
                ),
            ),
            spacing="3",
            align="stretch",
            width="100%",
        ),
        size="2",
        width="100%",
        on_click=LookupState.select_provider(row["name"].to(str)),
        cursor="pointer",
        _hover={
            "border_color": "var(--teal-7)",
            "background_color": "var(--gray-a2)",
        },
        transition="border-color 0.15s ease, background-color 0.15s ease",
    )


def _provider_table_row(row: rx.Var) -> rx.Component:
    return rx.table.row(
        rx.table.cell(rx.text(row["name"].to(str), weight="medium")),
        rx.table.cell(row["tech_label"].to(str)),
        rx.table.cell(row["category_label"].to(str)),
        rx.table.cell(row["coverage_display"].to(str)),
        rx.table.cell(row["locations_display"].to(str), justify="end"),
        rx.table.cell(row["max_down_display"].to(str), justify="end"),
        rx.table.cell(
            rx.cond(
                row["has_subs"],
                rx.text(row["subs_display"].to(str), size="2"),
                rx.text("—", color_scheme="gray", size="2"),
            ),
        ),
        rx.table.cell(
            rx.cond(
                row["has_rating"],
                rx.text(row["rating_display"].to(str) + " ★", size="2"),
                rx.text("—", color_scheme="gray", size="2"),
            ),
        ),
        on_click=LookupState.select_provider(row["name"].to(str)),
        style={"cursor": "pointer"},
        _hover={"background_color": "var(--gray-a2)"},
    )


def _competitors_cards() -> rx.Component:
    return rx.grid(
        rx.foreach(LookupState.visible_providers, _provider_card),
        columns="3",
        spacing="4",
        width="100%",
    )


def _competitors_table() -> rx.Component:
    return rx.table.root(
        rx.table.header(
            rx.table.row(
                rx.table.column_header_cell("Provider"),
                rx.table.column_header_cell("Tech"),
                rx.table.column_header_cell("Category"),
                rx.table.column_header_cell("Coverage"),
                rx.table.column_header_cell("Locations", justify="end"),
                rx.table.column_header_cell("Max down", justify="end"),
                rx.table.column_header_cell("Est. subs"),
                rx.table.column_header_cell("Rating"),
            ),
        ),
        rx.table.body(
            rx.foreach(LookupState.visible_providers, _provider_table_row),
        ),
        variant="surface",
        size="2",
        width="100%",
    )


def _competitors_tab() -> rx.Component:
    return rx.cond(
        LookupState.providers_data.length() > 0,
        rx.vstack(
            _lens_banner(),
            _competitors_controls(),
            _summary_strip(),
            rx.segmented_control.root(
                rx.segmented_control.item("Cards", value="Cards"),
                rx.segmented_control.item("Table", value="Table"),
                value=LookupState.cp_view,
                on_change=LookupState.set_cp_view,
                size="2",
            ),
            rx.cond(
                LookupState.cp_view == "Table",
                _competitors_table(),
                _competitors_cards(),
            ),
            spacing="5",
            padding="6",
            width="100%",
            align="stretch",
        ),
        rx.box(
            rx.callout(
                rx.cond(
                    LookupState.providers_note != "",
                    LookupState.providers_note,
                    "No providers found in FCC BDC for this market.",
                ),
                icon="info",
                color_scheme="amber",
            ),
            padding="6",
        ),
    )


# ---------------------------------------------------------------------------
# Housing tab

def _housing_summary() -> rx.Component:
    """4-card summary strip: SFH share, MDU share, Mobile/other, Total."""
    return rx.grid(
        _kpi_card(
            "Single-family share",
            LookupState.sfh_share_display,
            delta=LookupState.sfh_delta_pp_display,
            help_text="1-unit detached + 1-unit attached. National SFH share ~66%.",
        ),
        _kpi_card(
            "MDU share",
            LookupState.mdu_share_display,
            delta=LookupState.mdu_delta_pp_display,
            help_text="All 2+ unit structures. National MDU share ~27%.",
        ),
        _kpi_card(
            "Mobile/other share",
            LookupState.other_share_display,
            help_text="Mobile homes, boats, RVs, vans.",
        ),
        _kpi_card("Total housing units", LookupState.housing_total_display),
        columns="4",
        spacing="3",
        width="100%",
    )


def _housing_mdu_sub_metrics() -> rx.Component:
    """Sub-row that breaks MDU into small / mid / large."""
    return rx.grid(
        _kpi_card(
            "Small MDU (2-4u)",
            LookupState.mdu_small_display,
            delta=LookupState.mdu_small_share_display,
            help_text="Easier MDU fiber economics — surface-mount drops or shared aerial.",
        ),
        _kpi_card(
            "Mid MDU (5-19u)",
            LookupState.mdu_mid_display,
            delta=LookupState.mdu_mid_share_display,
            help_text="Mid-rise. Often the most contested segment.",
        ),
        _kpi_card(
            "Large MDU (20+u)",
            LookupState.mdu_large_display,
            delta=LookupState.mdu_large_share_display,
            help_text="High-rise. Bulk-deal economics, longer sales cycle.",
        ),
        columns="3",
        spacing="3",
        width="100%",
    )


def _unit_bucket_row(b: rx.Var) -> rx.Component:
    """One horizontal bar in the B25024 unit-type chart.

    Width = bucket count / max bucket count, computed in JS at render
    time. Color comes from pre-baked `bar_color` Var string.
    """
    # Bar width as a pure JS expression: (count / max) * 100 %.
    # Using rx.Var arithmetic so the expression is reactive.
    width_var = (
        (b["count"].to(int) / LookupState.unit_buckets_max.to(int)) * 100
    ).to_string() + "%"
    return rx.hstack(
        rx.text(
            b["label"].to(str),
            size="2",
            color_scheme="gray",
            width="160px",
            class_name="text-right",
        ),
        rx.box(
            rx.box(
                background_color=b["bar_color"].to(str),
                height="100%",
                width=width_var,
                border_radius="3px",
                transition="width 0.3s ease",
            ),
            height="18px",
            background_color="var(--gray-a3)",
            border_radius="3px",
            flex_grow="1",
        ),
        rx.text(
            b["count_display"].to(str),
            size="2",
            weight="medium",
            width="100px",
            class_name="text-right",
        ),
        spacing="3",
        align="center",
        width="100%",
    )


def _housing_unit_type_chart() -> rx.Component:
    return rx.cond(
        LookupState.unit_buckets.length() > 0,
        rx.card(
            rx.vstack(
                rx.foreach(LookupState.unit_buckets, _unit_bucket_row),
                spacing="2",
                width="100%",
            ),
            size="2",
            width="100%",
        ),
        rx.text(
            "Per-bucket B25024 data unavailable.",
            size="2",
            color_scheme="gray",
        ),
    )


def _tract_housing_row(r: rx.Var) -> rx.Component:
    """One row in the per-tract MDU detail table."""
    return rx.table.row(
        rx.table.cell(rx.code(r["geoid"].to(str), size="1")),
        rx.table.cell(r["population_display"].to(str), justify="end"),
        rx.table.cell(r["housing_display"].to(str), justify="end"),
        rx.table.cell(r["sfh_display"].to(str), justify="end"),
        rx.table.cell(r["mdu_display"].to(str), justify="end"),
        rx.table.cell(r["other_display"].to(str), justify="end"),
        rx.table.cell(
            rx.hstack(
                rx.box(
                    rx.box(
                        background="var(--orange-9)",
                        height="100%",
                        width=r["mdu_pct_str"].to(str) + "%",
                        border_radius="2px",
                    ),
                    height="8px",
                    background="var(--gray-a3)",
                    border_radius="2px",
                    width="80px",
                ),
                rx.text(r["mdu_share_display"].to(str), size="2"),
                spacing="2",
                align="center",
            ),
        ),
    )


def _housing_tract_table() -> rx.Component:
    return rx.cond(
        LookupState.tract_housing_rows.length() > 0,
        rx.table.root(
            rx.table.header(
                rx.table.row(
                    rx.table.column_header_cell("Tract GEOID"),
                    rx.table.column_header_cell("Population", justify="end"),
                    rx.table.column_header_cell("Housing units", justify="end"),
                    rx.table.column_header_cell("SFH", justify="end"),
                    rx.table.column_header_cell("MDU", justify="end"),
                    rx.table.column_header_cell("Other", justify="end"),
                    rx.table.column_header_cell("MDU share"),
                ),
            ),
            rx.table.body(
                rx.foreach(LookupState.tract_housing_rows, _tract_housing_row),
            ),
            variant="surface",
            size="1",
            width="100%",
        ),
    )


def _housing_tab() -> rx.Component:
    return rx.cond(
        LookupState.has_housing,
        rx.vstack(
            _section_title("Housing stock summary"),
            _housing_summary(),
            _housing_mdu_sub_metrics(),
            _section_title("Unit-type breakdown (ACS B25024)"),
            rx.text(
                "Counts are total housing units, not occupied. SFH bars are blue, MDU orange, other gray.",
                size="2",
                color_scheme="gray",
                margin_bottom="2",
            ),
            _housing_unit_type_chart(),
            _section_title("Per-tract detail"),
            rx.text(
                "Sorted by MDU share (highest first) to spot MDU pockets within otherwise SFH-heavy markets.",
                size="2",
                color_scheme="gray",
                margin_bottom="2",
            ),
            _housing_tract_table(),
            spacing="3",
            width="100%",
            padding="6",
            align="stretch",
        ),
        rx.box(
            rx.callout(
                "No housing-stock data for this market.",
                icon="info",
                color_scheme="amber",
            ),
            padding="6",
        ),
    )


# ---------------------------------------------------------------------------
# Methodology tab — static documentation

_METHODOLOGY_INTRO = """
**ftth-compete** is a single-input competitive-intelligence tool. Enter a city + state,
get a tear-sheet covering demographics, every provider's footprint, technology mix,
estimated penetration, MDU/SFH split, advertised-vs-measured speeds, and Google
ratings — all from free public data with a tightly-budgeted Google Places quota for
ratings.

This page documents every metric, its source, and its limitations. Use it as the
authoritative reference when interpreting numbers in the rest of the dashboard.
"""

_METHODOLOGY_DATA_SOURCES = """
| Source | Use | Cadence | Cost |
| --- | --- | --- | --- |
| **FCC BDC** (Broadband Data Collection) | Per-provider × tech × location coverage | Semi-annual | Free (auth required) |
| **FCC IAS** (Internet Access Services) | Tract-level subscription density (bucketed, all-tech) | Semi-annual, ~1.5-year lag | Free |
| **Census ACS 5-Year** | Demographics, poverty, housing stock (B25024) | Annual | Free (API key required) |
| **Census TIGER/Line** | Tract polygons, city boundaries | Annual | Free |
| **Ookla Open Speedtest** | Measured down/up/latency at ~610m hex tile | Quarterly | Free (CC BY-NC-SA, non-commercial only) |
| **Google Places API (New)** | Provider star rating + review count | On demand | $17 per 1K Place Details lookups after 1K free events/mo |
| **Provider 10-Ks** | National subscriber totals as anchors | Annual | Free (manual scrape) |
"""

_METHODOLOGY_METRICS = """
### Market scale and demographics

- **Population** — sum of ACS B01003 across analyzed tracts.
- **Median HH income** — population-weighted mean of tract medians (ACS B19013). A
  proxy for the true market median; computing the exact median needs raw ACS PUMS data.
- **Poverty rate** — share of poverty-universe population below the federal poverty
  line (ACS B17001). National average is ~12.4%.
- **Housing units** — ACS B25001 total across analyzed tracts.

### Competitive landscape

- **MDU share** — share of housing units in 2+ unit structures (derived from B25024).
  National baseline ~27%.
- **Providers** — distinct canonical providers serving any location (FCC BDC).
  Multi-tech providers (e.g. Lumen offering both DSL and Fiber) count once here.
- **Fiber available** — share of FCC BSL locations where any provider offers FTTP
  (tech code 50). This is the household-availability question, NOT the provider-count
  question. A market with 12 providers but only 3 fiber providers can still have 80%
  fiber availability if those 3 cover most addresses.

### Location-level technology availability

Computed from BDC's `location_id`-level data. For each tech code, count distinct
BSL locations where AT LEAST ONE provider offers that technology. Tech codes:
10 DSL, 40 Cable, 50 Fiber, 60/61 Satellite, 70/71/72 Fixed Wireless.

### Speed-tier breakdown (Competitors tab)

Per provider × tech, the share of locations served at:

- **Gig+** — `max_advertised_download_speed >= 1000 Mbps`
- **100Mbps+** — `100 <= ... < 1000`
- **<100** — anything slower

Same source field as Max-down; this surfaces the *distribution* of advertised
speeds, not just the maximum.
"""

_METHODOLOGY_PENETRATION = """
### Penetration estimation — read this carefully

We do **not** publish per-provider "true" penetration. Public data doesn't support it.
What we publish:

1. **Heuristic estimates** per (provider, tech), computed as
   `locations_served × national_take_rate` where the take rate comes from each
   provider's 10-K (33 providers with explicit anchors; category defaults otherwise).
   Each estimate is a low/mid/high range with `±25%` confidence intervals.
2. **FCC IAS calibration anchor** — the public IAS dataset is **all-tech and
   all-provider aggregate** at the tract level, bucketed (not raw counts), with
   ~12-18 month lag. We use IAS as a *market-total* anchor to scale the
   heuristic per-provider estimates so the sum matches the IAS-implied market
   total. Calibration corrects the inherent double-counting where overlapping
   fiber footprints would otherwise be summed independently.

**What we don't claim.** No "Verizon Fios has 41.3% penetration in tract X." Always
"estimated 30–55% of fiber subscribers in this tract" with the methodology link.

**Improvement roadmap.** Per-fiber-provider tract penetration is the holy grail.
Realistic paths: (a) buy CostQuest BDC fabric for address-level, (b) ingest
internal subscriber data when available, (c) build a provider-name fuzzy normalizer
on Ookla `provider_name` to attribute measured speedtests to specific providers.
"""

_METHODOLOGY_VELOCITY = """
### 12-month velocity + multi-release trajectory

Two-snapshot version: compare current BDC release vs ~12 months ago. Per (provider,
tech), report absolute and percent delta in locations served. "NEW" flag fires when
a provider had zero footprint at this tech in the prior release. "Discontinued"
fires when they did and now don't.

Multi-release version (opt-in): 4 BDC releases ~6 months apart, ~2 years of data.
Rendered as an inline SVG sparkline on each provider card. Tells the
"how fast is overbuild happening" story across multiple snapshots, not just one
year-over-year delta.

### Measured speeds (Ookla)

Per-tract median down/up/latency, computed from Ookla's quarterly open speedtest
tiles (~610m hex resolution). Sample count surfaced — low-sample tracts should
be interpreted cautiously. Provider attribution is intentionally absent: Ookla's
tile aggregates don't include provider_name. We use the tract median as a measure
of *delivered* network reality alongside FCC BDC's max-advertised.
"""

_METHODOLOGY_LENSES = """
### Strategic lenses

Three lenses re-weight the same data:

- **Neutral** — alphabetical / coverage-first sort. The default.
- **Incumbent-defensive** — pick an incumbent in the sidebar; other providers
  ranked by threat (fiber attack potential, coverage, rating advantage vs incumbent,
  expansion velocity).
- **New-entrant-offensive** — providers ranked by vulnerability to disruption
  (no fiber offered, cable-only incumbents, low ratings, MDU build economics).
  The Overview tab also shows a market-level **Opportunity Score** in this lens.

Lens scoring is a thin re-ranking layer (`analysis/lenses.py`) — never mutates
the underlying data.
"""

_METHODOLOGY_LIMITATIONS = """
### Known limitations

- **IAS lag** — ~12-18 months. Fast-moving overbuild markets get understated.
- **IAS aggregation** — all-tech and all-provider only at tract level. No per-fiber-provider truth.
- **Provider name canonicalization** — manual curated registry covering ~50 providers.
  Long-tail regional WISPs may show as "Unknown."
- **Ookla provider attribution** — not available in open tile aggregates.
- **Boundary tracts** — defaults to centroid-in-city; toggleable in sidebar.
- **Cross-state metros** — supported via metro aliases (e.g. Kansas City Metro).
"""

_METHODOLOGY_ATTRIBUTIONS = """
### Attributions

- Speed test data © Ookla, 2019-present, distributed under
  [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/).
  Personal / non-commercial use only.
- FCC Broadband Data Collection, FCC IAS, Census ACS 5-Year, Census TIGER/Line —
  all public-domain US government data.
- Google Places API © Google. Star ratings used under Place Details billing tier.
"""


def _methodology_section(title: str, body: str) -> rx.Component:
    return rx.vstack(
        rx.heading(title, size="5", weight="bold", margin_top="3"),
        rx.markdown(body),
        spacing="2",
        align="start",
        width="100%",
    )


def _methodology_tab() -> rx.Component:
    return rx.vstack(
        rx.heading("Methodology and data sources", size="7", weight="bold"),
        rx.markdown(_METHODOLOGY_INTRO),
        _methodology_section("Data sources", _METHODOLOGY_DATA_SOURCES),
        _methodology_section("Metric definitions", _METHODOLOGY_METRICS),
        _methodology_section("Penetration estimation", _METHODOLOGY_PENETRATION),
        _methodology_section("Velocity and measured speeds", _METHODOLOGY_VELOCITY),
        _methodology_section("Strategic lenses", _METHODOLOGY_LENSES),
        _methodology_section("Limitations", _METHODOLOGY_LIMITATIONS),
        _methodology_section("Attributions", _METHODOLOGY_ATTRIBUTIONS),
        spacing="3",
        padding="6",
        width="100%",
        max_width="900px",
        align="stretch",
    )


# ---------------------------------------------------------------------------
# Compare tab — save & recall multiple markets

def _compare_kpi_cell(value: rx.Var, *, bold: bool = False) -> rx.Component:
    return rx.table.cell(
        rx.text(
            value.to(str),
            size="2",
            weight=rx.cond(bold, "bold", "regular"),
        ),
    )


def _compare_row(m: rx.Var) -> rx.Component:
    return rx.table.row(
        rx.table.cell(
            rx.hstack(
                rx.text(m["title"].to(str), weight="bold", size="2"),
                rx.icon_button(
                    rx.icon("x", size=12),
                    on_click=lambda: LookupState.remove_saved_market(m["title"].to(str)),
                    size="1",
                    variant="ghost",
                    color_scheme="gray",
                ),
                spacing="1",
                align="center",
            ),
        ),
        _compare_kpi_cell(m["population_display"]),
        _compare_kpi_cell(m["mfi_display"]),
        _compare_kpi_cell(m["mdu_share_display"]),
        _compare_kpi_cell(m["providers_display"]),
        _compare_kpi_cell(m["fiber_avail_display"]),
        _compare_kpi_cell(m["ias_take_rate_display"]),
    )


def _compare_tab() -> rx.Component:
    return rx.vstack(
        rx.hstack(
            rx.heading("Compare markets", size="6", weight="bold"),
            rx.spacer(),
            rx.button(
                rx.icon("plus", size=16),
                "Save current market",
                on_click=LookupState.save_current_market,
                variant="soft",
                size="2",
            ),
            rx.button(
                rx.icon("trash_2", size=16),
                "Clear",
                on_click=LookupState.clear_saved_markets,
                variant="outline",
                color_scheme="gray",
                size="2",
                disabled=LookupState.saved_markets.length() == 0,
            ),
            width="100%",
            align="center",
        ),
        rx.cond(
            LookupState.saved_markets.length() > 0,
            rx.table.root(
                rx.table.header(
                    rx.table.row(
                        rx.table.column_header_cell("Market"),
                        rx.table.column_header_cell("Population"),
                        rx.table.column_header_cell("Median HH income"),
                        rx.table.column_header_cell("MDU share"),
                        rx.table.column_header_cell("Providers"),
                        rx.table.column_header_cell("Fiber available"),
                        rx.table.column_header_cell("IAS take rate"),
                    ),
                ),
                rx.table.body(
                    rx.foreach(LookupState.saved_markets, _compare_row),
                ),
                variant="surface",
                size="2",
                width="100%",
            ),
            rx.callout(
                "No saved markets yet. Look up a market, then click 'Save current market' to add it.",
                icon="info",
                color_scheme="gray",
            ),
        ),
        spacing="4",
        padding="6",
        width="100%",
        align="stretch",
    )


# ---------------------------------------------------------------------------
# Map tab — Folium HTML rendered server-side, embedded in an iframe

def _map_tab() -> rx.Component:
    return rx.vstack(
        rx.hstack(
            rx.heading("Tract map", size="6", weight="bold"),
            rx.spacer(),
            rx.select(
                LookupState.map_layer_options,
                value=LookupState.map_layer,
                on_change=LookupState.set_map_layer,
                disabled=LookupState.map_layer_options.length() == 0,
                width="280px",
            ),
            rx.button(
                rx.cond(LookupState.map_loading, "Rendering...", "Refresh map"),
                on_click=LookupState.render_map,
                loading=LookupState.map_loading,
                variant="soft",
                size="2",
            ),
            width="100%",
            align="center",
        ),
        rx.cond(
            LookupState.map_html != "",
            rx.box(
                rx.el.iframe(
                    src_doc=LookupState.map_html,
                    width="100%",
                    height="640px",
                    style={"border": "1px solid var(--gray-a4)", "border_radius": "6px"},
                ),
                width="100%",
            ),
            rx.cond(
                LookupState.map_loading,
                rx.center(
                    rx.spinner(size="3"),
                    height="640px",
                ),
                rx.center(
                    rx.vstack(
                        rx.icon("map", size=32, color="var(--gray-8)"),
                        rx.text(
                            "Click 'Refresh map' to render the choropleth.",
                            color_scheme="gray",
                            size="3",
                        ),
                        rx.cond(
                            LookupState.map_note != "",
                            rx.text(LookupState.map_note, size="2", color_scheme="gray"),
                        ),
                        spacing="2",
                        align="center",
                    ),
                    height="640px",
                ),
            ),
        ),
        spacing="4",
        padding="6",
        width="100%",
        align="stretch",
    )


# ---------------------------------------------------------------------------
# Other tabs (placeholders for later phases)

def _tab_placeholder(name: str) -> rx.Component:
    return rx.box(
        rx.callout(
            f"{name} content will land in a later phase.",
            icon="info",
            color_scheme="blue",
        ),
        padding="6",
    )


# ---------------------------------------------------------------------------
# Tab nav + content

def _tab_button(tab_id: str, label: str) -> rx.Component:
    """Pill-style tab button with visible active state.

    Uses inline padding + border-bottom highlighting via Radix tokens
    (instead of Tailwind classes, which weren't loading correctly in
    dark mode). Active tab gets an accent underline + bold text.
    """
    is_active = LookupState.active_tab == tab_id
    return rx.box(
        rx.text(
            label,
            size="2",
            weight=rx.cond(is_active, "bold", "regular"),
            color=rx.cond(is_active, "var(--accent-11)", "var(--gray-11)"),
        ),
        on_click=lambda: LookupState.set_active_tab(tab_id),
        padding_x="4",
        padding_y="3",
        border_bottom=rx.cond(
            is_active,
            "2px solid var(--accent-9)",
            "2px solid transparent",
        ),
        cursor="pointer",
        _hover={"color": "var(--accent-10)"},
    )


def _tab_bar() -> rx.Component:
    return rx.hstack(
        *[_tab_button(tid, label) for tid, label in TABS],
        spacing="2",
        width="100%",
        border_bottom="1px solid var(--gray-a4)",
        padding_x="6",
        align="end",
    )


def _empty_state() -> rx.Component:
    """Landing screen before any lookup.

    Centered icon + headline + hint + sample-market button. The button
    is wired to set the preset and immediately trigger a lookup so users
    can demo the app in one click.
    """
    return rx.center(
        rx.vstack(
            rx.box(
                rx.icon("radio_tower", size=40, color="var(--accent-9)"),
                padding="5",
                border_radius="full",
                background_color="var(--accent-a3)",
            ),
            rx.heading("Welcome to ftth-compete", size="7", weight="bold"),
            rx.text(
                "Pick a market in the sidebar and click Look up to begin.",
                color_scheme="gray",
                size="3",
            ),
            rx.text(
                "First market per state takes ~90 seconds while FCC BDC bulk data downloads. "
                "Subsequent markets in the same state are near-instant.",
                color_scheme="gray",
                size="2",
                class_name="max-w-md text-center",
                line_height="1.5",
            ),
            rx.button(
                "Try Evans, CO",
                on_click=[
                    LookupState.set_preset("Evans, CO"),
                    LookupState.run_lookup,
                ],
                variant="soft",
                size="2",
                margin_top="3",
            ),
            spacing="4",
            align="center",
        ),
        height="70vh",
    )


def _tab_content() -> rx.Component:
    return rx.cond(
        LookupState.has_result,
        rx.match(
            LookupState.active_tab,
            ("overview", _overview_tab()),
            ("competitors", _competitors_tab()),
            ("housing", _housing_tab()),
            ("map", _map_tab()),
            ("compare", _compare_tab()),
            ("methodology", _methodology_tab()),
            _overview_tab(),
        ),
        rx.cond(
            LookupState.is_loading,
            rx.center(
                rx.vstack(
                    rx.spinner(size="3"),
                    rx.text("Looking up...", size="3", color_scheme="gray"),
                    rx.cond(
                        LookupState.progress_label != "",
                        rx.text(LookupState.progress_label, size="2", color_scheme="gray"),
                    ),
                    spacing="3",
                    align="center",
                ),
                height="60vh",
            ),
            _empty_state(),
        ),
    )


def _main_header() -> rx.Component:
    return rx.cond(
        LookupState.has_result,
        rx.vstack(
            rx.hstack(
                rx.heading(LookupState.market_title, size="6", weight="bold"),
                rx.spacer(),
                rx.color_mode.button(),
                width="100%",
                align="center",
            ),
            _tab_bar(),
            spacing="3",
            width="100%",
            padding_x="6",
            padding_top="5",
            padding_bottom="0",
            background_color="var(--color-background)",
            border_bottom="1px solid var(--gray-a3)",
        ),
        rx.hstack(
            rx.spacer(),
            rx.color_mode.button(),
            padding="4",
            width="100%",
        ),
    )


def _footer() -> rx.Component:
    return rx.box(
        rx.text(
            "Speed test data © Ookla, 2019-present, distributed under "
            "CC BY-NC-SA 4.0. Personal / non-commercial use only.",
            size="1", color_scheme="gray",
        ),
        padding="4",
        border_top="1px solid var(--gray-a3)",
        width="100%",
    )


# ---------------------------------------------------------------------------
# v2 page — three-panel layout, Plotly map as canvas
#
# Mounted at `/v2`. Shares `LookupState` with the v1 index page so a market
# loaded in one is loaded in the other. The v2 layout is a research-driven
# redesign per `.claude/design_v2.md`:
#
#   - LEFT rail: market form + layer picker + lens (compact, sticky)
#   - CENTER: Plotly choropleth (real React component, click events flow
#             back into state via on_click → on_v2_map_click)
#   - RIGHT rail: context-aware — defaults to market summary; switches to
#                 tract-detail when a tract is selected; provider-detail
#                 when a provider is selected.
#   - BOTTOM: detail tabs (Providers list / Housing breakdown / Compare /
#             Methodology) — secondary to the spatial story above.

def _v2_footprint_search() -> rx.Component:
    """Search + filtered-list combobox for picking a provider's footprint.

    Replaces the old per-provider radio entries (which scaled badly to
    markets with 15+ fiber providers). When a provider is selected, the
    map paints their network and the radio layer is overridden until the
    user clicks Clear.

    Implementation: rows are plain `rx.box` (not `rx.button`) so we can
    style them freely + control padding/height without fighting Radix's
    button slot semantics. Selected row gets a teal left-border accent.
    """
    return _sidebar_section(
        "Provider footprint",
        # Selected indicator + clear button when something is active.
        rx.cond(
            LookupState.footprint_provider != "",
            rx.hstack(
                rx.badge(
                    LookupState.footprint_provider,
                    color_scheme="teal", size="2", variant="soft",
                ),
                rx.spacer(),
                rx.icon_button(
                    rx.icon("x", size=12),
                    on_click=LookupState.clear_footprint,
                    variant="ghost",
                    size="1",
                    color_scheme="gray",
                ),
                width="100%",
                align="center",
                spacing="1",
            ),
        ),
        rx.input(
            placeholder="Search providers...",
            value=LookupState.footprint_search,
            on_change=LookupState.set_footprint_search,
            size="2",
            width="100%",
        ),
        # Filtered results list. Fixed height so the box is always
        # visible regardless of match count; scrolls when overflow.
        rx.box(
            rx.cond(
                LookupState.filtered_footprint_options.length() > 0,
                rx.vstack(
                    rx.foreach(
                        LookupState.filtered_footprint_options,
                        lambda name: rx.box(
                            rx.text(
                                name,
                                size="2",
                                weight=rx.cond(
                                    LookupState.footprint_provider == name,
                                    "bold", "regular",
                                ),
                                color=rx.cond(
                                    LookupState.footprint_provider == name,
                                    "var(--teal-11)", "var(--gray-12)",
                                ),
                            ),
                            on_click=LookupState.set_footprint_provider(name),
                            padding_x="2",
                            padding_y="2",
                            border_radius="4px",
                            background_color=rx.cond(
                                LookupState.footprint_provider == name,
                                "var(--teal-3)", "transparent",
                            ),
                            border_left=rx.cond(
                                LookupState.footprint_provider == name,
                                "3px solid var(--teal-9)",
                                "3px solid transparent",
                            ),
                            cursor="pointer",
                            width="100%",
                            _hover={"background_color": "var(--gray-3)"},
                        ),
                    ),
                    spacing="1",
                    align="stretch",
                    width="100%",
                ),
                rx.text(
                    rx.cond(
                        LookupState.footprint_search != "",
                        "No matches.",
                        "No fiber providers in this market.",
                    ),
                    size="1",
                    color_scheme="gray",
                    padding="2",
                ),
            ),
            width="100%",
            min_height="60px",
            max_height="260px",
            overflow_y="auto",
            border="1px solid var(--gray-a4)",
            border_radius="6px",
            padding="1",
            background_color="var(--color-background)",
        ),
    )


def _v2_left_rail() -> rx.Component:
    """Compact form + layer + lens. Narrower than v1 sidebar; sticky."""
    return rx.vstack(
        rx.hstack(
            rx.icon("radio_tower", size=20, color="var(--accent-9)"),
            rx.vstack(
                rx.heading("ftth-compete", size="3", weight="bold"),
                rx.text("v2", size="1", color_scheme="gray"),
                spacing="0",
                align="start",
            ),
            spacing="2",
            align="center",
            width="100%",
        ),
        rx.divider(),
        _sidebar_section(
            "Market",
            rx.select(
                PRESET_LABELS,
                value=LookupState.preset_label,
                on_change=LookupState.set_preset,
                placeholder="Quick-pick",
                width="100%",
            ),
            rx.input(
                placeholder="City",
                value=LookupState.city,
                on_change=LookupState.set_city,
                width="100%",
            ),
            rx.select(
                STATE_LIST,
                value=LookupState.state,
                on_change=LookupState.set_state,
                width="100%",
            ),
            rx.button(
                rx.cond(LookupState.is_loading, "Looking up...", "Look up"),
                on_click=LookupState.run_lookup,
                loading=LookupState.is_loading,
                width="100%",
                size="2",
            ),
            rx.cond(
                LookupState.lookup_error != "",
                rx.callout(
                    LookupState.lookup_error, icon="triangle_alert",
                    color_scheme="red", size="1",
                ),
            ),
        ),
        rx.cond(
            LookupState.has_result,
            rx.vstack(
                rx.divider(),
                _sidebar_section(
                    "Layer",
                    rx.radio(
                        LookupState.map_layer_options,
                        value=LookupState.v2_map_layer,
                        on_change=LookupState.set_v2_map_layer,
                        direction="column",
                        spacing="1",
                        size="1",
                    ),
                ),
                rx.divider(),
                _v2_footprint_search(),
                rx.divider(),
                _sidebar_section(
                    "Strategic lens",
                    rx.radio(
                        [label for label, _ in LENS_OPTIONS],
                        value=rx.cond(
                            LookupState.lens == "neutral", LENS_OPTIONS[0][0],
                            rx.cond(
                                LookupState.lens == "defensive", LENS_OPTIONS[1][0],
                                LENS_OPTIONS[2][0],
                            ),
                        ),
                        on_change=LookupState.set_lens,
                        direction="column",
                        spacing="1",
                        size="1",
                    ),
                    rx.cond(
                        LookupState.lens == "defensive",
                        rx.select(
                            LookupState.incumbent_options,
                            value=LookupState.incumbent,
                            on_change=LookupState.set_incumbent,
                            width="100%",
                            size="1",
                        ),
                    ),
                ),
                width="100%",
                align="stretch",
                spacing="3",
            ),
        ),
        spacing="3",
        align="stretch",
        width="260px",
        height="calc(100vh - 70px)",
        padding="4",
        border_right="1px solid var(--gray-a4)",
        background_color="var(--gray-1)",
        overflow_y="auto",
        position="sticky",
        top="0",
    )


def _v2_map() -> rx.Component:
    """Plotly choropleth — center canvas of v2 page.

    Width is computed explicitly as `calc(100vw - 260px - 340px)` (window
    minus left+right rails). `flex_grow` style props weren't reliably
    propagating through Reflex's `rx.hstack` in 0.9 — explicit width is
    more robust and works regardless of flex behavior.
    """
    return rx.box(
        rx.cond(
            LookupState.has_result,
            # `<iframe src=URL>` (not src_doc) loading from our own backend
            # at /v2_map_html. Same-origin context = OSM tiles fetch
            # normally and Plotly scripts execute. The endpoint caches per
            # (city, state, layer) so layer-switches are sub-second after
            # the first render.
            rx.el.iframe(
                src=LookupState.v2_map_iframe_url,
                width="100%",
                height="100%",
                style={"border": "0", "display": "block"},
            ),
            rx.center(
                rx.vstack(
                    rx.icon("map", size=40, color="var(--gray-7)"),
                    rx.text(
                        "Pick a market in the left rail to start.",
                        size="3", color_scheme="gray",
                    ),
                    spacing="3",
                    align="center",
                ),
                height="calc(100vh - 80px)",
                width="100%",
            ),
        ),
        width="calc(100vw - 600px)",
        min_width="500px",
        height="calc(100vh - 80px)",
        background_color="var(--gray-2)",
    )


def _v2_right_rail() -> rx.Component:
    """Polymorphic context rail. Three states keyed on selection:
    - (no selection)     → market summary
    - selected_tract     → tract detail
    - selected_provider  → provider detail
    """
    return rx.box(
        rx.cond(
            LookupState.selected_tract != "",
            _v2_tract_detail_panel(),
            rx.cond(
                LookupState.selected_provider != "",
                _v2_provider_detail_panel(),
                _v2_market_summary_panel(),
            ),
        ),
        width="340px",
        height="calc(100vh - 70px)",
        padding="4",
        border_left="1px solid var(--gray-a4)",
        background_color="var(--gray-1)",
        overflow_y="auto",
        position="sticky",
        top="0",
    )


def _v2_take_rate_trajectory() -> rx.Component:
    """Right-rail sparkline of historical broadband take-rate per the
    FCC IAS public dataset. Renders only when momentum backfill has
    landed a non-empty `subs_history_sparkline_d`.
    """
    return rx.cond(
        LookupState.has_subs_history,
        rx.vstack(
            rx.divider(margin_y="2"),
            _section_title("Take-rate trajectory"),
            rx.el.svg(
                rx.el.path(
                    d=LookupState.subs_history_sparkline_d,
                    fill="none",
                    stroke="var(--teal-9)",
                    stroke_width="1.5",
                    stroke_linecap="round",
                    stroke_linejoin="round",
                ),
                rx.el.circle(
                    cx=LookupState.subs_history_dot_x.to_string(),
                    cy=LookupState.subs_history_dot_y.to_string(),
                    r="2",
                    fill="var(--teal-10)",
                ),
                viewBox="0 0 100 30",
                preserveAspectRatio="none",
                width="100%",
                height="40px",
                style={"display": "block"},
            ),
            rx.hstack(
                rx.text(
                    LookupState.subs_history_first_label
                    + " " + LookupState.subs_history_first_pct,
                    size="1", color_scheme="gray",
                ),
                rx.spacer(),
                rx.text(
                    LookupState.subs_history_last_label
                    + " " + LookupState.subs_history_last_pct,
                    size="1", weight="medium",
                ),
                width="100%",
            ),
            rx.text(
                LookupState.subs_history_summary,
                size="1", color_scheme="gray",
            ),
            spacing="1",
            width="100%",
            align="stretch",
        ),
    )


def _v2_market_summary_panel() -> rx.Component:
    """Default right-rail view: narrative + hero KPIs + provider ranking +
    opportunity score. The 'cover story' of the market."""
    return rx.cond(
        LookupState.has_result,
        rx.vstack(
            _section_title("This Market"),
            rx.text(
                rx.cond(
                    LookupState.narrative_text != "",
                    LookupState.narrative_text,
                    "Narrative unavailable.",
                ),
                size="2",
                color="var(--gray-12)",
                line_height="1.5",
            ),
            rx.divider(margin_y="2"),
            _section_title("Headline metrics"),
            rx.vstack(
                _v2_metric_row("Population", LookupState.population_display),
                _v2_metric_row("Tracts analyzed", LookupState.n_tracts.to_string()),
                _v2_metric_row("Median HH income", LookupState.mfi_display),
                _v2_metric_row("MDU share", LookupState.mdu_share_display),
                _v2_metric_row("Fiber availability", LookupState.fiber_avail_display),
                _v2_metric_row("Providers", LookupState.n_distinct_providers_display),
                _v2_metric_row(
                    "ACP density", LookupState.market_acp_density_display,
                ),
                spacing="2",
                width="100%",
                align="stretch",
            ),
            _v2_take_rate_trajectory(),
            rx.divider(margin_y="2"),
            _section_title("Top providers"),
            rx.foreach(
                LookupState.visible_providers,
                lambda p: _v2_provider_row(p),
            ),
            rx.cond(
                LookupState.has_opportunity & (LookupState.lens == "offensive"),
                rx.vstack(
                    rx.divider(margin_y="2"),
                    _section_title("Opportunity"),
                    rx.text(
                        LookupState.opp_headline,
                        weight="bold", size="3",
                    ),
                    rx.badge(
                        "Score " + LookupState.opp_score_display,
                        color_scheme=rx.cond(
                            LookupState.opp_score >= 0.55, "green",
                            rx.cond(LookupState.opp_score >= 0.35, "orange", "red"),
                        ),
                        size="2",
                    ),
                    spacing="2",
                    align="stretch",
                    width="100%",
                ),
            ),
            spacing="2",
            align="stretch",
            width="100%",
        ),
        rx.text(
            "No market loaded.",
            size="2", color_scheme="gray",
        ),
    )


def _v2_tract_detail_panel() -> rx.Component:
    """Right-rail view when a tract is selected. Surfaces every per-tract
    field we have for that GEOID from the state's `tract_values` dict
    plus the provider list filtered to providers serving this tract.
    """
    return rx.vstack(
        rx.hstack(
            rx.icon_button(
                rx.icon("arrow_left", size=14),
                on_click=LookupState.clear_selection,
                variant="ghost", size="1",
            ),
            rx.text("Tract detail", size="1", color_scheme="gray", weight="medium"),
            spacing="2",
            align="center",
        ),
        rx.heading(LookupState.selected_tract, size="3", weight="bold"),
        rx.text(
            "All per-tract layer values for this GEOID.",
            size="1", color_scheme="gray",
        ),
        rx.divider(margin_y="2"),
        # All 11 layer values for the selected tract, pulled from
        # the computed var `selected_tract_rows` (one row per defined
        # layer; "—" when the layer has no value for this tract).
        rx.foreach(
            LookupState.selected_tract_rows,
            lambda r: _v2_metric_row(r[0], r[1]),
        ),
        rx.divider(margin_y="2"),
        _section_title("Providers in this tract"),
        rx.foreach(
            LookupState.selected_tract_providers,
            lambda p: _v2_provider_row(p),
        ),
        rx.cond(
            LookupState.selected_tract_providers.length() == 0,
            rx.text(
                "No BDC providers recorded in this tract.",
                size="1", color_scheme="gray",
            ),
        ),
        spacing="3",
        align="stretch",
        width="100%",
    )


def _v2_provider_detail_panel() -> rx.Component:
    """Right-rail view when a provider is selected. Pulls the full row
    from `selected_provider_row` and surfaces every field we have."""
    row = LookupState.selected_provider_row
    return rx.vstack(
        rx.hstack(
            rx.icon_button(
                rx.icon("arrow_left", size=14),
                on_click=LookupState.clear_selection,
                variant="ghost", size="1",
            ),
            rx.text("Provider detail", size="1", color_scheme="gray", weight="medium"),
            spacing="2",
            align="center",
        ),
        rx.heading(LookupState.selected_provider, size="4", weight="bold"),
        rx.text(
            row["holding"].to(str),
            size="1",
            color_scheme="gray",
        ),
        rx.hstack(
            rx.badge(
                row["category_label"].to(str),
                color_scheme=row["category_color"].to(str),
                variant="soft",
            ),
            rx.badge(
                row["tech_label"].to(str),
                color_scheme=row["tech_color"].to(str),
                variant="soft",
            ),
            spacing="2",
        ),
        rx.divider(margin_y="2"),
        _section_title("Footprint"),
        _v2_metric_row("Coverage", row["coverage_display"].to(str)),
        _v2_metric_row("Locations served", row["locations_display"].to(str)),
        _v2_metric_row("Max down (advertised)", row["max_down_display"].to(str)),
        rx.divider(margin_y="2"),
        _section_title("Speed tier mix"),
        _speed_tier_bar(row),
        rx.divider(margin_y="2"),
        _section_title("Customer signals"),
        _rating_block(row),
        _subs_block(row),
        rx.divider(margin_y="2"),
        _section_title("Momentum"),
        _velocity_block(row),
        _trajectory_block(row),
        rx.cond(
            ~row["has_velocity"] & ~row["has_trajectory"],
            rx.text(
                "No 12-month delta loaded. Enable 'Include 12-month velocity' "
                "or 'Include multi-release trajectory' in the sidebar to surface "
                "expansion data for this provider.",
                size="1",
                color_scheme="gray",
            ),
        ),
        rx.cond(
            row["raw_brands_str"] != "",
            rx.text(
                row["raw_brands_str"].to(str),
                size="1",
                color_scheme="gray",
                class_name="italic",
                margin_top="2",
            ),
        ),
        spacing="3",
        align="stretch",
        width="100%",
    )


def _v2_metric_row(label: str | rx.Var, value: rx.Var | str) -> rx.Component:
    """One compact metric row for the right rail. Label left, value right."""
    return rx.hstack(
        rx.text(label, size="2", color_scheme="gray"),
        rx.spacer(),
        rx.text(value, size="2", weight="bold", color="var(--gray-12)"),
        width="100%",
        align="center",
    )


def _v2_provider_row(p: rx.Var) -> rx.Component:
    """Provider mini-row in the right rail. Click → select_provider."""
    pct_label = p["coverage_display"].to(str)
    width_var = (p["coverage_pct"].to(float) * 100).to_string() + "%"
    return rx.box(
        rx.vstack(
            rx.hstack(
                rx.text(
                    p["name"].to(str),
                    size="2",
                    weight="medium",
                ),
                rx.spacer(),
                rx.text(
                    pct_label,
                    size="1",
                    color_scheme="gray",
                ),
                width="100%",
                align="center",
            ),
            rx.box(
                rx.box(
                    background_color=p["category_color"].to(str),
                    height="100%",
                    width=width_var,
                    border_radius="2px",
                ),
                height="4px",
                width="100%",
                background_color="var(--gray-a3)",
                border_radius="2px",
            ),
            spacing="1",
            width="100%",
            align="stretch",
        ),
        on_click=LookupState.select_provider(p["name"].to(str)),
        cursor="pointer",
        padding="2",
        _hover={"background_color": "var(--gray-a2)"},
        border_radius="3px",
        width="100%",
    )


def _v2_top_strip() -> rx.Component:
    """Slim top bar: brand + market title + cross-page nav + hero metrics + v1 link."""
    nav_link_style = {
        "padding": "6px 12px",
        "border_radius": "6px",
        "font_size": "13px",
        "font_weight": "500",
        "color": "var(--gray-11)",
        "text_decoration": "none",
        "transition": "all 0.15s ease",
        "_hover": {
            "background_color": "var(--gray-a3)",
            "color": "var(--gray-12)",
        },
    }
    return rx.hstack(
        rx.hstack(
            rx.icon("radio_tower", size=18, color="var(--accent-9)"),
            rx.cond(
                LookupState.has_result,
                rx.text(LookupState.market_title, size="4", weight="bold"),
                rx.text("ftth-compete", size="4", weight="bold"),
            ),
            spacing="2",
            align="center",
        ),
        rx.hstack(
            rx.link(
                "Market",
                href="/v2",
                style={
                    **nav_link_style,
                    "background_color": "var(--accent-a3)",
                    "color": "var(--accent-11)",
                },
            ),
            rx.link("Screener", href="/screener", style=nav_link_style),
            rx.link("Providers", href="/providers", style=nav_link_style),
            spacing="1",
            align="center",
            padding_left="4",
        ),
        rx.spacer(),
        rx.cond(
            LookupState.has_result,
            rx.hstack(
                _v2_hero_chip("Pop", LookupState.population_display),
                _v2_hero_chip("ISPs", LookupState.n_distinct_providers_display),
                _v2_hero_chip("Fiber", LookupState.fiber_avail_display),
                _v2_hero_chip("MDU", LookupState.mdu_share_display),
                spacing="4",
                align="center",
            ),
        ),
        rx.spacer(),
        rx.cond(
            LookupState.enrich_loading,
            rx.hstack(
                rx.spinner(size="1"),
                rx.text(
                    "Loading map and data...",
                    size="1", color_scheme="gray",
                ),
                spacing="2",
                align="center",
            ),
        ),
        rx.cond(
            LookupState.momentum_loading,
            rx.hstack(
                rx.spinner(size="1"),
                rx.text(
                    "Loading momentum data...",
                    size="1", color_scheme="gray",
                ),
                spacing="2",
                align="center",
            ),
        ),
        rx.link(
            rx.button("v1", variant="ghost", size="1"),
            href="/v1",
        ),
        rx.color_mode.button(),
        width="100%",
        height="56px",
        padding_x="4",
        border_bottom="1px solid var(--gray-a4)",
        background_color="var(--color-background)",
        align="center",
    )


def _v2_hero_chip(label: str, value: rx.Var | str) -> rx.Component:
    return rx.vstack(
        rx.text(label, size="1", color_scheme="gray"),
        rx.text(value, size="3", weight="bold", color="var(--gray-12)"),
        spacing="0",
        align="start",
    )


def _v2_competitive_strip() -> rx.Component:
    """Below-the-fold deep-dive on competitors.

    Reuses the polished provider-card grid we already have from the v1
    Competitors tab — so the v2 page surfaces the full competitive data
    set without requiring tab navigation. Sits below the three-panel
    map+rails so it scrolls into view on demand.

    Click any provider card → updates `selected_provider` → right rail
    above shows that provider's detail (with the map highlighting the
    provider's footprint, future iteration).
    """
    return rx.cond(
        LookupState.has_result,
        rx.vstack(
            rx.hstack(
                _section_title("Competitive deep-dive"),
                rx.spacer(),
                rx.text(
                    "Click any provider card to focus the map and right rail.",
                    size="1", color_scheme="gray",
                ),
                width="100%",
                align="end",
            ),
            _lens_banner(),
            _competitors_controls(),
            _summary_strip(),
            rx.segmented_control.root(
                rx.segmented_control.item("Cards", value="Cards"),
                rx.segmented_control.item("Table", value="Table"),
                value=LookupState.cp_view,
                on_change=LookupState.set_cp_view,
                size="2",
            ),
            rx.cond(
                LookupState.cp_view == "Table",
                _competitors_table(),
                _competitors_cards(),
            ),
            spacing="4",
            padding="6",
            width="100%",
            align="stretch",
            background_color="var(--color-background)",
            border_top="1px solid var(--gray-a4)",
        ),
    )


def v2_page() -> rx.Component:
    return rx.vstack(
        _v2_top_strip(),
        rx.hstack(
            _v2_left_rail(),
            _v2_map(),
            _v2_right_rail(),
            spacing="0",
            width="100%",
            align="start",
        ),
        _v2_competitive_strip(),
        # Hidden bridge: iframe map posts `{type:'ftth_tract_click', geoid}`
        # to window.parent; the script below writes the geoid into the
        # hidden input via React's native-setter, which fires `on_change`
        # → `LookupState.select_tract` → right rail repaints.
        rx.el.input(
            id="ftth_tract_geoid_input",
            value=LookupState.selected_tract,
            on_change=LookupState.select_tract,
            style={"display": "none"},
            read_only=False,
        ),
        rx.script(
            """
            (function(){
              if (window.__ftth_tract_bridge__) return;
              window.__ftth_tract_bridge__ = true;
              var setter = Object.getOwnPropertyDescriptor(
                window.HTMLInputElement.prototype, 'value'
              ).set;
              window.addEventListener('message', function(ev){
                var d = ev && ev.data;
                if (!d || d.type !== 'ftth_tract_click' || !d.geoid) return;
                var inp = document.getElementById('ftth_tract_geoid_input');
                if (!inp) return;
                setter.call(inp, String(d.geoid));
                inp.dispatchEvent(new Event('input', { bubbles: true }));
                inp.dispatchEvent(new Event('change', { bubbles: true }));
              });
            })();
            """
        ),
        _page_footer(
            [
                "FCC Broadband Data Collection (BDC)",
                "FCC Internet Access Services (IAS)",
                "Census ACS 5-year + TIGER/Line",
                "Ookla open speedtest tiles",
                "Google Places (ratings)",
                "USAC ACP/EBB enrollment",
            ],
            with_ookla=True,
        ),
        spacing="0",
        width="100vw",
        min_height="100vh",
        align="stretch",
    )


def index() -> rx.Component:
    return rx.hstack(
        _sidebar(),
        rx.vstack(
            _main_header(),
            _tab_content(),
            rx.spacer(),
            _footer(),
            spacing="0",
            width="100%",
            height="100vh",
            overflow_y="auto",
        ),
        spacing="0",
        align="start",
        width="100vw",
        height="100vh",
    )


# ---------------------------------------------------------------------------
# Phase 7 — Batch market screener
#
# Separate route, separate state object. Workflow:
#   1. User picks a state (or "all") + filter ranges (min pop, min MDU %, etc).
#   2. ScreenerState.start_screen enumerates places in the state via
#      tiger.places_in_state(), then iterates screen_market() per place in a
#      background task. Progress streams to progress_done / progress_total.
#   3. Results land in `results: list[dict]`. The user can sort by any column,
#      apply post-hoc filters, download as CSV, or click "Open" to jump to /v2
#      pre-loaded with that market.

def _kpis_to_display_dict(kpis_dict: dict[str, Any]) -> dict[str, Any]:
    """Add pre-formatted display strings to a MarketKpis dict so the
    table doesn't have to do number formatting in the JSX layer (which
    is awkward with Reflex Vars and produces strings like '34.59382%').

    Also normalizes `top_providers` into a list of dicts with display
    strings baked in (`tech_label`, `locations_display`).
    """
    d = dict(kpis_dict)
    if d.get("error"):
        d.setdefault("pop_display", "")
        d.setdefault("mfi_display", "")
        d.setdefault("mdu_display", "")
        d.setdefault("fiber_display", "")
        d.setdefault("score_display", "")
        d.setdefault("top_providers", [])
        return d
    try:
        d["pop_display"] = f"{int(d.get('population') or 0):,}"
    except (TypeError, ValueError):
        d["pop_display"] = "0"
    try:
        d["mfi_display"] = f"${int(d.get('median_hh_income') or 0):,}"
    except (TypeError, ValueError):
        d["mfi_display"] = "$0"
    try:
        d["mdu_display"] = f"{float(d.get('mdu_share') or 0.0) * 100:.1f}%"
    except (TypeError, ValueError):
        d["mdu_display"] = "—"
    try:
        d["fiber_display"] = f"{float(d.get('fiber_avail_pct') or 0.0) * 100:.1f}%"
    except (TypeError, ValueError):
        d["fiber_display"] = "—"
    try:
        d["score_display"] = f"{float(d.get('opportunity_score') or 0.0):.2f}"
    except (TypeError, ValueError):
        d["score_display"] = "0.00"
    # Pre-bake per-provider display strings — Reflex foreach can't easily
    # do number formatting client-side, so we do it here.
    out_provs: list[dict[str, Any]] = []
    for p in d.get("top_providers") or []:
        if not p.get("name"):
            continue
        try:
            locs = int(p.get("locations") or 0)
        except (TypeError, ValueError):
            locs = 0
        out_provs.append({
            "name": str(p.get("name") or ""),
            "tech_label": str(p.get("tech_label") or ""),
            "locations": locs,
            "locations_display": f"{locs:,}",
            "has_fiber": bool(p.get("has_fiber")),
        })
    d["top_providers"] = out_provs
    return d


class ScreenerState(rx.State):
    """State for the /screener route. Lives separately from LookupState
    so the deep-dive page's heavy state doesn't bleed in here."""

    # Filter form
    states_csv: str = "CO"     # comma-separated states e.g. "CO,WY,NM"
    min_population: int = 5000
    min_mdu_share_pct: int = 0       # 0-100, applied post-screen
    max_fiber_avail_pct: int = 100   # 0-100, applied post-screen
    sort_key: str = "opportunity_score"
    sort_desc: bool = True

    # Run state
    is_running: bool = False
    progress_done: int = 0
    progress_total: int = 0
    status_message: str = ""
    last_error: str = ""

    # Results
    results: list[dict[str, Any]] = []
    # Cache age caption when results were loaded from disk
    cache_age_label: str = ""
    # Force-rebuild even when a fresh cache exists
    force_rebuild: bool = False

    @rx.event
    def set_force_rebuild(self, value: bool) -> None:
        self.force_rebuild = bool(value)

    @rx.event
    def set_states_csv(self, value: str) -> None:
        self.states_csv = value.upper().strip()

    @rx.event
    def set_min_population(self, value: str) -> None:
        try:
            self.min_population = max(0, int(value))
        except (TypeError, ValueError):
            self.min_population = 0

    @rx.event
    def set_min_mdu_share_pct(self, value: str) -> None:
        try:
            self.min_mdu_share_pct = max(0, min(100, int(value)))
        except (TypeError, ValueError):
            self.min_mdu_share_pct = 0

    @rx.event
    def set_max_fiber_avail_pct(self, value: str) -> None:
        try:
            self.max_fiber_avail_pct = max(0, min(100, int(value)))
        except (TypeError, ValueError):
            self.max_fiber_avail_pct = 100

    @rx.event
    def set_sort_key(self, value: str) -> None:
        # Click same header → toggle direction; different header → desc default.
        if value == self.sort_key:
            self.sort_desc = not self.sort_desc
        else:
            self.sort_key = value
            self.sort_desc = True

    @rx.event
    def clear_results(self) -> None:
        self.results = []
        self.progress_done = 0
        self.progress_total = 0
        self.status_message = ""
        self.last_error = ""

    @rx.var(cache=True)
    def visible_results(self) -> list[dict[str, Any]]:
        """Apply post-screen filters + sort."""
        rows = [
            r for r in self.results
            if not r.get("error")
            and int(r.get("population") or 0) >= self.min_population
            and float(r.get("mdu_share") or 0.0) * 100 >= self.min_mdu_share_pct
            and float(r.get("fiber_avail_pct") or 0.0) * 100 <= self.max_fiber_avail_pct
        ]
        key = self.sort_key

        def _key_fn(r: dict[str, Any]):
            v = r.get(key)
            if isinstance(v, (int, float)):
                return v
            return str(v or "").lower()

        rows.sort(key=_key_fn, reverse=self.sort_desc)
        return rows[:200]  # cap render at 200; user can narrow filters

    @rx.var(cache=True)
    def visible_count(self) -> int:
        return len(self.visible_results)

    @rx.var(cache=True)
    def total_count(self) -> int:
        return sum(1 for r in self.results if not r.get("error"))

    @rx.var(cache=True)
    def error_count(self) -> int:
        return sum(1 for r in self.results if r.get("error"))

    @rx.var(cache=True)
    def sample_errors(self) -> list[str]:
        """Top 5 distinct error messages from the results, formatted for
        display. Helps diagnose 'every market errored' situations where
        the user otherwise just sees an empty table."""
        seen: dict[str, int] = {}
        for r in self.results:
            err = r.get("error") or ""
            if not err:
                continue
            seen[err] = seen.get(err, 0) + 1
        # Sort by frequency desc.
        ordered = sorted(seen.items(), key=lambda kv: -kv[1])
        return [f"{n}× {msg}" for msg, n in ordered[:5]]

    @rx.var(cache=True)
    def all_errored(self) -> bool:
        """True when results is non-empty but every row is an error.
        Drives the diagnostic-panel display so the user knows the screener
        ran but every market failed (e.g. missing API key)."""
        n = len(self.results)
        return n > 0 and sum(1 for r in self.results if r.get("error")) == n

    @rx.var(cache=True)
    def progress_pct(self) -> int:
        if self.progress_total <= 0:
            return 0
        return int(100 * self.progress_done / self.progress_total)

    @rx.var(cache=True)
    def results_csv(self) -> str:
        """Full results (including errored rows) as CSV string for download."""
        from dataclasses import asdict
        from ftth_compete.analysis.screener import MarketKpis, kpis_to_csv
        rows: list[MarketKpis] = []
        for r in self.results:
            try:
                rows.append(MarketKpis(**r))
            except Exception:  # noqa: BLE001
                continue
        return kpis_to_csv(rows)

    # --- The batch runner ---------------------------------------------
    @rx.event(background=True)
    async def start_screen(self):
        """Enumerate places per state and run screen_market() for each.

        Disk cache first: if a fresh screener cache exists for this
        (states, BDC release) and the user didn't flip Force Rebuild,
        load it in <1s. Otherwise full sequential run (5-15 min cold).

        Sequential for now — Reflex 0.9 background events don't compose
        cleanly with concurrent.futures, and the FCC BDC + Census APIs
        don't love being hit with 8 parallel state-level fetches anyway.
        Each state's BDC parquet is warm-cached after the first market,
        so subsequent markets in the same state run in 1-3s.
        """
        import time
        from dataclasses import asdict
        from ftth_compete.analysis.screener import (
            load_cached_run, save_run, screen_market,
        )
        from ftth_compete.data import fcc_bdc
        from ftth_compete.data.tiger import places_in_state

        async with self:
            states = [s.strip().upper() for s in self.states_csv.split(",") if s.strip()]
            if not states:
                self.last_error = "Pick at least one state (e.g. CO,WY)."
                return
            self.is_running = True
            self.results = []
            self.progress_done = 0
            self.status_message = "Enumerating places..."
            self.last_error = ""
            self.cache_age_label = ""
            force_rebuild_local = self.force_rebuild
            states_csv_local = ",".join(states)

        # Disk-cache fast path.
        try:
            release = await asyncio.to_thread(fcc_bdc.latest_release)
        except Exception as exc:  # noqa: BLE001
            log.warning("Could not resolve latest BDC release: %s", exc)
            release = ""
        if release and not force_rebuild_local:
            cached = await asyncio.to_thread(
                load_cached_run, states_csv_local, release,
            )
            if cached is not None:
                rows, built_at = cached
                age_secs = max(0, int(time.time() - built_at))
                if age_secs < 3600:
                    age_label = f"{age_secs // 60} min ago"
                elif age_secs < 86400:
                    age_label = f"{age_secs // 3600} hr ago"
                else:
                    age_label = f"{age_secs // 86400} day(s) ago"
                async with self:
                    self.results = [
                        _kpis_to_display_dict(asdict(r)) for r in rows
                    ]
                    self.progress_done = len(rows)
                    self.progress_total = len(rows)
                    self.cache_age_label = (
                        f"Loaded {len(rows)} markets from disk cache "
                        f"(built {age_label}). Flip Force rebuild to refresh."
                    )
                    self.status_message = "Cache hit."
                    self.is_running = False
                return

        # Enumerate candidates per state.
        candidates: list[tuple[str, str]] = []
        for st in states:
            try:
                places = await asyncio.to_thread(places_in_state, st)
            except Exception as exc:  # noqa: BLE001
                log.exception("place enumeration failed for %s", st)
                async with self:
                    self.last_error = f"Place enumeration failed for {st}: {exc}"
                continue
            candidates.extend((p["name"], st) for p in places)

        # Pre-warm state BDC parquets sequentially BEFORE fanning workers
        # out. Otherwise 6 concurrent workers race on the same `ingest_state`
        # path and all hit the FCC API (rate-limited to ~10 req/min),
        # blowing through the budget and 429-ing every market.
        async with self:
            self.status_message = (
                f"Pre-warming BDC parquets for {', '.join(states)} "
                f"(~3-5min per cold state)..."
            )
        for st in states:
            try:
                await asyncio.to_thread(fcc_bdc.ingest_state, st)
            except Exception as exc:  # noqa: BLE001
                log.warning("BDC pre-warm failed for %s: %s", st, exc)

        async with self:
            self.progress_total = len(candidates)
            self.status_message = (
                f"Screening {len(candidates)} candidates across "
                f"{', '.join(states)}..."
            )

        # Concurrent worker pool. Bound at 6 because each worker runs a
        # full `screen_market` (Census ACS + BDC reads + Polars/DuckDB);
        # going wider thrashes Polars' thread pool and triggers Census
        # API rate-limit responses on cold-state lookups. The first
        # in-flight worker per state warms the BDC parquet, after which
        # all subsequent workers in that state run sub-second.
        sem = asyncio.Semaphore(6)

        async def run_one(city: str, st: str) -> dict[str, Any]:
            async with sem:
                try:
                    kpis = await asyncio.to_thread(screen_market, city, st)
                except Exception as exc:  # noqa: BLE001
                    log.exception("screen_market raised for %s, %s", city, st)
                    return _kpis_to_display_dict({
                        "city": city, "state": st,
                        "market_id": f"{city}|{st}",
                        "error": f"{type(exc).__name__}: {exc}",
                    })
                return _kpis_to_display_dict(asdict(kpis))

        tasks = [
            asyncio.create_task(run_one(city, st)) for city, st in candidates
        ]

        batch: list[dict[str, Any]] = []
        done = 0
        for fut in asyncio.as_completed(tasks):
            row = await fut
            batch.append(row)
            done += 1
            if len(batch) >= 10 or done == len(tasks):
                async with self:
                    self.results = [*self.results, *batch]
                    self.progress_done = done
                batch = []

        async with self:
            self.is_running = False
            self.status_message = (
                f"Done. {self.total_count} markets scored, "
                f"{self.error_count} errors."
            )

        # Persist successful run for next time. Excludes errored rows so
        # the cache doesn't lock in temporary failures.
        if release:
            try:
                from ftth_compete.analysis.screener import MarketKpis
                successful: list[MarketKpis] = []
                async with self:
                    for r in self.results:
                        if r.get("error"):
                            continue
                        try:
                            successful.append(MarketKpis(**r))
                        except (TypeError, ValueError):
                            continue
                if successful:
                    await asyncio.to_thread(
                        save_run, states_csv_local, release, successful,
                    )
            except Exception as exc:  # noqa: BLE001
                log.warning("Failed to persist screener results: %s", exc)

    @rx.event
    def open_in_v2(self, market_id: str):
        """Click 'Open' on a result row → navigate to /v2 with city+state set.

        Routes via /v2 with query params; LookupState reads on first load.
        """
        try:
            city, state = market_id.split("|", 1)
        except ValueError:
            return
        from urllib.parse import quote
        return rx.redirect(f"/v2?city={quote(city)}&state={quote(state)}&autorun=1")


# ---------------------------------------------------------------------------
# Phase 7 — UI helpers

def _page_footer(sources: list[str], *, with_ookla: bool = False) -> rx.Component:
    """Small gray sources strip at the bottom of every page.

    `with_ookla=True` adds the legally-required Ookla CC BY-NC-SA 4.0
    attribution. Pages that render Ookla measured-speed data must
    include this — currently the v2 market deep-dive.
    """
    items: list[rx.Component] = []
    for i, src in enumerate(sources):
        if i > 0:
            items.append(rx.text("·", size="1", color="var(--gray-7)"))
        items.append(rx.text(src, size="1", color_scheme="gray"))
    extras: list[rx.Component] = []
    if with_ookla:
        extras.append(rx.text(
            "Speed test data © Ookla, distributed under CC BY-NC-SA 4.0",
            size="1",
            color_scheme="gray",
            class_name="italic",
        ))
    return rx.vstack(
        rx.hstack(
            rx.text("Sources:", size="1", weight="medium", color_scheme="gray"),
            *items,
            spacing="2",
            align="center",
            wrap="wrap",
        ),
        *extras,
        spacing="1",
        align="start",
        width="100%",
        padding="4",
        border_top="1px solid var(--gray-a4)",
        background_color="var(--gray-1)",
    )


def _top_nav() -> rx.Component:
    """Slim cross-page nav strip. Same shell on every page so users can
    switch between Market deep-dive / Screener / Providers in one click.
    """
    nav_link_style = {
        "padding": "6px 12px",
        "border_radius": "6px",
        "font_size": "13px",
        "font_weight": "500",
        "color": "var(--gray-11)",
        "text_decoration": "none",
        "transition": "all 0.15s ease",
        "_hover": {
            "background_color": "var(--gray-a3)",
            "color": "var(--gray-12)",
        },
    }
    return rx.hstack(
        rx.hstack(
            rx.icon("radio_tower", size=18, color="var(--accent-9)"),
            rx.text("ftth-compete", size="3", weight="bold"),
            spacing="2",
            align="center",
        ),
        rx.hstack(
            rx.link("Market", href="/v2", style=nav_link_style),
            rx.link("Screener", href="/screener", style=nav_link_style),
            rx.link("Providers", href="/providers", style=nav_link_style),
            spacing="1",
            align="center",
        ),
        rx.spacer(),
        rx.color_mode.button(),
        width="100%",
        height="44px",
        padding_x="4",
        border_bottom="1px solid var(--gray-a4)",
        background_color="var(--color-background)",
        align="center",
        position="sticky",
        top="0",
        z_index="100",
    )


def _screener_filters() -> rx.Component:
    """Left-rail filter form for the screener page."""
    return rx.vstack(
        _section_title("Scope"),
        rx.vstack(
            rx.text("States (comma-separated)", size="1", color_scheme="gray"),
            rx.input(
                placeholder="CO, WY, NM",
                value=ScreenerState.states_csv,
                on_change=ScreenerState.set_states_csv,
                size="2",
            ),
            spacing="1",
            align="stretch",
            width="100%",
        ),
        rx.divider(margin_y="2"),
        _section_title("Post-screen filters"),
        rx.vstack(
            rx.text("Minimum population", size="1", color_scheme="gray"),
            rx.input(
                value=ScreenerState.min_population.to_string(),
                on_change=ScreenerState.set_min_population,
                type="number", size="2",
            ),
            rx.text("Minimum MDU share (%)", size="1", color_scheme="gray"),
            rx.input(
                value=ScreenerState.min_mdu_share_pct.to_string(),
                on_change=ScreenerState.set_min_mdu_share_pct,
                type="number", size="2",
            ),
            rx.text("Maximum fiber availability (%)", size="1", color_scheme="gray"),
            rx.input(
                value=ScreenerState.max_fiber_avail_pct.to_string(),
                on_change=ScreenerState.set_max_fiber_avail_pct,
                type="number", size="2",
            ),
            spacing="2",
            align="stretch",
            width="100%",
        ),
        rx.divider(margin_y="2"),
        rx.vstack(
            rx.button(
                rx.cond(
                    ScreenerState.is_running,
                    "Running...",
                    "Run screener",
                ),
                on_click=ScreenerState.start_screen,
                loading=ScreenerState.is_running,
                disabled=ScreenerState.is_running,
                width="100%",
                size="2",
                color_scheme="teal",
            ),
            rx.hstack(
                rx.checkbox(
                    "Force rebuild",
                    checked=ScreenerState.force_rebuild,
                    on_change=ScreenerState.set_force_rebuild,
                    disabled=ScreenerState.is_running,
                    size="1",
                ),
                rx.tooltip(
                    rx.icon("circle_help", size=12, color="var(--gray-9)"),
                    content="Bypass the disk cache and re-score every market from scratch. Use after major BDC updates.",
                ),
                spacing="1",
                align="center",
            ),
            rx.button(
                "Clear results",
                on_click=ScreenerState.clear_results,
                disabled=ScreenerState.is_running,
                width="100%",
                size="2",
                variant="soft",
                color_scheme="gray",
            ),
            rx.cond(
                ScreenerState.last_error != "",
                rx.callout(
                    ScreenerState.last_error, icon="triangle_alert",
                    color_scheme="red", size="1",
                ),
            ),
            rx.cond(
                ScreenerState.results.length() > 0,
                rx.button(
                    rx.icon("download", size=14),
                    "Download CSV",
                    on_click=rx.download(
                        data=ScreenerState.results_csv,
                        filename="screener-results.csv",
                    ),
                    width="100%",
                    size="2",
                    variant="surface",
                ),
            ),
            spacing="2",
            align="stretch",
            width="100%",
        ),
        spacing="3",
        align="stretch",
        width="260px",
        height="calc(100vh - 60px)",
        padding="4",
        border_right="1px solid var(--gray-a4)",
        background_color="var(--gray-1)",
        overflow_y="auto",
        position="sticky",
        top="0",
    )


def _screener_progress() -> rx.Component:
    return rx.vstack(
        rx.cond(
            ScreenerState.cache_age_label != "",
            rx.callout(
                ScreenerState.cache_age_label,
                icon="database",
                color_scheme="teal",
                size="1",
            ),
        ),
        rx.cond(
            (ScreenerState.progress_total > 0) & ScreenerState.is_running,
            rx.vstack(
                rx.hstack(
                    rx.text(
                        ScreenerState.progress_done.to_string()
                        + " / "
                        + ScreenerState.progress_total.to_string(),
                        size="2", weight="medium",
                    ),
                    rx.text(ScreenerState.status_message, size="1", color_scheme="gray"),
                    spacing="3",
                    align="center",
                    width="100%",
                ),
                rx.progress(
                    value=ScreenerState.progress_pct,
                    max=100,
                    size="2",
                    color_scheme="teal",
                ),
                spacing="1",
                width="100%",
                align="stretch",
            ),
        ),
        spacing="2",
        width="100%",
        align="stretch",
        padding_y="2",
    )


def _screener_header_cell(label: str, key: str) -> rx.Component:
    """Sortable column header. Clicking toggles direction / switches column."""
    return rx.table.column_header_cell(
        rx.hstack(
            rx.text(label, size="1", weight="bold"),
            rx.cond(
                ScreenerState.sort_key == key,
                rx.cond(
                    ScreenerState.sort_desc,
                    rx.icon("chevron_down", size=12),
                    rx.icon("chevron_up", size=12),
                ),
            ),
            spacing="1",
            align="center",
        ),
        on_click=ScreenerState.set_sort_key(key),
        style={"cursor": "pointer", "_hover": {"background_color": "var(--gray-a3)"}},
    )


def _screener_table() -> rx.Component:
    return rx.cond(
        ScreenerState.visible_count > 0,
        rx.table.root(
            rx.table.header(
                rx.table.row(
                    _screener_header_cell("Market", "city"),
                    _screener_header_cell("Pop", "population"),
                    _screener_header_cell("MFI", "median_hh_income"),
                    _screener_header_cell("MDU %", "mdu_share"),
                    _screener_header_cell("Providers", "n_providers"),
                    _screener_header_cell("Fiber %", "fiber_avail_pct"),
                    rx.table.column_header_cell(
                        rx.text("Competition", size="1", weight="bold"),
                    ),
                    _screener_header_cell("Score", "opportunity_score"),
                    rx.table.column_header_cell(""),
                )
            ),
            rx.table.body(
                rx.foreach(
                    ScreenerState.visible_results,
                    _screener_table_row,
                ),
            ),
            size="1",
            variant="surface",
        ),
        rx.cond(
            ScreenerState.all_errored,
            # Diagnostic panel: every market in the run errored.
            rx.vstack(
                rx.callout(
                    "Every market in the run failed ("
                    + ScreenerState.error_count.to_string()
                    + " errors total). Common causes: missing CENSUS_API_KEY "
                    "in .env, missing FCC BDC credentials, or tract resolution "
                    "failure for very small CDPs.",
                    icon="triangle_alert",
                    color_scheme="red",
                    size="2",
                ),
                rx.text(
                    "Top errors observed:",
                    size="1", color_scheme="gray", weight="medium",
                ),
                rx.foreach(
                    ScreenerState.sample_errors,
                    lambda e: rx.code(e, size="1", color_scheme="gray"),
                ),
                spacing="2",
                align="stretch",
                width="100%",
                padding="6",
            ),
            rx.center(
                rx.vstack(
                    rx.icon("search", size=36, color="var(--gray-7)"),
                    rx.text(
                        rx.cond(
                            ScreenerState.is_running,
                            "Scoring markets...",
                            rx.cond(
                                ScreenerState.results.length() > 0,
                                "No markets match the current filters. Loosen the post-screen filters in the left rail.",
                                "Pick a state and click Run screener to begin.",
                            ),
                        ),
                        size="2", color_scheme="gray",
                    ),
                    spacing="2",
                    align="center",
                ),
                padding="10",
                min_height="300px",
            ),
        ),
    )


def _screener_provider_chip(p: rx.Var) -> rx.Component:
    """One provider in the Competition column. Fiber providers get a
    teal accent dot; non-fiber stay gray. Hover tooltip shows tech +
    location count."""
    return rx.tooltip(
        rx.hstack(
            rx.box(
                width="6px",
                height="6px",
                border_radius="50%",
                background_color=rx.cond(
                    p["has_fiber"].to(bool),
                    "var(--teal-9)",
                    "var(--gray-7)",
                ),
                flex_shrink="0",
            ),
            rx.text(p["name"].to(str), size="1"),
            spacing="1",
            align="center",
            padding_x="2",
            padding_y="1",
            background_color="var(--gray-a3)",
            border_radius="4px",
        ),
        content=p["name"].to(str) + " · " + p["tech_label"].to(str)
        + " · " + p["locations_display"].to(str) + " locations",
    )


def _screener_table_row(r: rx.Var) -> rx.Component:
    return rx.table.row(
        rx.table.cell(
            rx.vstack(
                rx.text(r["city"].to(str), weight="medium", size="2"),
                rx.text(r["state"].to(str), size="1", color_scheme="gray"),
                spacing="0",
                align="start",
            ),
        ),
        rx.table.cell(
            rx.text(r["pop_display"].to(str), size="2"),
            justify="end",
        ),
        rx.table.cell(
            rx.text(r["mfi_display"].to(str), size="2"),
            justify="end",
        ),
        rx.table.cell(
            rx.text(r["mdu_display"].to(str), size="2"),
            justify="end",
        ),
        rx.table.cell(
            rx.text(r["n_providers"].to(int).to_string(), size="2"),
            justify="end",
        ),
        rx.table.cell(
            rx.text(r["fiber_display"].to(str), size="2"),
            justify="end",
        ),
        rx.table.cell(
            rx.flex(
                rx.foreach(
                    r["top_providers"].to(list[dict[str, Any]]),
                    _screener_provider_chip,
                ),
                gap="2",
                wrap="wrap",
                max_width="280px",
            ),
        ),
        rx.table.cell(
            rx.badge(
                r["score_display"].to(str),
                color_scheme=rx.cond(
                    r["opportunity_score"].to(float) >= 0.55, "green",
                    rx.cond(
                        r["opportunity_score"].to(float) >= 0.35, "orange", "gray",
                    ),
                ),
                size="2",
            ),
        ),
        rx.table.cell(
            rx.button(
                "Open",
                on_click=ScreenerState.open_in_v2(r["market_id"].to(str)),
                size="1",
                variant="soft",
                color_scheme="teal",
            ),
        ),
    )


def screener_page() -> rx.Component:
    return rx.vstack(
        _top_nav(),
        rx.hstack(
            _screener_filters(),
            rx.vstack(
                rx.hstack(
                    rx.heading("Market screener", size="5", weight="bold"),
                    rx.spacer(),
                    rx.cond(
                        ScreenerState.results.length() > 0,
                        rx.text(
                            ScreenerState.visible_count.to_string()
                            + " of "
                            + ScreenerState.total_count.to_string()
                            + " markets visible",
                            size="1", color_scheme="gray",
                        ),
                    ),
                    width="100%",
                    align="end",
                ),
                _screener_progress(),
                _screener_table(),
                spacing="3",
                align="stretch",
                width="100%",
                padding="6",
            ),
            spacing="0",
            align="start",
            width="100%",
        ),
        _page_footer([
            "FCC Broadband Data Collection (BDC)",
            "Census ACS 5-year",
            "Census TIGER/Line PLACE shapefiles",
        ]),
        spacing="0",
        width="100vw",
        min_height="100vh",
        align="stretch",
    )


# ---------------------------------------------------------------------------
# Phase 8 — Provider-centric view
#
# Separate route + state. Reads cached BDC parquets (no new network).
# Two pages:
#   /providers          → directory of every canonical provider with quick stats
#   /provider/<slug>    → single provider detail: per-state breakdown,
#                         head-to-head competitor overlap, trajectory

class ProviderViewState(rx.State):
    """State for /providers (directory) and /provider/<slug> (detail)."""

    # Directory
    directory_loading: bool = False
    directory_error: str = ""
    directory: list[dict[str, Any]] = []
    dir_search: str = ""
    dir_sort_key: str = "n_tracts"
    dir_sort_desc: bool = True
    cached_releases: list[str] = []

    # Detail
    detail_loading: bool = False
    detail_error: str = ""
    detail_canonical: str = ""
    detail_slug: str = ""
    # Prewarm trajectory state
    prewarm_running: bool = False
    prewarm_done: int = 0
    prewarm_total: int = 0
    prewarm_label: str = ""
    detail_summary: dict[str, Any] = {}
    detail_states: list[dict[str, Any]] = []
    detail_head_to_head: list[dict[str, Any]] = []
    detail_trajectory: list[dict[str, Any]] = []
    detail_raw_brands: list[str] = []

    @rx.var
    def detail_map_iframe_url(self) -> str:
        """Backend URL for the national-footprint iframe."""
        if not self.detail_slug:
            return ""
        # Relative URL — same reasoning as v2_map_iframe_url above.
        return f"/provider_map_html?slug={self.detail_slug}"

    @rx.event(background=True)
    async def prewarm_trajectory(self):
        """Iterate every cached BDC release and force aggregation if not
        already cached. After completion, trajectory points across all
        releases will be available on detail pages. Heavy: ~100s per
        release that isn't already disk-cached. The in-process cache
        keeps subsequent renders instant.
        """
        from ftth_compete.analysis.provider_view import (
            _aggregate_parquets,
            _parquets_for_release,
            list_cached_releases,
        )
        async with self:
            if self.prewarm_running:
                return
            releases = list_cached_releases()
            self.prewarm_running = True
            self.prewarm_total = len(releases)
            self.prewarm_done = 0
            self.prewarm_label = f"Pre-warming {len(releases)} releases..."

        for i, rel in enumerate(releases, 1):
            parquets = _parquets_for_release(rel)
            if not parquets:
                async with self:
                    self.prewarm_done = i
                continue
            try:
                await asyncio.to_thread(_aggregate_parquets, parquets)
            except Exception as exc:  # noqa: BLE001
                log.warning("prewarm %s failed: %s", rel, exc)
            async with self:
                self.prewarm_done = i
                self.prewarm_label = f"Pre-warmed {i}/{len(releases)} ({rel})"

        # Reload the detail so trajectory points show up.
        if self.detail_slug:
            from ftth_compete.analysis.provider_view import (
                find_by_slug, provider_detail,
            )
            from dataclasses import asdict
            slug = self.detail_slug
            try:
                canonical = await asyncio.to_thread(find_by_slug, slug)
                if canonical:
                    detail = await asyncio.to_thread(provider_detail, canonical)
                    if detail is not None:
                        async with self:
                            self.detail_trajectory = [
                                asdict(t) for t in detail.trajectory
                            ]
            except Exception as exc:  # noqa: BLE001
                log.warning("post-prewarm detail reload failed: %s", exc)

        async with self:
            self.prewarm_running = False
            self.prewarm_label = (
                f"Done. Trajectory now spans {self.prewarm_done} releases."
            )

    @rx.event
    def set_dir_search(self, value: str) -> None:
        self.dir_search = value

    @rx.event
    def set_dir_sort_key(self, value: str) -> None:
        if value == self.dir_sort_key:
            self.dir_sort_desc = not self.dir_sort_desc
        else:
            self.dir_sort_key = value
            self.dir_sort_desc = True

    @rx.var(cache=True)
    def filtered_directory(self) -> list[dict[str, Any]]:
        q = (self.dir_search or "").strip().lower()
        rows = list(self.directory or [])
        if q:
            rows = [r for r in rows if q in str(r.get("canonical", "")).lower()]
        key = self.dir_sort_key

        def _key_fn(r: dict[str, Any]):
            v = r.get(key)
            if isinstance(v, (int, float)):
                return v
            return str(v or "").lower()

        rows.sort(key=_key_fn, reverse=self.dir_sort_desc)
        return rows[:300]

    @rx.event(background=True)
    async def load_directory(self):
        """Build the cross-market provider directory from cached BDC parquets.

        Cheap once cached (~2-3s). Heavy first time per release.
        """
        from dataclasses import asdict
        from ftth_compete.analysis.provider_view import (
            list_cached_releases, provider_directory,
        )
        async with self:
            if self.directory_loading:
                return
            self.directory_loading = True
            self.directory_error = ""

        try:
            releases = await asyncio.to_thread(list_cached_releases)
            stats = await asyncio.to_thread(provider_directory)
        except Exception as exc:  # noqa: BLE001
            log.exception("provider directory failed")
            async with self:
                self.directory_loading = False
                self.directory_error = f"{type(exc).__name__}: {exc}"
            return

        # Precompute slugs server-side so the directory rows can link via
        # `/provider/<slug>` without doing the slugify in the JSX layer.
        from ftth_compete.analysis.provider_view import slugify
        rows: list[dict[str, Any]] = []
        for s in stats:
            d = asdict(s)
            d["slug"] = slugify(s.canonical)
            rows.append(d)

        async with self:
            self.cached_releases = releases
            self.directory = rows
            self.directory_loading = False
            if not stats:
                self.directory_error = (
                    "No cached BDC parquets found. Run a market lookup on "
                    "/v2 first to populate the cache, then return here."
                )

    @rx.event(background=True)
    async def load_detail(self):
        """Resolve the route slug → canonical → populate detail fields.

        Fires from `on_load` of /provider/[slug]; the slug comes from
        `self.router.page.params["slug"]`.
        """
        from dataclasses import asdict
        from ftth_compete.analysis.provider_view import (
            find_by_slug, provider_detail,
        )
        async with self:
            page = getattr(self.router, "page", None)
            params = getattr(page, "params", {}) if page else {}
            slug = (params or {}).get("slug", "")
            if not slug:
                self.detail_error = "Missing provider slug in URL."
                return
            self.detail_loading = True
            self.detail_error = ""
            self.detail_canonical = ""
            self.detail_slug = ""
            self.detail_summary = {}
            self.detail_states = []
            self.detail_head_to_head = []
            self.detail_trajectory = []
            self.detail_raw_brands = []

        try:
            canonical = await asyncio.to_thread(find_by_slug, slug)
            if not canonical:
                async with self:
                    self.detail_loading = False
                    self.detail_error = (
                        f"No provider matches slug {slug!r}. "
                        "They may not have a footprint in any cached state."
                    )
                return
            detail = await asyncio.to_thread(provider_detail, canonical)
        except Exception as exc:  # noqa: BLE001
            log.exception("provider detail failed")
            async with self:
                self.detail_loading = False
                self.detail_error = f"{type(exc).__name__}: {exc}"
            return

        async with self:
            self.detail_loading = False
            if detail is None:
                self.detail_error = f"No data for {canonical}."
                return
            self.detail_canonical = detail.canonical
            self.detail_slug = slug
            self.detail_summary = asdict(detail.summary)
            self.detail_states = [asdict(s) for s in detail.states]
            self.detail_head_to_head = [asdict(h) for h in detail.head_to_head]
            self.detail_trajectory = [asdict(t) for t in detail.trajectory]
            self.detail_raw_brands = list(detail.raw_brand_names)


# ---------------------------------------------------------------------------
# Provider directory UI

def _provider_dir_header(label: str, key: str) -> rx.Component:
    return rx.table.column_header_cell(
        rx.hstack(
            rx.text(label, size="1", weight="bold"),
            rx.cond(
                ProviderViewState.dir_sort_key == key,
                rx.cond(
                    ProviderViewState.dir_sort_desc,
                    rx.icon("chevron_down", size=12),
                    rx.icon("chevron_up", size=12),
                ),
            ),
            spacing="1",
            align="center",
        ),
        on_click=ProviderViewState.set_dir_sort_key(key),
        style={"cursor": "pointer", "_hover": {"background_color": "var(--gray-a3)"}},
    )


def _provider_dir_row(r: rx.Var) -> rx.Component:
    return rx.table.row(
        rx.table.cell(
            rx.link(
                rx.text(r["canonical"].to(str), weight="medium", size="2"),
                href="/provider/" + r["slug"].to(str),
                color_scheme="teal",
            ),
        ),
        rx.table.cell(r["n_states"].to(int).to_string(), justify="end"),
        rx.table.cell(r["n_tracts"].to(int).to_string(), justify="end"),
        rx.table.cell(r["n_fiber_tracts"].to(int).to_string(), justify="end"),
        rx.table.cell(r["total_locations"].to(int).to_string(), justify="end"),
        rx.table.cell(
            rx.cond(
                r["has_fiber"],
                rx.badge("Fiber", color_scheme="green", size="1"),
                rx.badge("No fiber", color_scheme="gray", size="1"),
            ),
        ),
    )


def providers_directory_page() -> rx.Component:
    return rx.vstack(
        _top_nav(),
        rx.vstack(
            rx.hstack(
                rx.heading("Provider directory", size="5", weight="bold"),
                rx.spacer(),
                rx.cond(
                    ProviderViewState.cached_releases.length() > 0,
                    rx.text(
                        "Release " + ProviderViewState.cached_releases[0].to(str)
                        + "  ·  " + ProviderViewState.directory.length().to_string()
                        + " providers across "
                        + ProviderViewState.cached_releases.length().to_string()
                        + " cached release(s)",
                        size="1", color_scheme="gray",
                    ),
                ),
                width="100%", align="end",
            ),
            rx.text(
                "Every provider with a footprint in any cached BDC release. "
                "Click a name for the per-state breakdown, competitor overlap, "
                "and trajectory across releases.",
                size="2", color_scheme="gray",
            ),
            rx.cond(
                ProviderViewState.directory_error != "",
                rx.callout(
                    ProviderViewState.directory_error,
                    icon="triangle_alert", color_scheme="orange", size="1",
                ),
            ),
            rx.hstack(
                rx.input(
                    placeholder="Search providers...",
                    value=ProviderViewState.dir_search,
                    on_change=ProviderViewState.set_dir_search,
                    size="2",
                    width="320px",
                ),
                rx.cond(
                    ProviderViewState.directory_loading,
                    rx.hstack(
                        rx.spinner(size="1"),
                        rx.text("Aggregating cached parquets...", size="1", color_scheme="gray"),
                        spacing="2", align="center",
                    ),
                ),
                spacing="2",
                align="center",
            ),
            rx.cond(
                ProviderViewState.filtered_directory.length() > 0,
                rx.table.root(
                    rx.table.header(
                        rx.table.row(
                            _provider_dir_header("Provider", "canonical"),
                            _provider_dir_header("States", "n_states"),
                            _provider_dir_header("Tracts", "n_tracts"),
                            _provider_dir_header("Fiber tracts", "n_fiber_tracts"),
                            _provider_dir_header("Locations", "total_locations"),
                            rx.table.column_header_cell(""),
                        ),
                    ),
                    rx.table.body(
                        rx.foreach(
                            ProviderViewState.filtered_directory,
                            _provider_dir_row,
                        ),
                    ),
                    size="1",
                    variant="surface",
                ),
                rx.cond(
                    ~ProviderViewState.directory_loading,
                    rx.center(
                        rx.vstack(
                            rx.icon("database", size=36, color="var(--gray-7)"),
                            rx.text(
                                rx.cond(
                                    ProviderViewState.directory.length() > 0,
                                    "No matches for search.",
                                    "No data yet. Run a market lookup on /v2 to populate the cache.",
                                ),
                                size="2", color_scheme="gray",
                            ),
                            spacing="2", align="center",
                        ),
                        padding="10",
                        min_height="300px",
                    ),
                ),
            ),
            spacing="3",
            align="stretch",
            width="100%",
            padding="6",
        ),
        _page_footer([
            "FCC Broadband Data Collection (BDC)",
            "Provider canonicalization registry (curated)",
            "10-K national subscriber anchors",
        ]),
        spacing="0",
        width="100vw",
        min_height="100vh",
        align="stretch",
    )


# ---------------------------------------------------------------------------
# Provider detail UI

def _provider_state_row(r: rx.Var) -> rx.Component:
    return rx.table.row(
        rx.table.cell(rx.text(r["state"].to(str), weight="medium", size="2")),
        rx.table.cell(r["n_tracts"].to(int).to_string(), justify="end"),
        rx.table.cell(r["n_fiber_tracts"].to(int).to_string(), justify="end"),
        rx.table.cell(r["total_locations"].to(int).to_string(), justify="end"),
    )


def _provider_h2h_row(r: rx.Var) -> rx.Component:
    return rx.table.row(
        rx.table.cell(rx.text(r["canonical"].to(str), size="2")),
        rx.table.cell(r["shared_tracts"].to(int).to_string(), justify="end"),
        rx.table.cell(
            (r["pct_overlap"].to(float) * 100).to_string() + "%",
            justify="end",
        ),
    )


def _provider_trajectory_chart() -> rx.Component:
    """Trajectory across cached BDC releases. Shows pre-warm button so
    the user can force-build older release aggregations when they only
    have the latest cached."""
    return rx.vstack(
        rx.hstack(
            _section_title("Trajectory across cached BDC releases"),
            rx.spacer(),
            rx.cond(
                ProviderViewState.prewarm_running,
                rx.hstack(
                    rx.spinner(size="1"),
                    rx.text(ProviderViewState.prewarm_label, size="1", color_scheme="gray"),
                    spacing="2",
                    align="center",
                ),
                rx.button(
                    rx.icon("refresh_cw", size=12),
                    "Pre-warm older releases",
                    on_click=ProviderViewState.prewarm_trajectory,
                    size="1",
                    variant="soft",
                    color_scheme="gray",
                ),
            ),
            width="100%",
            align="center",
        ),
        rx.text(
            "Tract count per release. Each uncached release takes ~100s to "
            "build on first pre-warm; subsequent loads are instant.",
            size="1", color_scheme="gray",
        ),
        rx.cond(
            ProviderViewState.detail_trajectory.length() > 0,
            rx.table.root(
                rx.table.header(
                    rx.table.row(
                        rx.table.column_header_cell("Release"),
                        rx.table.column_header_cell("Tracts", justify="end"),
                        rx.table.column_header_cell("Fiber tracts", justify="end"),
                        rx.table.column_header_cell("Locations", justify="end"),
                    ),
                ),
                rx.table.body(
                    rx.foreach(
                        ProviderViewState.detail_trajectory,
                        lambda p: rx.table.row(
                            rx.table.cell(p["release"].to(str)),
                            rx.table.cell(p["n_tracts"].to(int).to_string(), justify="end"),
                            rx.table.cell(p["n_fiber_tracts"].to(int).to_string(), justify="end"),
                            rx.table.cell(p["total_locations"].to(int).to_string(), justify="end"),
                        ),
                    ),
                ),
                size="1", variant="surface",
            ),
            rx.text(
                "Only one release cached yet. Click 'Pre-warm older releases' "
                "to build trajectory data across all cached BDC releases.",
                size="1", color_scheme="gray",
            ),
        ),
        spacing="2",
        align="stretch",
        width="100%",
    )


def provider_detail_page() -> rx.Component:
    return rx.vstack(
        _top_nav(),
        rx.cond(
            ProviderViewState.detail_loading,
            rx.center(
                rx.hstack(
                    rx.spinner(size="3"),
                    rx.text("Aggregating provider footprint...", size="2"),
                    spacing="3",
                    align="center",
                ),
                padding="10",
                min_height="400px",
            ),
            rx.cond(
                ProviderViewState.detail_error != "",
                rx.center(
                    rx.callout(
                        ProviderViewState.detail_error,
                        icon="triangle_alert", color_scheme="red", size="1",
                    ),
                    padding="10",
                ),
                rx.vstack(
                    # Header
                    rx.hstack(
                        rx.link(
                            rx.hstack(
                                rx.icon("arrow_left", size=14),
                                rx.text("All providers", size="1"),
                                spacing="1",
                                align="center",
                            ),
                            href="/providers",
                            color_scheme="gray",
                        ),
                        spacing="3", align="center",
                        width="100%",
                    ),
                    rx.heading(
                        ProviderViewState.detail_canonical,
                        size="6", weight="bold",
                    ),
                    rx.hstack(
                        rx.badge(
                            ProviderViewState.detail_summary["n_states"].to(int).to_string()
                            + " states",
                            color_scheme="teal", size="2",
                        ),
                        rx.badge(
                            ProviderViewState.detail_summary["n_tracts"].to(int).to_string()
                            + " tracts",
                            color_scheme="blue", size="2",
                        ),
                        rx.cond(
                            ProviderViewState.detail_summary["has_fiber"].to(bool),
                            rx.badge(
                                ProviderViewState.detail_summary["n_fiber_tracts"].to(int).to_string()
                                + " fiber tracts",
                                color_scheme="green", size="2",
                            ),
                        ),
                        rx.badge(
                            ProviderViewState.detail_summary["total_locations"].to(int).to_string()
                            + " locations",
                            color_scheme="gray", size="2",
                        ),
                        spacing="2",
                        wrap="wrap",
                    ),
                    rx.cond(
                        ProviderViewState.detail_raw_brands.length() > 1,
                        rx.text(
                            "Brand aliases: "
                            + ProviderViewState.detail_raw_brands.join(", "),
                            size="1", color_scheme="gray",
                        ),
                    ),
                    rx.divider(margin_y="3"),
                    # National footprint map — state-level choropleth painting
                    # `n_tracts` per state served by this provider.
                    rx.vstack(
                        _section_title("National footprint"),
                        rx.text(
                            "State-level intensity = tract count served by this provider "
                            "in cached BDC data. Currently shows only states whose BDC "
                            "parquets are cached on this machine.",
                            size="1", color_scheme="gray",
                        ),
                        rx.box(
                            rx.el.iframe(
                                src=ProviderViewState.detail_map_iframe_url,
                                width="100%",
                                height="100%",
                                style={"border": "0", "display": "block"},
                            ),
                            width="100%",
                            height="380px",
                            background_color="var(--gray-2)",
                            border_radius="8px",
                            overflow="hidden",
                            border="1px solid var(--gray-a4)",
                        ),
                        spacing="2",
                        align="stretch",
                        width="100%",
                    ),
                    rx.divider(margin_y="3"),
                    # Three-column body
                    rx.grid(
                        # Left: per-state breakdown
                        rx.vstack(
                            _section_title("Per-state breakdown"),
                            rx.table.root(
                                rx.table.header(
                                    rx.table.row(
                                        rx.table.column_header_cell("State"),
                                        rx.table.column_header_cell("Tracts", justify="end"),
                                        rx.table.column_header_cell("Fiber", justify="end"),
                                        rx.table.column_header_cell("Locations", justify="end"),
                                    ),
                                ),
                                rx.table.body(
                                    rx.foreach(
                                        ProviderViewState.detail_states,
                                        _provider_state_row,
                                    ),
                                ),
                                size="1", variant="surface",
                            ),
                            spacing="2",
                            align="stretch",
                            width="100%",
                        ),
                        # Right: head-to-head
                        rx.vstack(
                            _section_title("Head-to-head overlap (top 10)"),
                            rx.text(
                                "Other providers sharing the most tracts with "
                                + ProviderViewState.detail_canonical
                                + ".",
                                size="1", color_scheme="gray",
                            ),
                            rx.cond(
                                ProviderViewState.detail_head_to_head.length() > 0,
                                rx.table.root(
                                    rx.table.header(
                                        rx.table.row(
                                            rx.table.column_header_cell("Competitor"),
                                            rx.table.column_header_cell("Shared", justify="end"),
                                            rx.table.column_header_cell("% overlap", justify="end"),
                                        ),
                                    ),
                                    rx.table.body(
                                        rx.foreach(
                                            ProviderViewState.detail_head_to_head,
                                            _provider_h2h_row,
                                        ),
                                    ),
                                    size="1", variant="surface",
                                ),
                                rx.text(
                                    "No overlapping competitors in the cached data.",
                                    size="1", color_scheme="gray",
                                ),
                            ),
                            spacing="2",
                            align="stretch",
                            width="100%",
                        ),
                        columns="2",
                        spacing="6",
                        width="100%",
                    ),
                    rx.divider(margin_y="3"),
                    _provider_trajectory_chart(),
                    spacing="3",
                    align="stretch",
                    width="100%",
                    padding="6",
                ),
            ),
        ),
        _page_footer([
            "FCC Broadband Data Collection (BDC)",
            "Provider canonicalization registry (curated)",
        ]),
        spacing="0",
        width="100vw",
        min_height="100vh",
        align="stretch",
    )


# ---------------------------------------------------------------------------

def _root_redirect() -> rx.Component:
    """Bare route at `/` that immediately bounces visitors to the v2
    map-canvas UI. v1 (legacy tabbed) stays accessible at `/v1`.
    """
    return rx.fragment(
        rx.script("window.location.replace('/v2');"),
        rx.center(
            rx.text(
                "Loading…",
                size="3", color_scheme="gray",
            ),
            padding="10",
            min_height="60vh",
        ),
    )


app = rx.App()
app.add_page(_root_redirect, route="/", title="ftth-compete")
app.add_page(index, route="/v1", title="ftth-compete v1")
app.add_page(
    v2_page,
    route="/v2",
    title="ftth-compete v2",
    on_load=LookupState.maybe_autorun,
)
app.add_page(screener_page, route="/screener", title="ftth-compete · Screener")
app.add_page(
    providers_directory_page,
    route="/providers",
    title="ftth-compete · Providers",
    on_load=ProviderViewState.load_directory,
)
app.add_page(
    provider_detail_page,
    route="/provider/[slug]",
    title="ftth-compete · Provider",
    on_load=ProviderViewState.load_detail,
)


# ---------------------------------------------------------------------------
# FastAPI endpoint serving the v2 map figure as standalone HTML.
#
# Why this exists: `rx.plotly` silently failed to render the Choroplethmap
# trace through Reflex's Var-serialization path. `rx.html` doesn't execute
# embedded `<script>` tags (React's dangerouslySetInnerHTML limitation).
# `rx.el.iframe(src_doc=...)` works around React but `about:srcdoc` origin
# blocks OSM tile fetches.
#
# This endpoint serves the figure as a fully-rendered HTML page at a real
# same-origin URL. The v2 page embeds via `<iframe src="/v2_map_html?...">`
# — regular browsing context, scripts execute, tiles load.
#
# Cache is in-process to avoid re-running the pipeline on every layer
# switch. Keyed by (city, state, layer); evicted manually on new lookup.

from starlette.requests import Request  # noqa: E402
from starlette.responses import Response  # noqa: E402

_v2_html_cache: dict[tuple[str, str, str], str] = {}


def _v2_html_for(city: str, state: str, layer: str) -> str:
    """Compute (or return cached) Plotly HTML for the given market+layer."""
    key = (city.strip().lower(), state.strip().upper(), layer)
    if key in _v2_html_cache:
        return _v2_html_cache[key]
    try:
        from ftth_compete.pipeline import run_market
        sheet = run_market(
            city, state,
            no_speeds=True, no_ratings=True, no_ias=True,
        )
        geoids = list(sheet.tracts.get("inside_city", []))
        geojson = _build_tract_geojson(geoids)
        tract_values = _build_tract_values(sheet)
        tract_providers = _build_tract_provider_hover(sheet)
        fig = build_v2_plotly_figure(
            geojson, tract_values, layer, "",
            tract_providers=tract_providers,
        )
        html = fig.to_html(
            full_html=True,
            include_plotlyjs="cdn",
            config={"displayModeBar": False, "responsive": True},
            default_height="100%",
            default_width="100%",
        )
        injected_style = (
            "<style>html,body{margin:0;padding:0;height:100vh;width:100vw;"
            "overflow:hidden;background:#fff;}"
            ".plotly-graph-div{height:100vh !important;width:100vw !important;}"
            "</style>"
        )
        # Bridge: on tract click, post the GEOID to the parent window. The
        # parent page picks this up via a window 'message' listener and
        # dispatches `LookupState.select_tract(geoid)` so the right rail
        # repaints with the tract's provider list + per-layer values.
        injected_script = (
            "<script>"
            "document.addEventListener('DOMContentLoaded', function(){"
            "  var gd = document.querySelector('.plotly-graph-div');"
            "  if (!gd) return;"
            "  var bind = function(){"
            "    gd.on('plotly_click', function(ev){"
            "      try {"
            "        var pt = ev && ev.points && ev.points[0];"
            "        if (!pt) return;"
            "        var geoid = pt.location || pt.id || '';"
            "        if (!geoid) return;"
            "        window.parent.postMessage("
            "          {type:'ftth_tract_click', geoid: String(geoid)}, '*');"
            "      } catch(e) { console.error('ftth click bridge', e); }"
            "    });"
            "  };"
            "  if (gd.on) { bind(); }"
            "  else { gd.addEventListener('plotly_afterplot', bind, {once:true}); }"
            "});"
            "</script>"
        )
        html = html.replace(
            "</head>", injected_style + injected_script + "</head>", 1,
        )
        _v2_html_cache[key] = html
        return html
    except Exception as exc:  # noqa: BLE001
        return (
            "<html><body style='font-family:sans-serif;padding:20px;color:#444'>"
            f"<h3>Map render failed</h3><pre>{exc}</pre></body></html>"
        )


async def _serve_v2_map(request: Request) -> Response:
    """Starlette endpoint: GET /v2_map_html?city=...&state=...&layer=...

    Reflex 0.9 registers backend routes via the underlying Starlette
    app at `app._api.add_route(path, handler, methods=...)`. Handlers
    take a `Request` and return a `Response` — FastAPI's auto-parsed
    query params don't apply here (it's bare Starlette).
    """
    qp = request.query_params
    city = qp.get("city", "")
    state = qp.get("state", "")
    layer = qp.get("layer", "Fiber providers per tract")
    if not city or not state:
        return Response(
            content="<html><body>Need city + state query params.</body></html>",
            media_type="text/html",
        )
    html = _v2_html_for(city, state, layer)
    return Response(content=html, media_type="text/html")


# Reflex 0.9 exposes the underlying Starlette app as `app._api`. The
# `prepend_backend_path` config-call normalizes the path with the
# `backend_path` prefix (usually empty in dev).
try:
    app._api.add_route("/v2_map_html", _serve_v2_map, methods=["GET"])
    print("[ftth_compete_web] Registered v2 map endpoint at /v2_map_html")
except Exception as exc:  # noqa: BLE001
    print(f"[ftth_compete_web] FAILED to register /v2_map_html: {exc!r}")


# Phase 8 — provider national-footprint map endpoint.
# Renders a US state-level choropleth of one provider's footprint from
# the cached BDC parquets. Cached in-process so layer switches and
# multiple opens of the same provider are instant.

_provider_map_cache: dict[str, str] = {}


def _provider_map_html(slug: str) -> str:
    """Compute (or return cached) Plotly HTML for a provider's national map."""
    key = slug.strip().lower()
    if key in _provider_map_cache:
        return _provider_map_cache[key]
    try:
        from dataclasses import asdict
        from ftth_compete.analysis.provider_view import (
            find_by_slug, provider_detail,
        )
        canonical = find_by_slug(key)
        if not canonical:
            return (
                "<html><body style='font-family:sans-serif;padding:20px;color:#444'>"
                f"<h3>Unknown provider</h3><p>No match for slug <code>{key}</code>."
                "</p></body></html>"
            )
        detail = provider_detail(canonical)
        if detail is None:
            return (
                "<html><body style='font-family:sans-serif;padding:20px;color:#444'>"
                f"<h3>No data</h3><p>{canonical} has no footprint in the cached "
                "BDC releases.</p></body></html>"
            )
        states_data = [asdict(s) for s in detail.states]
        fig = build_provider_footprint_figure(states_data)
        html = fig.to_html(
            full_html=True,
            include_plotlyjs="cdn",
            config={"displayModeBar": False, "responsive": True},
            default_height="100%",
            default_width="100%",
        )
        injected_style = (
            "<style>html,body{margin:0;padding:0;height:100vh;width:100vw;"
            "overflow:hidden;background:#fff;}"
            ".plotly-graph-div{height:100vh !important;width:100vw !important;}"
            "</style>"
        )
        html = html.replace("</head>", injected_style + "</head>", 1)
        _provider_map_cache[key] = html
        return html
    except Exception as exc:  # noqa: BLE001
        return (
            "<html><body style='font-family:sans-serif;padding:20px;color:#444'>"
            f"<h3>Map render failed</h3><pre>{exc}</pre></body></html>"
        )


async def _serve_provider_map(request: Request) -> Response:
    """GET /provider_map_html?slug=<canonical-slug>"""
    slug = request.query_params.get("slug", "")
    if not slug:
        return Response(
            content="<html><body>Need slug query param.</body></html>",
            media_type="text/html",
        )
    html = _provider_map_html(slug)
    return Response(content=html, media_type="text/html")


try:
    app._api.add_route(
        "/provider_map_html", _serve_provider_map, methods=["GET"],
    )
    print(
        "[ftth_compete_web] Registered provider map endpoint at /provider_map_html"
    )
except Exception as exc:  # noqa: BLE001
    print(f"[ftth_compete_web] FAILED to register /provider_map_html: {exc!r}")


# ---------------------------------------------------------------------------
# Admin sidecar — private visitor/event log.
#
# Gated by `?key=<ADMIN_KEY>` matching the `ADMIN_KEY` env var. Wrong /
# missing key returns 404 so the route is indistinguishable from "no
# such route" to anonymous visitors. Without `ADMIN_KEY` set the route
# is permanently disabled (always 404).

import html as _html  # local alias to avoid clobbering any `html` name
import os as _os


def _render_admin_html() -> str:
    summary = analytics.summary()
    events = analytics.recent(limit=300)

    def _esc(v) -> str:
        return _html.escape(str(v if v is not None else ""))

    rows_html = "".join(
        f"<tr>"
        f"<td class='nowrap'>{_esc(e['ts'])}</td>"
        f"<td class='nowrap'>{_esc((e['session_id'] or '')[:8])}</td>"
        f"<td class='nowrap'>{_esc(e['ip_hash'] or '')}</td>"
        f"<td>{_esc(e['kind'])}</td>"
        f"<td><code>{_esc((e['payload'] or '')[:300])}</code></td>"
        f"<td class='ua'>{_esc((e['ua'] or '')[:80])}</td>"
        f"</tr>"
        for e in events
    )
    by_kind = ", ".join(
        f"{_esc(k)}: {n}" for k, n in summary["by_kind_today"]
    ) or "(none)"

    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>ftth-compete · admin</title>
<style>
body{{font-family:system-ui,-apple-system,Segoe UI,sans-serif;padding:20px;color:#222;background:#fafafa;margin:0;}}
h1{{font-size:18px;margin:0 0 12px 0;}}
.summary{{background:white;padding:14px 18px;border:1px solid #ddd;margin-bottom:14px;border-radius:6px;}}
.summary div{{margin:3px 0;font-size:14px;}}
.summary strong{{display:inline-block;min-width:200px;color:#444;}}
table{{border-collapse:collapse;width:100%;font-size:13px;background:white;}}
th,td{{border:1px solid #ddd;padding:6px 10px;text-align:left;vertical-align:top;}}
th{{background:#eee;font-weight:600;position:sticky;top:0;}}
tr:nth-child(even){{background:#f7f7f7;}}
code{{font-size:12px;color:#555;font-family:ui-monospace,Menlo,Consolas,monospace;}}
.nowrap{{white-space:nowrap;}}
.ua{{color:#888;font-size:11px;max-width:240px;overflow:hidden;text-overflow:ellipsis;}}
.muted{{color:#777;font-size:12px;margin-top:6px;}}
</style></head>
<body>
<h1>ftth-compete · admin sidecar</h1>
<div class="summary">
  <div><strong>Sessions today (UTC)</strong> {summary['sessions_today']}</div>
  <div><strong>Distinct IPs today</strong> {summary['unique_ips_today']}</div>
  <div><strong>Total events (all time)</strong> {summary['total_events']}</div>
  <div><strong>Events today by kind</strong> {by_kind}</div>
  <div class="muted">
    Storage is ephemeral on HF Spaces free tier — wipes on container restart.
    IPs are stored as an 8-char SHA-256 prefix, not raw.
  </div>
</div>
<table>
  <thead><tr>
    <th>UTC timestamp</th>
    <th>Session</th>
    <th>IP hash</th>
    <th>Event</th>
    <th>Payload</th>
    <th>User-Agent</th>
  </tr></thead>
  <tbody>
    {rows_html or '<tr><td colspan="6" class="muted">No events yet.</td></tr>'}
  </tbody>
</table>
</body></html>"""


async def _serve_admin(request: Request) -> Response:
    """GET /admin?key=<ADMIN_KEY>

    Wrong or missing key returns 404 (indistinguishable from "no such
    route"). If ADMIN_KEY env var is unset, the route is disabled
    entirely — same 404 response.
    """
    expected = _os.environ.get("ADMIN_KEY", "").strip()
    given = (request.query_params.get("key", "") or "").strip()
    if not expected or given != expected:
        return Response(
            content="<html><body>Not found.</body></html>",
            media_type="text/html",
            status_code=404,
        )
    return Response(content=_render_admin_html(), media_type="text/html")


try:
    app._api.add_route("/admin", _serve_admin, methods=["GET"])
    print("[ftth_compete_web] Registered admin sidecar at /admin")
except Exception as exc:  # noqa: BLE001
    print(f"[ftth_compete_web] FAILED to register /admin: {exc!r}")
