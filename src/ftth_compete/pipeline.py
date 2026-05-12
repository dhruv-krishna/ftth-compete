"""End-to-end market pipeline: city + state -> tear-sheet.

Single source of truth for both the CLI (`ftth-compete market ...`) and the
Reflex dashboard. Keep this thin: orchestration only, no rendering or
formatting. UI-specific transforms live next to the Reflex app in
`ftth_compete_web/`; domain-level math lives in `analysis/`.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from typing import Any

from .analysis import competitors as competitors_mod
from .analysis import housing as housing_mod
from .analysis import market as market_mod
from .analysis import penetration as penetration_mod
from .analysis import trajectory as trajectory_mod
from .analysis import velocity as velocity_mod
from .analysis.competitors import ProviderSummary
from .analysis.housing import HousingSplit
from .analysis.market import MarketMetrics
from .config import get_settings
from .data import census_acs, fcc_bdc, fcc_ias, google_places, ookla, tiger

log = logging.getLogger(__name__)


def _emit(label: str) -> None:
    """Log a phase message at INFO. Used to be a callback hook for
    Streamlit's `st.status` progress UI; now it's just a logger since
    the Reflex UI consumes structured state, not free-text phase
    messages. Kept as a function so all the `_emit("...")` call sites
    stay readable as "this is a pipeline phase boundary."
    """
    log.info("phase: %s", label)


@dataclass(frozen=True)
class TearSheet:
    """Self-contained snapshot of a single market.

    Immutable so it can be cached safely (e.g., via Streamlit's @st.cache_data).
    """

    market: dict[str, Any]
    tracts: dict[str, list[str]]
    demographics: MarketMetrics
    housing: HousingSplit
    tract_acs: list[dict[str, Any]]  # per-tract ACS rows (raw, for tab-level analyses)
    coverage_matrix: list[dict[str, Any]]  # raw BDC tract x provider x tech rows
    location_availability: list[dict[str, Any]]  # per-tract BSL availability by tech
    providers: list[ProviderSummary] | None
    providers_note: str | None
    provider_subs: list[dict[str, Any]]  # SubsEstimate per (provider, tech) — heuristic 10-K-anchor based
    market_subs_anchor: dict[str, Any] | None  # IAS market-total subs anchor (None if IAS unavailable)
    tract_subs: list[dict[str, Any]]  # raw IAS tract subscription buckets (for tab-level use)
    ias_note: str | None  # null when populated, otherwise reason (skipped, no data, etc.)
    provider_velocity: list[dict[str, Any]]  # CoverageVelocity per (provider, tech) when include_velocity=True
    velocity_note: str | None  # null when populated, otherwise why it isn't (e.g. opt-in not set)
    provider_trajectory: list[dict[str, Any]]  # ProviderTrajectory per (provider, tech) when include_trajectory=True
    trajectory_note: str | None  # null when populated, otherwise why it isn't
    tract_speeds: list[dict[str, Any]]  # per-tract Ookla aggregates
    speeds_note: str | None  # null if speeds populated, otherwise reason
    provider_ratings: dict[str, dict[str, Any] | None]  # canonical_name -> rating dict (or None)
    ratings_note: str | None
    data_versions: dict[str, Any] = field(default_factory=dict)
    # Phase 6c — ACP enrollment density (added late, so kept at the end
    # to preserve dataclass field ordering: defaulted fields after required).
    acp_density: list[dict[str, Any]] = field(default_factory=list)
    market_acp_density: float | None = None
    # Phase 6e — Historical IAS take-rate trajectory. List of
    # MarketSubscriptionPoint dicts (one per IAS release, newest-first).
    # Empty list = not loaded (opt-in via `include_subs_history=True`).
    market_subscription_history: list[dict[str, Any]] = field(default_factory=list)
    subs_history_note: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """JSON-friendly dict with dataclasses unpacked recursively."""
        return {
            "market": self.market,
            "tracts": self.tracts,
            "demographics": asdict(self.demographics),
            "housing": asdict(self.housing),
            "tract_acs": self.tract_acs,
            "coverage_matrix_rows": len(self.coverage_matrix),  # avoid bloating CLI JSON
            "location_availability": self.location_availability,
            "providers": (
                [asdict(p) for p in self.providers] if self.providers is not None else None
            ),
            "providers_note": self.providers_note,
            "provider_subs": self.provider_subs,
            "market_subs_anchor": self.market_subs_anchor,
            "tract_subs": self.tract_subs,
            "ias_note": self.ias_note,
            "provider_velocity": self.provider_velocity,
            "velocity_note": self.velocity_note,
            "provider_trajectory": self.provider_trajectory,
            "trajectory_note": self.trajectory_note,
            "acp_density": self.acp_density,
            "market_acp_density": self.market_acp_density,
            "market_subscription_history": self.market_subscription_history,
            "subs_history_note": self.subs_history_note,
            "tract_speeds": self.tract_speeds,
            "speeds_note": self.speeds_note,
            "provider_ratings": self.provider_ratings,
            "ratings_note": self.ratings_note,
            "data_versions": self.data_versions,
        }


def run_market(
    city: str,
    state: str,
    *,
    include_boundary: bool = False,
    no_providers: bool = False,
    no_speeds: bool = False,
    no_ratings: bool = False,
    no_ias: bool = False,
    include_velocity: bool = False,
    include_trajectory: bool = False,
    trajectory_points: int = 4,
    include_subs_history: bool = False,
) -> TearSheet:
    """Resolve a market end-to-end and return a TearSheet.

    Args:
        city: City name (e.g., "Evans").
        state: 2-letter state abbreviation (e.g., "CO").
        include_boundary: If True, include tracts that intersect the city
            but whose centroid falls outside.
        no_providers: If True, skip the FCC BDC provider lookup. Faster and
            avoids requiring FCC credentials. Demographics-only output.
        no_speeds: If True, skip the Ookla measured-speed query. Faster but
            loses the advertised-vs-measured story.
        no_ratings: If True, skip Google Places rating lookups. Saves API
            quota. Required if GOOGLE_PLACES_KEY is unset.
        no_ias: If True, skip FCC IAS subscription-density lookup. Without
            it, per-provider subs estimates remain heuristic (medium / low
            confidence); with it, they're calibrated against the IAS
            tract-level subscriber anchor (high confidence).
        include_velocity: If True, ALSO fetch the previous BDC release
            (~12 months prior) and compute per-(provider, tech) coverage
            deltas. Triggers an additional state-level BDC ingest (~5 min
            on first state lookup). Off by default for that reason.
    """
    settings = get_settings()
    settings.ensure_dirs()

    # 1) City -> tracts via TIGER
    _emit(f"Resolving {city}, {state} via TIGER...")
    try:
        res = tiger.city_to_tracts(city, state)
    except ValueError as exc:
        # Surface a friendlier message; let caller handle the actual exception
        # (CLI prints, UI st.exception). Also emit so any partial UI updates.
        _emit(f"Could not resolve {city}, {state}: {exc}")
        raise
    geoids = list(res.geoids)
    if include_boundary:
        geoids.extend(res.boundary_tract_geoids)

    # 2) Independent fetches: ACS demographics, FCC BDC providers, Ookla
    # measured speeds. None depends on the others' results, so run them
    # concurrently in a small ThreadPool. On cold lookups this cuts wall
    # time by 10-20s (BDC is the long pole at ~30-90s, ACS ~5-15s, Ookla
    # ~5-15s — running them in parallel collapses to roughly max(...)).
    # The DuckDB / httpx / GeoPandas calls each create their own
    # connection/session, so they're thread-safe in concurrent threads.
    # `_emit` is the only shared global; its callback runs are best-
    # effort (Streamlit-only) and the Reflex UI doesn't use it.
    def _phase_acs():
        _emit(f"Fetching Census ACS demographics for {len(geoids)} tracts...")
        return census_acs.fetch_market_metrics(geoids)

    def _phase_bdc():
        if no_providers:
            return None, None, [], [], None
        if not (settings.fcc_username and settings.fcc_api_token):
            return None, None, [], [], (
                "FCC_USERNAME / FCC_API_TOKEN not set in .env; provider data skipped. "
                "Register at https://apps.fcc.gov/cores/userLogin.do."
            )
        try:
            _emit("Querying FCC BDC for latest release...")
            release = fcc_bdc.latest_release()
            _emit(
                f"Downloading / loading BDC providers for {state} "
                f"(release {release}; ~90s on first state)..."
            )
            coverage = fcc_bdc.coverage_matrix(geoids, as_of=release)
            providers_local = competitors_mod.score(coverage, n_tracts=len(geoids))
            cov_rows = coverage.to_dicts() if not coverage.is_empty() else []
            _emit("Computing location-level availability per tract...")
            avail = fcc_bdc.location_availability(geoids, as_of=release)
            avail_rows = avail.to_dicts() if not avail.is_empty() else []
            return release, providers_local, cov_rows, avail_rows, None
        except Exception as exc:  # noqa: BLE001
            log.exception("FCC BDC ingest failed")
            return None, None, [], [], f"FCC BDC ingest failed: {exc}"

    def _phase_ookla():
        if no_speeds:
            return [], "Ookla speed query skipped (no_speeds=True)."
        try:
            _emit("Aggregating Ookla measured speeds for market tracts...")
            polys = tiger.tract_polygons(geoids, state)
            speed_frame = ookla.fetch_tract_speeds(polys)
            speeds_local = (
                speed_frame.to_dicts() if not speed_frame.is_empty() else []
            )
            if not speeds_local:
                return [], "No Ookla tiles in market bbox; low-sample area."
            return speeds_local, None
        except Exception as exc:  # noqa: BLE001
            log.exception("Ookla fetch failed")
            return [], f"Ookla fetch failed: {exc}"

    with ThreadPoolExecutor(max_workers=3) as _pool:
        f_acs = _pool.submit(_phase_acs)
        f_bdc = _pool.submit(_phase_bdc)
        f_ookla = _pool.submit(_phase_ookla)
        acs = f_acs.result()
        (
            bdc_release,
            providers_block,
            coverage_rows,
            location_avail_rows,
            providers_note,
        ) = f_bdc.result()
        tract_speeds, speeds_note = f_ookla.result()

    metrics = market_mod.aggregate(acs.frame)
    housing = housing_mod.split(acs.frame)

    # Penetration estimates (heuristic from national take rates). Runs after
    # the BDC block so we have providers_block populated.
    provider_subs_rows: list[dict[str, Any]] = []
    market_subs_anchor_dict: dict[str, Any] | None = None
    tract_subs_rows: list[dict[str, Any]] = []
    ias_note: str | None = None

    # Compute per-tract ACP density first so the penetration estimator can
    # use it as a take-rate covariate. Silent when no ACP file present.
    acp_density_rows: list[dict[str, Any]] = []
    market_acp_density: float | None = None
    try:
        from .data import acp as acp_mod
        housing_by_tract = {
            str(r.get("geoid") or ""): int(float(r.get("housing_units_total") or 0))
            for r in (acs.frame.to_dicts() if not acs.frame.is_empty() else [])
            if r.get("geoid")
        }
        density_rows = acp_mod.acp_density_for_tracts(geoids, housing_by_tract)
        if density_rows:
            acp_density_rows = [
                {
                    "tract_geoid": d.tract_geoid,
                    "allocated_households": d.allocated_households,
                    "density": d.density,
                }
                for d in density_rows
            ]
            # Market-level density = total allocated households / total HU
            total_allocated = sum(d.allocated_households for d in density_rows)
            total_hu = sum(
                housing_by_tract.get(d.tract_geoid, 0) for d in density_rows
            )
            if total_hu > 0:
                market_acp_density = total_allocated / total_hu
    except Exception as exc:  # noqa: BLE001
        log.warning("ACP density skipped: %s", exc)

    if providers_block:
        market_ctx = penetration_mod.MarketContext(
            city_state=f"{res.city_name}, {res.state}",
            state=res.state,
            metros=(),  # Phase 6b will populate from TIGER metro-area lookup
        )
        estimates = penetration_mod.estimate_all(
            providers_block,
            market_context=market_ctx,
            market_acp_density=market_acp_density,
        )

        # Try to anchor with FCC IAS tract-level subscription data.
        if not no_ias:
            try:
                _emit("Loading FCC IAS subscription density for calibration...")
                ias_result = fcc_ias.load_tract_subs(geoids)
                if not ias_result.frame.is_empty():
                    tract_subs_rows = ias_result.frame.to_dicts()
                    anchor = penetration_mod.market_subscription_anchor(
                        tract_subs_rows,
                        tract_acs=acs.frame.to_dicts(),
                        ias_release=ias_result.as_of,
                    )
                    if anchor is not None:
                        market_subs_anchor_dict = asdict(anchor)
                        estimates = penetration_mod.calibrate_with_ias(
                            estimates, anchor
                        )
                    else:
                        ias_note = (
                            f"IAS data loaded ({ias_result.as_of}) but no overlap "
                            "with market tracts; using heuristic estimates as-is."
                        )
                else:
                    ias_note = (
                        f"IAS release {ias_result.as_of} has no data for "
                        "the analysis tracts; using heuristic estimates as-is."
                    )
            except Exception as exc:  # noqa: BLE001
                log.exception("FCC IAS load failed")
                ias_note = f"IAS load failed: {exc}"
        else:
            ias_note = "IAS lookup skipped (no_ias=True)."

        provider_subs_rows = [asdict(e) for e in estimates]

    # Phase 6e — Historical IAS take-rate trajectory (market-level only).
    # Opt-in because cold-fetching ~17 release ZIPs is multi-minute on first
    # call, then becomes near-instant once cached.
    market_subscription_history_rows: list[dict[str, Any]] = []
    subs_history_note: str | None = None
    if include_subs_history:
        if not geoids:
            subs_history_note = "No tracts resolved — subscription history skipped."
        else:
            try:
                _emit("Building historical IAS subscription trajectory (~17 releases)...")
                points = fcc_ias.market_subscription_history(geoids)
                market_subscription_history_rows = [asdict(p) for p in points]
                if not market_subscription_history_rows:
                    subs_history_note = (
                        "No IAS releases available — drop ZIPs into data/raw/ias/ "
                        "or check network access to fcc.gov."
                    )
            except Exception as exc:  # noqa: BLE001
                log.exception("Historical IAS load failed")
                subs_history_note = f"Subscription history failed: {exc}"
    else:
        subs_history_note = "Subscription history skipped (include_subs_history=False)."

    # Provider expansion velocity (12-month coverage delta) — opt-in.
    provider_velocity_rows: list[dict[str, Any]] = []
    velocity_note: str | None = None
    if include_velocity:
        if providers_block and bdc_release:
            try:
                _emit("Resolving previous BDC release (~12mo prior)...")
                prev_release = fcc_bdc.previous_release(bdc_release, months_back=12)
                _emit(
                    f"Downloading / loading prior BDC release {prev_release} "
                    f"for {state} (~5 min on first cold lookup)..."
                )
                prev_coverage = fcc_bdc.coverage_matrix(geoids, as_of=prev_release)
                prev_providers = competitors_mod.score(prev_coverage, n_tracts=len(geoids))
                velocity = velocity_mod.compute(
                    providers_block, prev_providers,
                    current_release=bdc_release, prev_release=prev_release,
                )
                provider_velocity_rows = [asdict(v) for v in velocity]
            except Exception as exc:  # noqa: BLE001
                log.exception("BDC velocity computation failed")
                velocity_note = f"Velocity computation failed: {exc}"
        else:
            velocity_note = "include_velocity requested but no BDC providers loaded."
    else:
        velocity_note = "Velocity skipped (include_velocity=False)."

    # Multi-release trajectory (4 BDC releases stepping ~6mo apart) — opt-in.
    provider_trajectory_rows: list[dict[str, Any]] = []
    trajectory_note: str | None = None
    if include_trajectory:
        if providers_block and bdc_release:
            try:
                _emit(
                    f"Resolving {trajectory_points} BDC releases for trajectory..."
                )
                releases = fcc_bdc.trajectory_releases(
                    bdc_release, n_points=trajectory_points, months_step=6
                )
                snapshots: list[tuple[str, list[ProviderSummary]]] = []
                for rel in releases:
                    if rel == bdc_release:
                        snapshots.append((rel, providers_block))
                        continue
                    _emit(f"Loading BDC release {rel} for trajectory...")
                    snap_cov = fcc_bdc.coverage_matrix(geoids, as_of=rel)
                    snap_providers = competitors_mod.score(snap_cov, n_tracts=len(geoids))
                    snapshots.append((rel, snap_providers))
                trajectory = trajectory_mod.compute(snapshots)
                provider_trajectory_rows = [
                    {
                        "canonical_name": t.canonical_name,
                        "technology": t.technology,
                        "tech_code": t.tech_code,
                        "series": [
                            {"release": pt.release, "locations": pt.locations}
                            for pt in t.series
                        ],
                    }
                    for t in trajectory
                ]
            except Exception as exc:  # noqa: BLE001
                log.exception("BDC trajectory computation failed")
                trajectory_note = f"Trajectory computation failed: {exc}"
        else:
            trajectory_note = "include_trajectory requested but no BDC providers loaded."
    else:
        trajectory_note = "Trajectory skipped (include_trajectory=False)."

    tract_acs = acs.frame.to_dicts() if not acs.frame.is_empty() else []

    # (Ookla measured speeds are fetched concurrently with ACS + BDC at
    # the top of the function — see `_phase_ookla`. `tract_speeds` and
    # `speeds_note` are set there.)

    # Google Places ratings (optional; needs GOOGLE_PLACES_KEY)
    provider_ratings: dict[str, dict[str, Any] | None] = {}
    ratings_note: str | None = None
    if not no_ratings and providers_block:
        if settings.google_places_key:
            try:
                _emit("Looking up Google ratings for providers...")
                market_label = f"{res.city_name}, {res.state}"
                names = [p.canonical_name for p in providers_block]
                # Skip ultra-thin / unknown providers to conserve quota.
                names = [n for n in names if n and n != "Unknown"]
                ratings = google_places.batch_get_ratings(names, market_label)
                provider_ratings = {
                    name: (r.to_dict() if r is not None else None)
                    for name, r in ratings.items()
                }
            except Exception as exc:  # noqa: BLE001
                log.exception("Google Places batch failed")
                ratings_note = f"Google Places lookup failed: {exc}"
        else:
            ratings_note = (
                "GOOGLE_PLACES_KEY not set in .env; ratings skipped. "
                "Get a key at https://console.cloud.google.com/apis/credentials."
            )
    elif no_ratings:
        ratings_note = "Google ratings skipped (no_ratings=True)."

    _emit("Assembling tear-sheet...")

    return TearSheet(
        market={
            "city": res.city_name,
            "state": res.state,
            "place_geoid": res.place_geoid,
            "state_fips": res.state_fips,
        },
        tracts={
            "inside_city": res.geoids,
            "boundary": res.boundary_tract_geoids,
            "included_in_analysis": geoids,
        },
        demographics=metrics,
        housing=housing,
        tract_acs=tract_acs,
        coverage_matrix=coverage_rows,
        location_availability=location_avail_rows,
        providers=providers_block,
        providers_note=providers_note,
        provider_subs=provider_subs_rows,
        market_subs_anchor=market_subs_anchor_dict,
        tract_subs=tract_subs_rows,
        ias_note=ias_note,
        provider_velocity=provider_velocity_rows,
        velocity_note=velocity_note,
        provider_trajectory=provider_trajectory_rows,
        trajectory_note=trajectory_note,
        acp_density=acp_density_rows,
        market_acp_density=market_acp_density,
        market_subscription_history=market_subscription_history_rows,
        subs_history_note=subs_history_note,
        tract_speeds=tract_speeds,
        speeds_note=speeds_note,
        provider_ratings=provider_ratings,
        ratings_note=ratings_note,
        data_versions={
            "tiger": tiger.TIGER_YEAR,
            "acs5": acs.vintage,
            "bdc": bdc_release,
            "ookla": "latest" if tract_speeds else None,
            "google_places": "latest" if provider_ratings else None,
            "ias": market_subs_anchor_dict.get("ias_release") if market_subs_anchor_dict else None,
        },
    )
