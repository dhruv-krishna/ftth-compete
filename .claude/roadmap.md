# Roadmap

Single source of truth for phase status, backlog, and deferred work. Update this file when phases ship or scope shifts. The original approved plan lives at `C:\Users\dkrishn3\.claude\plans\want-you-to-think-playful-cosmos.md` — this roadmap supersedes its phase order based on what we've actually learned.

## Status legend

- ✅ Done
- 🚧 In progress
- ⏳ Queued (next up)
- ⏪ Deferred (tracked here so we don't lose it; reason captured)
- 💭 Future / post-v1

## Phase status

### ✅ Phase 0 — Bootstrap
Repo skeleton, uv venv (outside OneDrive), pyproject, lint, pytest scaffold, `.claude/` doc seeds, Makefile, `.env` template. **Acceptance met:** 38 tests green on stubs.

### ✅ Phase 1 — Data pipeline
TIGER tract resolver, Census ACS5 client, FCC BDC API client + Parquet/DuckDB query, provider canonicalization, basic analysis (market, housing, competitors). **Acceptance met:** `ftth-compete market "Evans, CO"` returns a 12-provider tear-sheet end-to-end in ~90s cold / ~3s warm. 82 tests green.

### ✅ Phase 3a — Streamlit Overview tab
KPI cards (8: pop, MFI, poverty, housing units, MDU share, providers, has-fiber, boundary tracts), generated narrative, tract-detail expander, measured-speed strip when Ookla data loaded. Reuses pipeline.run_market(), same source of truth as CLI.

### ✅ Phase 3b — Streamlit Competitors tab
Provider card grid with category badges, tech badges, coverage progress bar, max advertised speeds, locations served. Sortable by 5 criteria. Filter by category + fiber-only toggle. Sortable table view alternative.

### ✅ Phase 3c — Streamlit Housing tab
Summary metrics with national-avg deltas. Plotly horizontal bar chart of all 10 B25024 unit-type buckets, color-coded by SFH/MDU/other group. Per-tract drilldown table with sortable MDU share progress column.

### ✅ Phase 4a — Streamlit Map tab
Folium choropleth with 8 selectable layers (fiber providers, all providers, measured down/up, latency, MDU share, poverty rate, median income). Click any tract → detail card below with full provider table, demographics, and measured speeds. Boundary tracts toggle (dashed gray outlines).

### ✅ Phase 4b — Ookla measured speeds
`data/ookla.py` queries AWS Open Data Registry parquet via DuckDB httpfs (~8s warm). Bbox-filtered, then spatial-joined to tract polygons via GeoPandas. Per-tract median down/up/latency + sample count. Latest release auto-detected. Wired into pipeline → TearSheet → Map tab + Overview tab + tract detail. Attribution rendered in app footer.

### ✅ Phase 6 — Google ratings
`data/google_places.py` with `get_rating()` / `batch_get_ratings()`. Calls Places API (New) Text Search + Place Details with FieldMask limited to `rating` + `userRatingCount`. 30-day TTL cache on both the place_id lookup and the rating itself. Wired into pipeline → TearSheet → competitor cards (color-coded badge with star count) and table view. Gracefully skips when `GOOGLE_PLACES_KEY` unset, surfacing a note. Sidebar toggle ("Skip Google ratings") to conserve quota on demand.

### ✅ PDF tear-sheet export
`src/ftth_compete/export.py` with `build_tearsheet_pdf(sheet) -> bytes`. ReportLab Platypus, single-page Letter portrait. Title block, 8-cell KPI grid, market narrative, top-12 providers table (zebra-striped, fiber-first), measured-speeds line, footer with data versions and Ookla attribution. Sidebar download button serves the PDF directly via `st.download_button`. Verified output ~4KB for Evans, CO.

### ⏳ analysis/speeds.py — advertised-vs-measured gap
Currently we show advertised (BDC) and measured (Ookla) speeds side-by-side at the tract level, but not joined per-provider — Ookla's open data parquet doesn't include `provider_name` at the tile aggregate level. Two paths to provider attribution:
1. Use Ookla's per-test data (different, larger dataset) and aggregate ourselves.
2. Heuristic: assume the tract-median measured speed reflects the dominant fiber provider's actual delivery, attribute back to whichever provider has the highest advertised speed in that tract.
Defer until Phase 6 or later — current tract-level signal already tells most of the story.

### ✅ Provider expansion velocity (year-over-year coverage delta)
`analysis/velocity.py` with `compute()` over two `ProviderSummary` lists. `data/fcc_bdc.previous_release(months_back=12)` resolves the year-prior published release. Pipeline opt-in via `include_velocity` flag (sidebar checkbox + CLI `--include-velocity`) — default OFF because it triggers an additional state-level BDC ingest (~5 min cold). UI: green/orange/red badges on Competitor cards (`+3,318 / +33180% YoY`), `🆕 NEW` for new offerings, table columns (`12mo Δ locations`, `12mo Δ %`). Narrative gets a year-over-year sentence highlighting top fiber grower. **Overview tab adds a "12-month fiber footprint change" panel with top 3 expanders + top 3 decliners** when velocity is loaded. Verified for Evans CO: **Allo went from 10 → 3,328 fiber locations in 12 months; Vero Fiber doubled.**

### 🚧 UI migration: Streamlit → Reflex
Streamlit's notebook-y look caps at "polished prototype." Migrating to **Reflex** (pure Python, compiles to Next.js/React under the hood, Apache 2.0 / free, self-hostable). The Streamlit app at `src/ftth_compete/ui/` stays the source of truth until the Reflex port reaches parity. Pipeline / analysis / data layers are untouched — only the UI changes.

**Phase status:**
- ✅ **Phase 1 — Bootstrap.** `reflex>=0.7` added to deps. `rxconfig.py` at repo root with Tailwind plugin enabled. Sibling top-level package `ftth_compete_web/` (parallels `src/ftth_compete/`) with `ftth_compete_web.py` containing the app shell: branded sidebar (logo, market form with quick-pick + presets, advanced options accordion, look-up button, strategic lens placeholder), top tab bar (6 tabs), per-tab placeholder content, empty state when no lookup loaded, sticky theme toggle, Ookla attribution footer. **Windows / OneDrive gotchas captured:** Reflex 0.9 dropped auto-generated state setters (must define `@rx.event def set_*`); `App(theme=...)` deprecated → moved to `RadixThemesPlugin` in rxconfig; npm's optional-deps bug drops `@rolldown/binding-win32-x64-msvc` on Windows installs (fix: download the tarball directly from npmjs and extract with `tar`); OneDrive cloud-virtualizes `.node` binaries → `.web/` should be a junction to `C:\Users\dkrishn3\ftth-compete-web\` outside OneDrive.
- ✅ **Phase 2 — State + Overview tab.** `LookupState.run_lookup` is now an `@rx.event(background=True)` async handler that snapshots the form inputs, calls `pipeline.run_market()` via `asyncio.to_thread` (so the 90s cold lookup doesn't block the event loop), then re-enters state context to populate ~40 typed result fields. `_populate_from_sheet()` flattens the TearSheet into Reflex-friendly primitives (no nested Optional types) and pre-computes Overview-specific things like the top-3 fiber growers/decliners. The Overview tab itself is fully ported: subtitle line (tracts count, boundary), 4×2 KPI grid (population / MFI / poverty / housing units, then MDU share / providers / fiber available / boundary tracts), 5-col BSL availability strip, 4-col IAS subscription anchor, 4-col Ookla measured-speeds strip, 12-month velocity highlights (top 3 expanders + top 3 decliners with NEW / Discontinued badges), market-opportunity card (rendered only when offensive lens active), narrative card, collapsible tract-details accordion, providers-note callout. Loading state uses `rx.spinner` with the placeholder progress label.
- ✅ **Phase 3 — Competitors tab + polish.** `LookupState` gained ~25 new fields for Competitors: `providers_data` (one dict per (canonical, tech) with all display strings pre-baked — coverage, locations, max-down, speed-tier %, rating stars, est-subs range, velocity badge text, trajectory SVG sparkline string), `ratings_data` (lens scoring input), `visible_providers` (filtered/sorted view), filter state (`cp_sort_key`, `cp_categories_csv`, `cp_fiber_only`, `cp_view`), summary-strip counts, and lens UI state (`incumbent_options`, `lens_banner_kind`). `_recompute_visible_providers()` is the single re-derivation entry point — called on every filter / lens / incumbent change. It reconstructs minimal `ProviderSummary` instances from the stored dicts so `analysis.lenses.apply()` can re-score without re-fetching the TearSheet. **UI:** color-coded lens banner (defensive/offensive/missing-incumbent), polished controls row (sort dropdown, multi-select category chips, fiber-only checkbox), summary KPI strip, segmented control for Cards/Table view. **Provider cards:** sharp 3-column grid, name × tech header + holding company + lens-score badge, category & tech badges, full-width accent coverage bar, 2-col metrics (max-down / locations), 3-segment speed-tier mini-bar (gig+/100Mbps+/<100), color-coded star rating row, subs estimate with confidence badge, velocity badge, inline SVG trajectory sparkline with caption. **Table view:** Radix surface table with 8 columns. **Sidebar:** incumbent dropdown auto-shows when defensive lens is picked; auto-defaults to local cable provider. **Polish pass:** consistent uppercase section headers with letter-spacing, KPI cards gained subtle accent-7 hover border + info-icon hint when help text exists + spaced label/value rhythm, empty-state landing screen with brand-icon halo + one-click "Try Evans, CO" demo button.
- ✅ **Phase 4 — Housing / Map / Compare / Methodology.** **Housing tab:** 4-card summary strip (SFH share + national delta, MDU share + national delta, mobile/other, total housing units), MDU sub-row (small/mid/large), full 10-bucket B25024 horizontal bar chart with per-group colors (blue SFH / orange MDU / gray other) and bars sized by share of max, per-tract table (geoid · pop · housing · SFH · MDU · other · MDU-share progress bar) sorted by MDU share desc. **Methodology tab:** rendered via `rx.markdown` — Intro, data-sources table, metric definitions (market scale, competitive landscape, BSL availability, speed-tier breakdown), penetration estimation with limits + improvement roadmap, velocity + measured speeds, strategic lenses, limitations, attributions. 900px max-width column for readability. **Compare tab:** "Save current market" + "Clear" buttons; tablular Radix surface with one row per saved market showing population / MFI / MDU share / providers / fiber available / IAS take rate; per-row `x` remove button. **Map tab:** Folium choropleth rendered server-side (off-thread via `asyncio.to_thread`); HTML returned and embedded via `rx.el.iframe(src_doc=...)`. Layer dropdown (10 layers; Phase 4 implements fiber-providers-per-tract, rest fall through). Refresh button + spinner during render.
- ⏳ **Phase 5 — Retire Streamlit.** Once Reflex hits user-approved parity, delete `src/ftth_compete/ui/` and the `streamlit` + `streamlit-folium` deps.

### 🚧 Phase 6 — True penetration (free-data layered approach)

Research brief lives at [penetration_research.md](penetration_research.md) — inventories every free data source and the layered confidence resolution we'll use. Current state: per-(provider, tech) penetration estimates use national 10-K take rate × BDC locations, calibrated by IAS market anchor. Gaps: no per-provider truth, no sub-state granularity.

**Phase status:**

- ✅ **Phase 6a — Market-level anchor registry.** New `MarketLevelAnchor` dataclass + `MARKET_LEVEL_ANCHORS` curated list in `analysis/penetration.py` (seed entries for EPB Chattanooga, Allo Lincoln NE, Verizon Fios NY/NJ, Frontier Fiber CT). New `MarketContext` dataclass carries city/state/metro labels through the pipeline. New `find_anchor()` resolves anchors in priority order: city > metro > state > national. `estimate_market_subs()` accepts `market_context=` kwarg and gains a 3rd resolution branch: when an anchor matches, the implied take rate (`subscribers / locations_passed`) is applied, confidence is bumped to **high**, range tightened from ±25% to ±12.5%, and the estimate is clamped to never exceed the disclosed anchor total. Pipeline auto-builds `MarketContext` from the TIGER `CityResolution` output. **6 new tests** in `test_analysis_penetration.py` cover registry shape, lookup priority order, anchor caps, and confidence promotion. Reflex Competitors card automatically renders the high-confidence badge (color map was already in place).
- ✅ **Map legend + multi-layer rendering.** New `LookupState.tract_values` field: a per-tract scalar lookup keyed by GEOID with one value per map layer, pre-computed in `_populate_from_sheet` from `sheet.tract_acs`, `sheet.tract_speeds`, `sheet.location_availability`, and `sheet.coverage_matrix`. `_render_folium_map` rewritten to read this dict — eliminates the previous re-runs of `run_market()` per map render. All 10 dropdown layers now produce data-driven choropleths: fiber-providers-per-tract, total-providers-per-tract, fiber/cable availability %, measured down/up/latency, MDU share, poverty rate, median HH income. **Color-coded legends** via `branca.colormap.LinearColormap` with per-layer palettes (blue for counts, green for desirable metrics, red-reversed for latency / poverty, purple for MDU, orange for cable). Hover tooltips show the formatted value per tract. Top-left title overlay names the market + active layer. Layers without source data (e.g. measured speeds when Ookla wasn't loaded) render a friendly "this layer needs Ookla data" placeholder. **Geometry bug** in the centroid computation fixed by using `gpd.GeoSeries(...).to_crs(epsg=4326).iloc[0]` instead of `polys_proj.set_geometry([center])` (which was a row-count mismatch).

- ✅ **Round 2 anchor research.** `MARKET_LEVEL_ANCHORS` grew from 37 to **66 entries**. New additions: Xfinity major metros (Philadelphia/Boston/Chicago/Seattle/Atlanta), Spectrum (LA/NYC + TX/FL/OH state-level), AT&T Fiber state-level (CA/GA) + additional metros (LA/Miami/Nashville), Ziply Fiber (WA/OR), MetroNet (IN/MI), GoNetspeed (CT), Brightspeed (NC), Astound Broadband metros (Chicago/Boston/Seattle), Optimum Long Island + NJ, Verizon Fios DE. Also added **Astound Broadband** (RCN/Wave/Grande/enTouch consolidation under Stonepeak) and **Cable One** (Sparklight brand) to the provider canonicalization registry so the new anchor entries actually match BDC data. New national take rates added for both.

- ✅ **Phase 6b — Expanded anchor research seed batch.** `MARKET_LEVEL_ANCHORS` grew from 5 to 37 entries spanning EPB Chattanooga + Allo's three CO/NE cities + Verizon Fios state-by-state breakdown (NY/NJ/PA/MA/VA/MD/DC/RI) + Frontier Fiber state-by-state (CT/CA/FL/TX/IN) + AT&T Fiber metros (DFW/Austin/Atlanta/Houston/San Antonio) + Google Fiber metros (KC/Austin/Provo/SLC/Huntsville) + Cox metros (Phoenix/San Diego/Las Vegas/Omaha) + Optimum (CT/St. Louis) + Lumen/Quantum Fiber metros (Denver/Phoenix/Las Vegas). Source citation discipline: each entry labels itself "Direct disclosure" (from a specific 10-K table or press release) vs "Estimate" (footprint-pro-rata allocation of the disclosed national total). Verification recommended quarterly for material providers.
- ✅ **Phase 6c — USAC ACP/EBB density covariate.** **Important pivot:** verified May 2026 that USAC's public ACP/EBB data has **no provider-level breakdowns** — only ZIP/county/state geographic totals. We can't do direct provider × tract subscriber allocation. Instead, ACP becomes a **demand-side covariate** that modifies each provider's take rate based on the market's low-income broadband density × that provider's known national ACP capture share. Components: `src/ftth_compete/data/acp.py` parser handles real USAC Excel format (auto-detects header row, drops description text + Grand Total footer + redacted "00000" aggregate), `find_acp_zip_file()` discovers files in `data/raw/acp/`, `acp_density_for_tracts()` cross-walks ZIP-level enrollment to tracts via Census ZCTA crosswalk (~30MB, downloaded once), `NATIONAL_ACP_CAPTURE_SHARE` table covers ~30 major providers (Xfinity 40%, Spectrum 25%, AT&T 6%, Cox 5%, Verizon Fios 3%, fiber overbuilders ~0.1% because their plans exceeded the $30 cap). `analysis/penetration.py` `estimate_market_subs(..., market_acp_density=)` applies a bounded `[0.8, 1.3]` modifier: `1 + 5.0 × provider_share × (market_density - 0.18)` where 0.18 is the peak ACP enrollment baseline. Pipeline auto-computes density per lookup (silently skipped when no ACP file present). `TearSheet` gains `acp_density: list[dict]` + `market_acp_density: float | None`. Reflex UI gains Overview "ACP enrollment density" panel + new "ACP enrollment density %" map layer (amber palette). **Verified on Evans CO** — 5-9% per tract, market avg ~7%, drives a slight take-rate decrease for Comcast there (below the national baseline) and a tiny boost for fiber overbuilders by relative comparison. **End-to-end working with the user's EBB Dec 2021 file (~9M households, 30,181 ZIPs).** `openpyxl` added to environment for Excel parsing.
- ✅ **Phase 6d — M-Lab BigQuery scaffolding (pivoted from Ookla).** **Correction:** Ookla's per-test data with `provider_name` is their paid commercial product (Speedtest Intelligence) — not part of the free Ookla open-data bucket. The actual free path to per-test, provider-attributable speed measurements is **M-Lab** (Google / Internet Society / Princeton NDT speed test, published to free BigQuery public dataset `measurement-lab.ndt.unified`). New module `src/ftth_compete/data/mlab.py` with: `ASN_TO_CANONICAL` registry mapping ~30 major US broadband ASNs → canonical provider names (Comcast 7922, Charter 20115/11427/..., Cox 22773, AT&T 7018, Verizon 701-703, Lumen 209/3356, T-Mobile 21928, etc.), `asn_to_provider()` lookup, `MLabTest` per-test record, `MLabProviderTractStats` aggregated output, `_build_query()` BigQuery SQL composer (with `_TABLE_SUFFIX` partition filter + bbox), `fetch_tests_for_bbox()` stub, `shares_from_tests()` aggregator stub. Real implementation requires `google-cloud-bigquery` install + `GOOGLE_APPLICATION_CREDENTIALS` setup. BigQuery free tier (1 TB/month) is sufficient for one query per market per quarter with aggressive caching.
- 🚧 **Phase 6e — Historical IAS / 477 subscription archive (market-level only).** **Scope pivoted May 2026** after research confirmed FCC's 477 microdata with provider IDs is filed under Q8 confidentiality (state PUCs only, under NDA). Public tract CSVs are pre-aggregated to *connections per 1,000 households* with NO provider identifiers — same dataset that already powers `data/fcc_ias.py`. **Revised scope:** ingest historical IAS releases 2014→2024 to produce **market-level take-rate trajectories** ("Evans CO grew 28% → 71% subscription over 8 years"). Real penetration-trend signal without per-provider attribution (which simply isn't public). Subtasks P1.2-P1.9 in TodoWrite. Touchpoints:
  - `data/fcc_ias.py` — historical release index + downloader + Parquet partition by release date
  - Schema normalizer (column casing / tier names drift across 2014-2024 releases)
  - `pipeline.TearSheet.market_subscription_history: list[dict]` new field
  - UI: "Take-rate trajectory" chart on Overview tab
- ⏳ **Phase 6f — State broadband data plug-ins.** Folded into Phase 7 — see P2.10.

Run with `uv run reflex run` (hot-reload dev on `localhost:3000`).

### ⏳ Phase 7 — Batch market screener (post-MVP)

Scales the tool from "look up one market" to "rank 50-500 markets by lens score". Filter by state(s) + MFI + MDU + fiber-availability ranges, get back a sortable table of candidates with per-row drill-into-v2. Subtasks tracked in TodoWrite as P2.1-P2.10. Key design points:

- New `/screener` route, lean `run_market_for_screener()` variant (no Ookla, no Places — keeps batch latency manageable)
- Disk cache: `data/processed/screener/<release>/<state>/<city>.parquet` keyed on (market_id, BDC release) so re-runs are instant
- Worker pool: 4-8 concurrent lookups, throttled to respect Census API rate limits
- CSV export of full ranked results
- CA PUC + NY PSC plug-ins (originally Phase 6f) folded in — they enrich the per-state subscriber data for batch runs

### ⏳ Phase 8 — Provider-centric view (post-screener)

Flips the perspective from market-first to provider-first. Pick Allo / Comcast / Verizon Fios → see every market they're in, footprint trajectory, head-to-head competitor overlap, aggregated speed/rating signals. Subtasks tracked in TodoWrite as P3.1-P3.9. Key design points:

- Routes `/providers` (directory) + `/provider/<canonical>` (detail)
- Cross-market aggregation reads all cached BDC parquets — no new fetches
- National footprint map: top-N markets where they serve
- Trajectory chart: tracts served per BDC release across the whole portfolio
- Head-to-head overlap matrix: for top-5 competitors, % of provider's tracts also served by each
- Subscriber growth chart (depends on Phase 6e / 477 history landing)

### ✅ Speed tier breakdown per (provider, tech)
`fcc_bdc.coverage_matrix` SQL now emits `gig_locations` / `hundred_locations` / `sub_hundred_locations` alongside the total distinct-location count, derived from `max_advertised_download_speed` thresholds at 1 Gbps / 100 Mbps. `competitors.score()` aggregates these into new `ProviderSummary.gig_locations` / `hundred_locations` / `sub_hundred_locations` fields (default 0 for back-compat with old fixtures). Competitor cards render a thin 3-segment stacked SVG bar (green/blue/gray) with a legend showing the per-tier share; table view adds sortable `Gig+ %` / `100Mbps+ %` progress columns. Answers "what % of a provider's locations get gigabit?" — material for the "advertised speed quality" story per provider, not just the market max.

### ✅ Multi-release BDC trajectory (sparklines)
`fcc_bdc.trajectory_releases(current, n_points=4, months_step=6)` walks the BDC release calendar and returns up to N published releases stepping ~6 months back (covers ~2 years given the biannual BDC cadence). New `analysis/trajectory.py` with `ProviderTrajectory`/`TrajectoryPoint` and `compute()` builds per-(canonical, tech) zero-filled time series from a list of `(release, ProviderSummary[])` snapshots. Pipeline opt-in via `include_trajectory: bool` (CLI `--include-trajectory`, sidebar "Include multi-release trajectory (sparklines)" checkbox); fetches 4 BDC state-level parquets (~15-20 min cold first time). Competitor cards render an inline SVG polyline sparkline with endpoint dots + a "{first_release} → {last_release} · {first_locs:,} → {last_locs:,} locations" caption when trajectory data is loaded. Subsumes the velocity story with richer "how fast is overbuild happening?" detail.

### ✅ Cross-state metro markets (KC across MO/KS)
New `tiger._METRO_ALIASES` table maps `(label_lower, primary_state) -> [(component_city, component_state), ...]`. `_resolve_via_metro()` is checked first in `city_to_tracts`, falling through to the existing PLACE path then borough fallback. Each component is resolved independently and the tract lists merged with dedupe. `tiger.tract_polygons()` now infers state(s) from GEOID prefixes (state arg optional) so downstream callers can pass multi-state GEOIDs without code changes — the BDC `coverage_matrix` / `location_availability` already handled per-state ingest. Initial entry: `Kansas City Metro, MO` (or KS, or `kc metro`) → Kansas City MO + Kansas City KS. Sidebar quick-pick gains a "Kansas City Metro, MO" preset. Add more cross-state metros to `_METRO_ALIASES` as needed.

### ✅ Polish pass (logging hygiene + methodology page + help text + layout)

- **Logging hygiene:** `_install_log_redactor()` in `ftth_compete/__init__.py` installs a `LogRecordFactory` that scrubs sensitive query params (`key=`, `api_key=`, `hash_value=`, `token=`, `access_token=`) from every log record at creation. Census API keys no longer leak into httpx INFO logs / terminal output. 4 redaction tests.
- **Methodology tab:** new 6th tab `📖 Methodology` ([src/ftth_compete/ui/tabs/methodology.py](src/ftth_compete/ui/tabs/methodology.py)) — static documentation page covering data sources, every Overview/Competitors/Map metric, penetration math, velocity, lens scoring, limitations, attributions, and refresh procedures. Linked from KPI tooltips throughout the app.
- **Help text audit:** tightened help= on KPIs in Overview, Competitors, Map, sidebar — every metric now has a clear "what / source / caveat" tooltip; multi-line where useful.
- **Layout tightening:** `st.divider()` separators between Overview sections (KPIs → location-availability → IAS → measured-speeds → velocity → opportunity → snapshot), smaller `####` card headers in Competitors (less visual overwhelm in 3-col grid), sidebar uses `st.divider()` to group form / lens / comparison / export / config blocks.

### ✅ Per-fiber-provider map layer
Map dropdown dynamically gains "Fiber footprint: X" entries for each fiber provider in the current market (Evans CO has 5: Allo, Lumen / Quantum Fiber, Vero Fiber, Xfinity, Zayo). Each renders a binary 0/1 mask — lit tracts are served by that provider, dim tracts aren't. Lets you flip between providers to see footprint overlap. Shares the same click-tract-for-detail UX as the static layers.

### ✅ Provider drill-down (per-tract detail)
Each Competitor card has an expander "Per-tract detail (N tracts)" showing per-tract location counts, max advertised down/up, and a flag if speed varies across tracts. Uses raw BDC coverage matrix data already on the TearSheet — no new fetch.

### ✅ NYC borough alias (multi-place markets)
`tiger._BOROUGH_ALIASES` maps Brooklyn/Manhattan/Queens/Bronx/Staten Island to county FIPS codes. Resolver tries PLACE first, falls back to county-prefix tract filter. Verified for Brooklyn: 805 tracts, 2.63M pop, 86.6% MDU.

### ✅ Per-(provider, tech) split + household fiber availability
`analysis/competitors.score()` now groups by `(canonical_name, technology)` so multi-tech providers (Lumen DSL+Fiber, Comcast Cable+Fiber) split into honest separate rows. New `data/fcc_bdc.location_availability()` query returns per-tract BSL-level availability by tech. New "Fiber available" KPI = % of locations with fiber from any provider (79.1% in Evans, vs misleading 41.7% provider-count metric). 5-tech KPI strip (Fiber/Cable/DSL/FW/Sat). 2 new map layers for fiber & cable availability %.

### ⏪ Phase 2 — Core analysis
**Mostly done in flight.** market.py + housing.py + competitors.py all implemented during Phase 1. Outstanding: `analysis/speeds.py` (folds into Phase 4) and `analysis/penetration.py` (see Phase 5 deferral).

### ✅ Phase 5 — Penetration estimation (10-K heuristic + FCC IAS calibration)
`analysis/penetration.py` with `NATIONAL_TAKE_RATES` table (33 providers, 10-K-anchored 2024 numbers) + category-default fallbacks. Per-(provider, tech) `SubsEstimate` ranges (low/mid/high) with confidence labels.

**FCC IAS calibration (`data/fcc_ias.py`):** ingests tract-level subscription-density buckets (per 1,000 HH) from the public FCC IAS dataset. Auto-downloads Jun 2022 (latest direct ZIP); newer releases (Dec 2022+) are Box-hosted and require a manual drop into `data/raw/ias/`. `market_subscription_anchor()` converts the bucketed tract data + ACS housing units into a market-total subscriber range. `calibrate_with_ias()` scales heuristic per-provider estimates so their sum matches the IAS anchor — corrects the inherent double-counting where the heuristic treats overlapping fiber footprints independently. Confidence is bumped to "high" when calibrated; "medium"/"low" otherwise.

**Verified for Evans CO:** heuristic sum was ~11,000 subs (overcount). IAS Jun 2022 anchor: 6,913 mid (90% take rate at ≥25/3 across 7,681 housing units). After calibration: per-provider scaled by ~0.76 (Xfinity 2,807, Allo 1,144, Lumen DSL 1,121, Vero 773), sum = 6,912 — matches anchor exactly, proportions preserved.

**Public IAS data limits (worth knowing):** all-tech aggregate only at tract level (no per-tech breakdown), all-provider aggregate (no per-provider), bucketed values not raw counts, ~12-18 month lag. We use it as an aggregate calibration anchor — NOT as per-provider ground truth. Fiber-vs-cable per-provider penetration remains heuristic (10-K-anchored).

### ✅ Phase 7 — Lenses (defensive / offensive / neutral)
`analysis/lenses.py` with 3 lens scoring functions + `market_opportunity()` for entrant-side composite scoring. Sidebar reactive lens selector (no re-fetch). When defensive: incumbent dropdown auto-defaults to local cable provider. Competitors tab shows lens banner + per-card score badges (color-coded by intensity) + score progress column in table view. Overview tab shows the market-opportunity panel under offensive lens. Lens scoring respects Google ratings when available, falls back to neutral 3.5 default. 11 dedicated lens tests verify scoring math and edge cases.

### ✅ Phase 8 — Multi-market comparison
Sidebar "Save to comparison" + "Clear" buttons populate `st.session_state["saved_sheets"]`. New 5th tab "Compare (N)" renders side-by-side: KPI table with progress columns + opportunity score, grouped Plotly bar chart for normalized metric comparison, top-providers-per-market alignment, provider-overlap matrix sorted by ubiquity. Lens-aware: offensive lens ranks markets by entrant-opportunity score. Per-state caching means 2nd+ markets in the same state are near-instant.

### ✅ Loading UX + error handling
Pipeline emits 6 progress phases via `set_progress_callback`. Sidebar uses `st.status()` to show live phase updates during cold lookups (e.g. "Downloading BDC providers for CO (release 2025-06-30; ~90s on first state)..."). Bad-city `ValueError` is caught and rendered as a friendly error message naming the typed input + Quick-pick fallback, instead of a red Python traceback.

### 💭 Future (post-v1)
- LLM-generated narrative summary (currently deterministic templating).
- Time-series of BDC releases — provider footprint trajectory across last 4 quarters.
- BDC fabric integration via CostQuest Tier 4 research license — gives address-level outputs.
- Internal subscriber data integration (if available; Altice context).
- Custom incumbent picker for defensive lens (any provider, not just hard-coded).
- Streamlit Cloud deployment for shareable URLs (post-v1, requires keeping Ookla non-commercial).
- Browser screenshot generation as alternative to PDF.

## Polish / quality backlog

These aren't phase-gated — pick them up opportunistically. Listed here so we don't lose track.

### Data correctness
- **Multi-state markets.** NYC tracts span NY/NJ/CT (and some CO/UT borders are messy too). `tiger.city_to_tracts` currently assumes one state. Need to expand TIGER PLACE search across multiple states or use a different resolver (e.g., Census Geocoder API).
- **Boundary tract UX.** The 6 boundary tracts for Evans CO are 1.5× the "in-city" count. UI should surface them as a togglable layer with a clear count. CLI already has `--include-boundary` but defaults exclude.
- **Provider registry growth.** Each new market lookup surfaces 3-5 long-tail providers (regional fiber, WISPs, munis). Track `unknown` category counts in CI and add real entries as they accumulate.
- **Ookla provider-name matching.** Ookla's `provider_name` field is user-self-reported and noisy. Need a separate fuzzy normalizer mapping Ookla names → canonical provider entries before joining with BDC.
- **Multi-tech provider counts.** When Comcast files as `"40, 50"` it means it serves both Cable AND Fiber locations, but the same `location_id` may appear in both tech rows. Verify our `COUNT(DISTINCT location_id)` doesn't double-count.

### UX polish
- **Number formatting helpers.** Consistent `22,324` not `22324`; `$74,411` not `74410.69754279`; `13.5%` not `0.13466164389778967`. Build `format.py` with `fmt_int`, `fmt_currency`, `fmt_pct`. Use everywhere.
- **Generated narrative quality.** Current narrative is implicit (we don't even generate one yet). Build a deterministic templating system that handles missing data gracefully and reads natural.
- **Help text / tooltips.** Each KPI card should have a `help=` tooltip explaining the methodology (e.g., "Pop-weighted mean of tract medians; not exact market median").
- **Error states.** What does the dashboard show if Census API is down? If FCC creds are missing? If a market has zero tracts? Each needs a clear non-crash UI message.
- **Loading states.** Cold market lookup is ~90s. Streamlit's default spinner is fine but progress phases ("Resolving tracts → fetching ACS → downloading BDC...") would be better.
- **Boundary tract toggle in sidebar.** Include/exclude with count display.
- **Provider drill-down.** Click a provider card → see per-tract presence, raw brand names, file IDs. For debugging and trust.

### Performance
- **Streamlit cache strategy.** `@st.cache_data` on `pipeline.run_market` keyed by (city, state, options). `@st.cache_resource` for DuckDB connections. Cache versioning on data version changes.
- **HTTP caching headers.** TIGER files are large and don't change often within a year. Add `If-Modified-Since` checks before re-downloading.
- **Per-state BDC pre-warm.** Optional `make refresh-state STATE=CO` to download a state ahead of first market lookup.
- **Polars / DuckDB query review.** The `coverage_matrix` query loads the entire state parquet — fine for small states, may need pushdown for CA/TX.

### Hygiene
- **Logging cleanup.** httpx logs the full URL including the Census API key as a query param. Filter or redact. Same for any FCC API params.
- **Test coverage for network code.** `data/fcc_bdc.py`, `data/census_acs.py`, `data/tiger.py`, `data/google_places.py`, `data/ookla.py` all hit external APIs. Add `respx`-based mock tests for response parsing without actual network calls.
- **Pyproject `dependency-groups` vs `optional-dependencies`.** Currently both are defined; clean up to one.
- **Mypy strict pass.** Currently configured but not enforced. Several modules have `Any` slipping through.
- **Ruff format pass.** Ensure `uv run ruff format .` is clean.

### Analysis depth
- **Tract-level coverage stats.** Currently aggregated to "tracts_served" and "coverage_pct". Could surface per-tract coverage for the map's click popup.
- **Provider expansion velocity.** Compare BDC release N to release N-2 (1yr ago) — show new locations a provider has added. Powers the "12-month delta" KPI in the plan.
- **Speed tier breakdown.** Currently we report max advertised speeds. Could break down what % of locations get gigabit+ vs 100Mbps+ vs <100Mbps per provider.
- **Competitive density metric.** Per-tract count of fiber providers — already feasible from the matrix; surface as a heatmap layer.

### Documentation
- **README usage section.** Currently sparse. Add a "what does this output look like" section with screenshots once dashboard exists.
- **Methodology page in app.** A separate Streamlit page that explains every metric and its caveats. Linked from each KPI tooltip.
- **`make refresh` user-facing CLI.** Currently `pipelines/refresh_*.py` are stubs. Wire them so `make refresh` actually re-pulls all sources.

## How to use this roadmap

- **Before starting a phase**, mark it 🚧 with the date. When done, mark ✅.
- **Polish backlog items** can be picked up between phases or as part of a phase if they're naturally adjacent.
- **Deferred items** must keep their "why deferred" rationale current. If the rationale changes, move them out of deferred.
- **Future ideas** are aspirational — do not commit to them in the plan, but capture them so they're not forgotten.
- **When the user asks "what's next"**, this is the file to read.
