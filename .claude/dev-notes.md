# Dev notes — gotchas, debugging recipes, things that bit us

This is the running log of "we learned this the hard way." When something surprises you in a session, add it here.

## Setup gotchas

- **Repo lives in OneDrive, but `.venv` and bulk data do NOT.** Source/tests/docs sync via OneDrive — fine. The venv (thousands of small files) and the data dir (multi-GB parquet, SQLite caches) live outside OneDrive via two env vars:
  - `UV_PROJECT_ENVIRONMENT=C:\Users\dkrishn3\.venvs\ftth-compete` — uv reads this and creates the venv there instead of `./.venv`.
  - `FTTH_DATA_DIR=C:\Users\dkrishn3\ftth-compete-data` — `config.py` reads this and routes `data/raw`, `data/processed`, `cache.db` there.
  Both default to in-repo if unset, so a fresh clone still works (with degraded OneDrive performance).
- **uv first-time install** on Windows: if `uv` isn't on PATH after install, open a fresh PowerShell. The installer adds it but the current shell doesn't see it.

## Data source gotchas

- **FCC BDC bulk files are big.** Plan ~5GB raw download, ~1.5GB after parquet conversion. Partition by state to avoid loading the world.
- **FCC tech codes are NOT speed tiers.** Tech 50 = fiber regardless of speed. Speed comes from `max_advertised_download` and `max_advertised_upload` columns in BDC.
- **Provider IDs change very rarely** but provider *names* in BDC raw data drift (different LLC formations, mergers). Match on provider_id first.
- **Census API rate limits** are gentle but real (~500 calls/day per IP without a key). Always pass `&key=` from `.env`.
- **TIGER place geometries** can include water — use `MTFCC=G4110` filter for incorporated places.
- **Ookla tile attribution** — `provider_name` field exists but is user-self-reported. Treat as best-effort, not authoritative.
- **Google Places `rating` is Enterprise-tier.** As of Mar 2025, requesting `rating` or `userRatingCount` via FieldMask drops you into Enterprise pricing for the entire request. Cache aggressively.

## Code gotchas

- **GeoPandas requires GDAL/PROJ.** On Windows, install via `uv` works because uv ships pre-built wheels, but a manual pip install can fail. Always use `uv sync`.
- **Streamlit reruns the entire script on any widget interaction.** Use `@st.cache_data` and `@st.cache_resource` aggressively for expensive operations. Pipeline calls especially.
- **Folium maps inside Streamlit** need `streamlit-folium`'s `st_folium()` not `folium_static()` if we want click events.
- **Polars expression API is great** for joins but watch out for null handling — left joins default to keeping nulls, which can break downstream sums.
- **Avoid em-dashes (—) in CLI strings.** PowerShell on Windows often renders them as `�` due to console code page. Use ASCII `-` in `click.echo` and other terminal output. Markdown / Streamlit / docs are unaffected.
- **`uv run` + a stale `VIRTUAL_ENV` shell var** prints a benign warning each invocation. Harmless — uv ignores it because we set `UV_PROJECT_ENVIRONMENT`. Open a fresh PowerShell to silence.

## Pydantic-settings fixture pitfall

`monkeypatch.delenv("KEY")` does NOT prevent pydantic-settings from reading `.env`. If the dev's real `.env` has a key, tests that expect "no key set" will see the real one. Use `monkeypatch.setenv("KEY", "")` instead — env vars take precedence over `.env`.

## Corporate SSL inspection (AlticeUSA / Zscaler / similar)

Network calls fail with `[SSL: CERTIFICATE_VERIFY_FAILED]` because Python's `certifi` bundle lacks the corporate CA. The OS trust store has it (IT installs it). Fix is `truststore` — patches Python's `ssl` module to use the OS store. Loaded once in `ftth_compete/__init__.py` via `truststore.inject_into_ssl()`. Works for httpx, requests, urllib, and any other lib using stdlib `ssl`.

If a future session sees SSL errors despite truststore, check that the package import actually ran (anything that bypasses `import ftth_compete` will skip the patch).

## FCC BDC API gotchas (verified May 2026)

These are not in any single FCC doc but are essential to make the API actually work:

1. **Auth is plain headers, not HMAC.** Despite the misleading name, the `hash_value` header is the raw 44-char API token verbatim. No HMAC, no signing. Pair with `username` (your registered FCC email).

2. **`downloadFile` URL needs three path segments**: `/downloads/downloadFile/{data_type}/{file_id}/{file_type_num}`. Single-segment GETs (`/downloads/downloadFile/{file_id}`) return `405 Method Not Available`. For files from `listAvailabilityData`, use `data_type="availability"` and `file_type_num=1` (CSV variant).

3. **`technology_code` is a comma-separated string for multi-tech providers.** Comcast files Cable+Fiber as `"40, 50"`. Lumen/CenturyLink files DSL+Fiber as `"10, 50"`. T-Mobile files just `"71"`. Parse as a list, not an int. Filtering with `int(tech_code) in valid_codes` silently drops every multi-tech provider — including all the major incumbents.

4. **Latest release dates lag their published files by ~1 month.** The API's `/listAsOfDates` returns dates that have been declared but not yet had files uploaded (e.g., 2026-04-30 declared but empty). `latest_release(with_files=True)` walks newest-first until it finds a release whose `listAvailabilityData` is non-empty.

5. **Three subcategories per state per release.** "Location Coverage" = BSL-level CSVs (what we use for tract aggregation). "Hexagon Coverage" = H3 hex shapefiles. "Raw Coverage" = provider-submitted shapes. All three are "Provider" category.

6. **Per-state CO download is 117 fixed-broadband CSVs** in the 2025-06-30 release (after the multi-tech filter fix). End-to-end first-run is ~90 seconds at typical broadband speeds.

## Multi-state markets via the borough-alias table

NYC boroughs aren't TIGER PLACE entries (the only PLACE for those tracts is "New York" the city itself). For "Brooklyn, NY"-style inputs we fall back to a county-prefix filter on the state's tract shapefile, no extra download.

The mapping lives in `tiger._BOROUGH_ALIASES`:
- `("brooklyn", "NY")` → Kings County (FIPS 36047)
- `("manhattan", "NY")` → New York County (36061) (yes, Manhattan = New York County, confusing)
- `("queens", "NY")` → Queens County (36081)
- `("bronx", "NY")` → Bronx County (36005)
- `("staten island", "NY")` → Richmond County (36085)

To extend for other multi-place cities (Boston neighborhoods, Houston wards, KC across states), add entries to `_BOROUGH_ALIASES`. The resolver tries PLACE first, then alias on `ValueError`. County boundaries == tract boundaries, so boundary tracts are always empty for alias lookups.

Cross-state cities (KC spanning MO+KS) are NOT yet supported by this table — would need a different design that loads multiple states' shapefiles and aggregates. Deferred.

## Per-(provider, technology) split — and the "fiber available" trap

Multi-tech providers (Lumen offers DSL + Fiber, Comcast offers Cable + Fiber) MUST appear as separate `ProviderSummary` rows. Aggregating them into one row produces wrong-looking cards:

- "Lumen / Quantum Fiber: 5,934 locations served, 3 Gbps max down" was conflating ~5K DSL locations (capped at 100 Mbps) with ~1K fiber locations (3 Gbps). The 3 Gbps tag implied the whole footprint was fiber.

`competitors.score()` groups by `(canonical_name, holding_company, category, technology)`. Helpers `distinct_providers()` and `has_fiber_by_provider()` dedupe back to canonical-provider when needed (KPIs, market opportunity scoring).

### Two different "fiber" metrics — keep them separated

- **`fiber_share()`** (provider-count): share of distinct canonical providers offering any fiber. For Evans CO = 5 / 12 = 41.7%. Useful for "how many fiber options does a household have to choose from?"
- **`fiber_availability_share()`** (household-availability): share of locations (BSLs) where AT LEAST ONE provider offers fiber. For Evans CO = 5,316 / 6,717 = **79.1%**. This is the "can I get fiber here?" metric.

These almost always differ — a small number of fiber providers can blanket most of a market, so household availability runs much higher than provider share. The Overview's "Fiber available" KPI uses the household metric. The provider-count metric is shown as context in the help tooltip.

`fcc_bdc.location_availability(geoids)` is the SQL that computes per-tract location-level availability. The same approach gives Cable / DSL / FW / Satellite availability — all surfaced as a 5-tech KPI strip on Overview and as map layers.

## Provider canonicalization lessons

Real raw `brand_name` values in BDC data are often the bare brand without sub-product:
- "T-Mobile" (FW) — not "T-Mobile Home Internet"
- "Verizon" (FW) — not "Verizon 5G Home"
- "AT&T" (FW) — not "AT&T Internet Air"
- "Comcast Corporation" (Cable+Fiber) — but the brand_name in the CSV rows says "Xfinity"

Lesson: keep raw_aliases broad (include bare brand names) but rely on `primary_techs` for tech-aware disambiguation. The bare "Verizon" string with tech 71 (Licensed FW) resolves to Verizon 5G Home only because the FW canonical has the tech filter — without it, it would mismatch to Verizon Fios.

When extending the registry: every new market lookup may surface 3-5 new long-tail providers (regional fiber, WISPs, munis). Add them as encountered. The pass-through fallback ensures unknowns don't break — they just appear with category=unknown.

## Things to verify before each phase

- Run `make refresh` after any data-source code change to confirm the pipeline still works end-to-end.
- Run `make smoke` before declaring a phase done. Three sample markets must complete without errors.
- Spot-check Evans CO output against FCC National Broadband Map UI (https://broadbandmap.fcc.gov/) to catch silent data-shape bugs.

## Test markets and why they're picked

| Market | Why |
|---|---|
| Evans, CO | Small (~22K), real fiber competition (Lumen, Xfinity, Allo). Non-Altice market — validates generic-incumbent design. Primary smoke test. |
| Plano, TX | Large suburban, Verizon Fios + AT&T Fiber overbuild on Spectrum. Good for testing dense competitor matrices. |
| Brooklyn, NY | Dense MDU-heavy. Tests MDU/SFH split and Optimum/Verizon Fios competitive dynamics. |
| Mountain View, CA | Has Google Fiber. Tests less-common provider canonicalization. |

## Things we deliberately deferred (not bugs)

- Multi-market comparison view (Phase 8).
- LLM-generated narrative summaries (v2). Stick to deterministic templating in v1.
- Address-level (BSL) outputs. Tract-only by design.
- Real-time refresh / scheduled cron. Manual `make refresh` is fine for v1.
- Internal Altice subscriber data integration. Out of scope for the personal/non-commercial tool.
