# Architecture

## Layered design

```
[ Bulk data layer ]      [ API layer ]              [ Cache layer ]
 FCC BDC (Parquet)        Census ACS (json→Polars)   SQLite kv-cache
 FCC IAS (Parquet)        Google Places (FieldMask)  by (source, key)
 TIGER tracts (gpkg)
 Ookla tiles (Parquet)
        │                       │                       │
        └───────────┬───────────┴───────────┬───────────┘
                    ▼                       ▼
            [ Normalization & joining ]   src/ftth_compete/data/
            - Provider-name canonicalization
            - City → tract list (TIGER spatial join)
            - Tract × provider × tech matrix
                    │
                    ▼
            [ Analysis modules ]           src/ftth_compete/analysis/
            - market.py / housing.py / competitors.py
            - speeds.py (BDC vs Ookla deltas)
            - penetration.py (IAS allocation)
            - lenses.py (re-weighting)
                    │
                    ▼
            [ Reflex UI ]                  ftth_compete_web/
            - /v2: single-market deep-dive (Overview/Competitors/...)
            - /screener: batch state rank
            - /providers + /provider/<slug>: provider-centric view
            - /admin: private visitor log
```

## Why these choices

- **DuckDB over pandas for FCC BDC.** Bulk files are ~100M rows nationally. DuckDB queries Parquet directly without a load step and is dramatically faster at this scale than a pandas read.
- **Polars over pandas for in-memory transforms.** Lazy execution + faster joins on the data sizes we hit (per-market subsets are still hundreds of thousands of rows).
- **uv over pip/poetry.** Faster installs, deterministic lockfile, modern standard.
- **Reflex over Streamlit/Dash/Flask.** Started in Streamlit for speed; migrated to Reflex to escape Streamlit's notebook-y ceiling and compile to a real Next.js/React app that survives polish. Pure-Python state model with explicit `@rx.event` handlers, no Streamlit-style top-to-bottom re-runs.
- **Folium over Plotly maps.** Better choropleth UX with leaflet under the hood; tract polygons render well.
- **SQLite over Redis for API caching.** Single file, no service to run, easy to inspect, fine for our throughput.

## Module responsibilities

| Module | Responsibility | Key types/functions |
|---|---|---|
| `data/fcc_bdc.py` | Download/convert/query FCC BDC bulk | `download_release()`, `coverage_matrix(geoids)` |
| `data/fcc_ias.py` | Tract-level subscription counts | `subscription_estimates(geoids)` |
| `data/census_acs.py` | ACS5 API client | `fetch_market_metrics(geoids)` |
| `data/tiger.py` | Tract polygons + city resolver | `city_to_tracts(city, state)`, `tract_polygons(geoids)` |
| `data/ookla.py` | Speedtest tile aggregation | `tile_stats(polygons)` — measured median down/up/latency |
| `data/google_places.py` | Provider rating lookups | `batch_lookup(providers, market_label)` |
| `data/providers.py` | Canonical provider registry | `canonicalize(raw_names)`, `Provider` dataclass |
| `data/cache.py` | SQLite kv-cache backend | `get(key)`, `set(key, value, ttl)` |
| `analysis/market.py` | Demographics roll-ups | `compute(acs_frame)` |
| `analysis/housing.py` | MDU/SFH split | `mdu_share(acs_frame)`, `housing_breakdown(acs_frame)` |
| `analysis/competitors.py` | Per-provider scoring | `score(coverage, ias, ookla, places)` |
| `analysis/speeds.py` | Advertised-vs-measured | `gap(bdc, ookla)` |
| `analysis/penetration.py` | IAS allocation methodology | `allocate(ias, coverage, anchors) → range` |
| `analysis/lenses.py` | Lens re-weighting | `apply(scores, lens, incumbent=None)` |

## Data flow: cold market lookup

1. UI form → `City="Evans", State="CO"`
2. `tiger.city_to_tracts()` → list of GEOIDs
3. Parallel: `census_acs.fetch_market_metrics`, `fcc_bdc.coverage_matrix`, `fcc_ias.subscription_estimates`, `ookla.tile_stats`
4. `providers.canonicalize` → resolve raw names
5. `google_places.batch_lookup` → cached ratings
6. `analysis.competitors.score()` → joined frame
7. `analysis.lenses.apply(scores, lens)` → re-weighted
8. UI renders

## Open architectural questions

- Whether to make `analysis/` modules pure functions (current direction) or a stateful `MarketSnapshot` class. Currently leaning pure — easier to test, no hidden state.
- Whether Streamlit's session_state is enough for caching warm market lookups across user clicks, or if we need a shared in-process cache layer.
