# ftth-compete — Project context for Claude

This is the entry-point doc Claude Code auto-loads. **Keep it short.** Deeper context lives in the sibling docs in this folder.

## What this project is

`ftth-compete` is a personal, non-commercial FTTH market competitive intelligence tool. Four cross-linked Reflex pages + a private admin sidecar:

| Route | Use |
|------|-----|
| `/v2` | **Market deep-dive.** Type `City, ST` → polished map canvas with KPI strip, provider-footprint search, click-tract drill-down, lens scoring, take-rate trajectory, full competitor list |
| `/screener` | **Batch screener.** Pick states + filter ranges → ranked table of candidate markets with opportunity score, CSV export, "Open in v2" deep-link per row |
| `/providers` | **Provider directory.** Every canonical provider with a footprint in cached BDC data, sortable by states / tracts / fiber tracts / locations |
| `/provider/<slug>` | **Provider detail.** National state-level footprint map, per-state breakdown, head-to-head competitor overlap, trajectory across cached BDC releases |
| `/admin?key=<ADMIN_KEY>` | **Private visitor log.** SQLite-backed event store (`market_lookup`, `tab`, `lens`, `tract_click`, `provider_click`). Wrong / missing key → 404. See [reference_admin_sidecar memory](../../../Users/dkrishn3/.claude/projects/.../memory/reference_admin_sidecar.md). |

All four user-facing routes share data via cached BDC parquets + ACS + TIGER + IAS. The v2 page is the canonical entry point.

## Use scope

**Personal / non-commercial only.** This is not an Altice product. Ookla data is included under CC BY-NC-SA 4.0 — any commercial use would breach the license.

## Stack

Python 3.12 · uv · **Reflex** (UI; compiles to Next.js/React) · Plotly 6 / MapLibre · DuckDB · Polars · GeoPandas · Folium (for the `/v2` Map tab) · httpx + tenacity · pydantic-settings · SQLite for API caching · Starlette endpoints (`/v2_map_html`, `/provider_map_html`, `/admin`) hung off `app._api` for iframe-served Plotly figures + the admin sidecar.

## Run commands

```powershell
uv sync                 # install deps
uv run pytest           # tests (186 currently)
uv run reflex run       # dashboard at localhost:3000 (backend :8000)
make refresh            # download/refresh datasets (~30 min cold)
make smoke              # E2E test against Evans CO, Plano TX, Brooklyn NY
```

## Deployment

Hosted free-tier on Hugging Face Spaces: <https://dhruvkrishna49-ftth-compete.hf.space/>. Visibility is **public** (private Spaces 404 anonymous visitors). Pushes to `main` on the GitHub repo trigger `.github/workflows/deploy.yml`, which force-pushes the branch to the HF git remote. HF then rebuilds the Docker image (Caddy fronting Reflex at :7860 → :3000 frontend + :8000 backend) and restarts the container. Rebuild + boot ≈ 5–10 min. See [reference_hf_space memory](../../../Users/dkrishn3/.claude/projects/.../memory/reference_hf_space.md) for the gotchas — most notably HF rejects every binary file on push without Xet/LFS, so binary assets (like the IAS history zips) are fetched at Docker build time via `RUN curl`.

## Repo location

**Dev work happens at `C:\dev\ftth-compete\`.** That's where the git remote is wired up and where Reflex hot-reload runs cleanly. The legacy OneDrive copy at `C:\Users\dkrishn3\OneDrive - AlticeUSA\Personal\FTTH\` is now read-only / archival only — file-watch sync conflicts with `.web/`, `.venv/`, and parquet caches made it untenable for live work.

`.env` defaults still steer the heavy stuff outside any sync path:

- `.venv/` lives at `UV_PROJECT_ENVIRONMENT=C:\Users\dkrishn3\.venvs\ftth-compete`.
- Data caches live at `FTTH_DATA_DIR=C:\Users\dkrishn3\ftth-compete-data`.

## Caches

Three tiers, all auto-invalidating:

| Cache | Path | Cold cost | Warm cost |
|-------|------|-----------|-----------|
| BDC state parquets | `data/processed/bdc/<release>/state=NN.parquet` | ~3-5 min per state | Instant |
| Provider aggregation | `data/processed/provider_view/<release>.parquet` (+ `.meta` mtime stamp) | ~100s scanning all cached BDC | <1s |
| Screener results | `data/processed/screener/<release>__<states>.parquet` | ~2-15 min depending on scope | <1s (Force Rebuild bypasses) |
| In-process aggregation | `_AGG_CACHE` dict in `provider_view.py` | Built from one of the above | <100ms |

## Conventions

- **Tract-level resolution everywhere.** No address-level outputs in the UI.
- **Penetration shown as ranges, never point estimates.** IAS data lags ~1.5yr.
- **Provider name canonicalization is a single source of truth** in `src/ftth_compete/data/providers.py`. Update both the code and `.claude/providers.md` together.
- **Lenses are thin re-weighting layers.** Underlying data isn't mutated. Defensive lens picks any incumbent, not just Optimum.
- **No emojis in code or docs unless the user asks.** They asked to strip them from UI strings (Nov 2026); keep prose plain text.
- **State isolation per route.** `LookupState` (market deep-dive) / `ScreenerState` (batch) / `ProviderViewState` (directory + detail) are separate so heavy state from one workflow doesn't bleed into another.
- **Staged `/v2` paint via four phases.** `run_lookup` paints in stages so users see progress sooner: **A1** = fast base (TIGER + ACS + BDC + housing + heuristic penetration, no IAS/Ookla/Places), **A2** = enrichment (IAS anchor + Ookla speeds + Google ratings), **B1** = subs-history sparkline, **B2** = velocity + trajectory. Each phase ends with an `async with self:` block to publish state and either yields the next event handler or paints + waits. Nav-bar spinner labels (`Loading map and data...`, `Loading momentum data...`) track which phase is in flight.

## Sibling docs

- **[roadmap.md](roadmap.md)** — phase status (✅ done / 🚧 in-progress / ⏳ queued / ⏪ deferred). **Read this when the user asks what's next.**
- [architecture.md](architecture.md) — layered design, why DuckDB/Polars/Reflex
- [data-sources.md](data-sources.md) — FCC BDC / IAS / ACS / TIGER / Ookla / Places API quirks and validated state
- [providers.md](providers.md) — canonical provider registry + 10-K subscriber anchors
- [methodology.md](methodology.md) — penetration estimation math, IAS lag handling, lens weighting, take-rate trajectory scope
- [ux-spec.md](ux-spec.md) — Reflex page layout, lens semantics, copy guidelines
- [dev-notes.md](dev-notes.md) — gotchas, debugging recipes, things that bit us
- [refresh-bdc-workflow.md](refresh-bdc-workflow.md) — how to refresh the public HF Dataset of pre-converted BDC parquets after each biannual FCC release; the Dockerfile curls from there at build time so cold containers boot warm

## When updating these docs

If a session establishes new conventions, fixes a tricky bug, or makes a methodology decision — update the relevant doc *and* commit it. Future sessions need this context. Don't write it only into the chat.
