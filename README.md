---
title: ftth-compete
emoji: 📡
colorFrom: blue
colorTo: indigo
sdk: docker
app_port: 7860
pinned: false
license: other
short_description: FTTH market competitive intelligence (personal, non-commercial)
---

# ftth-compete

Personal, non-commercial FTTH competitive intelligence tool. Four cross-linked Reflex pages:

- **`/v2`** — Single-market deep-dive. Type `City, ST` → polished map canvas with KPI strip, provider-footprint search, click-tract drill-down, lens scoring, take-rate trajectory, full competitor list.
- **`/screener`** — Batch-screen 50-500 markets by state + filter ranges. Sortable opportunity score, CSV export, per-row deep-link into `/v2`.
- **`/providers`** — Directory of every canonical provider with a footprint in cached BDC data. Sortable by states / tracts / fiber tracts / locations.
- **`/provider/<slug>`** — Single-provider portfolio view: national state-level footprint map, per-state breakdown, head-to-head competitor overlap, trajectory across BDC releases.

Backed by free public data (FCC BDC, FCC IAS, Census ACS, TIGER, Ookla open speedtest) plus a small Google Places quota for ratings.

## Quickstart

```powershell
# Install uv if you don't have it
irm https://astral.sh/uv/install.ps1 | iex

# Copy env template and fill in keys + venv/data overrides
Copy-Item .env.example .env
# Edit .env: set CENSUS_API_KEY, GOOGLE_PLACES_KEY
# Defaults already steer .venv and data outside OneDrive — adjust paths if needed

# Sync deps (creates venv at the path in UV_PROJECT_ENVIRONMENT)
uv sync

# First-time data refresh (~30 min, downloads FCC BDC + IAS + ACS + TIGER)
uv run python -m ftth_compete.pipelines.refresh_all

# Launch the dashboard (Reflex compiles + serves on :3000, backend on :8000)
uv run reflex run
```

## Routes

| Route | Lookup time (cold / warm) | Notes |
|------|---------------------------|-------|
| `/v2` | 30-90s / instant | Primary entry point. Momentum data loads as a background backfill. |
| `/screener` | 2-15 min / <1s | Disk-cached per (states, BDC release). Force Rebuild toggle to refresh. |
| `/providers` | ~100s / <1s | Disk-cached aggregation persists across Reflex restarts. |
| `/provider/<slug>` | <1s when directory is cached | Pre-warm older releases for trajectory data. |

## Why is the repo in OneDrive but `.venv` and `data/` are not?

OneDrive is great for source-code backup but bad at syncing thousands-of-files venvs and multi-GB parquet. `.env` defaults route `UV_PROJECT_ENVIRONMENT` and `FTTH_DATA_DIR` to local-only paths outside OneDrive sync.

## Test markets

- **Evans, CO** — small (~22K), mixed demographics, real fiber competition (Lumen, Xfinity, Allo). Primary smoke-test market.
- **Plano, TX** — large suburban, Verizon Fios + AT&T Fiber overbuild on Spectrum.
- **Brooklyn, NY** — dense MDU-heavy, Optimum + Verizon Fios.
- **Mountain View, CA** — dense, Google Fiber present, AT&T + Comcast.

## Documentation

Project context for Claude (and humans) lives in [.claude/](.claude/). Start at [.claude/CLAUDE.md](.claude/CLAUDE.md).

## License

Personal / non-commercial use only. Includes Ookla open speedtest data under CC BY-NC-SA 4.0 — see footer attribution in the dashboard. Do not redistribute or use in any commercial context.
