# UX specification

Multi-page Reflex app. The canonical entry point is `/v2` (single-market deep-dive); `/screener`, `/providers`, `/provider/<slug>` are sibling routes that compose the same cached data into different shapes.

## `/v2` — Market deep-dive

Three-panel layout (sticky 56px nav bar on top, then left rail / center canvas / right rail).

### Left rail (260px, sticky)

- **City + State input** — text + state dropdown (50 states + DC + PR). Quick-pick presets row above the input (Evans CO / Plano TX / Brooklyn NY / Manhattan NY / Queens NY / Mountain View CA / Kansas City Metro MO).
- **Advanced options accordion** — include-boundary tracts, skip-Ookla, skip-Google-ratings, include-velocity, include-trajectory checkboxes.
- **Look-up button** — kicks off `LookupState.run_lookup`. Disabled while a lookup is in flight.
- **Lens selector** — radio: Defensive / Offensive / Neutral. Default: Neutral.
- **Incumbent dropdown** — only shown when Defensive lens is active. Populated from canonical providers in the current market.
- **Footer**:
  - Data version row showing each source's resolved release ("BDC: <release> · ACS5: <vintage> · IAS: <release> · Ookla: latest").
  - Ookla attribution: "Speed test data © Ookla, 2019–present, distributed under CC BY-NC-SA 4.0".

### Center canvas

Plotly choropleth iframe (`/v2_map_html?...` Starlette endpoint, cached per `(city, state, layer)` so layer-switches are sub-second after the first render). Layer dropdown above the map: fiber providers per tract, total providers, fiber/cable availability %, measured down/up/latency, MDU share, poverty rate, median HH income, ACP density, plus per-fiber-provider footprint layers (one per fiber ISP in the market).

Click a tract → right rail switches to **tract detail** view. Click a provider in the right rail → map auto-flips to that provider's footprint layer.

### Right rail (340px)

Polymorphic context panel, three states:
- **No selection** → market summary (KPIs + provider list + IAS take-rate anchor + measured speeds + velocity highlights + opportunity panel under offensive lens).
- **Selected tract** → per-tract demographics + full provider list with tech badges.
- **Selected provider** → coverage % / tracts / locations / max speeds / star rating / velocity badge / inline SVG trajectory sparkline / lens-score badge.

### Six tabs

The center-canvas tab bar exposes the same right-rail data plus deeper views:

1. **Overview** — KPI grid, IAS subscription anchor, Ookla measured-speeds strip, 12-month velocity highlights, opportunity card (offensive lens only), narrative paragraph, take-rate trajectory sparkline + summary line, ACP density panel, tract-detail accordion.
2. **Competitors** — color-coded lens banner, filter controls (sort, category chips, fiber-only toggle), summary KPI strip, segmented Cards / Table view. Provider cards have 3-col grid layout with category & tech badges, full-width coverage bar, 2-col metrics, 3-segment speed-tier mini-bar (gig+/100Mbps+/<100), color-coded star rating, subs estimate with confidence badge, velocity badge, inline SVG trajectory sparkline.
3. **Housing** — 4-card summary strip (SFH share with national delta, MDU share with national delta, mobile/other, total units), MDU sub-row (small/mid/large), full 10-bucket B25024 horizontal bar chart, per-tract table with MDU-share progress bar.
4. **Map** — Folium choropleth alternative to the Plotly center canvas. 10 selectable layers with branca color scales.
5. **Compare** — Save current market button populates a Radix surface table comparing saved markets side-by-side. Lens-aware ranking.
6. **Methodology** — Markdown-rendered doc page covering data sources, metrics, penetration math, velocity, lens scoring, limitations, attributions.

### Staged paint

`run_lookup` paints in four phases so users see progress sooner. Spinner labels in the nav bar reflect the active phase:

- **A1** (no spinner; just the main loading state) — fast base: TIGER + ACS + BDC + housing + heuristic penetration. ~30-50s cold. Paints KPIs, provider list, fiber availability.
- **A2** ("Loading map and data...") — enrichment: IAS anchor + Ookla speeds + Google ratings. ~10-20s after A1.
- **B1** ("Loading momentum data...") — historical IAS subscription trajectory. ~30s warm.
- **B2** (continues "Loading momentum data...") — velocity + trajectory across 1–4 BDC releases. ~5 min cold per state.

## `/screener` — Batch market screener

Pick one or more states + filter ranges (MFI band, MDU share, fiber-availability %, population band). Returns a sortable opportunity-score table covering every Census PLACE in the selected state(s). CSV export. Per-row **Open** button deep-links to `/v2?city=X&state=Y&autorun=1`.

Disk-cached at `data/processed/screener/<release>__<states>.parquet`. Force Rebuild toggle bypasses cache.

## `/providers` — Provider directory

Every canonical provider with a footprint in any cached BDC state parquet. Sortable by states served / tracts / fiber tracts / locations. Click a row → `/provider/<slug>`.

## `/provider/<slug>` — Provider detail

National state-level footprint choropleth (Plotly iframe at `/provider_map_html?slug=...`), per-state breakdown, head-to-head competitor overlap matrix, trajectory across cached BDC releases.

## `/admin?key=<ADMIN_KEY>` — Private visitor log

SQLite-backed event store rendered as an HTML table: timestamp, session id (first 8 chars), SHA-256-prefix-hashed IP, event kind, payload, user-agent. Wrong / missing key → 404. Storage is ephemeral on HF Spaces free tier; wipes on container restart.

## Copy guidelines

- **Honest, not hedge-y.** "Estimated 30–55%" not "approximately around 30 to 55 percent perhaps."
- **Surface caveats without burying.** Data-lag and confidence labels are visible, not tooltip-only.
- **Avoid telecom jargon in headlines.** "Fiber-served addresses" not "BSL fiber availability."
- **Numbers are formatted human-friendly.** "21,847" not "21847". "$54,310" not "54310".
- **Ranges always shown lo–mid–hi.** Not "30%±25%" or "30% (CI: 5–55%)" — too clinical.
- **No emojis in UI strings** (per user direction, Nov 2026). Use Radix icons or plain text.

## Performance targets

- Cold market lookup (uncached): A1 paint ≤ 60s, A2 ≤ +30s, B1 ≤ +60s, B2 ≤ +5 min.
- Warm market lookup: < 5s end-to-end.
- Map iframe render: < 2s for any market up to 200 tracts (cached per layer, sub-second on subsequent layer switches).
- Screener single-state cold: 2–15 min depending on PLACE count. Warm: < 1s (disk cache hit).
