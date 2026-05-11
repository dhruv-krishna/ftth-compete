# UX specification

Single Streamlit app, sidebar + four tabs.

## Sidebar

- **City + State input** — text + state dropdown (50 states + DC + PR).
- **Lens selector** — radio: ⚔️ Defensive / 🚀 Offensive / ⚖️ Neutral. Default: Neutral.
- **Incumbent dropdown** — only shown when Defensive lens is active. Populated from canonical providers in current market.
- **Refresh button** — re-runs the pipeline ignoring cache for the current market.
- **Export** — buttons for "Download PDF tear-sheet" and "Download CSV bundle".
- **Footer**:
  - Data version row: "BDC: Dec 2024 · ACS5: 2020–2024 · IAS: Jun 2025 · Refreshed: <local date>"
  - Ookla attribution: "Speed test data © Ookla, 2019–present, distributed under CC BY-NC-SA 4.0"

## Tab 1 — Overview

**KPI cards** (single row, 7 cards):

1. Population
2. # Census tracts
3. Poverty rate (with national avg context)
4. Median HH income (with national avg context)
5. MDU %
6. # Active providers
7. % addresses fiber-served

**Generated narrative paragraph** (~3-5 sentences). Deterministic templating, no LLM in v1. Example:

> Evans, CO has a population of 21,847 across 6 census tracts, with a 17.2% poverty rate (vs 12.4% national) and median HH income of $54,310. Housing is 82% single-family, 18% multi-dwelling. 7 broadband providers serve the market, with 64% of addresses having access to fiber. Lumen and Allo Communications are the dominant fiber providers; Xfinity is the cable incumbent.

**Mini-map thumbnail** — full map lives in Tab 3.

## Tab 2 — Competitors

**Card grid view** (default), sortable by lens score.

Each card:
- Provider logo (cached locally) + canonical name
- Tech badges (Fiber 50 / Cable 40 / FW 71-72)
- Coverage %: portion of tracts where provider serves any address
- Estimated subscribers: range "low–high (mid)" with confidence label
- Speed: "Advertised: 1000/500 · Measured (median): 410/220"
  - Show speed-gap visual if gap_pct > 30%
- Google rating: stars + review count, color-coded (green ≥4, yellow 3-4, red <3)
- 12-month coverage delta (expansion velocity)

**Lens flags overlay:**
- Defensive: red badge on fiber-equipped competitors of the selected incumbent
- Offensive: green opportunity badge on low-rating or cable-only providers

**Table view alternative** — same data, sortable spreadsheet-style. Toggle in tab header.

## Tab 3 — Map

- **Folium choropleth** of tracts.
- **Layer toggles** (left panel):
  - Default: Competitor density (count of fiber providers per tract)
  - Per-provider coverage layers
  - Poverty rate
  - MDU concentration
  - Ookla measured median speed
- **Click tract** → popup with:
  - GEOID
  - Population, MDU %
  - Full per-tract provider list with tech tags
  - Measured speed median + sample count

## Tab 4 — Housing

- **Stacked bar** of unit-type breakdown from B25024 (1u / 2-4u / 5-19u / 20-49u / 50+u).
- **Cross-tab** housing type × fiber availability — heatmap.
- **MDU concentration map** — small inset choropleth.

## Creative additions

- **Provider radar chart.** 4-axis spider: coverage / measured-speed / rating / penetration. One per provider, side-by-side comparable.
- **Advertised-vs-measured speed gap chart.** Bar chart per provider with two bars (advertised, measured). Surfaces "promised but not delivered" providers visually.
- **BDC sparkline per provider.** Small line chart on the provider card showing fiber coverage % across last 4 BDC releases. Tells the "who's expanding" story.

## Copy guidelines

- **Honest, not hedge-y.** "Estimated 30–55%" not "approximately around 30 to 55 percent perhaps."
- **Surface caveats without burying.** Data lag and confidence labels are visible, not in tooltip-only.
- **Avoid telecom jargon in headlines.** "Fiber-served addresses" not "BSL fiber availability."
- **Numbers are formatted human-friendly.** "21,847" not "21847". "$54,310" not "54310".
- **Ranges always shown lo–mid–hi.** Not "30%±25%" or "30% (CI: 5–55%)" — too clinical.

## Performance targets

- Cold lookup (uncached): ≤ 2 minutes.
- Warm lookup (cached): ≤ 60 seconds.
- Map render: ≤ 2 seconds for any market up to 200 tracts.
