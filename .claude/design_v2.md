# ftth-compete v2 — design + data strategy

This doc covers two intertwined questions you raised:
1. Best way to display the data we have — current layout isn't doing it justice.
2. Path to actual subscriber counts, not heuristics.

They're entangled because better data wants a different surface to live on, and a better surface depends on knowing what data we're committing to.

## Part 1 — Research: what gold-standard map dashboards do differently

Surveyed FCC's National Broadband Map (the closest domain peer — built on Mapbox GL JS, 11M blocks × 441 providers, 5B vector combinations), Mapbox showcase pieces, and current dashboard UX research consolidated in [DataCamp's dashboard design guide](https://www.datacamp.com/tutorial/dashboard-design-tutorial), [Pencil & Paper's UX-pattern analysis](https://www.pencilandpaper.io/articles/ux-pattern-analysis-data-dashboards), and [Justinmind's 2025 best-practices](https://www.justinmind.com/ui-design/dashboard-design-best-practices-ux).

Patterns that consistently win:

| Pattern | Why it works | Where we violate it |
|---|---|---|
| **Map as the primary visual** when content is geographic | Geography IS the story. Burying the map in a tab makes users translate text→space mentally. | Map is one of 6 tabs; requires explicit "Refresh" click. |
| **Persistent context rail** showing summary KPIs while drilling | Users keep their bearings while exploring detail. | Tabs swap entire content; you lose Overview KPIs the moment you click Competitors. |
| **F-pattern reading flow**: headline narrative → top KPIs → detail | Eye-tracking research shows users scan top-left first, lose attention going down/right. | Narrative is at the BOTTOM. KPIs upper-left is correct, but they're 12 cards of equal weight. |
| **Click-to-filter cross-component reactivity** | Click an element in any view → others filter to match. | Map doesn't talk to provider list. Provider list doesn't talk to map. |
| **One accent color + categorical palette for providers** | Visual restraint; let data carry the color story. | Currently uses 5-6 accent colors for badges. |
| **Density hierarchy**: 2-3 hero metrics, then secondary | "Five-second scan test" — can a user know status in 5s? | All 8 Overview KPIs are visually identical weight. |
| **Status colors only for actual status** (green/amber/red) | Status colors used for decoration dilute signal. | Color-coded provider/category badges create noise. |
| **Generated narrative AS the entry point**, not a footnote | One-pager / opening paragraph is what executives read first. | `market_narrative()` is buried under tract details. |

## Part 2 — Current design diagnosis

What's working:
- Sidebar form pattern is fine — that's the input affordance.
- Per-(provider, tech) split (Phase 3 work) was a real improvement.
- Layered confidence on subs estimates is the right abstraction.
- Anchor registry + ACP covariate are unique to this tool.

What isn't working:
1. **6 tabs is too many.** Overview / Competitors / Housing / Map are facets of the same question ("tell me about this market") — they should compose into one experience, not require navigation. Compare and Methodology are different beasts and stay separate.
2. **The map is a destination, not a canvas.** Folium-in-iframe is non-interactive — you can't click a tract and have anything else respond. The map should be the spatial substrate everything else hangs off.
3. **KPI density is uniform.** 12+ cards across Overview, all rendered with the same card primitive at the same size. There's no visual hierarchy telling you which metrics actually matter.
4. **The narrative is buried.** `market_narrative()` produces a paragraph that should be a *headline* — but it sits at the bottom under the tract list.
5. **Per-provider info is fragmented.** Card → expander → tooltip → modal? Currently a 3-col card grid where each card has 8+ pieces of info competing.
6. **Lens score badge is the same color as everything else.** It should pop.
7. **Empty states are good** (the "Try Evans, CO" CTA) but `is_loading` states are sparse.

## Part 3 — Proposed v2 layout

### Layout: three-panel with the map as the canvas

```
┌────────────────────────────────────────────────────────────────────────────────────┐
│ 📡 ftth-compete   Brooklyn, NY ▾   Pop 2.6M • 12 ISPs • 79% fiber    [⚖ Neutral ▾] │
├──────────────┬─────────────────────────────────────────┬───────────────────────────┤
│              │                                         │ ─── This Market ────────── │
│ Market       │                                         │                            │
│  Brooklyn    │                                         │ Brooklyn, NY is a high-MDU │
│  NY ▾        │                                         │ urban market dominated by  │
│              │                                         │ cable incumbents …         │
│ Layer        │                                         │ (generated narrative,      │
│  ● Providers │       ▓▓▓▓ Map canvas ▓▓▓▓             │  prominent, 2-3 sentences) │
│  ○ Speed     │       ▓▓▓▓ (Mapbox GL JS, ▓▓▓▓         │                            │
│  ○ MDU %     │       ▓▓▓▓  reactive,     ▓▓▓▓         │ ─── Top metrics ─────────── │
│  ○ ACP %     │       ▓▓▓▓  click-to-     ▓▓▓▓         │                            │
│  ○ Income    │       ▓▓▓▓  drill)        ▓▓▓▓         │  Pop      2.63M  ↑          │
│  ○ Poverty   │       ▓▓▓▓                ▓▓▓▓         │  Tracts   805               │
│              │                                         │  Fiber    79.1%             │
│ Lens         │       [Legend bottom-right]             │  MDU      86.6%             │
│  ⚖ Neutral   │                                         │  Income   $76k              │
│  ⚔ Defensive │                                         │  Poverty  31.5%             │
│  🚀 Offensive│                                         │                            │
│              │                                         │ ─── Providers (12) ─────── │
│ Filters      │                                         │                            │
│  ☑ Fiber-cap │                                         │  Spectrum   ████████░ 96% │
│  ☐ MDU only  │                                         │  Xfinity    ███████░░ 75% │
│              │                                         │  Verizon    ████░░░░░ 42% │
│ [Look up]    │                                         │  Optimum    ███░░░░░░ 28% │
│              │                                         │  AT&T Fiber █░░░░░░░░  8% │
│              │                                         │  …  show all                │
│              │                                         │                            │
│              │                                         │ ─── Opportunity ──────────── │
│              │                                         │  Score 0.62 / 1.00         │
│              │                                         │  ▌▌▌▌▌▌░░░░ Strong target │
│              │                                         │                            │
├──────────────┴─────────────────────────────────────────┴───────────────────────────┤
│  Detail:  📊 Snapshot │ 🏢 Providers │ 🏘 Housing │ 📊 Compare │ 📖 Methodology       │
├────────────────────────────────────────────────────────────────────────────────────┤
│  [Selected tab's detail content — secondary; map+rails stay visible above]         │
└────────────────────────────────────────────────────────────────────────────────────┘
```

### Why this layout

- **Map is permanently visible** — the spatial story is the cover story, not a destination.
- **Right rail is context-aware**:
  - Default: market summary (narrative + top KPIs + provider ranking + opportunity).
  - Click a tract → swap to tract detail (its population, housing, providers, measured speeds).
  - Click a provider → swap to provider deep-dive (coverage detail, velocity, sparkline).
- **Top strip has the 3-4 hero KPIs** — what an exec sees in 5 seconds.
- **Bottom tabs are for tabular detail views** — the existing Competitors table, Housing per-tract breakdown, Compare, Methodology. These are secondary to the spatial story but still accessible.
- **One accent color** (blue/purple) for actions/selections. **Categorical palette** for providers used consistently across map and list.
- **Lens picker sits in the top-right** as a global toggle (current sidebar location is fine too).

### Interactivity model

| User action | What happens |
|---|---|
| Click tract on map | Right rail shows tract-detail card; provider list filters to providers serving that tract; map zooms slightly |
| Click provider in right rail | Map highlights that provider's footprint; tract list filters to served tracts |
| Switch map layer | Map repaints; right rail KPIs stay; legend updates |
| Switch lens | Right rail provider list re-ranks; map color can optionally rebase to lens score; tab content reflows |
| Hover tract on map | Inline tooltip showing GEOID + active-layer value (already implemented) |

### What this requires technically

- **Replace Folium with Plotly choropleth via `rx.plotly`.** Folium in an iframe cannot dispatch click events back to Python state. Plotly:
  - Already a Reflex first-class component (`reflex_components_plotly`).
  - `Choroplethmapbox` renders tract polygons with the same OSM / Carto base tiles we use today — **no API token, no credit card, no rate-limit risk**.
  - `on_click` event flows directly into Reflex state handlers — true click-to-drill reactivity.
  - **Originally considered Mapbox GL JS; pivoted off it because their free tier asks for a credit card up-front.** Plotly + OSM tiles gives us the same UX result with zero registration.
- **State refactor**: introduce `selected_tract` and `selected_provider` state fields. All right-rail components key off these.
- **Right rail becomes a polymorphic component** dispatching on the (selected_tract, selected_provider, lens) tuple.

This is real work — call it Phase 7. Probably 3-5 days end-to-end. Streamlit could not have supported this kind of interactivity; the migration to Reflex is what makes it possible.

## Part 4 — Smarter subs data: paths beyond heuristics

You asked for actual subscriber counts, not heuristics. Free-data ranking:

| # | Source | Granularity | Realness | Effort | Bang/buck |
|---|---|---|---|---|---|
| 1 | **FCC Form 477 historical archive (2014-2021)** | Per-provider × per-census-tract subscriber **bands** (7 buckets: <10, 10-99, 100-499, 500-999, 1K-5K, 5K-20K, 20K+) | **Real, bucketed.** Direct provider disclosures, just coarsened for privacy. | Medium — download archive, schema-map, ingest, growth-rate-adjust forward to 2024 | **★★★★★** — replaces heuristic with real bucketed data for every major provider × every US tract back to 2014 |
| 2 | **California PUC CASF reports** | Per-provider × per-census-block **exact counts** | **Real, exact.** Best dataset in the US. | Medium-heavy — scrape CA PUC, parse the formats they use, plumb through tiger.py | ★★★★☆ — for CA markets (~12% of US population), this becomes ground truth |
| 3 | **NY PSC Form 477 successor + filings** | Per-provider × per-state-detail subs | **Real, granular.** | Medium-heavy — similar effort to CA, fewer years | ★★★☆☆ — for NY markets |
| 4 | **City franchise renewal filings** | Per-city × per-cable-operator subs | **Real.** Cable operators must disclose at renewal. | Heavy — fragmented per city, no consistent schema; would need to target top 30-50 cities | ★★★☆☆ — gold for cable in those cities; doesn't cover fiber |
| 5 | **State BEAD plans** | Per-county broadband adoption | Quasi-real — derived from surveys + FCC | Medium — each state has its own format | ★★☆☆☆ |
| 6 | **M-Lab BigQuery (Phase 6d)** | Per-provider × per-tract **inferred from measured tests** | Inferred, biased sample | Medium (once GCP set up) | ★★★★☆ — different signal than the others; complementary |
| 7 | **ACS B28011** | Per-tract household subscription rate, **all providers aggregate** | Real but aggregate | Tiny — already accessible via Census API | ★★☆☆☆ — sanity check / denominator only |

### Recommended sequence (replaces what's left of Phase 6)

**Phase 7a — Form 477 historical ingest. (Biggest single win.)**

- Download the FCC Form 477 archive (publicly hosted at `transition.fcc.gov/wcb/iatd/comm.html` and `www.fcc.gov/general/form-477-internet-fixed-broadband-deployment-data`).
- Parse the 2021 final release CSV — schema is `provider_id, census_tract, technology, tier, subscribers_band, ...`.
- Build a `historical_subs_lookup(provider, tract) → band` API.
- Apply forward growth: for each provider, look up their 2021→2024 national sub change in the existing `NATIONAL_TAKE_RATES` source materials; apply uniformly to each tract's 2021 band midpoint.
- **In the estimator, this becomes the new tier-1 anchor** (above the 10-K take-rate fallback). Confidence: "historical-band" — a new level between "anchor" and "medium" that we surface in the UI.
- ~3-5 days work.

**Phase 7b — CA PUC CASF ingest.**

- Download CASF semi-annual reports.
- Parse provider × census block subscriber counts.
- When a market is in CA, prefer this over Form 477 historical.
- ~3-4 days.

**Phase 7c — M-Lab integration (Phase 6d).**

- The original plan. Now positioned as a "measured share" covariate that adjusts whichever historical/CA-PUC anchor we have.
- ~3-4 days once GCP set up.

**Phase 7d — Per-city franchise filings.**

- Targeted: top 30 cities by population.
- ~5-7 days; long tail unless we accept partial coverage.

### Confidence ladder after all phases

For any (provider, tract) cell:

1. **Direct disclosure** — city/metro/state anchor matched (current Phase 6a) → "high"
2. **CA PUC CASF block-level** (Phase 7b, CA only) → "high" with exact-count semantics
3. **City franchise filing** (Phase 7d, top 30 cities, cable only) → "high"
4. **Form 477 2021 band, growth-adjusted** (Phase 7a) → **NEW "historical-band"** — replaces most "medium" confidence
5. **M-Lab share × IAS total** (Phase 7c) → "measured-inferred"
6. **National 10-K take rate** (current) → "medium"
7. **Category default** (current) → "low"

## Part 5 — Recommended order

1. **Phase 7a (Form 477 historical)** — single biggest improvement to the underlying data. Do this BEFORE redesigning the UI, so the v2 layout has the right data to display.
2. **v2 UI redesign with Mapbox/Leaflet custom component** — the layout proposed above. Includes click-to-drill spatial reactivity.
3. **Phase 7b (CA PUC)** — for the cities where it matters.
4. **Phase 7c (M-Lab)** — when GCP is set up.
5. **Anchor registry round 3** — opportunistic, ongoing.

Doing the data first (7a) means the redesigned UI gets to surface "Spectrum: 1,200-4,999 subs (Form 477 band, 2021, growth-adjusted)" instead of "Spectrum: ~3,800 subs (heuristic)." The data IS the product; the UI should make it shine.

## Open questions for you

1. **Mapbox vs Leaflet for the new map.** Mapbox is better but requires a free token (`pk.eyJ…`). Leaflet is fully open and tile-flexible. Recommend Mapbox; OK if you'd rather avoid the token registration.
2. **Should the v2 redesign be a parallel page** (`/v2`) so the current UI stays working while we build? Or a hard cutover?
3. **Phase 7a Form 477 — do you want bands surfaced as bands in the UI** (e.g. "Spectrum: 1K-5K subs"), or **rendered as the band midpoint with a note**? Bands are more honest; midpoints look cleaner.
4. **For Phase 7b CA PUC**, would you commit to building the CA-specific ingestor knowing it only helps CA markets? Or do you want every state treated equally even if data quality varies?

I'd default to: **build Form 477 first**, then **redesign with Mapbox**, then ship a "release" before going deeper on state-specific data.
