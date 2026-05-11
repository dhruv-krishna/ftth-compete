"""Methodology tab: static documentation of every metric, source, and caveat.

The goal is to make the tool's outputs legible to a skeptical stakeholder
("how do you know that 90% take rate?"). No per-market computation here —
just text content.
"""

from __future__ import annotations

import streamlit as st


def render_methodology() -> None:
    """Render the static methodology / data-sources page."""

    st.markdown(
        """
### How to read this tool

ftth-compete combines six public data sources into a single per-market
tear-sheet. Each metric below tells you where it came from, what it's
trying to measure, and what to NOT read into it.
        """
    )

    st.divider()

    # ------------------------------------------------------------------
    st.markdown("### Data sources")
    st.markdown(
        """
| Source | What it gives us | Cadence | Lag | License |
|---|---|---|---|---|
| **FCC BDC** (Broadband Data Collection) | Per-location provider availability by technology. Block-geoid level rolled up to tract. | Semi-annual | ~6mo | US public domain |
| **FCC IAS** (Internet Access Services) | Per-tract residential broadband subscription density, bucketed (per 1,000 HH). All-tech, all-provider aggregate. | Semi-annual | ~12–18mo | US public domain |
| **Census ACS 5-Year** | Demographics, poverty, housing-unit counts, MDU/SFH split. ACS variables B01003, B17001, B19013, B25001, B25024. | Annual | ~1yr | US public domain |
| **Census TIGER/Line** | Tract polygons, city (PLACE) boundaries. | Annual | ~6mo | US public domain |
| **Ookla Open Speedtest** | User-measured throughput / latency, aggregated to z=16 hex tiles (~610m). Spatially joined to tracts. | Quarterly | ~3mo | CC BY-NC-SA 4.0 — **non-commercial only** |
| **Google Places API (New)** | Provider Google rating + review count. | On-demand | live | Paid; rating field is Enterprise-tier (1K free events/mo) |
| **Provider 10-K filings** | National subscriber totals used to anchor per-provider take-rate heuristic. | Annual | ~3mo | Public SEC filings |
        """
    )

    st.caption(
        "BDC and IAS were unified under FCC 25-34 in July 2025. Both data feeds "
        "continue under their historical schemas."
    )

    st.divider()

    # ------------------------------------------------------------------
    st.markdown("### Overview tab metrics")
    st.markdown(
        """
- **Population** — ACS B01003 total population, summed across analyzed tracts.
- **Median HH income** — Population-weighted mean of tract medians (ACS B19013).
  Note: this is NOT the exact market median, which would require raw ACS PUMS
  data. The weighted-mean proxy is close in homogeneous markets, can diverge
  in markets with bimodal income.
- **Poverty rate** — ACS B17001: poverty_below / poverty_universe, summed
  across analyzed tracts.
- **Housing units** — ACS B25001 total housing units.
- **MDU share** — ACS B25024 units-in-structure summed: (2+u + 3-4u + 5-9u +
  10-19u + 20-49u + 50+u) / total. Mobile homes and boats/RVs are excluded
  from numerator and denominator both — SFH share + MDU share + other share
  should equal 100%.
- **Providers** — Distinct canonical provider count (deduped after per-tech
  split). A provider offering both Cable and Fiber counts once.
- **Fiber available** — *household-availability metric.* Share of FCC BSLs
  in the market where at least one provider offers tech 50 (Fiber to the
  Premises). Different from "% of providers offering fiber" (which is shown
  in the tooltip as a reference number).
- **Take rate (≥25/3 Mbps)** — FCC IAS tract-level subscription density,
  bucketed to per-1,000-HH ranges then weighted by tract housing units.
  All-tech and all-provider; **not** a per-fiber-provider take rate.
- **Boundary tracts** — Tracts whose polygon intersects the city but whose
  centroid falls outside. Excluded from analysis by default; toggle in the
  sidebar to include.
        """
    )

    st.divider()

    # ------------------------------------------------------------------
    st.markdown("### Competitors tab")
    st.markdown(
        """
Each card represents one **(provider, technology)** offering. A multi-tech
provider like Lumen (DSL + Fiber) or Xfinity (Cable + some Fiber) appears
as two cards with separate metrics — fiber footprint at fiber speeds, DSL
footprint at DSL speeds.

- **Coverage %** — Distinct tracts where this provider serves this tech /
  total analyzed tracts.
- **Locations served** — Distinct BSL count from FCC BDC, scoped to this
  provider × tech.
- **Max down (advertised)** — Maximum `max_advertised_download_speed` reported
  by this provider in BDC. *Marketing claim, not measured.* See the Map
  tab's measured-speed layers for what users actually get.
- **Est. subs** — See penetration methodology below.
- **Google rating** — Place Details `rating` + `userRatingCount`. Limited to
  1,000 free Enterprise-tier events per month; lookup cached 30 days.
- **12-month Δ** — Coverage delta vs the BDC release ~12 months prior. Only
  populated when "Include 12-month coverage change" is enabled in sidebar.
        """
    )

    st.divider()

    # ------------------------------------------------------------------
    st.markdown("### Penetration estimation")
    st.markdown(
        """
**The hard part.** Per-tract, per-provider subscriber counts are confidential
(filed to the FCC under data-sharing rules). What we can build from public
data is a calibrated heuristic.

**Inputs:**
- FCC BDC: locations served by each provider×tech in the market.
- 10-K filings: national subscriber totals → national take rate per major
  provider (e.g., Comcast 32M subs / ~58M HP ≈ 55% take rate).
- FCC IAS: market-total subscriber density (bucketed per 1,000 HH, all-tech
  aggregate).

**Procedure:**
1. **Heuristic estimate per (provider, tech)** = locations_served × national
   take rate × (1 ± 25% range). For providers without a 10-K anchor, fall
   back to a category-level default (national_fiber 30%, cable 45%, fixed
   wireless 5%, satellite 1%, etc.).
2. **IAS market anchor** = sum across tracts of (housing_units × bucket
   midpoint / 1,000). Range comes from bucket bounds.
3. **Calibrate**: scale all per-(provider, tech) estimates so their sum equals
   the IAS anchor mid. Preserves per-provider proportions; corrects the
   inherent double-count where overlapping fiber footprints would otherwise
   each get credited with the same households.
4. Calibration scale is capped at ±2x to avoid pathological cases.

**Confidence labels:**
- **high** — calibrated to FCC IAS subscription anchor.
- **medium** — 10-K-anchored take rate, not IAS-calibrated.
- **low** — category-default take rate (no 10-K data for this provider).

**Limits:**
- IAS data is publicly aggregated across all techs; we can't get
  "Xfinity fiber vs Xfinity cable" subscriber split from IAS.
- IAS lags 12–18 months. Auto-downloaded release is Jun 2022 (4 years stale).
- 10-K take rates are national; real per-market deviation can be ±20-30%.
- Treats household subscription as 1:1 — doesn't model second-line / backup
  subscriptions (Starlink at the cabin, etc.).
        """
    )

    st.divider()

    # ------------------------------------------------------------------
    st.markdown("### Velocity (12-month coverage change)")
    st.markdown(
        """
Compares the current FCC BDC release to the closest published release ~12
months prior (by default 2025-06-30 vs 2024-06-30). For each
(canonical_provider, technology), computes:

- Absolute delta in locations served
- Percent delta (None when prev was zero)
- "NEW offering" flag when prev was zero and current > 0
- "Discontinued" flag when prev > 0 and current is zero

Opt-in via sidebar — triggers a second state-level BDC ingest (~5 minutes
cold on first lookup of a state).

**Caveat:** if a provider's BDC `provider_id` changed between releases (e.g.
post-M&A) our canonical mapping should resolve them to the same canonical,
but corner cases can show as "NEW + discontinued" pairs. See `providers.md`
for the canonicalization table.
        """
    )

    st.divider()

    # ------------------------------------------------------------------
    st.markdown("### Lens scoring")
    st.markdown(
        """
Three lenses re-rank the same provider list under a strategic perspective.
Lenses don't mutate underlying data — they're pure re-weighting layers.

- **⚔️ Incumbent-defensive** — sidebar picks the incumbent. Other providers
  score as *threats*: 0.5×fiber-attack potential + 0.3×coverage % +
  0.2×Google-rating advantage over incumbent.
- **🚀 New-entrant-offensive** — providers score as *vulnerability* to
  disruption: 0.5×no-fiber + 0.3×cable-only + 0.2×rating weakness. Top of
  list = easiest targets for a new fiber overbuild.
- **⚖️ Neutral** — default fiber-first ranking, no lens score.

The Overview tab also surfaces a market-level **opportunity score** under
the offensive lens: a composite of underserved-by-fiber share, cable-only
share, average rating weakness, MDU density, and thin-competition bonus.
        """
    )

    st.divider()

    # ------------------------------------------------------------------
    st.markdown("### Map tab layers")
    st.markdown(
        """
All layers operate on the same tract polygons (TIGER 2024, EPSG:4326).

- **Fiber availability % (locations)** — From BDC location_id-level
  aggregation. "Lit" tracts have ≥1 provider offering tech 50 at most BSLs.
- **Fiber providers per tract** — Provider COUNT, not coverage %. Different
  from above; a market with 5 fiber providers each covering 80% of locations
  may show 5 in this layer and 99% in the availability layer.
- **Measured median down / up / latency** — Ookla open speedtest tiles
  spatially joined to tract polygons. Median across all tile centroids
  falling within the tract.
- **Fiber footprint: X** — Dynamic per-provider layers. Binary 0/1 mask
  showing where each fiber provider serves. Toggle different ones to see
  overlap and gaps.
- **MDU share / Poverty rate / Median income** — Direct ACS pass-through.

**Click any tract** for a detail card below the map: population, MDU share,
poverty rate, median income, fiber-provider count, measured speeds (when
present), and full per-provider list serving that tract.
        """
    )

    st.divider()

    # ------------------------------------------------------------------
    st.markdown("### Known limitations & caveats")
    st.markdown(
        """
- **Per-provider subscriber counts are estimates.** Public data has no
  tract-level per-provider subscription truth. See penetration methodology.
- **IAS data is bucketed** (6 codes 0–5, ranges 0 / 0–200 / 200–400 /
  400–600 / 600–800 / 800+ per 1,000 HH). Market totals derived from
  midpoints have ±100 per 1,000 HH uncertainty per tract.
- **BDC tech codes don't equal speed tiers.** A provider with tech 50
  (fiber) might advertise 100 Mbps in one tract and 5 Gbps in another;
  cards show the *max* across tracts but the per-tract drill-down has the
  variance.
- **Ookla data is licensed CC BY-NC-SA 4.0** — non-commercial only.
  Personal use is fine; do not redistribute or use commercially.
- **Multi-state markets** (e.g., Kansas City spanning MO/KS) are not yet
  supported. NYC borough aliases (Brooklyn → Kings County, etc.) work, but
  cross-state metros fall back to a single state's tracts.
- **Google ratings** depend on `GOOGLE_PLACES_KEY` in `.env`; without it,
  rating data is just skipped (no crash, no error).
        """
    )

    st.divider()

    # ------------------------------------------------------------------
    st.markdown("### Required attributions")
    st.markdown(
        """
The Ookla open speedtest dataset requires this attribution wherever the
tool's output is shared or screenshot:

> Speed test data © Ookla, 2019–present, distributed under CC BY-NC-SA 4.0.

The footer of every page renders this automatically.

US Government sources (FCC, Census) are public domain and do not require
attribution, but are credited in tooltips and the Data Sources table above.
        """
    )

    st.divider()

    # ------------------------------------------------------------------
    st.markdown("### Refreshing data")
    st.markdown(
        """
- **FCC BDC** — re-runs automatically when `latest_release()` finds a newer
  published release. Cached parquet per state per release lives in
  `data/processed/bdc/`. To force a fresh ingest, delete the relevant
  parquet file.
- **FCC IAS** — auto-downloads Jun 2022 (latest direct ZIP). For newer
  releases (Dec 2022, Jun 2023, Dec 2023, Jun 2024, Dec 2024), visit
  [the FCC IAS tract page](https://www.fcc.gov/form-477-census-tract-data-internet-access-services)
  → click the "Box" link for the release → download the ZIP → drop it into
  `data/raw/ias/`. The code picks the newest cached file automatically.
- **Census ACS** — annually published, auto-fetched per-tract via API.
- **Ookla** — auto-fetched from S3 (`s3://ookla-open-data`) at lookup time;
  no local cache needed.
- **Google Places** — 30-day TTL cache.
        """
    )
