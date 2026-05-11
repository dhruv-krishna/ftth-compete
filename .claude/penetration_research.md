# Penetration estimation — research brief

**Goal.** Produce best-available per-(provider, tract) FTTH/broadband penetration estimates using only **free** public data. We do NOT pay for CostQuest fabric, NielsenIQ, S&P MoffettNathanson, or comparable subscriber datasets.

**Current state.** `analysis/penetration.py` produces three-tier estimates (low / mid / high) by multiplying BDC `locations_served` × a national take rate from each provider's 10-K, with category-default fallbacks. We calibrate the per-market sum against the FCC IAS tract-level subscription anchor (all-tech aggregate). The result is honest about its limits but has two real gaps:

1. **No per-provider truth.** IAS gives us a market-total; the per-provider split is heuristic.
2. **No sub-state granularity.** National take rates ignore that Verizon Fios's penetration in NYC is very different from its penetration in Albany.

This document inventories the free data we can layer in to close those gaps.

## Free data sources, ranked by impact × effort

### Tier 1 — High impact, moderate effort

**1. Provider 10-K / 10-Q / investor materials — DMA & market-level anchors.**
National subscriber totals already feed the heuristic. The unexploited material is finer geographic disclosures publicly companies make in:
- Investor day decks (PDFs on IR sites): metro / DMA / region splits.
- 10-Q segment notes: regional segment counts (Comcast Northeast vs West, etc.).
- Press releases for smaller fiber overbuilders (Allo, Ziply, Tachus, Vexus, Brightspeed, Race, GoNetspeed, Quantum Fiber, Optimum Fiber buildout): city-level "passed locations" + "customers" counts.
- M&A filings: acquired networks often have target-market subscriber disclosures.
Worth building a structured `MARKET_LEVEL_ANCHORS` registry — keyed by `(provider, market_geo, vintage)` — that the estimator prefers over national take rate when a match exists.

**2. FCC IAS Tract-Level Subscription Estimates — extending what we already use.**
The current Jun 2022 release is what we auto-download. Newer releases (Dec 2022, Jun 2023, Dec 2023) are Box-hosted at `us-fcc.app.box.com` and require manual download. Schema hasn't changed. **Two enhancements:**
- Pull the **per-speed-tier** breakdown (≥200kbps, ≥25/3 Mbps; ≥100/20 isn't tract-resolved but is state-resolved). Lets us isolate "fiber-tier" subs from "any broadband" subs.
- Cross-check IAS implied take rate against ACS B28011 (computer/internet use). Where they disagree materially, surface a confidence-downgrade flag.

**3. USAC ACP / EBB historical claims data.**
The Affordable Connectivity Program (ended June 2024) and its predecessor EBB published **per-provider per-ZIP claim counts** in USAC's public quarterly reports. Even though the program is over, the data archive is alive and roughly 23M households were on ACP at peak — disproportionately Comcast, Charter, Cox, AT&T. Path: scrape USAC public dashboards (`acpdata.usac.org`) or download the archived quarterly CSVs. Caveat: ACP enrollees are a biased slice (low-income), so the ratio "ACP enrollees / total subs" varies by provider — but as a relative-share signal at ZIP resolution this is gold for the bottom of the market.

### Tier 2 — High impact, high effort

**4. M-Lab (Measurement Lab) NDT data via BigQuery — per-test provider attribution.**
**Correction from initial brief:** Ookla's per-test data with `provider_name` is their *paid* commercial product (Speedtest Intelligence), not part of the free Ookla open-data S3 bucket. The free Ookla bucket is tile aggregates only and has no provider attribution.

The actual free path to per-test provider data is **M-Lab** — a Google / Internet Society / Princeton collaboration that runs the open-source NDT speed test and publishes every test to a free BigQuery public dataset (`measurement-lab.ndt.unified`). Each row includes:
- `client.Network.ASNumber` — client ASN (maps cleanly to provider via PeeringDB or MaxMind GeoLite2-ASN)
- `client.Geo.City` / `Geo.Subdivision1ISOCode` / lat-lon — geographic context
- `a.MeanThroughputMbps` / `a.MinRTT` — measured speed + latency
- `date`, `id` — temporal

Free tier: BigQuery gives every GCP account 1 TB of query data/month. Even with tight geographic filters M-Lab tables are large, so we'd need to budget queries carefully (probably one quarterly aggregation per market region, cached aggressively).

What this unlocks:
- Per-(provider, tract) **share of test volume** — a usable relative-market-share proxy within a tract.
- Per-(provider, tract) **median measured speed** — closes the "advertised vs delivered, per provider" story.
- Per-tract **dominant provider** = rough penetration proxy.

Caveats: NDT methodology differs from Ookla (NDT uses TCP-saturation single-stream measurements; Ookla uses multi-stream); they're not directly comparable. Sample bias toward technical users + WFH/gamers (same as Ookla). And NDT volume is lower than Ookla globally so sparse-tract issues are worse.

**Heavy lift** but highest-ROI free per-provider signal available. Requires GCP project (free to set up; no card needed for the free tier).

**5. FCC Form 477 historical archive.**
Form 477 was the predecessor to IAS/BDC, retired in 2022. The historical archives (2014–2021) include per-provider per-census-block subscription bands. Even though it's stale, it's a useful **rate-of-change** anchor — comparing modern BDC footprint to old Form 477 subscriber data lets us estimate growth trajectory for incumbents. Free, archived at FCC's open data portal.

### Tier 3 — Targeted, moderate effort

**6. State broadband office data.**
Several states publish enhanced subscription data beyond what FCC publishes nationally:
- California PUC: per-provider census-block subscription data (CASF report).
- New York PSC: Form 477 enhanced + state subsidy program data.
- North Carolina DIT: county-level penetration data from state surveys.
- Michigan, Texas, Florida: varying datasets via state broadband offices.
Most useful when you're analyzing a market in one of these states.

**7. ARPU + revenue back-calculation.**
For publicly-traded providers reporting metro / DMA revenue (rare, but exists for some — Charter discloses some metro detail, Cable One has small-market disclosure), `subscribers = revenue / ARPU` lets us derive subscriber counts. Effort: tracking down filings per provider per quarter. Best as a validation layer, not primary source.

**8. Cable franchise renewal filings.**
Cable operators file periodic renewal materials with municipal franchising authorities. These typically disclose subscriber counts per franchise area. Hugely fragmented (every city filed separately), but for major target markets it's a verifiable per-market truth source. Free; public records.

## Recommended methodology — layered confidence

For each `(provider, tract)` cell, walk anchors in priority order, take the first that resolves:

1. **City-level disclosure** (from press release or filing). Confidence: high.
2. **DMA / metro disclosure** (from investor deck / 10-Q segment). Confidence: high.
3. **State-level disclosure** (from state broadband office or 10-Q geo segment). Confidence: medium-high.
4. **ACP claim share × IAS market total** (where the provider is ACP-active). Confidence: medium.
5. **National take rate × BDC location count**, calibrated by IAS market anchor. Confidence: medium-low. (Current behavior.)
6. **Category default take rate × BDC location count**. Confidence: low.

Surface the confidence tier on every estimate in the UI.

## Implementation roadmap

1. **Phase 6a — Anchor registry data model.** Build the `MARKET_LEVEL_ANCHORS` table structure + extended estimator that resolves anchors in priority order. Seed with the data we already know from 10-Ks. Wire confidence label through to TearSheet + UI.
2. **Phase 6b — Expanded 10-K + press-release research.** Manually research top 30 providers; populate the registry with city/DMA/state numbers where disclosed. Document source URLs.
3. **Phase 6c — ACP archive ingest.** Pull USAC ACP claims data; build provider×ZIP→tract aggregation; layer into estimator as a tier-4 anchor.
4. **Phase 6d — M-Lab per-test attribution.** Set up GCP project + service account credentials; query `measurement-lab.ndt.unified` BigQuery dataset with tract-bounded filters; build ASN → canonical-provider mapping (via PeeringDB or hand-curated table); aggregate per (provider, tract) share-of-tests + median measured speed; layer into estimator as a "relative share" multiplier on the IAS anchor.
5. **Phase 6e — Form 477 historical baseline.** Ingest 2018-2021 archives; expose "subscriber growth rate" per (provider, tract).
6. **Phase 6f — State broadband data.** Per-state plugin pattern; CA + NY first.

Start with 6a (foundation) + 6b (research). Other phases stack on the same anchor-resolver pattern.

## Open questions

- **Anchor staleness handling.** A 10-K subscriber count from FY2023 is stale by FY2025. Apply a growth-rate adjustment from 10-Q quarterly disclosures? Or surface the anchor's vintage and let users discount it?
- **Disagreement resolution.** If a city-level press release contradicts a DMA-level investor deck, which wins? (Recommendation: source closer to ground truth wins — city > DMA > state > national.)
- **Validation set.** What's our "ground truth" for testing the estimator? Probably the few markets where we have unusually detailed disclosure: Verizon Fios + NYC, AT&T Fiber + Dallas, Allo + Lincoln NE.
