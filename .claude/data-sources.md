# Data sources

Validated state as of May 2026. Re-verify before assuming any of this is current.

## FCC Broadband Data Collection (BDC)

- **URL:** https://broadbandmap.fcc.gov/data-download
- **Latest release:** Dec 2024 as-of, released May 12 2025, revised Jun 10 2025.
- **Granularity:** BSL (Broadband Serviceable Location) — we aggregate to tract.
- **Format:** CSV by state/provider; also Shapefile/GeoPackage.
- **Auth:** None for bulk. Free FCC account for the Public Data API.
- **Cadence:** Semi-annual.
- **License:** US Government work / public domain.

### Fixed technology codes

| Code | Tech |
|---|---|
| 10 | Copper / DSL |
| 40 | Cable / HFC |
| **50** | **Fiber to the Premises** ← headline |
| 60 | GSO Satellite |
| 61 | Non-GSO Satellite (Starlink etc.) |
| 70 | Unlicensed Fixed Wireless |
| 71 | Licensed Fixed Wireless |
| 72 | Licensed-by-Rule FW (e.g. CBRS) |

Source: https://help.bdc.fcc.gov/hc/en-us/articles/5290793888795

## FCC Internet Access Services (IAS)

- **URL:** https://www.fcc.gov/internet-access-services-reports
- **Status:** Form 477 was formally modernized into BDC by FCC 25-34 (effective Jul 1 2025). IAS is the publication layer.
- **Latest:** "Status as of Dec 31 2024" + "Status as of Jun 30 2025" (released May 8 2026).
- **Tract granularity:** ≥200 kbps and ≥25/3 Mbps tiers per 1,000 HH. Higher tiers (100/20+) available only at national/state/county level.
- **Lag:** ~1.5–2 years. **Surface this in the UI.**
- **Auth:** None. Bulk PDF + tabular files.

## Census ACS 5-Year API

- **URL:** `https://api.census.gov/data/{year}/acs/acs5`
- **Latest vintage:** 2020–2024 (`year=2024`) — most recent as of May 2026.
- **Auth:** Free API key at https://www.census.gov/data/developers/api-key.html
- **Cadence:** Annual.

### Tables we use

| Table | Use |
|---|---|
| B01003 | Total population |
| B17001 | Poverty status |
| B25024 | Units in structure (MDU/SFH split) |
| B19013 | Median household income |
| B25001 | Total housing units |

## Census TIGER/Line

- **URL:** https://www.census.gov/cgi-bin/geo/shapefiles/index.php
- **Latest:** 2024 Shapefiles AND GeoPackages published.
- **Auth:** None. Public domain.
- **Files we need:** Census Tract (TRACT) and Place (PLACE — city boundaries) per state.

## Ookla Open Speedtest

- **URL:** https://registry.opendata.aws/speedtest-global-performance/
- **Format:** Parquet/WKT + Shapefile, ~610.8m hex tiles (zoom level 16).
- **Storage:** Anonymous S3, AWS Open Data Registry.
- **Cadence:** Quarterly.
- **License:** **CC BY-NC-SA 4.0 — non-commercial only.** Attribution required in every output.
- **Repo for ingestion examples:** https://github.com/teamookla/ookla-open-data

### Required attribution string

> Speed test data © Ookla, 2019–present, distributed under CC BY-NC-SA 4.0.

Place this in the Reflex UI footer and any exported PDF/CSV.

## Google Places API (New)

- **Pricing changed Mar 1 2025.** $200/mo pooled credit was retired.
- **`rating` and `userRatingCount` are Enterprise-tier fields.** Only **1,000 free events/month**.
- **Beyond free tier:** Place Details ~$17/1K, Text Search ~$32/1K.
- **Endpoint:** Place Details with FieldMask limited to `rating,userRatingCount,id,displayName`.
- **Auth:** API key + billing account (required even at free tier).

### Cost-control strategy

- 30-day TTL on ratings (they change slowly).
- Batch lookups per market.
- FieldMask must request only what we need — Enterprise-tier billing applies to the highest-tier field requested.
- Estimated usage: ~10 markets × ~10 providers/month = 100 calls/month, well within free tier.

## Provider 10-Ks (subscriber anchors)

Manually scraped once per quarter. Source of truth for national subscriber totals used in the penetration allocation.

| Provider | Source |
|---|---|
| Verizon Fios | Verizon Communications 10-K, Consumer Wireline segment |
| AT&T Fiber | AT&T 10-K, Consumer Wireline / Fiber sub count |
| Frontier | Frontier 10-K, Fiber broadband subs |
| Comcast/Xfinity | Comcast 10-K, Connectivity segment |
| Charter/Spectrum | Charter 10-K, Internet customer relationships |
| Optimum/Altice USA | Altice USA 10-K, Broadband customers |
| T-Mobile Home | T-Mobile 10-K, High Speed Internet customers |
| Verizon 5G Home | Verizon 10-K, Fixed Wireless Access subs |
| Lumen/Quantum | Lumen 10-K, Quantum Fiber subs |

Stored in `src/ftth_compete/data/providers.py` as a constant table with the as-of date.

## Deliberately excluded

- **BroadbandNow API.** Useful but paid; can revisit if free tier proves insufficient.
- **S&P Kagan / Leichtman.** Best-in-class but expensive paid feeds.
- **State PUC data.** Quality varies wildly state-to-state; not worth the integration cost in v1.
