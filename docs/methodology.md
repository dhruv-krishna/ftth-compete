# Methodology

## Penetration estimation

The trickiest piece. Be honest about what's inferred.

### Inputs

- **FCC IAS** tract-level total subscribers by speed tier. ~1.5-2yr lag. Tract-resolved only at ≥200kbps and ≥25/3 Mbps; faster tiers are state-level only.
- **FCC BDC** which providers *can* serve each tract.
- **Provider 10-K subscriber totals** national counts for publicly-traded providers. See `providers.md`.

### Method

1. For each tract, get IAS total subs by tier.
2. Allocate tract total across providers proportional to:
   - (a) Provider's coverage share *within* that tract.
   - (b) Provider's national share among providers serving that tract.
3. Output as a **range**, not a point estimate:
   - **Low bound:** pure coverage-share allocation. Assumes equal subscriber distribution among co-serving providers.
   - **Mid:** weighted by national share.
   - **High bound:** maximum plausible if provider is locally dominant (capped by national cap).
4. Surface every output with:
   - Data freshness timestamp
   - Confidence label (high / medium / low) based on sample size and tier resolution

### What we don't claim

> "Verizon Fios has 41.3% penetration in tract 17031..."

is **wrong** for our data. We say:

> "Verizon Fios: estimated 30–55% of fiber subscribers in this tract. Based on Jun 2025 IAS, 1.5yr lag. Confidence: medium."

### Edge cases

- **Single-provider tracts.** No allocation needed; tract IAS total = that provider's subs.
- **No IAS data for tract.** Fall back to county-level estimate, flag clearly.
- **New entrants since IAS as-of date.** They appear in BDC but not IAS — show coverage only, no subscriber estimate, with explicit "too new for subscription data" label.

## Lens re-weighting

Pure function `analysis.lenses.apply(scores: pl.DataFrame, lens: str, incumbent: str | None) → pl.DataFrame`.

### Incumbent-defensive

- Adds `threat_score` column = weighted sum of:
  - 0.4 × competitor fiber coverage % (excluding incumbent)
  - 0.3 × competitor fiber expansion velocity (12mo BDC delta)
  - 0.2 × incumbent's measured-vs-advertised speed gap (lower advertised performance = more vulnerable)
  - 0.1 × competitor Google rating advantage over incumbent
- Sort descending by threat_score. Incumbent row pinned to top.

### New-entrant-offensive

- Adds `opportunity_score` column = weighted sum of:
  - 0.3 × underserved % (no fiber available)
  - 0.25 × cable-only % (vulnerable to fiber overbuild)
  - 0.2 × low-rating incumbent presence (avg incumbent rating < 3.0)
  - 0.15 × MDU density (faster build economics)
  - 0.1 × low Ookla measured speeds (delivery quality gap)
- Sort descending by opportunity_score.

### Neutral

- No re-weighting. Sort: alphabetical by canonical name, with major incumbents first.

## Speed gap (advertised vs measured)

Pure function `analysis.speeds.gap(bdc, ookla, geoids) → pl.DataFrame`.

For each (provider × tract):

- `advertised_down`: max BDC-reported download speed for that provider in that tract.
- `measured_down_p50`: median Ookla measured download in tiles intersecting that tract, *attributed to the provider* via Ookla's `provider_name` field.
- `gap_pct`: `(advertised - measured) / advertised`.

**Caveats.**
- Ookla provider attribution is imperfect (users self-report ISP, sometimes via auto-detect that gets it wrong).
- Tile coverage is sparse for low-density tracts. Show sample count alongside median.
- Don't compute the gap if sample count < 30. Show "insufficient measurements" instead.

## MDU/SFH split

Direct from ACS B25024 "Units in Structure":

- **SFH** = `B25024_002E` (1, detached) + `B25024_003E` (1, attached)
- **Small MDU** (2–4 units) = `B25024_004E` + `B25024_005E`
- **Mid MDU** (5–19) = `B25024_006E` + `B25024_007E`
- **Large MDU** (20+) = `B25024_008E` + `B25024_009E`
- **Other** (mobile home, boat, RV, van) = `B25024_010E` + `B25024_011E`

`mdu_share = (small + mid + large) / total`. Other excluded from denominator.

## Take-rate trajectory (Phase 6e)

**Source:** FCC public Form 477 / Internet Access Services tract-level subscription density, semi-annual releases 2014→present (releases pre-Jun 2020 fetched on demand; Dec 2022+ requires manual drop into `data/raw/ias/`).

**Scope:** Market-total only. FCC's per-(provider, tract) subscription data is filed under Q8 confidentiality and **not public** — state PUCs get it under NDA, the general public does not. The public CSVs are pre-aggregated to "connections per 1,000 households" with no provider identifiers.

**Bucket math:** FCC publishes density as a code 0–5; we use bucket midpoints `(0, 100, 300, 500, 700, 900)` and propagate `(low, high)` bounds for uncertainty. Two tiers: `bucket_all` (any speed ≥ 200 kbps) and `bucket_25` (≥ 25/3 Mbps, added ~Dec 2016). Older releases (2014–2016) only have `bucket_all`; we fall back to it when the 25/3 series is uniformly zero.

**What it tells you:** Whether a market's broadband take rate is growing, flat, or saturated. NOT which provider is winning the share — that requires sources we don't have.

**Caveats:** Pre-2020 census-tract IDs differ from post-2020 (decennial re-tracting). Missing GEOIDs contribute zero to the market average, which biases pre-2020 numbers slightly low for re-tracted markets. The trajectory is most meaningful for direction-of-travel, not absolute level.

## Open methodology questions

- How to handle providers that show up in BDC but with very small footprints — include in main analysis or roll into "other"? Currently leaning: <0.5% coverage = "other".
- Whether to weight the speed gap by tier (1Gbps advertised but 200Mbps measured is a bigger story than 100/100 advertised vs 80/80 measured).
