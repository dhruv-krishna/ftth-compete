# Provider canonicalization

The single source of truth for provider names lives in `src/ftth_compete/data/providers.py`. This doc explains *why* each entry exists.

## Canonical entries (v1 target: ~50 covering >95% of US households)

### National fiber majors

| Canonical name | BDC raw aliases | Tech | Notes |
|---|---|---|---|
| Verizon Fios | "Verizon", "Verizon New York", "Cellco Partnership" (subset) | Fiber (50) | Northeast + mid-Atlantic |
| AT&T Fiber | "AT&T Inc.", "AT&T Mobility", "AT&T Services" | Fiber (50) | National, branded "AT&T Fiber" |
| Frontier Fiber | "Frontier Communications" | Fiber (50) | Heavy expansion 2023+ |
| Google Fiber | "Google Fiber Inc." | Fiber (50) | Limited markets |
| Lumen / Quantum Fiber | "CenturyLink", "Lumen Technologies", "Embarq" | Fiber (50) | Quantum Fiber is the consumer brand |
| Ziply Fiber | "Ziply Fiber" | Fiber (50) | PNW |

### Cable incumbents

| Canonical | BDC raw | Tech |
|---|---|---|
| Comcast / Xfinity | "Comcast Cable Communications, LLC" | Cable (40) + some Fiber (50) |
| Charter / Spectrum | "Charter Communications Operating, LLC" | Cable (40) + some Fiber (50) |
| Cox | "Cox Communications, Inc." | Cable (40) |
| Optimum / Altice USA | "CSC Holdings, LLC", "Altice USA", "Optimum" | Cable (40) + Fiber (50) |
| Mediacom | "Mediacom Communications" | Cable (40) |
| WOW! | "WideOpenWest" | Cable (40) + Fiber (50) |

### Fixed wireless / 5G home

| Canonical | BDC raw | Tech |
|---|---|---|
| T-Mobile Home Internet | "T-Mobile Wireless LLC", "T-Mobile USA" | Licensed FW (71) |
| Verizon 5G Home | "Verizon" subset | Licensed FW (71) |
| Starlink | "SpaceX Services, Inc." | Non-GSO Sat (61) |

### Regional fiber overbuilders & municipal

These are the long tail. Add aliases as encountered in BDC.

| Canonical | Notes |
|---|---|
| Allo Communications | NE / CO regional fiber |
| Tillman Fiber | Build-to-suit fiber overbuilder |
| GoNetspeed | Northeast |
| Brightspeed | CenturyLink copper spinoff |
| Conexon Connect | Rural electric co-op fiber |
| EPB Chattanooga | Municipal fiber |
| MetroNet | Midwest |
| Hotwire Communications | MDU specialist |

## Canonicalization rules

1. **Match BDC `provider_id` first.** FCC assigns stable IDs — the most reliable join key.
2. **Then match raw name with regex.** Case-insensitive, strip "LLC", "Inc.", "Communications", etc.
3. **Resolve corporate parents to consumer brand.** "CSC Holdings" → "Optimum", not "Altice USA".
4. **Don't over-merge.** Keep wireless and wireline subsidiaries of the same parent separate where they're tracked separately in BDC.
5. **Unknown providers stay unknown.** Log them, surface in the UI as "Other small providers." Manually curate into the registry over time.

## 10-K subscriber anchors

Used as inputs to `analysis/penetration.py`. Refresh annually in early March (most 10-Ks file Feb-Mar).

The anchor table maps canonical name → (national subs, as-of date, source URL). Source URL is captured so future verification is fast.
