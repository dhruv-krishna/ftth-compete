"""M-Lab (Measurement Lab) NDT speed-test ingest.

**Why this module exists:** Ookla's free open-data is tile aggregates only —
no `provider_name`. The Ookla per-test data with provider attribution is a
*paid* commercial product (Speedtest Intelligence). M-Lab is the actual free
path to per-test, provider-attributable speed measurements.

**What M-Lab is:** A Google / Internet Society / Princeton-led open-source
network measurement platform. They run the NDT (Network Diagnostic Tool)
speed test from points-of-presence near major CDNs. Every test is published
to a free BigQuery public dataset:

    measurement-lab.ndt.unified  (also: ndt.ndt7, ndt.ndt5)

Each test row includes the client's ASN (which we map to a canonical
provider name), geographic context (city, lat/lon, ISO-3166-2 subdivision),
and measured throughput + RTT.

**Caveats:**
- NDT vs Ookla: different methodology (single-stream TCP saturation vs
  multi-stream Ookla). Numbers are NOT directly comparable across the two,
  but they're comparable WITHIN each dataset.
- Sample bias: NDT users skew technical / engaged. WFH and gamer-heavy.
- Volume: lower than Ookla globally. Sparse-tract issues worse.
- BigQuery free tier: 1 TB of query data/month. M-Lab tables are large;
  every query needs tight `_TABLE_SUFFIX` + geographic filters. Cache
  aggressively.

**Auth model:** standard Google Cloud Application Default Credentials. The
user creates a GCP project (free tier), enables BigQuery API, downloads a
service-account JSON key, and sets `GOOGLE_APPLICATION_CREDENTIALS` in
`.env`. We detect missing creds and skip gracefully (same pattern as
`fcc_bdc`, `google_places`).

**Status:** SCAFFOLDING. The BigQuery client setup + the actual query SQL
need to be wired up in Phase 6d proper. This module establishes the data
shape, the integration contract with `analysis/penetration.py`, and the
ASN→provider mapping registry.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Final

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ASN → canonical provider mapping
#
# Each major US broadband provider operates under one or a small number of
# stable AS numbers (PeeringDB-confirmed). When a measurement's client ASN
# matches one of these, we attribute the test to that provider.
#
# **Maintenance.** Refresh from PeeringDB once a year or after M&A events
# (Frontier-Verizon CT 2014, Lumen rebrand, Altice US acquisitions, etc.).
# Multiple ASNs per provider are common because of historical acquisitions
# and regional operating-unit boundaries.
ASN_TO_CANONICAL: Final[dict[int, str]] = {
    # Comcast / Xfinity
    7922: "Xfinity",
    33651: "Xfinity",
    33652: "Xfinity",
    # Charter / Spectrum
    20115: "Spectrum",      # Charter Communications
    11427: "Spectrum",      # Time Warner Cable legacy
    7843: "Spectrum",       # TWC northeast
    11351: "Spectrum",      # TWC southeast
    10796: "Spectrum",      # TWC midwest
    12271: "Spectrum",      # TWC west
    33588: "Spectrum",      # Bright House Networks (now Spectrum)
    # Cox
    22773: "Cox",
    # Optimum / Altice USA
    6128: "Optimum",        # Cablevision legacy
    19108: "Optimum",       # Suddenlink legacy
    7029: "Optimum",        # Windstream-Altice peering (less common)
    # AT&T
    7018: "AT&T Fiber",     # AT&T national; covers both fiber + DSL — disambiguate at provider lookup
    20057: "AT&T Internet Air",  # AT&T Mobility / FWA
    # Verizon Fios
    701: "Verizon Fios",
    702: "Verizon Fios",
    703: "Verizon Fios",
    22394: "Verizon Fios",
    # Verizon Wireless / 5G Home
    6167: "Verizon 5G Home",
    # Frontier
    5650: "Frontier Fiber", # Frontier national; covers fiber + DSL
    # Lumen / CenturyLink / Quantum Fiber
    209: "Lumen / Quantum Fiber",
    3356: "Lumen / Quantum Fiber",
    # T-Mobile
    21928: "T-Mobile Home Internet",
    # Mediacom
    30036: "Mediacom",
    # WOW!
    12083: "WOW!",
    # Astound (RCN / Wave / Grande / enTouch)
    6079: "Astound Broadband",
    11232: "Astound Broadband",
    # Cable One (Sparklight)
    11492: "Cable One",
    # Ziply Fiber
    11404: "Ziply Fiber",
    # MetroNet
    11796: "MetroNet",
    # Google Fiber
    36492: "Google Fiber",
    # Starlink
    14593: "Starlink",
    # HughesNet (EchoStar)
    6621: "HughesNet",
    # Viasat
    7155: "Viasat",
    # Allo
    21501: "Allo Communications",  # placeholder — verify with PeeringDB
    # EPB Chattanooga
    11969: "EPB Chattanooga",
}


def asn_to_provider(asn: int | None) -> str | None:
    """Look up a canonical provider name from a client ASN.

    Returns None when the ASN isn't in our registry (skip the test or
    surface it for registry expansion).
    """
    if asn is None:
        return None
    return ASN_TO_CANONICAL.get(int(asn))


# ---------------------------------------------------------------------------
# Per-test record (one row of M-Lab data, post-normalization)

@dataclass(frozen=True)
class MLabTest:
    """One NDT speed test, normalized.

    Maps to M-Lab BigQuery row schema:
        date          -> test_date
        client.Network.ASNumber -> client_asn  -> provider via ASN_TO_CANONICAL
        client.Geo.City         -> client_city
        client.Geo.Latitude / Longitude -> client_lat / client_lon
        a.MeanThroughputMbps    -> download_mbps
        a.MinRTT                -> rtt_ms
    """

    test_date: date
    client_asn: int
    provider_canonical: str | None  # asn_to_provider() result
    client_city: str
    client_lat: float
    client_lon: float
    download_mbps: float
    rtt_ms: float


# ---------------------------------------------------------------------------
# Per-(provider, tract) aggregated output — consumed by penetration estimator

@dataclass(frozen=True)
class MLabProviderTractStats:
    """Aggregated M-Lab statistics for one (canonical_name, tract_geoid)."""

    canonical_name: str
    tract_geoid: str
    n_tests: int
    median_down_mbps: float
    p90_down_mbps: float
    median_rtt_ms: float
    share_of_tract_tests: float  # n_tests / total tests in this tract (0..1)
    quarter: str  # "2024-Q4"


# ---------------------------------------------------------------------------
# BigQuery client + query (scaffolding)

_BQ_TABLE: Final = "measurement-lab.ndt.unified"

# Reasonable defaults: pull the most recent complete quarter, filter to US
# only, exclude server-side measurements (we want client-side perspective).
# Joining to tracts happens client-side after coarse geographic filtering
# in SQL (BigQuery has limited spatial functions, but lat/lon + tract
# polygon intersect is cheap in Python after the rough filter).


def _quarter_table_suffix(d: date) -> str:
    """Generate the `_TABLE_SUFFIX` filter for a given quarter (M-Lab
    partitions tables by month). Filtering on _TABLE_SUFFIX is the way to
    cut BigQuery scan cost.

    Example: date(2024, 11, 1) -> BETWEEN '20241001' AND '20241231'
    """
    quarter = (d.month - 1) // 3
    start_month = quarter * 3 + 1
    end_month = start_month + 2
    year = d.year
    last_day = 31  # safe over-estimate for SUFFIX filter
    return (
        f"_TABLE_SUFFIX BETWEEN '{year}{start_month:02d}01' "
        f"AND '{year}{end_month:02d}{last_day:02d}'"
    )


def _build_query(
    lat_min: float, lat_max: float, lon_min: float, lon_max: float,
    quarter_date: date,
) -> str:
    """Compose the BigQuery SQL for a market bounding box + quarter.

    Returns a query string. Estimated scan size: 10-50 GB per US-metro
    bbox per quarter (well within the 1 TB/month free tier).
    """
    return f"""
    SELECT
      DATE(date) AS test_date,
      client.Network.ASNumber AS client_asn,
      client.Geo.City AS client_city,
      client.Geo.Latitude AS client_lat,
      client.Geo.Longitude AS client_lon,
      a.MeanThroughputMbps AS download_mbps,
      a.MinRTT AS rtt_ms
    FROM `{_BQ_TABLE}`
    WHERE {_quarter_table_suffix(quarter_date)}
      AND client.Geo.CountryCode = 'US'
      AND client.Geo.Latitude BETWEEN {lat_min} AND {lat_max}
      AND client.Geo.Longitude BETWEEN {lon_min} AND {lon_max}
      AND a.MeanThroughputMbps IS NOT NULL
      AND a.MeanThroughputMbps > 0
    """


def fetch_tests_for_bbox(
    lat_min: float, lat_max: float, lon_min: float, lon_max: float,
    quarter_date: date,
) -> list[MLabTest]:
    """Pull M-Lab tests within a geographic bounding box for a quarter.

    Phase 6d scaffolding — full implementation needs:
      1. `google-cloud-bigquery` Python client install (defer in pyproject
         until we commit to this path; it's a heavy dep with grpcio).
      2. Credential loading from `GOOGLE_APPLICATION_CREDENTIALS`.
      3. Query execution + Polars DataFrame conversion.
      4. Tract-polygon spatial join (reuse the pattern from `ookla.py`).
      5. Per-(provider, tract) aggregation -> MLabProviderTractStats.

    Until then, raises NotImplementedError to make the missing step
    explicit at call time.
    """
    raise NotImplementedError(
        "M-Lab BigQuery ingest is scaffolding-only. Phase 6d: install "
        "`google-cloud-bigquery`, set `GOOGLE_APPLICATION_CREDENTIALS`, "
        "and replace this stub with the BigQuery client call. The query "
        "SQL is ready in `_build_query()`."
    )


# ---------------------------------------------------------------------------
# Penetration integration contract

def shares_from_tests(
    tests: list[MLabTest], geoid_for_point: dict[tuple[float, float], str] | None = None,
) -> list[MLabProviderTractStats]:
    """Compute per-(provider, tract) stats from a list of MLabTest rows.

    Args:
        tests: pre-fetched M-Lab tests for a market.
        geoid_for_point: map (lat, lon) -> tract_geoid for the points we
            care about. Built by the caller via a GeoPandas spatial join
            of the test points against the market's tract polygons.

    Returns one MLabProviderTractStats per (canonical_name, tract_geoid)
    with at least N tests (we filter sparse cells to reduce noise).

    Downstream: penetration estimator uses `share_of_tract_tests` as a
    multiplier to allocate the IAS market-total across providers in
    proportion to their M-Lab dominance. Each test's provider is
    `asn_to_provider(client_asn)` — tests with unknown ASN are dropped.
    """
    raise NotImplementedError(
        "Aggregation pending: requires the geoid_for_point map (built via "
        "GeoPandas spatial join) and the per-tract grouping. Phase 6d proper."
    )
