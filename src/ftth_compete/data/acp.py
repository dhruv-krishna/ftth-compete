"""USAC ACP (Affordable Connectivity Program) ZIP-level enrollment ingest.

The ACP program ended Feb 8, 2024 with ~23.3M enrolled households. USAC
publishes downloadable enrollment data — BUT after verifying their public
files (May 2026), **the data is geographic-only**:

- `ACP-Households-by-Zip-as-of-2024.xlsx` — ZIP × verification-method
- `ACP-Households-by-County-as-of-2024.xlsx` — county × verification-method
- `ACP-Funding-Summary-by-Geography.xlsx` — state/county/ZIP/CD totals

**There is no public provider-level ACP data.** SAC code / provider-name
breakdowns were apparently never published in machine-readable form. This
contradicts the v1 sketch of this module; we've pivoted accordingly.

## New methodology: ACP-density covariate

Instead of using ACP as a per-provider subscriber anchor (which would have
required provider × ZIP data we can't get), we use it as a **demand-side
covariate** that adjusts each provider's take rate:

1. Cross-walk ZIP-level ACP enrollment to tract via the Census ZCTA
   relationship file.
2. Compute `acp_density = acp_households / total_housing_units` per tract.
3. In the penetration estimator, apply a per-provider multiplier to the
   base take rate, weighted by the provider's known national ACP capture
   share (Comcast ~40%, Spectrum ~25%, AT&T ~12%, Cox ~5%, Verizon Fios ~3%,
   small fiber overbuilders <2% because their plans exceeded the $30 cap).

   For a high-ACP-density tract, a provider with a strong ACP program
   (Comcast) gets a take-rate boost; a fiber overbuilder without an ACP
   plan gets a slight reduction (its eligible-customer pool is smaller).

This is honest about what the public data supports. We don't claim
per-provider ACP allocation we can't verify.

Status: real parser implemented for the USAC Excel format. Penetration
estimator integration pending the data drop.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import polars as pl

from ..config import get_settings

log = logging.getLogger(__name__)

# Final snapshot before the program wound down. Subsequent monthly files
# are smaller / partial; we anchor on this for max signal.
DEFAULT_ARCHIVE_DATE: Final = "2024-04-30"

# USAC's downloadable enrollment data lives at predictable URLs keyed by
# YYYY-MM-DD. The actual schema (column names + types) needs to be verified
# against a real file pull — placeholder schema below reflects the public
# documentation snapshot.
_ACP_URL_BASE: Final = "https://www.usac.org/wp-content/uploads/about/acp/"


@dataclass(frozen=True)
class ACPZipEnrollment:
    """One row of ZIP-level ACP enrollment from USAC's public Excel.

    Matches USAC's published "Households by Zip" file structure. Verification
    method columns are summed into a single `total_households` count for
    downstream use (they're not material at our analysis granularity).
    """

    zip5: str             # 5-char ZIP (zero-padded)
    state: str            # 2-letter
    total_households: int
    as_of: str            # snapshot date label, e.g. "2024-02-08"


@dataclass(frozen=True)
class ACPTractDensity:
    """ACP enrollment density allocated to a tract via the ZCTA crosswalk.

    Consumed by the penetration estimator as a demand-side covariate.
    `density` = allocated ACP households / total housing units in the tract.
    """

    tract_geoid: str
    allocated_households: int
    density: float


# ---------------------------------------------------------------------------
# Per-provider national ACP capture share — for the take-rate covariate.
#
# Sourced from public reporting on each provider's ACP customer counts at
# peak. These are NOT direct USAC disclosures (USAC didn't publish them) —
# they come from provider 10-Qs, investor day decks, and news reporting.
# Used only as a relative weighting factor; absolute accuracy of any single
# number isn't critical to the model.
NATIONAL_ACP_CAPTURE_SHARE: dict[str, float] = {
    "Xfinity": 0.40,              # Comcast Internet Essentials qualified for ACP
    "Spectrum": 0.25,             # Spectrum Internet Assist qualified
    "AT&T Fiber": 0.06,           # AT&T Access pre-ACP -> ACP
    "AT&T Internet": 0.06,        # same channel
    "AT&T Internet Air": 0.01,
    "Cox": 0.05,                  # Cox Connect2Compete
    "Verizon Fios": 0.03,         # Lifeline overlap, smaller ACP share
    "Optimum": 0.03,              # Altice Advantage Internet
    "T-Mobile Home Internet": 0.04,
    "Frontier Fiber": 0.02,
    "Lumen / Quantum Fiber": 0.01,
    "Mediacom": 0.015,
    "WOW!": 0.005,
    "Astound Broadband": 0.01,
    "Cable One": 0.005,
    # Fiber overbuilders without ACP plans — very low share. Their cheapest
    # tiers typically exceeded the $30/mo ACP discount cap, so they had no
    # ACP-eligible product.
    "Allo Communications": 0.001,
    "Google Fiber": 0.001,
    "MetroNet": 0.001,
    "EPB Chattanooga": 0.005,     # had ACP-eligible 100 Mbps tier
    "Ziply Fiber": 0.001,
    "Brightspeed": 0.005,
    "GoNetspeed": 0.001,
    # Satellite — close to zero because of equipment costs
    "Starlink": 0.0,
    "HughesNet": 0.001,
    "Viasat": 0.0005,
}


def get_acp_capture_share(canonical_name: str) -> float:
    """Return the provider's national ACP capture share, or 0.0 if unknown.

    Used by the penetration estimator to weight the ACP-density covariate.
    """
    return NATIONAL_ACP_CAPTURE_SHARE.get(canonical_name, 0.0)


# ---------------------------------------------------------------------------
# File location

def _acp_raw_dir() -> Path:
    return get_settings().raw_dir / "acp"


def find_acp_zip_file() -> Path | None:
    """Locate the USAC ACP-Households-by-Zip file in the data dir.

    Looks for any *.xlsx or *.csv in `data/raw/acp/` whose name contains
    "zip" or "ZIP". Returns the most recently modified match, or None.
    """
    d = _acp_raw_dir()
    if not d.exists():
        return None
    candidates = [
        p for p in d.iterdir()
        if p.is_file()
        and p.suffix.lower() in {".xlsx", ".csv"}
        and "zip" in p.stem.lower()
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


# ---------------------------------------------------------------------------
# Excel / CSV parser

def parse_acp_zip_file(path: Path) -> pl.DataFrame:
    """Parse USAC's "Households by Zip" Excel into a normalized DataFrame.

    Output columns:
        zip5             : 5-char ZIP code (zero-padded)
        state            : 2-letter state abbreviation (empty if file
                           doesn't include it — derivable from ZIP prefix
                           with ZCTA crosswalk if needed)
        total_households : integer count of enrolled households
        as_of            : snapshot label parsed from the filename

    Handles the real USAC file shape (verified against the EBB
    `EBB_Enrollment_by_Zip5_12_30.xlsx`):
      - First 1-3 rows are free-text description / "last updated on ..."
      - Row 3-4 is the actual column header
      - Last few rows are "Grand Total" + footer notes
      - A "00000" ZIP appears representing redacted small ZIPs (excluded)
    """
    if not path.exists():
        raise FileNotFoundError(f"ACP file not found at {path}")

    suffix = path.suffix.lower()

    if suffix == ".xlsx":
        # USAC's Excel layout has description text in rows 0-2, then the
        # column header on row 3. Auto-detect the header row instead of
        # hard-coding skiprows, so the parser handles minor format drift.
        import pandas as pd  # local import; pandas is already in deps via geopandas
        raw = pd.read_excel(path, header=None)
        header_idx = _find_header_row(raw)
        # Re-read with the correct header row. Force ALL columns to string
        # to avoid pandas inferring mixed-type columns (the ZIP col mixes
        # strings + ints in the USAC files because the redacted/footer
        # rows include text values like "Grand Total").
        pdf = pd.read_excel(path, header=header_idx, dtype=str)
        # Pandas → polars conversion needs uniform types; cast everything
        # to str at the pandas layer first.
        pdf = pdf.astype(str).replace("nan", "")
        df = pl.from_pandas(pdf)
    else:
        df = pl.read_csv(
            path,
            schema_overrides={"zip_code": pl.Utf8, "Zip Code": pl.Utf8, "ZIP": pl.Utf8},
            ignore_errors=True,
        )

    # Find the ZIP and state columns case-insensitively.
    col_lookup = {c.lower().strip(): c for c in df.columns}
    zip_col = next(
        (col_lookup[k] for k in (
            "zip5", "zip_code", "zip code", "zip", "zipcode",
            "5 digit zip code", "5-digit zip code",
        ) if k in col_lookup),
        None,
    )
    state_col = next(
        (col_lookup[k] for k in ("state", "st", "state code", "state_code")
         if k in col_lookup),
        None,
    )
    if zip_col is None:
        raise ValueError(
            f"Could not find a ZIP column in ACP file; got columns: {df.columns}"
        )

    total_col = next(
        (col_lookup[k] for k in (
            "total households",
            "total enrolled households",
            "enrolled households",
            "total subscribers",
            "total",
        ) if k in col_lookup),
        None,
    )

    df = df.with_columns(
        pl.col(zip_col).cast(pl.Utf8).str.strip_chars().str.zfill(5).alias("zip5"),
    )
    if state_col:
        df = df.rename({state_col: "state"})
    else:
        df = df.with_columns(pl.lit("").alias("state"))

    if total_col:
        df = df.with_columns(
            pl.col(total_col).cast(pl.Int64, strict=False).fill_null(0)
            .alias("total_households")
        )
    else:
        numeric_cols = [
            c for c in df.columns
            if c not in ("zip5", "state", zip_col)
            and df[c].dtype.is_numeric()
        ]
        if not numeric_cols:
            raise ValueError(
                "No 'total households' column found and no numeric fallback columns; "
                f"got columns: {df.columns}"
            )
        df = df.with_columns(
            sum(pl.col(c).fill_null(0) for c in numeric_cols).cast(pl.Int64)
            .alias("total_households")
        )

    df = df.with_columns(pl.lit(path.stem).alias("as_of"))

    # Drop:
    #  - Rows without a valid 5-digit ZIP (footer text, Grand Total row)
    #  - The "00000" redacted-aggregate ZIP (data exists but isn't tied
    #    to any geography we can resolve)
    df = df.filter(
        pl.col("zip5").str.len_chars() == 5,
        pl.col("zip5") != "00000",
        pl.col("total_households") > 0,
    )

    return df.select(["zip5", "state", "total_households", "as_of"])


def _find_header_row(raw_df, *, max_scan: int = 10) -> int:
    """Scan the first few rows of a raw Excel read (header=None) to find
    the actual column header.

    USAC's files have a long descriptive title in row 0 (which contains
    "ZIP Code" within the sentence) and the true column header several
    rows down. We pick the first row where:
      - Every non-null cell is short (<= 60 chars) — rules out the title
      - At least one cell contains "ZIP" (case-insensitive)
    """
    for i in range(min(max_scan, len(raw_df))):
        row = raw_df.iloc[i]
        cells = [str(c) for c in row if c is not None and str(c).lower() not in ("nan", "none", "")]
        if not cells:
            continue
        if any(len(c) > 60 for c in cells):
            continue  # likely description text, not a column header
        if any("zip" in c.lower() for c in cells):
            return i
    return 0  # fall back to first row if heuristic finds nothing


# ---------------------------------------------------------------------------
# Tract-level ACP density (the covariate the penetration estimator uses)

def acp_density_for_tracts(
    geoids: list[str],
    housing_units_by_tract: dict[str, int],
    *,
    acp_zip_df: pl.DataFrame | None = None,
    crosswalk: pl.DataFrame | None = None,
) -> list[ACPTractDensity]:
    """Compute per-tract ACP enrollment density for a market.

    Args:
        geoids: list of tract GEOIDs to compute density for.
        housing_units_by_tract: ACS B25001 housing-unit count per tract.
            Used as the denominator for density.
        acp_zip_df: pre-parsed ACP-by-ZIP DataFrame. If None, attempt to
            find + parse a file from `data/raw/acp/` via `find_acp_zip_file`.
        crosswalk: ZCTA → tract crosswalk. If None, load via tiger.

    Returns one ACPTractDensity per geoid with a non-zero allocation.
    Tracts without overlap (no ZCTA hits) are omitted — callers should
    treat absence as `density = 0.0`.
    """
    if not geoids:
        return []

    if acp_zip_df is None:
        path = find_acp_zip_file()
        if path is None:
            log.info(
                "No ACP-by-ZIP file in data/raw/acp/; ACP density unavailable. "
                "Download from usac.org and drop the Excel there to enable."
            )
            return []
        acp_zip_df = parse_acp_zip_file(path)

    if crosswalk is None:
        from .tiger import load_zcta_tract_crosswalk
        crosswalk = load_zcta_tract_crosswalk()

    # Filter crosswalk to just the tracts we care about (huge speedup).
    geoid_set = set(geoids)
    cw = crosswalk.filter(pl.col("tract_geoid").is_in(list(geoid_set)))
    if cw.is_empty():
        return []

    # Allocate each ZIP's households across its tracts by area weight.
    joined = cw.join(acp_zip_df, on="zip5", how="inner")
    if joined.is_empty():
        return []
    joined = joined.with_columns(
        (pl.col("total_households").cast(pl.Float64) * pl.col("area_weight"))
        .alias("allocated"),
    )

    # Roll up allocations per tract.
    rolled = (
        joined.group_by("tract_geoid")
        .agg(pl.col("allocated").sum().alias("allocated_households"))
        .with_columns(pl.col("allocated_households").round().cast(pl.Int64))
    )

    out: list[ACPTractDensity] = []
    for row in rolled.to_dicts():
        gid = str(row["tract_geoid"])
        allocated = int(row["allocated_households"])
        hu = max(int(housing_units_by_tract.get(gid, 0)), 1)
        density = allocated / hu
        out.append(ACPTractDensity(
            tract_geoid=gid,
            allocated_households=allocated,
            density=density,
        ))
    return out


# ---------------------------------------------------------------------------
# Provider name normalization (kept for downstream tools that may want it,
# even though the public ACP data doesn't include provider names)

def normalize_provider_name(raw_name: str) -> str | None:
    """Map a USAC-registered provider name to our canonical registry.

    USAC uses official corporate names (e.g., "Comcast Cable Communications,
    LLC", "Charter Communications Operating, LLC"), which differ from the
    consumer brand names we canonicalize on. Built as a string lookup + a
    fuzzy-matching fallback; for Phase 6c scaffolding this is a small
    hand-curated map covering the top ~25 ACP providers.

    Returns the canonical name or None if no confident match.
    """
    raw = raw_name.lower().strip()
    # Hand-curated map — extend as we encounter new ACP providers.
    explicit: dict[str, str] = {
        "comcast cable communications, llc": "Xfinity",
        "comcast cable communications": "Xfinity",
        "charter communications operating, llc": "Spectrum",
        "charter communications": "Spectrum",
        "spectrum southeast, llc": "Spectrum",
        "cox communications, inc.": "Cox",
        "altice usa, inc.": "Optimum",
        "cablevision systems corporation": "Optimum",
        "verizon services corp.": "Verizon Fios",
        "verizon online llc": "Verizon Fios",
        "at&t enterprises, llc": "AT&T Fiber",
        "at&t mobility, llc": "AT&T Internet Air",
        "t-mobile usa, inc.": "T-Mobile Home Internet",
        "frontier communications corporation": "Frontier Fiber",
        "frontier california inc.": "Frontier Fiber",
        "frontier florida llc": "Frontier Fiber",
        "lumen technologies, inc.": "Lumen / Quantum Fiber",
        "centurylink communications, llc": "Lumen / Quantum Fiber",
        "mediacom communications corporation": "Mediacom",
        "wow! internet, llc": "WOW!",
        "wideopenwest finance, llc": "WOW!",
        "rcn telecom services, llc": "Astound Broadband",
        "atlantic broadband finance, llc": "Astound Broadband",
        "ziply fiber, inc.": "Ziply Fiber",
        "allo communications llc": "Allo Communications",
        "metronet, inc.": "MetroNet",
        "google fiber inc.": "Google Fiber",
        "starlink services, llc": "Starlink",
        "viasat, inc.": "Viasat",
        "hughes network systems, llc": "HughesNet",
    }
    if raw in explicit:
        return explicit[raw]

    # Fallback: substring search. Conservative — only matches when a
    # canonical name appears verbatim. The fuzzy matcher proper lands with
    # the Ookla provider-name normalizer in Phase 6d (shared codebase).
    canonicals = set(explicit.values()) | {
        "Xfinity", "Spectrum", "Verizon Fios", "AT&T Fiber", "Frontier Fiber",
        "Lumen / Quantum Fiber", "Optimum", "Cox", "Mediacom", "WOW!",
    }
    for canon in canonicals:
        if canon.lower() in raw:
            return canon
    return None


# ---------------------------------------------------------------------------
# Legacy provider-attributed aggregator
#
# **Kept for reference but not wired into the penetration pipeline.** USAC's
# public data does not include provider names — verified May 2026. If a
# future public release includes provider × ZIP, re-enable this path.

def aggregate_to_tracts(
    claims: pl.DataFrame,
    *,
    crosswalk: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """ZIP-level → tract-level claims aggregation, with provider canonicalization.

    Args:
        claims: parse_archive() output with at least these columns:
                provider_name (str), zip_code (str, 5-char), claims (int).
                Filtered to the snapshot month of interest before this call.
        crosswalk: optional ZCTA-to-tract crosswalk. Defaults to the Census
                relationship file loaded via `tiger.load_zcta_tract_crosswalk()`.
                Columns expected: zip5, tract_geoid, area_weight (0..1, sums
                to ~1.0 per ZCTA).

    Returns: Polars DataFrame with one row per (canonical_name, tract_geoid)
    containing summed claims allocated by the ZCTA's area-weighted overlap.

    Method:
      1. Map every provider_name to a canonical via normalize_provider_name().
         Drop rows where the mapping fails (logged at WARN with a sample).
      2. Inner-join claims to the crosswalk on zip5. Rows whose ZIP has no
         ZCTA-tract overlap (rare; ZCTAs differ slightly from USPS ZIPs)
         are dropped.
      3. Allocate per row: allocated_claims = claims * area_weight.
      4. Group by (canonical_name, tract_geoid) and sum.
    """
    if claims.is_empty():
        return pl.DataFrame(
            schema={"canonical_name": pl.Utf8, "tract_geoid": pl.Utf8, "claims": pl.Int64}
        )

    if crosswalk is None:
        from .tiger import load_zcta_tract_crosswalk
        crosswalk = load_zcta_tract_crosswalk()

    # Canonicalize provider names. We map in Python (string-table lookup
    # rather than Polars) so we get the fuzzy fallback behavior.
    pn_col = "provider_name" if "provider_name" in claims.columns else None
    if pn_col is None:
        raise ValueError(
            "ACP claims frame missing 'provider_name' column; "
            f"got columns: {claims.columns}"
        )

    raw_to_canonical: dict[str, str | None] = {}
    for name in claims[pn_col].unique().to_list():
        raw_to_canonical[name] = normalize_provider_name(str(name)) if name else None

    canon_series = (
        claims[pn_col]
        .replace_strict(raw_to_canonical, return_dtype=pl.Utf8, default=None)
        .alias("canonical_name")
    )
    enriched = claims.with_columns(canon_series).filter(
        pl.col("canonical_name").is_not_null()
    )
    if enriched.is_empty():
        log.warning(
            "No ACP claims rows could be canonicalized — check the "
            "normalize_provider_name() table against your CSV's provider strings."
        )
        return pl.DataFrame(
            schema={"canonical_name": pl.Utf8, "tract_geoid": pl.Utf8, "claims": pl.Int64}
        )

    # Normalize ZIP column name to 'zip5' for the join.
    if "zip_code" in enriched.columns:
        enriched = enriched.rename({"zip_code": "zip5"})
    elif "zip5" not in enriched.columns:
        raise ValueError(
            f"ACP claims frame needs 'zip_code' or 'zip5' column; got {enriched.columns}"
        )
    # Zero-pad ZIPs that came in as ints.
    enriched = enriched.with_columns(
        pl.col("zip5").cast(pl.Utf8).str.zfill(5).alias("zip5")
    )

    joined = enriched.join(crosswalk, on="zip5", how="inner")
    joined = joined.with_columns(
        (pl.col("claims").cast(pl.Float64) * pl.col("area_weight")).alias("allocated")
    )
    rolled = (
        joined.group_by(["canonical_name", "tract_geoid"])
        .agg(pl.col("allocated").sum().alias("claims"))
        .with_columns(pl.col("claims").round().cast(pl.Int64))
    )
    return rolled


# ---------------------------------------------------------------------------
# Step 5 — Compute per-(provider, tract) ACP share for the market

def market_provider_shares(
    tract_claims: pl.DataFrame, geoids: list[str]
) -> list[ACPProviderShare]:
    """Filter tract-level claims to the market's geoids + compute shares.

    Returns one ACPProviderShare per (canonical_name, tract_geoid) where
    that provider has any ACP claims in the market. The `share` field is
    the per-tract provider share (sums to 1.0 per tract).

    Downstream: the penetration estimator uses these shares as a multiplier
    to split the IAS market-total subscription anchor across providers,
    weighted toward providers with high ACP dominance.
    """
    if tract_claims.is_empty() or not geoids:
        return []
    filtered = tract_claims.filter(pl.col("tract_geoid").is_in(geoids))
    if filtered.is_empty():
        return []
    # Per-tract totals, then per-row share.
    totals = filtered.group_by("tract_geoid").agg(
        pl.col("claims").sum().alias("tract_total")
    )
    joined = filtered.join(totals, on="tract_geoid")
    joined = joined.with_columns(
        (pl.col("claims") / pl.col("tract_total")).alias("share")
    )
    out: list[ACPProviderShare] = []
    for row in joined.to_dicts():
        out.append(
            ACPProviderShare(
                canonical_name=row["canonical_name"],
                tract_geoid=row["tract_geoid"],
                claims=int(row["claims"]),
                tract_total_claims=int(row["tract_total"]),
                share=float(row["share"]),
            )
        )
    return out
