"""Per-(provider, technology) scoring for a market.

Takes the raw FCC BDC coverage_matrix frame (rows: tract x provider x tech)
and rolls it up to one record per (canonical_provider, technology) — NOT one
record per provider. This split is essential because providers like Lumen
sell fiber at 3 Gbps in some tracts and DSL at 100 Mbps in others; lumping
them together produces misleading "5,934 locations at 3 Gbps" cards. After
the split, we get separate "Lumen / Quantum Fiber — Fiber" (small footprint,
high speed) and "Lumen / Quantum Fiber — DSL" (large footprint, low speed)
records that are honest about what they are.

Helpers below let callers dedupe back to the canonical-provider level when
needed (e.g., for "active providers in market" KPIs).
"""

from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl

from ..data import providers as provider_registry
from ..data.fcc_bdc import TECH_FIBER, TECH_LABEL


@dataclass(frozen=True)
class ProviderSummary:
    """One provider's offering at a single technology in a market.

    For a multi-tech provider (e.g., Lumen offers DSL + Fiber, Comcast offers
    Cable + Fiber), `score()` produces multiple ProviderSummary rows — one per
    (canonical_name, tech_code) combination. Each row's `coverage_pct`,
    `locations_served`, and `max_advertised_*` are scoped to THAT tech only.
    """

    canonical_name: str
    holding_company: str
    category: str  # national_fiber / cable / fixed_wireless / satellite / regional_fiber / muni / unknown
    technology: str  # human label, e.g. "Fiber" / "Cable" / "DSL"
    tech_code: int  # FCC tech code, e.g. 50 = Fiber
    tracts_served: int  # tracts where this provider serves at THIS tech
    coverage_pct: float  # tracts_served / total_tracts
    locations_served: int  # sum across all tracts at THIS tech only
    has_fiber: bool  # True iff tech_code == 50 (kept as field for convenience)
    max_advertised_down: float | None
    max_advertised_up: float | None
    raw_brand_names: list[str] = field(default_factory=list)
    gig_locations: int = 0  # locations where max advertised down >= 1000 Mbps
    hundred_locations: int = 0  # locations where 100 <= max advertised down < 1000 Mbps
    sub_hundred_locations: int = 0  # locations where max advertised down < 100 Mbps


def score(coverage: pl.DataFrame, n_tracts: int) -> list[ProviderSummary]:
    """Aggregate raw BDC coverage matrix to per-(provider, tech) summaries.

    Args:
        coverage: Output of `data.fcc_bdc.coverage_matrix(geoids)`. Expected
            columns: tract_geoid, provider_id, brand_name, technology (int
            tech code), locations_served, max_down, max_up.
        n_tracts: Total tracts in the market (denominator for coverage %).

    Returns:
        List of ProviderSummary records, default-sorted fiber-first then by
        coverage descending. **One row per (canonical, tech_code)** —
        callers wanting per-provider rollups should use the helpers below.
    """
    if coverage.is_empty() or n_tracts == 0:
        return []

    # Backfill speed-tier columns with zeros if the upstream query didn't
    # include them (older fixtures / synthetic test data).
    for col in ("gig_locations", "hundred_locations", "sub_hundred_locations"):
        if col not in coverage.columns:
            coverage = coverage.with_columns(pl.lit(0).cast(pl.Int64).alias(col))

    # Resolve canonical name + holding + category per row. Canonicalization is
    # tech-aware (e.g. "Verizon"+71 -> Verizon 5G Home; "Verizon"+50 -> Verizon Fios).
    enriched = coverage.with_columns(
        [
            pl.struct(["brand_name", "technology"])
            .map_elements(_resolve, return_dtype=pl.Struct(_resolved_schema()))
            .alias("_resolved")
        ]
    ).unnest("_resolved")

    # Group by (canonical, tech) — the critical change. A multi-tech provider
    # produces multiple output rows.
    rolled = enriched.group_by(
        ["canonical_name", "holding_company", "category", "technology"]
    ).agg(
        [
            pl.col("tract_geoid").n_unique().alias("tracts_served"),
            pl.col("locations_served").sum().alias("locations_served"),
            pl.col("gig_locations").sum().alias("gig_locations"),
            pl.col("hundred_locations").sum().alias("hundred_locations"),
            pl.col("sub_hundred_locations").sum().alias("sub_hundred_locations"),
            pl.col("max_down").max().alias("max_advertised_down"),
            pl.col("max_up").max().alias("max_advertised_up"),
            pl.col("brand_name").unique().alias("raw_brand_names"),
        ]
    )

    summaries: list[ProviderSummary] = []
    for row in rolled.to_dicts():
        try:
            tech_code = int(row["technology"])
        except (TypeError, ValueError):
            continue
        tech_label = TECH_LABEL.get(tech_code, f"Tech {tech_code}")
        is_fiber = tech_code == TECH_FIBER
        tracts_served = int(row["tracts_served"])
        summaries.append(
            ProviderSummary(
                canonical_name=row["canonical_name"],
                holding_company=row["holding_company"],
                category=row["category"],
                technology=tech_label,
                tech_code=tech_code,
                tracts_served=tracts_served,
                coverage_pct=tracts_served / n_tracts,
                locations_served=int(row["locations_served"] or 0),
                gig_locations=int(row.get("gig_locations") or 0),
                hundred_locations=int(row.get("hundred_locations") or 0),
                sub_hundred_locations=int(row.get("sub_hundred_locations") or 0),
                has_fiber=is_fiber,
                max_advertised_down=(
                    float(row["max_advertised_down"])
                    if row["max_advertised_down"] is not None
                    else None
                ),
                max_advertised_up=(
                    float(row["max_advertised_up"])
                    if row["max_advertised_up"] is not None
                    else None
                ),
                raw_brand_names=list(row.get("raw_brand_names") or []),
            )
        )

    # Default sort: fiber rows first, then coverage desc, then locations desc,
    # then name+tech alphabetical. Callers can re-sort.
    summaries.sort(
        key=lambda s: (
            not s.has_fiber,
            -s.coverage_pct,
            -s.locations_served,
            s.canonical_name,
            s.technology,
        )
    )
    return summaries


# ---------------------------------------------------------------------------
# Per-canonical-provider rollups (helpers used by KPIs / market analyses)

def has_fiber_by_provider(rows: list[ProviderSummary]) -> dict[str, bool]:
    """Return {canonical_name: True} if ANY tech row for that provider is fiber."""
    out: dict[str, bool] = {}
    for r in rows:
        out[r.canonical_name] = out.get(r.canonical_name, False) or r.has_fiber
    return out


def distinct_providers(rows: list[ProviderSummary]) -> list[str]:
    """Distinct canonical provider names. Stable order (first-seen)."""
    seen: list[str] = []
    seen_set: set[str] = set()
    for r in rows:
        if r.canonical_name not in seen_set:
            seen.append(r.canonical_name)
            seen_set.add(r.canonical_name)
    return seen


def categories_by_provider(rows: list[ProviderSummary]) -> dict[str, set[str]]:
    """{canonical_name: {category, ...}} — usually a single category per provider."""
    out: dict[str, set[str]] = {}
    for r in rows:
        out.setdefault(r.canonical_name, set()).add(r.category)
    return out


# ---------------------------------------------------------------------------
# Internals

def _resolved_schema() -> dict[str, pl.DataType]:
    return {
        "canonical_name": pl.Utf8,
        "holding_company": pl.Utf8,
        "category": pl.Utf8,
    }


def _resolve(rec: dict[str, object]) -> dict[str, str]:
    brand = rec.get("brand_name")
    tech = rec.get("technology")
    tech_int: int | None = None
    if tech is not None:
        try:
            tech_int = int(tech)  # type: ignore[arg-type]
        except (ValueError, TypeError):
            tech_int = None

    p = provider_registry.canonicalize(
        brand_name=brand if isinstance(brand, str) else None,
        technology=tech_int,
    )
    if p is not None:
        return {
            "canonical_name": p.canonical,
            "holding_company": p.holding_company,
            "category": p.category,
        }
    return {
        "canonical_name": (str(brand).strip() if brand else "Unknown"),
        "holding_company": "Unknown",
        "category": "unknown",
    }
