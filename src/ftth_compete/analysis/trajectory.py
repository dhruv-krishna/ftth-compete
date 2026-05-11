"""Multi-release BDC trajectory: per-(provider, tech) location-count time series.

Whereas `velocity.py` compares two snapshots (current vs ~12 months ago),
this module assembles 3+ snapshots into a chronological series suitable for
sparklines. Tells the "how fast is overbuild happening?" story across
multiple BDC releases instead of a single year-over-year delta.

Series are sparse: a (provider, tech) key only contributes a point for
releases where it appeared in the BDC. Missing earlier points are rendered
as zero by convention (provider was not yet in the market at that release).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .competitors import ProviderSummary


@dataclass(frozen=True)
class TrajectoryPoint:
    release: str
    locations: int


@dataclass(frozen=True)
class ProviderTrajectory:
    canonical_name: str
    technology: str
    tech_code: int
    series: list[TrajectoryPoint] = field(default_factory=list)


def compute(
    snapshots: list[tuple[str, list[ProviderSummary]]],
) -> list[ProviderTrajectory]:
    """Build per-(canonical, tech) time series from a list of (release, snapshot) pairs.

    Args:
        snapshots: List of (release_date, ProviderSummary[]) tuples. Order
            does not matter — the result is sorted chronologically ASC.

    Returns:
        One ProviderTrajectory per (canonical_name, tech_code) seen in any
        snapshot. The `series` field has one TrajectoryPoint per input
        release (zero-filled for releases where the provider was absent).
    """
    if not snapshots:
        return []

    releases = sorted({r for r, _ in snapshots})
    by_key: dict[tuple[str, int], dict[str, int]] = {}
    meta: dict[tuple[str, int], tuple[str, str]] = {}  # (technology label, ignored)

    for release, snap in snapshots:
        for p in snap:
            key = (p.canonical_name, p.tech_code)
            by_key.setdefault(key, {})[release] = p.locations_served
            meta[key] = (p.technology, p.canonical_name)

    out: list[ProviderTrajectory] = []
    for key, per_release in by_key.items():
        tech_label, canonical = meta[key]
        series = [
            TrajectoryPoint(release=r, locations=int(per_release.get(r, 0)))
            for r in releases
        ]
        out.append(
            ProviderTrajectory(
                canonical_name=canonical,
                technology=tech_label,
                tech_code=key[1],
                series=series,
            )
        )

    # Sort by current (last) locations desc — biggest at top.
    out.sort(key=lambda t: -(t.series[-1].locations if t.series else 0))
    return out
