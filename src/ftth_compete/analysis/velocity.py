"""Provider expansion velocity: per-(provider, tech) coverage delta between
two BDC releases.

Compares two `ProviderSummary` lists (typically current vs. ~12 months
ago) and produces one `CoverageVelocity` row per (canonical_name, tech_code)
that appears in either snapshot. Captures:

- absolute delta in locations served
- percent delta (None when prev was zero)
- "new offering" flag — provider didn't serve this tech in prev, does now
- "discontinued" flag — was offering it, no longer

Useful for telling the "who's expanding fiber?" / "who's pulling back DSL?"
story that the static snapshot doesn't show.
"""

from __future__ import annotations

from dataclasses import dataclass

from .competitors import ProviderSummary


@dataclass(frozen=True)
class CoverageVelocity:
    canonical_name: str
    technology: str
    tech_code: int
    current_locations: int
    prev_locations: int
    delta_abs: int  # current - prev (negative = shrinking)
    delta_pct: float | None  # delta / prev; None if prev was 0
    new_offering: bool  # prev == 0 and current > 0
    discontinued: bool  # prev > 0 and current == 0
    current_release: str
    prev_release: str


def compute(
    current: list[ProviderSummary],
    prev: list[ProviderSummary],
    *,
    current_release: str,
    prev_release: str,
) -> list[CoverageVelocity]:
    """Compute per-(provider, tech) location-count delta between two releases.

    Result is sorted by absolute delta descending — biggest expanders first,
    biggest contractors last.
    """
    cur_map: dict[tuple[str, int], ProviderSummary] = {
        (p.canonical_name, p.tech_code): p for p in current
    }
    prev_map: dict[tuple[str, int], ProviderSummary] = {
        (p.canonical_name, p.tech_code): p for p in prev
    }

    all_keys = set(cur_map.keys()) | set(prev_map.keys())

    out: list[CoverageVelocity] = []
    for key in all_keys:
        c = cur_map.get(key)
        p = prev_map.get(key)
        cur_locs = c.locations_served if c else 0
        prev_locs = p.locations_served if p else 0
        delta_abs = cur_locs - prev_locs
        delta_pct = (delta_abs / prev_locs) if prev_locs > 0 else None
        # Display fields come from whichever ProviderSummary exists.
        primary = c if c is not None else p
        assert primary is not None  # at least one must exist (key is in union)
        out.append(
            CoverageVelocity(
                canonical_name=primary.canonical_name,
                technology=primary.technology,
                tech_code=primary.tech_code,
                current_locations=cur_locs,
                prev_locations=prev_locs,
                delta_abs=delta_abs,
                delta_pct=delta_pct,
                new_offering=(prev_locs == 0 and cur_locs > 0),
                discontinued=(prev_locs > 0 and cur_locs == 0),
                current_release=current_release,
                prev_release=prev_release,
            )
        )

    out.sort(key=lambda v: -v.delta_abs)
    return out
