"""Lens re-weighting: re-rank and score providers under a strategic perspective.

Three lenses, all running over the same underlying data:

- **Neutral** (default): the canonical fiber-first ranking. No score column.
- **Incumbent-defensive**: pick an incumbent (e.g., the local cable operator).
  Other providers are ranked by their *threat* to that incumbent — fiber
  attack potential + coverage + Google-rating advantage. Incumbent pinned
  to the top.
- **New-entrant-offensive**: rank by *vulnerability to disruption* —
  cable-only providers, low-rating ones, providers without fiber. The
  list now reads as "easiest first to peel users from" / "best targets
  for fiber overbuild."

Lenses don't mutate the underlying ProviderSummary list. They wrap it in
ScoredProvider records carrying the lens-specific score and a label.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Final

from .competitors import ProviderSummary


class Lens(str, Enum):
    NEUTRAL = "neutral"
    DEFENSIVE = "defensive"
    OFFENSIVE = "offensive"


@dataclass(frozen=True)
class ScoredProvider:
    """A ProviderSummary plus optional lens score + label."""

    provider: ProviderSummary
    score: float | None  # 0.0-1.0, lens-specific. None for neutral / incumbent itself.
    score_label: str | None  # e.g., "Threat 0.8", "Vulnerability 0.4", "Incumbent"
    is_incumbent: bool = False


# Default rating used as a neutral fallback when Google ratings are missing
# (so the math doesn't blow up). 3.5 is roughly the median ISP rating
# observed across major US providers.
_DEFAULT_RATING: Final = 3.5


def apply(
    providers: list[ProviderSummary],
    lens: Lens | str,
    *,
    incumbent: str | None = None,
    rating_lookup: dict[str, dict[str, object]] | None = None,
) -> list[ScoredProvider]:
    """Score and re-rank `providers` under the chosen `lens`.

    Args:
        providers: Output of `analysis.competitors.score()`.
        lens: One of "neutral" / "defensive" / "offensive" (or `Lens` enum).
        incumbent: Required for defensive lens. Canonical name of the
            provider being defended (e.g., "Optimum"). Ignored otherwise.
        rating_lookup: TearSheet.provider_ratings dict. If absent or a
            provider's rating is None, `_DEFAULT_RATING` is used.

    Returns:
        Re-ordered list of ScoredProvider records.
    """
    lens_str = lens.value if isinstance(lens, Lens) else str(lens).lower()
    rating_lookup = rating_lookup or {}

    if lens_str == Lens.DEFENSIVE.value:
        return _score_defensive(providers, incumbent, rating_lookup)
    if lens_str == Lens.OFFENSIVE.value:
        return _score_offensive(providers, rating_lookup)
    # neutral / unknown
    return [ScoredProvider(p, None, None) for p in providers]


def _rating_for(name: str, rating_lookup: dict[str, dict[str, object]]) -> float:
    r = rating_lookup.get(name) or {}
    val = r.get("rating")
    try:
        return float(val) if val is not None else _DEFAULT_RATING
    except (TypeError, ValueError):
        return _DEFAULT_RATING


def _score_defensive(
    providers: list[ProviderSummary],
    incumbent: str | None,
    rating_lookup: dict[str, dict[str, object]],
) -> list[ScoredProvider]:
    """Defensive: rank by threat to the named incumbent."""
    if not incumbent:
        # No incumbent picked — fall back to neutral but explain via label.
        return [
            ScoredProvider(p, None, "Pick an incumbent in the sidebar")
            for p in providers
        ]

    incumbent_p = next((p for p in providers if p.canonical_name == incumbent), None)
    if incumbent_p is None:
        return [
            ScoredProvider(p, None, f"Incumbent {incumbent!r} not in this market")
            for p in providers
        ]

    inc_has_fiber = incumbent_p.has_fiber
    inc_rating = _rating_for(incumbent, rating_lookup)

    scored: list[ScoredProvider] = []
    for p in providers:
        if p.canonical_name == incumbent:
            scored.append(ScoredProvider(p, None, "Incumbent", is_incumbent=True))
            continue

        # 1) Fiber-attack potential. Highest weight: a fiber competitor when
        #    incumbent has no fiber is an existential threat.
        if p.has_fiber and not inc_has_fiber:
            fiber_attack = 1.0
        elif p.has_fiber and inc_has_fiber:
            fiber_attack = 0.5
        else:
            fiber_attack = 0.0

        # 2) Coverage — bigger competitor footprint = more direct overlap.
        coverage = max(0.0, min(1.0, p.coverage_pct or 0.0))

        # 3) Rating advantage over incumbent. Capped at +1.5 stars => 1.0
        comp_rating = _rating_for(p.canonical_name, rating_lookup)
        rating_advantage = max(0.0, min(1.0, (comp_rating - inc_rating) / 1.5))

        score = round(0.5 * fiber_attack + 0.3 * coverage + 0.2 * rating_advantage, 3)
        scored.append(
            ScoredProvider(p, score, f"Threat {score:.2f}")
        )

    # Pin incumbent to the top, then sort threats descending.
    inc_first = [s for s in scored if s.is_incumbent]
    rest = sorted(
        (s for s in scored if not s.is_incumbent),
        key=lambda s: (-(s.score or 0.0), s.provider.canonical_name),
    )
    return inc_first + rest


def _score_offensive(
    providers: list[ProviderSummary],
    rating_lookup: dict[str, dict[str, object]],
) -> list[ScoredProvider]:
    """Offensive: rank by vulnerability to disruption."""
    scored: list[ScoredProvider] = []
    for p in providers:
        # 1) No-fiber providers are most vulnerable to fiber overbuild.
        no_fiber = 0.0 if p.has_fiber else 1.0

        # 2) Cable-only is the classic disruption target — locked into HFC,
        #    upload-asymmetric, and increasingly rate-pressured by fiber.
        is_cable = p.category == "cable"
        cable_only = 1.0 if (is_cable and not p.has_fiber) else 0.0

        # 3) Low rating signals user dissatisfaction. Below 3.0 stars is
        #    actively bad. Above 4.0 contributes nothing.
        rating = _rating_for(p.canonical_name, rating_lookup)
        # rating_weakness in [0, 1]: 0 at 4+ stars, 1 at 1 star.
        rating_weakness = max(0.0, min(1.0, (4.0 - rating) / 3.0))

        score = round(0.5 * no_fiber + 0.3 * cable_only + 0.2 * rating_weakness, 3)
        scored.append(ScoredProvider(p, score, f"Vulnerability {score:.2f}"))

    scored.sort(key=lambda s: (-(s.score or 0.0), s.provider.canonical_name))
    return scored


def market_opportunity(
    providers: list[ProviderSummary] | None,
    *,
    rating_lookup: dict[str, dict[str, object]] | None = None,
    mdu_share: float | None = None,
) -> dict[str, object]:
    """Market-level offensive summary: should a new fiber entrant target this market?

    Returns a dict with:
      - `score`: 0-1 composite (higher = better target)
      - `factors`: dict of contributor name -> weight contribution (debug)
      - `headline`: short string ("Strong target" / "Weak target" / etc.)
    """
    if not providers:
        return {"score": None, "factors": {}, "headline": "No providers — cannot score."}

    rating_lookup = rating_lookup or {}
    n = len(providers)

    # 1) Underserved by fiber: providers that DON'T offer fiber as share of all.
    no_fiber_share = sum(1 for p in providers if not p.has_fiber) / n

    # 2) Cable-only providers as share of all.
    cable_only_share = (
        sum(1 for p in providers if p.category == "cable" and not p.has_fiber) / n
    )

    # 3) Average rating — low ratings = unhappy users = peelable.
    ratings = [_rating_for(p.canonical_name, rating_lookup) for p in providers]
    avg_rating = sum(ratings) / len(ratings) if ratings else _DEFAULT_RATING
    rating_weakness = max(0.0, min(1.0, (4.0 - avg_rating) / 3.0))

    # 4) MDU density — better build economics. Optional input.
    mdu_score = max(0.0, min(1.0, (mdu_share or 0.0) / 0.5))  # 0.5 = strong MDU market

    score = round(
        0.30 * no_fiber_share
        + 0.25 * cable_only_share
        + 0.20 * rating_weakness
        + 0.15 * mdu_score
        + 0.10 * (1.0 if n < 5 else 0.0),  # thin competition bonus
        3,
    )

    if score >= 0.55:
        headline = "Strong target"
    elif score >= 0.35:
        headline = "Moderate target"
    else:
        headline = "Weak target (saturated/competitive)"

    return {
        "score": score,
        "factors": {
            "no_fiber_share": no_fiber_share,
            "cable_only_share": cable_only_share,
            "rating_weakness": rating_weakness,
            "mdu_score": mdu_score,
        },
        "headline": headline,
    }
