"""MDU vs SFH split from ACS B25024 (Units in Structure)."""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True)
class HousingSplit:
    sfh: int
    mdu_small: int  # 2-4 units
    mdu_mid: int    # 5-19 units
    mdu_large: int  # 20+ units
    mobile_home: int
    other: int
    total: int
    sfh_share: float
    mdu_share: float
    other_share: float


def _sum_int(df: pl.DataFrame, col: str) -> int:
    if df.is_empty() or col not in df.columns:
        return 0
    val = df[col].sum()
    return 0 if val is None else int(val)


def split(acs: pl.DataFrame) -> HousingSplit:
    """Aggregate B25024 unit-type counts and compute share buckets.

    SFH = 1-detached + 1-attached.
    MDU = anything 2+ units, broken into small (2-4), mid (5-19), large (20+).
    Other = mobile homes + boats/RVs/vans.
    Shares are over the total of all three groups; if total is 0, shares are 0.
    """
    sfh = _sum_int(acs, "units_1_detached") + _sum_int(acs, "units_1_attached")
    mdu_small = _sum_int(acs, "units_2") + _sum_int(acs, "units_3_4")
    mdu_mid = _sum_int(acs, "units_5_9") + _sum_int(acs, "units_10_19")
    mdu_large = _sum_int(acs, "units_20_49") + _sum_int(acs, "units_50_plus")
    mobile = _sum_int(acs, "units_mobile_home")
    other = _sum_int(acs, "units_other")

    total = sfh + mdu_small + mdu_mid + mdu_large + mobile + other
    if total == 0:
        return HousingSplit(0, 0, 0, 0, 0, 0, 0, 0.0, 0.0, 0.0)

    return HousingSplit(
        sfh=sfh,
        mdu_small=mdu_small,
        mdu_mid=mdu_mid,
        mdu_large=mdu_large,
        mobile_home=mobile,
        other=other,
        total=total,
        sfh_share=sfh / total,
        mdu_share=(mdu_small + mdu_mid + mdu_large) / total,
        other_share=(mobile + other) / total,
    )
