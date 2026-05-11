"""Tests for ftth_compete.analysis.housing."""

from __future__ import annotations

import polars as pl

from ftth_compete.analysis.housing import split

UNIT_COLS = [
    "units_1_detached", "units_1_attached", "units_2", "units_3_4",
    "units_5_9", "units_10_19", "units_20_49", "units_50_plus",
    "units_mobile_home", "units_other",
]


def _frame(values: dict[str, float]) -> pl.DataFrame:
    schema = {col: pl.Float64 for col in UNIT_COLS}
    row = {col: values.get(col, 0.0) for col in UNIT_COLS}
    return pl.DataFrame([row], schema=schema)


def test_empty_frame() -> None:
    s = split(pl.DataFrame())
    assert s.total == 0
    assert s.sfh_share == 0.0
    assert s.mdu_share == 0.0


def test_pure_sfh() -> None:
    s = split(_frame({"units_1_detached": 100.0}))
    assert s.sfh == 100
    assert s.mdu_share == 0.0
    assert s.sfh_share == 1.0


def test_mdu_buckets() -> None:
    s = split(_frame({
        "units_2": 10.0, "units_3_4": 20.0,
        "units_5_9": 30.0, "units_10_19": 40.0,
        "units_20_49": 50.0, "units_50_plus": 60.0,
    }))
    assert s.mdu_small == 30
    assert s.mdu_mid == 70
    assert s.mdu_large == 110
    assert s.mdu_share == 1.0


def test_mixed_with_other() -> None:
    s = split(_frame({
        "units_1_detached": 800.0,
        "units_5_9": 100.0,
        "units_mobile_home": 100.0,
    }))
    assert s.total == 1000
    assert s.sfh_share == 0.80
    assert s.mdu_share == 0.10
    assert s.other_share == 0.10
