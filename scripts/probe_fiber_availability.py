"""Probe: what's the REAL fiber-availability share for Evans CO?

Distinct from "% of providers offering fiber" — this counts locations where
ANY provider offers tech 50, divided by all serviceable locations.
"""

from __future__ import annotations

import duckdb

PARQUET = r"C:/Users/dkrishn3/ftth-compete-data/processed/bdc/2025-06-30/state=08.parquet"
EVANS_GEOIDS = ["08123001004", "08123001005", "08123001006", "08123001405"]


def main() -> None:
    geoid_list = ", ".join(f"'{g}'" for g in EVANS_GEOIDS)
    sql = f"""
    SELECT
        tract_geoid,
        COUNT(DISTINCT location_id) AS total_locations,
        COUNT(DISTINCT CASE WHEN technology = 50 THEN location_id END) AS fiber_locations,
        COUNT(DISTINCT CASE WHEN technology = 40 THEN location_id END) AS cable_locations,
        COUNT(DISTINCT CASE WHEN technology = 10 THEN location_id END) AS dsl_locations,
        COUNT(DISTINCT CASE WHEN technology IN (70, 71, 72) THEN location_id END) AS fw_locations,
        COUNT(DISTINCT CASE WHEN technology IN (60, 61) THEN location_id END) AS sat_locations
    FROM read_parquet('{PARQUET}')
    WHERE tract_geoid IN ({geoid_list})
    GROUP BY tract_geoid
    ORDER BY tract_geoid
    """
    con = duckdb.connect(":memory:")
    df = con.execute(sql).pl()
    print(df)

    # Market-level
    print()
    market_sql = f"""
    SELECT
        COUNT(DISTINCT location_id) AS total_locations,
        COUNT(DISTINCT CASE WHEN technology = 50 THEN location_id END) AS fiber_locations,
        COUNT(DISTINCT CASE WHEN technology = 40 THEN location_id END) AS cable_locations,
        COUNT(DISTINCT CASE WHEN technology = 10 THEN location_id END) AS dsl_locations
    FROM read_parquet('{PARQUET}')
    WHERE tract_geoid IN ({geoid_list})
    """
    res = con.execute(market_sql).fetchone()
    total, fiber, cable, dsl = res
    print(f"Market-level (across {len(EVANS_GEOIDS)} tracts):")
    print(f"  Total serviceable locations: {total:,}")
    print(f"  Fiber available:             {fiber:,}  ({100*fiber/total:.1f}%)")
    print(f"  Cable available:             {cable:,}  ({100*cable/total:.1f}%)")
    print(f"  DSL  available:              {dsl:,}  ({100*dsl/total:.1f}%)")


if __name__ == "__main__":
    main()
