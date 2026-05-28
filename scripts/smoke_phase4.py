"""Phase 4 smoke test: full pipeline including Ookla speeds for Evans, CO."""

from __future__ import annotations

import time

from ftth_compete.pipeline import run_market


def main() -> None:
    t0 = time.time()
    sheet = run_market("Evans", "CO")
    dt = time.time() - t0
    print(f"Full pipeline (warm): {dt:.1f}s")
    print(f"tract_acs rows:       {len(sheet.tract_acs)}")
    print(f"coverage_matrix rows: {len(sheet.coverage_matrix)}")
    print(f"providers:            {len(sheet.providers) if sheet.providers else 0}")
    print(f"tract_speeds rows:    {len(sheet.tract_speeds)}")

    if sheet.tract_speeds:
        print("\nMeasured speeds per tract:")
        for t in sheet.tract_speeds:
            geoid = t["tract_geoid"]
            d = t["median_down_mbps"]
            u = t["median_up_mbps"]
            lat = t["median_lat_ms"]
            n = t["n_tests"]
            print(f"  {geoid}: {d:.0f}/{u:.0f} Mbps, {lat:.0f}ms latency, {n} tests")

    print(f"\nspeeds_note: {sheet.speeds_note}")
    print(f"data_versions: {sheet.data_versions}")


if __name__ == "__main__":
    main()
