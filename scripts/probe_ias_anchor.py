"""Inspect IAS-calibrated penetration estimates for Evans, CO."""

from __future__ import annotations

from ftth_compete.pipeline import run_market


def main() -> None:
    sheet = run_market("Evans", "CO", no_ratings=True)
    print(f"IAS note: {sheet.ias_note}")
    print()

    anchor = sheet.market_subs_anchor
    if anchor:
        print("=== Market subscription anchor (FCC IAS) ===")
        print(f"  IAS release:        {anchor['ias_release']}")
        print(f"  Tracts with data:   {anchor['n_tracts_with_data']}")
        print(f"  Total housing units:{anchor['total_housing_units']:,}")
        print(f"  Market subs (lo/mid/hi): "
              f"{anchor['market_subs_low']:,} / "
              f"{anchor['market_subs_mid']:,} / "
              f"{anchor['market_subs_high']:,}")
        print(f"  Take rate (mid):    {anchor['take_rate_mid']:.1%}")

    print()
    print("=== Per-provider subs after IAS calibration ===")
    print(f"{'Provider':<32} {'Tech':<10} {'Locs':>6} {'Mid':>6} {'Conf':<7}")
    print("-" * 80)
    for s in sorted(sheet.provider_subs, key=lambda x: -x["estimate_mid"]):
        name = s["canonical_name"][:32]
        tech = s["technology"]
        locs = s["locations_served"]
        mid = s["estimate_mid"]
        conf = s["confidence"]
        print(f"{name:<32} {tech:<10} {locs:>6,} {mid:>6,} {conf:<7}")

    print()
    total_mid = sum(s["estimate_mid"] for s in sheet.provider_subs)
    print(f"Sum of per-provider mid estimates: {total_mid:,}")
    if anchor:
        anchor_mid = anchor["market_subs_mid"]
        print(f"IAS market anchor mid:             {anchor_mid:,}")
        if anchor_mid > 0:
            print(f"Ratio (sum / anchor):              {total_mid / anchor_mid:.2f}")


if __name__ == "__main__":
    main()
