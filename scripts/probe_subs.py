"""Inspect penetration estimates for Evans, CO."""

from __future__ import annotations

from ftth_compete.pipeline import run_market


def main() -> None:
    sheet = run_market("Evans", "CO", no_ratings=True)
    print(f"provider_subs rows: {len(sheet.provider_subs)}")
    print()
    print(f"{'Provider':<30} {'Tech':<8} {'Locs':>6} {'Rate':>6} "
          f"{'Mid':>6} {'Range':>15} {'Conf':<7}")
    print("-" * 90)
    for s in sorted(sheet.provider_subs, key=lambda x: -x["estimate_mid"]):
        name = s["canonical_name"][:30]
        tech = s["technology"]
        locs = s["locations_served"]
        rate = s["take_rate"]
        mid = s["estimate_mid"]
        rng = f"{s['estimate_low']:,}-{s['estimate_high']:,}"
        conf = s["confidence"]
        print(f"{name:<30} {tech:<8} {locs:>6,} {rate:>6.0%} "
              f"{mid:>6,} {rng:>15} {conf:<7}")


if __name__ == "__main__":
    main()
