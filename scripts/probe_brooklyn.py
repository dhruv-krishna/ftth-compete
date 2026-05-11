"""Smoke test Brooklyn, NY end-to-end (demographics only — fast)."""

from __future__ import annotations

from ftth_compete.pipeline import run_market


def main() -> None:
    sheet = run_market("Brooklyn", "NY", no_providers=True, no_speeds=True, no_ratings=True)
    m = sheet.market
    d = sheet.demographics
    h = sheet.housing
    print(f"Resolved: {m['city']}")
    print(f"  state_fips={m['state_fips']}  place_geoid={m['place_geoid']}")
    print(f"  tracts inside={len(sheet.tracts['inside_city']):,}  "
          f"boundary={len(sheet.tracts['boundary'])}")
    print(f"  population: {d.population:,}")
    print(f"  poverty rate: {d.poverty_rate:.1%}" if d.poverty_rate else "  poverty rate: -")
    print(f"  median HH income: ${d.median_household_income_weighted:,.0f}"
          if d.median_household_income_weighted else "  median HH income: -")
    print(f"  housing units: {d.housing_units_total:,}")
    print(f"  SFH share:  {h.sfh_share:.1%}")
    print(f"  MDU share:  {h.mdu_share:.1%}")
    print(f"  Other:      {h.other_share:.1%}")


if __name__ == "__main__":
    main()
