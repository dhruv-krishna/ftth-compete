"""Phase 1 smoke test: city -> tracts -> demographics for Evans, CO."""

from __future__ import annotations

import json

from ftth_compete.data import census_acs, tiger


def main() -> None:
    res = tiger.city_to_tracts("Evans", "CO")
    acs = census_acs.fetch_market_metrics(res.geoids)
    out = {
        "city": res.city_name,
        "state": res.state,
        "place_geoid": res.place_geoid,
        "n_tracts": len(res.geoids),
        "tract_geoids": res.geoids,
        "boundary_tract_geoids": res.boundary_tract_geoids,
        "acs_vintage": acs.vintage,
        "acs_rows": acs.frame.to_dicts(),
    }
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
