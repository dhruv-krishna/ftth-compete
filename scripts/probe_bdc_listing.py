"""Probe FCC BDC /downloads/listAvailabilityData to verify response shape."""

from __future__ import annotations

import json
from collections import Counter

from ftth_compete.data import fcc_bdc


def main() -> None:
    # Walk releases newest-first until we find one with files.
    dates = fcc_bdc.list_as_of_dates()
    sorted_dates = sorted(
        (d.get("as_of_date", "") for d in dates),
        reverse=True,
    )
    print(f"All releases (newest first, first 10): {sorted_dates[:10]}\n")

    as_of = ""
    files: list = []
    for candidate in sorted_dates:
        if not candidate:
            continue
        files = fcc_bdc.list_availability_data(candidate)
        print(f"  {candidate}: {len(files)} files")
        if files:
            as_of = candidate
            break

    print(f"\nUsing release: {as_of}\n")

    if files:
        print("--- Sample file (first row, raw keys) ---")
        print(json.dumps(files[0], indent=2, default=str))

        print("\n--- Distinct values per field (top 10) ---")
        for field in ("data_type", "category", "technology", "technology_name",
                      "tech_name", "file_type", "fileType", "format"):
            present = [f.get(field) for f in files if field in f]
            if not present:
                continue
            counter = Counter(str(v) for v in present)
            print(f"  {field}: {dict(counter.most_common(10))}")

        print("\n--- States represented (sample) ---")
        state_keys = ("state_fips", "state_code", "stateFips", "state")
        for k in state_keys:
            present = [f.get(k) for f in files if f.get(k)]
            if present:
                counter = Counter(str(v) for v in present)
                print(f"  {k}: {dict(counter.most_common(15))}")
                break

        # Find files for Colorado (FIPS 08)
        co_files = [f for f in files if str(f.get("state_fips")) == "08"]
        print(f"\n--- Colorado (08) files: {len(co_files)} ---")
        for f in co_files[:8]:
            print(json.dumps(f, indent=2, default=str))


if __name__ == "__main__":
    main()
