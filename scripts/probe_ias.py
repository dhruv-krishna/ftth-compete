"""Find the latest FCC IAS tract-level CSV download URL.

The landing page is https://www.fcc.gov/form-477-census-tract-data-internet-access-services
which contains links to per-release CSV files. We need the most recent tract-level
file (typically named like `tract_map_dec2024.zip` or `_tract_data_dec2024.csv`).
"""

from __future__ import annotations

import re

import httpx

# truststore-patched ssl via the package import
import ftth_compete  # noqa: F401

LANDING = "https://www.fcc.gov/form-477-census-tract-data-internet-access-services"


def main() -> None:
    print(f"GET {LANDING}")
    r = httpx.get(LANDING, timeout=30.0, follow_redirects=True,
                  headers={"User-Agent": "Mozilla/5.0 (compatible; ftth-compete/0.1)"})
    print(f"  status={r.status_code}, len={len(r.text):,}")

    # Look for hrefs to CSV / ZIP files
    csv_links = re.findall(r'href="([^"]+\.(?:csv|zip|xlsx))"', r.text, re.IGNORECASE)
    print(f"\nFound {len(csv_links)} csv/zip/xlsx links:")
    for link in csv_links[:30]:
        print(f"  {link}")

    # Also look for any link containing 'tract' in the URL
    tract_links = re.findall(r'href="([^"]*tract[^"]*)"', r.text, re.IGNORECASE)
    print(f"\nFound {len(tract_links)} tract-related links:")
    for link in tract_links[:30]:
        print(f"  {link}")

    # Headings / table rows that might describe what each link is
    # Sample some text around links
    print("\n--- First 3KB of rendered HTML body (after stripping whitespace) ---")
    text = re.sub(r"\s+", " ", r.text)
    # Find first occurrence of "tract" and print surrounding 500 chars
    idx = text.lower().find("tract")
    if idx >= 0:
        print(text[max(0, idx - 200):idx + 1500])


if __name__ == "__main__":
    main()
