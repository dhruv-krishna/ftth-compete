"""Download and inspect the Jun 2022 FCC IAS tract ZIP."""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

import httpx

import ftth_compete  # truststore

URL = "https://www.fcc.gov/sites/default/files/tract_map_jun_2022.zip"
DEST = Path("C:/Users/dkrishn3/ftth-compete-data/raw/ias")


def main() -> None:
    DEST.mkdir(parents=True, exist_ok=True)
    print(f"GET {URL}")
    with httpx.stream("GET", URL, timeout=120.0, follow_redirects=True,
                      headers={"User-Agent": "Mozilla/5.0"}) as r:
        r.raise_for_status()
        buf = io.BytesIO()
        for chunk in r.iter_bytes(chunk_size=1 << 20):
            buf.write(chunk)
    sz = buf.tell()
    print(f"  downloaded: {sz:,} bytes")

    # Inspect zip contents
    buf.seek(0)
    with zipfile.ZipFile(buf) as zf:
        print(f"\nZIP contents ({len(zf.namelist())} files):")
        for name in zf.namelist():
            info = zf.getinfo(name)
            print(f"  {name}  ({info.file_size:,} bytes)")

        # Open the first CSV / Excel / data file
        members = zf.namelist()
        for member in members:
            if member.lower().endswith((".csv", ".xlsx", ".txt")):
                print(f"\n--- First 30 lines of {member} ---")
                with zf.open(member) as f:
                    text = f.read(8192).decode("utf-8", errors="replace")
                    for i, line in enumerate(text.splitlines()[:30]):
                        print(f"  {i:3}: {line}")
                break

    # Save to disk
    out = DEST / "tract_map_jun_2022.zip"
    out.write_bytes(buf.getvalue())
    print(f"\nSaved to {out}")


if __name__ == "__main__":
    main()
