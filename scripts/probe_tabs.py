"""Probe each tab's render function with real Evans CO data.

Streamlit functions called outside a streamlit context print warnings but
generally don't crash on simple state. The tabs that DO crash here are the
ones likely to break in the actual app.
"""

from __future__ import annotations

import contextlib
import io
import sys
import traceback

from ftth_compete.pipeline import run_market


def _try(name, fn):
    print(f"\n=== {name} ===")
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
            fn()
        print("OK")
    except Exception:
        print("FAIL")
        traceback.print_exc()


def main() -> None:
    print("Loading Evans, CO sheet (with ratings off)...")
    sheet = run_market("Evans", "CO", no_ratings=True)
    print(
        f"  providers={len(sheet.providers) if sheet.providers else 0}  "
        f"tract_speeds={len(sheet.tract_speeds)}  "
        f"coverage_rows={len(sheet.coverage_matrix)}"
    )

    from ftth_compete.ui.tabs.competitors import render_competitors
    from ftth_compete.ui.tabs.housing import render_housing
    from ftth_compete.ui.tabs.map import _compute_tract_metrics, render_map
    from ftth_compete.ui.tabs.overview import render_overview

    _try("Overview", lambda: render_overview(sheet))
    _try("Competitors", lambda: render_competitors(sheet))
    _try("Housing", lambda: render_housing(sheet))
    _try("Map (compute_tract_metrics only)", lambda: _compute_tract_metrics(sheet))
    # Don't call render_map directly since it does st_folium which needs runtime


if __name__ == "__main__":
    sys.exit(main() or 0)
