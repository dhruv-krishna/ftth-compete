"""Probe the v2 Plotly map pipeline end-to-end without Reflex.

Runs the same code paths the running app uses to build the v2 map figure
for Evans, CO. Outputs:
- GeoJSON feature count
- Number of layers in the figure
- Whether the figure has data
- Saves a standalone HTML render to scripts/_v2_map_test.html so we can
  open it in a browser and see if the issue is Reflex-side or Plotly-side.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Add repo root so ftth_compete_web package is importable.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ftth_compete.pipeline import run_market
from ftth_compete_web.ftth_compete_web import (
    _build_tract_geojson,
    _build_tract_values,
    build_v2_plotly_figure,
)

print("=" * 60)
print("v2 map debug probe")
print("=" * 60)

# 1. Run Evans CO pipeline (warm-cached, fast).
print("\n[1] Running pipeline for Evans, CO...")
sheet = run_market("Evans", "CO", no_speeds=False, no_ratings=True)
print(f"    market_title: {sheet.market}")
print(f"    inside_city tracts: {len(sheet.tracts.get('inside_city', []))}")
print(f"    coverage_matrix rows: {len(sheet.coverage_matrix)}")
print(f"    tract_speeds rows: {len(sheet.tract_speeds)}")

geoids = list(sheet.tracts.get("inside_city", []))

# 2. Build geojson
print(f"\n[2] Building tract geojson for {len(geoids)} GEOIDs...")
geojson = _build_tract_geojson(geoids)
features = geojson.get("features", []) if geojson else []
print(f"    Features: {len(features)}")
if features:
    first = features[0]
    print(f"    First feature id: {first.get('id')}")
    print(f"    First feature props: {list(first.get('properties', {}).keys())}")
    geom = first.get("geometry", {})
    coords = geom.get("coordinates")
    print(f"    First feature geometry type: {geom.get('type')}")
    print(f"    Coord shape preview: {type(coords).__name__}", end="")
    if coords:
        print(f" outer={len(coords)}")
    else:
        print()

# 3. Build tract_values
print("\n[3] Building tract_values...")
tract_values = _build_tract_values(sheet)
print(f"    Tracts with any values: {len(tract_values)}")
if tract_values:
    sample_geoid = next(iter(tract_values))
    print(f"    Sample tract {sample_geoid} layers:")
    for layer, v in tract_values[sample_geoid].items():
        print(f"      {layer}: {v}")

# 4. Build figure
print("\n[4] Building Plotly figure for layer='Fiber providers per tract'...")
fig = build_v2_plotly_figure(
    geojson, tract_values, "Fiber providers per tract", selected_tract="",
)
print(f"    Figure type: {type(fig).__name__}")
print(f"    Figure data traces: {len(fig.data) if fig else 0}")
if fig and fig.data:
    trace = fig.data[0]
    print(f"    Trace type: {type(trace).__name__}")
    print(f"    Trace locations count: {len(getattr(trace, 'locations', []) or [])}")
    print(f"    Trace z count: {len(getattr(trace, 'z', []) or [])}")
print(f"    Layout mapbox_style: {fig.layout.mapbox.style if fig.layout.mapbox else None}")
print(f"    Layout mapbox_zoom: {fig.layout.mapbox.zoom if fig.layout.mapbox else None}")
print(f"    Layout mapbox_center: {fig.layout.mapbox.center if fig.layout.mapbox else None}")

# 5. Save to standalone HTML
out_html = Path(__file__).parent / "_v2_map_test.html"
fig.write_html(out_html)
print(f"\n[5] Saved standalone HTML to {out_html}")
print(f"    Open in browser: file:///{out_html.as_posix()}")
print()
print("If THAT html renders a map, the figure is fine -> Reflex serialization issue.")
print("If that html is empty too, the figure itself is broken.")
