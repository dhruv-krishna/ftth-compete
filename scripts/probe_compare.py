"""Probe the Compare tab against various sheet counts and lens settings."""

from __future__ import annotations

from ftth_compete.analysis.lenses import Lens
from ftth_compete.pipeline import run_market
from ftth_compete.ui.tabs.compare import render_compare


def main() -> None:
    print("Loading sheet for Evans, CO...")
    sheet = run_market("Evans", "CO", no_ratings=True)
    print(f"  providers={len(sheet.providers) if sheet.providers else 0}")

    cases = [
        ("0 sheets", []),
        ("1 sheet", [sheet]),
        ("2 sheets", [sheet, sheet]),
        ("offensive lens", [sheet, sheet]),
    ]
    for name, sheets in cases:
        print(f"\n=== {name} ===")
        try:
            lens = Lens.OFFENSIVE if name == "offensive lens" else Lens.NEUTRAL
            render_compare(sheets, lens=lens)
            print("OK")
        except Exception as exc:
            print(f"FAIL: {exc}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()
