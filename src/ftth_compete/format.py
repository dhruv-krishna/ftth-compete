"""Display formatting helpers used throughout the UI and CLI.

Conventions:
    - None / NaN -> em-dash placeholder.
    - Integers get thousands separators.
    - Currency defaults to USD with `$` prefix.
    - Speeds in Mbps; auto-collapse to Gbps at >=1000.
    - Percent inputs are 0..1 (not 0..100); display has 1 decimal by default.
"""

from __future__ import annotations

import math
from typing import Final

DASH: Final = "—"


def _is_missing(n: object) -> bool:
    if n is None:
        return True
    if isinstance(n, float) and math.isnan(n):
        return True
    return False


def fmt_int(n: int | float | None) -> str:
    """22324 -> '22,324'. Missing -> '—'."""
    if _is_missing(n):
        return DASH
    return f"{int(round(float(n))):,}"


def fmt_currency(n: int | float | None, *, symbol: str = "$") -> str:
    """50000 -> '$50,000'. Missing -> '—'."""
    if _is_missing(n):
        return DASH
    return f"{symbol}{int(round(float(n))):,}"


def fmt_pct(n: float | None, *, decimals: int = 1) -> str:
    """0.135 -> '13.5%'. Missing -> '—'. Input is fraction (0..1), not percent."""
    if _is_missing(n):
        return DASH
    return f"{float(n) * 100:.{decimals}f}%"


def fmt_speed(mbps: int | float | None) -> str:
    """500 -> '500 Mbps'; 2300 -> '2.3 Gbps'. Missing -> '—'."""
    if _is_missing(mbps):
        return DASH
    val = float(mbps)
    if val >= 1000:
        return f"{val / 1000:.1f} Gbps"
    return f"{int(round(val))} Mbps"


def fmt_speed_pair(down: int | float | None, up: int | float | None) -> str:
    """(2300, 2300) -> '2.3 Gbps / 2.3 Gbps'. Either missing -> '—'."""
    if _is_missing(down) or _is_missing(up):
        return DASH
    return f"{fmt_speed(down)} / {fmt_speed(up)}"
