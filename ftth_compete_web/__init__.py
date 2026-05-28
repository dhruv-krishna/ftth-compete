"""Reflex web UI for ftth-compete.

This package is the long-term replacement for the Streamlit dashboard at
`src/ftth_compete/ui/`. The entry point is `ftth_compete_web.py`, which
Reflex auto-discovers via `rxconfig.py` at the repo root.

On import, runs the cloud-seed bootstrap (copies any slim pre-computed
parquets from `data/seed/` into `data/processed/`). This is a no-op on
local dev where the parquets are already in place, and instantly
populates `/providers` + `/screener` on cold cloud starts.
"""

from . import cloud_seed  # noqa: F401 — side-effecting import (seed copy)
