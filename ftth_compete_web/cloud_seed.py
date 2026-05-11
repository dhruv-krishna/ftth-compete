"""Cloud-deploy seed bootstrap.

On cold cloud starts (HF Spaces, Render, any container that doesn't
persist `data/processed/` between deploys), copy the slim aggregates
shipped in `data/seed/` to their runtime location in
`<FTTH_DATA_DIR>/processed/` so `/providers`, `/provider/<slug>`,
and `/screener` work instantly without waiting for a cold BDC ingest.

Idempotent — re-running is a no-op once the destination files exist.
Runs at module-import time so we don't have to remember to call it
from anywhere; importing `ftth_compete_web` triggers it.
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

log = logging.getLogger(__name__)


def _repo_root() -> Path:
    """Repo root is two dirs up from this file (ftth_compete_web/cloud_seed.py)."""
    return Path(__file__).resolve().parent.parent


def _seed_root() -> Path:
    return _repo_root() / "data" / "seed"


def _processed_root() -> Path:
    """Mirror of `ftth_compete.config.get_settings().processed_dir`.

    We re-derive it here (instead of importing config) so the seed step
    runs even if config has issues at import time on the cloud.
    """
    import os
    base = os.environ.get("FTTH_DATA_DIR") or str(_repo_root() / "data")
    return Path(base) / "processed"


def bootstrap_cloud_seed() -> None:
    """Copy `data/seed/<bucket>/*` → `<processed_dir>/<bucket>/*` for any
    bucket where the destination is empty or missing.

    Walks the seed tree by bucket (e.g. `provider_view`, `screener`) and
    copies file-by-file so partial pre-existing buckets aren't clobbered.
    """
    seed = _seed_root()
    if not seed.exists():
        return
    dest_root = _processed_root()
    dest_root.mkdir(parents=True, exist_ok=True)

    copied = 0
    skipped = 0
    for bucket in seed.iterdir():
        if not bucket.is_dir():
            continue
        dest_bucket = dest_root / bucket.name
        dest_bucket.mkdir(parents=True, exist_ok=True)
        for src_file in bucket.rglob("*"):
            if not src_file.is_file():
                continue
            rel = src_file.relative_to(bucket)
            dest_file = dest_bucket / rel
            if dest_file.exists() and dest_file.stat().st_size > 0:
                skipped += 1
                continue
            dest_file.parent.mkdir(parents=True, exist_ok=True)
            try:
                shutil.copy2(src_file, dest_file)
                copied += 1
            except OSError as exc:
                log.warning("Failed to copy seed %s → %s: %s", src_file, dest_file, exc)
    if copied:
        log.info(
            "[cloud_seed] Copied %d seed file(s) to %s (skipped %d already-present).",
            copied, dest_root, skipped,
        )


# Run at import time so the seed is in place before any code touches
# `data/processed/`. Logs a single info line on cold start; silent after.
try:
    bootstrap_cloud_seed()
except Exception as exc:  # noqa: BLE001
    log.warning("[cloud_seed] bootstrap raised %s — continuing without seed.", exc)
