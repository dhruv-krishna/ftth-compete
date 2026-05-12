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


def _data_root() -> Path:
    import os
    base = os.environ.get("FTTH_DATA_DIR") or str(_repo_root() / "data")
    return Path(base)


def _processed_root() -> Path:
    """Mirror of `ftth_compete.config.get_settings().processed_dir`.

    We re-derive it here (instead of importing config) so the seed step
    runs even if config has issues at import time on the cloud.
    """
    return _data_root() / "processed"


def _raw_root() -> Path:
    """Mirror of `ftth_compete.config.get_settings().raw_dir`."""
    return _data_root() / "raw"


# Bucket names under `data/seed/` whose destination is `raw/` (not
# `processed/`). IAS history zips need to land in raw/ias/ where
# `fcc_ias._list_local_zips` discovers them.
_RAW_BUCKETS = {"ias"}


def bootstrap_cloud_seed() -> None:
    """Copy `data/seed/<bucket>/*` → `<dest_root>/<bucket>/*` for any
    bucket where the destination is empty or missing.

    Bucket-name → destination routing:
      - `ias` → `<data_dir>/raw/ias/` (so `fcc_ias` finds the ZIPs)
      - everything else → `<data_dir>/processed/<bucket>/`

    Walks file-by-file and skips files that already exist (non-empty) so
    pre-warmed caches from a previous container start aren't clobbered.
    """
    seed = _seed_root()
    if not seed.exists():
        return
    _processed_root().mkdir(parents=True, exist_ok=True)
    _raw_root().mkdir(parents=True, exist_ok=True)

    copied = 0
    skipped = 0
    for bucket in seed.iterdir():
        if not bucket.is_dir():
            continue
        if bucket.name in _RAW_BUCKETS:
            dest_bucket = _raw_root() / bucket.name
        else:
            dest_bucket = _processed_root() / bucket.name
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
            "[cloud_seed] Copied %d seed file(s) (skipped %d already-present).",
            copied, skipped,
        )


# Run at import time so the seed is in place before any code touches
# `data/processed/`. Logs a single info line on cold start; silent after.
try:
    bootstrap_cloud_seed()
except Exception as exc:  # noqa: BLE001
    log.warning("[cloud_seed] bootstrap raised %s — continuing without seed.", exc)
