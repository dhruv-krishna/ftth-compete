"""Refresh BDC parquets for all 50 states + DC + PR, then upload to HF.

Run this LOCALLY (on a machine with FCC credentials in `.env`) after a
new BDC release lands. It will:

1. Resolve the latest BDC release date.
2. Iterate every state FIPS in `STATE_FIPS`, calling
   `fcc_bdc.ingest_state()` for each. Pre-existing parquets are
   skipped — re-running is cheap.
3. Upload the resulting `<processed_dir>/bdc/<release>/` directory to
   the configured HF Dataset (`dhruvkrishna49/ftth-bdc-cache` by
   default).
4. Write/update a `latest.txt` pointer file at the dataset root so the
   Dockerfile can discover which release to fetch without hardcoding.

The Dockerfile then `curl`s these parquets at image build time, so HF
Spaces cold containers boot with `data/processed/bdc/<release>/state=NN.parquet`
already on disk — every market lookup gets the warm-cache fast path
without paying the 30-90s FCC BDC ingest cost.

Usage:
    uv run python scripts/refresh_bdc_states.py
    # or to limit scope:
    uv run python scripts/refresh_bdc_states.py --states CO TX NY

Env vars expected:
    FCC_USERNAME, FCC_API_TOKEN  — for the actual BDC ingest
    HF_TOKEN                     — write token for the HF Dataset push

The HF token must have **write** access to the dataset repo. Create
one at https://huggingface.co/settings/tokens.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

from ftth_compete.config import get_settings
from ftth_compete.data import fcc_bdc
from ftth_compete.data.tiger import STATE_FIPS

DEFAULT_DATASET = "dhruvkrishna49/ftth-bdc-cache"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("refresh_bdc")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset",
        default=os.environ.get("HF_BDC_DATASET", DEFAULT_DATASET),
        help=f"HF Dataset repo to push to (default: {DEFAULT_DATASET}).",
    )
    parser.add_argument(
        "--states",
        nargs="+",
        default=None,
        help="State abbrevs to refresh. Defaults to all 50 + DC + PR.",
    )
    parser.add_argument(
        "--release",
        default=None,
        help="BDC as-of date (YYYY-MM-DD). Defaults to latest.",
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Skip the HF Dataset upload — just refresh local parquets.",
    )
    args = parser.parse_args()

    settings = get_settings()
    settings.ensure_dirs()

    if not (settings.fcc_username and settings.fcc_api_token):
        log.error(
            "FCC_USERNAME / FCC_API_TOKEN must be set in .env. "
            "Register at https://apps.fcc.gov/cores/userLogin.do.",
        )
        return 2

    release = args.release or fcc_bdc.latest_release()
    log.info("Refreshing BDC release %s into %s", release, settings.processed_dir)

    states = [s.upper() for s in (args.states or list(STATE_FIPS.keys()))]
    failed: list[tuple[str, str]] = []
    ingested: list[Path] = []
    t0 = time.time()
    for i, state in enumerate(states, 1):
        try:
            log.info("[%d/%d] %s: ingest...", i, len(states), state)
            path = fcc_bdc.ingest_state(state, as_of=release)
            ingested.append(path)
        except Exception as exc:  # noqa: BLE001
            log.exception("%s ingest failed: %s", state, exc)
            failed.append((state, str(exc)))

    elapsed = time.time() - t0
    log.info(
        "Ingest done: %d ok, %d failed, %.0fs elapsed",
        len(ingested), len(failed), elapsed,
    )
    if failed:
        log.warning("Failed states:")
        for state, err in failed:
            log.warning("  %s: %s", state, err[:120])

    if args.skip_upload:
        log.info("Skipping HF upload (--skip-upload).")
        return 0 if not failed else 1

    if not os.environ.get("HF_TOKEN"):
        log.error(
            "HF_TOKEN env var not set. Create a write token at "
            "https://huggingface.co/settings/tokens then export it: "
            "$env:HF_TOKEN = 'hf_...'",
        )
        return 2

    # Lazy import so the script's `--skip-upload` path doesn't require
    # huggingface_hub. The package is in pyproject's dev group.
    from huggingface_hub import HfApi, create_repo

    api = HfApi(token=os.environ["HF_TOKEN"])

    # Make sure the dataset exists (idempotent).
    try:
        create_repo(
            args.dataset, repo_type="dataset", exist_ok=True,
            private=False, token=os.environ["HF_TOKEN"],
        )
    except Exception as exc:  # noqa: BLE001
        log.exception("create_repo for %s failed: %s", args.dataset, exc)
        return 3

    # Update the latest.txt pointer first so a partial upload mid-run
    # doesn't leave the Dockerfile pointing at a half-written release.
    # We push the parquets, THEN flip the pointer.
    release_dir = settings.processed_dir / "bdc" / release
    if not release_dir.exists():
        log.error("Release dir %s does not exist after ingest; aborting upload.", release_dir)
        return 4

    log.info("Uploading %s -> %s/%s ...", release_dir, args.dataset, release)
    api.upload_folder(
        repo_id=args.dataset,
        repo_type="dataset",
        folder_path=str(release_dir),
        path_in_repo=release,
        commit_message=f"Refresh BDC release {release}",
        token=os.environ["HF_TOKEN"],
    )

    # Flip the pointer to the just-uploaded release.
    latest_pointer = settings.processed_dir / "bdc" / "latest.txt"
    latest_pointer.parent.mkdir(parents=True, exist_ok=True)
    latest_pointer.write_text(release + "\n", encoding="utf-8")
    api.upload_file(
        repo_id=args.dataset,
        repo_type="dataset",
        path_or_fileobj=str(latest_pointer),
        path_in_repo="latest.txt",
        commit_message=f"Point latest -> {release}",
        token=os.environ["HF_TOKEN"],
    )
    log.info("Upload done. https://huggingface.co/datasets/%s", args.dataset)
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
