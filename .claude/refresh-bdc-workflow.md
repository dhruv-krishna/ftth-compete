# BDC parquet refresh workflow

Public FCC BDC data is the dominant cold-lookup cost (~30-90s per state to download + convert at runtime). We can't pre-fetch it at Docker build time on HF Spaces because the FCC API needs authenticated requests and HF Spaces doesn't expose runtime secrets to the build environment.

The workaround: publish the pre-converted per-state parquets to a public Hugging Face Dataset (`dhruvkrishna49/ftth-bdc-cache`), then the Dockerfile `curl`s them at image build time without needing FCC creds. Same pattern as the IAS history zips and TIGER state shapefiles.

## One-time setup

1. Create a **write-scoped** HF token at <https://huggingface.co/settings/tokens>. Name it `ftth-bdc-cache-write` or similar.
2. Add it as a GitHub Actions secret named `HF_TOKEN` (Settings → Secrets and variables → Actions → New repository secret).
3. The first run of the refresh script will auto-create the public dataset repo `dhruvkrishna49/ftth-bdc-cache`. If you want to change the name, override via `--dataset <user>/<name>` or `$env:HF_BDC_DATASET=...`.

## After every BDC release roll-over (~biannual: Jun + Dec)

From `C:\dev\ftth-compete\` with your local `.env` containing `FCC_USERNAME` + `FCC_API_TOKEN`:

```powershell
$env:HF_TOKEN = "hf_..."   # the write token from step 1 above
uv run python scripts/refresh_bdc_states.py
```

Total runtime: 30-60 min for all 50 states + DC + PR on a first run. On subsequent runs the script skips states already at the latest release locally (idempotent), so re-running mid-batch is cheap.

What it does:

1. Resolves the latest BDC release date from the FCC API.
2. Calls `fcc_bdc.ingest_state()` for each of `01 02 04 05 06 08 09 10 11 12 13 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 44 45 46 47 48 49 50 51 53 54 55 56 72`. Each one downloads the per-state BDC zip, converts to a single state-partitioned parquet at `<processed_dir>/bdc/<release>/state=<fips>.parquet`.
3. Uploads the resulting `<release>/` folder to the HF Dataset, then writes/updates `latest.txt` at the dataset root to point at the new release.
4. The Dockerfile picks this up automatically on the next image build: it `curl`s `latest.txt` first, then iterates state codes pulling the matching parquets into `data/processed/bdc/<release>/`.

## Cost model

- HF Datasets storage: free for public datasets, large file caps via Xet. Each release is ~10 GB for all 52 jurisdictions.
- HF bandwidth: free for public dataset downloads.
- Docker image size: +10 GB per release baked into the image. HF Spaces accepts images up to ~50 GB; we're well under.
- Docker build time: +2-3 min for the curls (sequential), once per deploy.

## Failure modes

- **Dataset unreachable / `latest.txt` missing**: Dockerfile prints a warning and skips the seed entirely. App still works — first lookup in each state pays the original ~30-90s FCC ingest cost. Runtime is the same as it was before this workflow existed.
- **Individual state parquet missing from the dataset**: that one state warns at build time, falls back to runtime ingest. Other states stay seeded.
- **FCC creds invalid during local refresh**: `ingest_state` raises early before any upload runs. Fix `.env`, re-run.
- **HF token expired / wrong scope**: `upload_folder` raises early before `latest.txt` is flipped. Old release stays pointed at.

The whole pipeline is fail-soft: a broken seed never blocks the build, it just degrades to the old runtime-ingest behavior.

## Re-publishing scope

If you want to ship parquets for fewer states (smaller image), pass `--states CO TX NY` to limit the local ingest. The dataset then only has those states, and the Dockerfile's `curl` for missing states warns + falls back to runtime ingest — fine.

If you want to ship more historical releases (for instant momentum/trajectory loading), re-run the script with `--release 2024-12-31` etc. The Dockerfile only fetches the `latest.txt` pointer release; older releases are runtime-fetched as today. Adding multi-release pre-seed to the Dockerfile is a future change if needed.
