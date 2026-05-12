# Hugging Face Spaces Dockerfile for ftth-compete.
#
# HF Spaces expose one public port (default 7860). Reflex runs on two
# ports (3000 frontend, 8000 backend), so we put a small Caddy reverse
# proxy in front that fans 7860 → frontend/backend based on path.
#
# Build is fully cloud-side. The user never runs `docker build` locally.
# HF pulls the repo, builds this Dockerfile, and runs it.

FROM python:3.12-slim

# System deps:
#   - caddy: tiny reverse proxy (single binary, ~50MB)
#   - curl + ca-certificates: for installing uv + downloading caddy
#   - nodejs: Reflex's frontend build step (Next.js export) needs node
#   - gosu: drop to non-root user at runtime (HF Spaces best practice)
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl ca-certificates debian-keyring debian-archive-keyring \
        apt-transport-https build-essential gnupg unzip \
    && curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' \
        | gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg \
    && curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' \
        | tee /etc/apt/sources.list.d/caddy-stable.list \
    && curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get update && apt-get install -y --no-install-recommends \
        caddy nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install uv to a system-wide location. The default install script
# drops it under /root/.local/bin which isn't readable by the non-root
# `user` (uid 1000) we drop privileges to at runtime. Copying from
# Astral's official uv image is the cleanest path.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# HF Spaces runs the container as user 1000 by default. We have to
# match that for filesystem writes to work. Pre-create the working
# directory with `user` ownership — WORKDIR alone would make it
# root-owned and Reflex can't create .web/ at runtime.
RUN useradd -m -u 1000 user && mkdir -p /home/user/app && chown user:user /home/user/app
WORKDIR /home/user/app

# Copy dependency manifests first so Docker layer-cache speeds up rebuilds.
COPY --chown=user:user pyproject.toml ./
COPY --chown=user:user README.md ./

# Install Python deps. --no-dev skips pytest/ruff/mypy — saves ~200MB.
# Runs as root then chowns the venv to user so the runtime `user`
# (uid 1000) can read it after we drop privileges at the bottom.
RUN uv sync --no-dev && chown -R user:user /home/user/app/.venv

# Copy app sources after deps so code edits don't bust the deps layer.
COPY --chown=user:user src/ ./src/
COPY --chown=user:user ftth_compete_web/ ./ftth_compete_web/
COPY --chown=user:user rxconfig.py ./
COPY --chown=user:user Caddyfile ./

# Pre-download the FCC IAS historical subscription archive (14 zips,
# ~4 MB total) at image build time. Bakes them into the image so the
# trendline sparkline paints in ~30s on every cold container start
# instead of redownloading from fcc.gov on every redeploy.
#
# Done at build time (not shipped in git) because HF Spaces rejects
# every binary file without Xet/LFS, regardless of size. Build-time
# curl avoids the git-side restriction entirely.
RUN mkdir -p /home/user/app/data/raw/ias && \
    cd /home/user/app/data/raw/ias && \
    for f in dec_2015 jun_2016 dec_2016 jun_2017 dec_2017 jun_2018 \
             dec_2018 jun_2019 dec_2019 jun_2020 dec_2020 jun_2021 \
             dec_2021 jun_2022; do \
        curl -fsSL -o "tract_map_${f}.zip" \
            "https://www.fcc.gov/sites/default/files/tract_map_${f}.zip" \
            || echo "WARN: failed to fetch tract_map_${f}.zip"; \
    done && \
    chown -R user:user /home/user/app/data

# Pre-download TIGER state shapefiles for the most-looked-up demo
# markets (CA / CO / KS / MO / NY / TX). Each state's TRACT + PLACE
# zip is fetched and extracted into the dir tiger.py expects
# (data/raw/tiger/<year>/{PLACE,TRACT}/tl_<year>_<fips>_<layer>.shp).
# Eliminates a 30-60s cold TIGER download on the first lookup in
# each of those states. ~120MB image-size bump for the six states.
#
# State FIPS:
#   06 California · 08 Colorado · 20 Kansas · 29 Missouri
#   36 New York   · 48 Texas
RUN mkdir -p /home/user/app/data/raw/tiger/2024/PLACE \
             /home/user/app/data/raw/tiger/2024/TRACT && \
    cd /home/user/app/data/raw/tiger/2024 && \
    for fips in 06 08 20 29 36 48; do \
        for pair in PLACE:place TRACT:tract; do \
            upper="${pair%:*}"; \
            lower="${pair#*:}"; \
            tmp="/tmp/tl_2024_${fips}_${lower}.zip"; \
            url="https://www2.census.gov/geo/tiger/TIGER2024/${upper}/tl_2024_${fips}_${lower}.zip"; \
            curl -fsSL -o "$tmp" "$url" \
                && unzip -oq "$tmp" -d "${upper}/" \
                && rm "$tmp" \
                || echo "WARN: failed to fetch ${url}"; \
        done; \
    done && \
    chown -R user:user /home/user/app/data/raw/tiger

# Pre-download the national ZCTA -> tract crosswalk (~30 MB single
# text file). Used by ACP density allocation and any other future
# ZIP-to-tract joins. Without seeding this, the first market lookup
# that triggers ACP density blocks on a 300s download timeout.
RUN mkdir -p /home/user/app/data/raw/tiger/2024/ZCTA_TRACT_REL && \
    curl -fsSL -o /home/user/app/data/raw/tiger/2024/ZCTA_TRACT_REL/tab20_zcta520_tract20_natl.txt \
        "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_tract20_natl.txt" \
        || echo "WARN: failed to fetch ZCTA->tract crosswalk" \
    ; chown -R user:user /home/user/app/data/raw/tiger

# Pre-seed FCC BDC state parquets for the latest release from a public
# HF Dataset (filled by `scripts/refresh_bdc_states.py`). The FCC BDC
# API requires authenticated requests with rate limits, so we can't
# fetch directly from FCC at Docker build time on HF Spaces (no
# build-time secrets). Instead we publish the pre-converted parquets
# to a public HF Dataset and curl from there — same auth-free fetch as
# the IAS + TIGER seeds.
#
# Without these the first lookup in any state takes 30-90s while
# `fcc_bdc.ingest_state` downloads + converts the BDC zip. With them
# every state's first lookup is warm-disk fast (~5s).
#
# Refresh workflow: see `.claude/refresh-bdc-workflow.md` — re-run the
# local refresh script after each new BDC release (biannual).
#
# `latest.txt` at the dataset root holds the current release as-of
# date (YYYY-MM-DD). If the dataset is empty / unreachable / not yet
# populated, every state download warns and falls back to the
# runtime ingest path. Build never fails on a missing seed.
ARG HF_BDC_DATASET=dhruvkrishna49/ftth-bdc-cache
RUN HF_BASE="https://huggingface.co/datasets/${HF_BDC_DATASET}/resolve/main" && \
    release=$(curl -fsSL "${HF_BASE}/latest.txt" 2>/dev/null | tr -d '[:space:]') && \
    if [ -n "$release" ]; then \
        echo "BDC seed: fetching release ${release} from ${HF_BDC_DATASET}"; \
        mkdir -p "/home/user/app/data/processed/bdc/${release}"; \
        cd "/home/user/app/data/processed/bdc/${release}"; \
        for fips in 01 02 04 05 06 08 09 10 11 12 13 15 16 17 18 19 \
                    20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 \
                    36 37 38 39 40 41 42 44 45 46 47 48 49 50 51 53 \
                    54 55 56 72; do \
            curl -fsSL -o "state=${fips}.parquet" \
                "${HF_BASE}/${release}/state=${fips}.parquet" \
                || echo "  WARN: state=${fips} not available in seed (will fetch from FCC at runtime)"; \
        done; \
    else \
        echo "BDC seed: latest.txt missing or dataset unreachable — skipping (states will ingest from FCC at runtime)."; \
    fi && \
    chown -R user:user /home/user/app/data/processed

# Pre-build the frontend at image build time.
#
# Without this, the first invocation of `reflex run --env prod
# --frontend-only` at CMD time spends 60-90s scaffolding `.web/` and
# compiling the Next.js production bundle. Doing it here moves that
# cost to `docker build` (where we don't care) and lets every cold
# container start serve traffic in seconds rather than minutes.
#
# `reflex init --loglevel info` is a no-op if the project's already
# set up but ensures the .web/ scaffold is in place before export.
# `reflex export --frontend-only --no-zip` writes the static export
# to `.web/_static/` which `reflex run --env prod --frontend-only`
# will then serve directly without re-building.
#
# RUN runs as root by default; chown the resulting .web/ tree so the
# uid 1000 runtime user can read it.
RUN uv run reflex init --loglevel info \
    && uv run reflex export --frontend-only --no-zip --loglevel info \
    && chown -R user:user /home/user/app

# HF Spaces expects port 7860.
ENV PORT=7860
EXPOSE 7860

# Pipeline writes to data/processed; HF gives us a writable workspace
# under /home/user. Set FTTH_DATA_DIR so the seed copy + cache land
# somewhere persistent within the container's lifetime.
ENV FTTH_DATA_DIR=/home/user/app/data

# Drop privileges + launch. Reflex 0.9 in `--env prod` mode runs ONLY the
# frontend by default (it's expected to be paired with a separately-run
# backend, the cloud-deploy pattern). So we start both explicitly:
# backend in background, frontend in foreground (via exec so SIGTERM
# propagates when HF stops the container). Caddy on :7860 fans both.
USER user
CMD ["bash", "-lc", "\
    caddy start --config /home/user/app/Caddyfile && \
    uv run reflex run --env prod --backend-only --loglevel info & \
    exec uv run reflex run --env prod --frontend-only --loglevel info \
"]
