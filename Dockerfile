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

# NOTE: we don't run `reflex export` at build time. The first invocation
# of `reflex run` at container startup does the .web/ scaffold + frontend
# build, and we get real-time logs if it errors. This costs ~2-3 min on
# first container start (subsequent starts reuse the built .web/), but
# avoids the opaque build-time failures we hit otherwise.

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
