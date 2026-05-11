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
        apt-transport-https build-essential gnupg \
    && curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' \
        | gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg \
    && curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' \
        | tee /etc/apt/sources.list.d/caddy-stable.list \
    && curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get update && apt-get install -y --no-install-recommends \
        caddy nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install uv (fast, no network surprises since uv is a single binary).
ADD https://astral.sh/uv/install.sh /uv-installer.sh
RUN sh /uv-installer.sh && rm /uv-installer.sh
ENV PATH="/root/.local/bin:${PATH}"

# HF Spaces runs the container as user 1000 by default. We have to
# match that for filesystem writes to work.
RUN useradd -m -u 1000 user
WORKDIR /home/user/app

# Copy dependency manifests first so Docker layer-cache speeds up rebuilds.
COPY --chown=user:user pyproject.toml ./
COPY --chown=user:user README.md ./

# Install Python deps. --no-dev skips pytest/ruff/mypy — saves ~200MB.
RUN uv sync --no-dev

# Copy app sources after deps so code edits don't bust the deps layer.
COPY --chown=user:user src/ ./src/
COPY --chown=user:user ftth_compete_web/ ./ftth_compete_web/
COPY --chown=user:user rxconfig.py ./
COPY --chown=user:user Caddyfile ./
# data/seed/ would be copied here if it existed, but the seed parquet
# is currently held out of git (HF Spaces blocks binary files without
# LFS). cloud_seed.py no-ops when data/seed/ is missing, so removing
# the COPY is safe.

# Reflex's `init` writes the .web/ dir (Next.js scaffold). `export`
# builds the static frontend bundle. Both need network access for npm,
# which HF Spaces' build runner has.
RUN uv run reflex init --template blank --loglevel error || true \
    && uv run reflex export --frontend-only --no-zip --loglevel error

# HF Spaces expects port 7860.
ENV PORT=7860
EXPOSE 7860

# Pipeline writes to data/processed; HF gives us a writable workspace
# under /home/user. Set FTTH_DATA_DIR so the seed copy + cache land
# somewhere persistent within the container's lifetime.
ENV FTTH_DATA_DIR=/home/user/app/data

# Drop privileges + launch.
USER user
CMD ["bash", "-lc", "caddy start --config /home/user/app/Caddyfile && uv run reflex run --env prod --backend-only --loglevel info"]
