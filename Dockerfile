# ============================================================
# Social Engineering Research Pipeline — Dockerfile
# Phase 3: Containerization
# ============================================================
#
# Key design: static config files → /app/config/ (image layer, read-only)
#             dynamic outputs     → /app/data/   (mounted volume, writable)
#
# This separation prevents macOS Docker Desktop VirtioFS EDEADLK errors
# that occur when reading and writing to the same mounted volume simultaneously.
#
# Build:
#   docker build -t pipeline .
#
# Full run via docker-compose (recommended):
#   docker compose up
# ============================================================

FROM python:3.11-slim AS base

# System dependencies for psycopg2 compilation (PostgreSQL adapter)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    gcc \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ── Install Python dependencies (cached layer) ──────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Copy application source ──────────────────────────────────
COPY alembic/    alembic/
COPY alembic.ini alembic.ini
COPY src/        src/
COPY pipeline.py pipeline.py

# ── Static config files → /app/config/ (NOT /app/data/) ─────
# These are baked into the image layer so they are never affected
# by the ./data volume mount. PIPELINE_CONFIG_DIR tells the app
# to read config from here instead of the writable data/ directory.
RUN mkdir -p config
COPY data/config.json             config/config.json
COPY data/system_prompt_final.txt config/system_prompt_final.txt

# ── Entrypoint ───────────────────────────────────────────────
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Output directories (overridden by volume mounts in docker-compose)
RUN mkdir -p data reports

# Ensure project root is on PYTHONPATH and point app to baked-in config
ENV PYTHONPATH=/app
ENV PIPELINE_CONFIG_DIR=/app/config

ENTRYPOINT ["/entrypoint.sh"]
