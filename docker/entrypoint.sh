#!/bin/bash
# docker/entrypoint.sh
# --------------------
# Container entrypoint for the Social Engineering Research Pipeline.
#
# Responsibilities:
#   1. Wait for the database to be reachable (extra safety on top of healthcheck)
#   2. Run Alembic migrations to ensure schema is up-to-date
#   3. Execute the pipeline with any arguments passed via docker-compose `command:`
#
# Usage (set via docker-compose command: or docker run args):
#   /entrypoint.sh --yes              # full pipeline, skip confirm
#   /entrypoint.sh --start 2 --yes   # start from filter stage

set -euo pipefail

echo "╔══════════════════════════════════════════╗"
echo "║   Pipeline Container Starting Up...     ║"
echo "╚══════════════════════════════════════════╝"

# ── 1. Wait for database ────────────────────────────────────────────────────
# Alembic will also fail fast if the DB is unreachable, but an explicit wait
# gives a friendlier message and avoids confusing migration errors.
if [[ "${DATABASE_URL:-}" == postgresql* ]]; then
    echo "[entrypoint] Waiting for PostgreSQL to be ready..."

    # Extract host and port from DATABASE_URL
    # e.g. postgresql://user:pass@db:5432/dbname → host=db, port=5432
    DB_HOST=$(echo "$DATABASE_URL" | sed -E 's|.*@([^:/]+).*|\1|')
    DB_PORT=$(echo "$DATABASE_URL" | sed -E 's|.*:([0-9]+)/.*|\1|')

    MAX_RETRIES=30
    RETRY=0
    until python -c "
import socket, sys
try:
    socket.create_connection(('${DB_HOST}', ${DB_PORT}), timeout=2)
    sys.exit(0)
except OSError:
    sys.exit(1)
"; do
        RETRY=$((RETRY + 1))
        if [ "$RETRY" -ge "$MAX_RETRIES" ]; then
            echo "[entrypoint] ERROR: Database not reachable after $MAX_RETRIES attempts. Aborting."
            exit 1
        fi
        echo "[entrypoint] Attempt $RETRY/$MAX_RETRIES — database not ready yet, retrying in 2s..."
        sleep 2
    done
    echo "[entrypoint] Database is reachable."
fi

# ── 2. Run Alembic migrations ───────────────────────────────────────────────
echo "[entrypoint] Running Alembic migrations..."
alembic upgrade head
echo "[entrypoint] Migrations complete."

# ── 3. Run the pipeline ─────────────────────────────────────────────────────
echo "[entrypoint] Starting pipeline with args: $*"
exec python pipeline.py "$@"
