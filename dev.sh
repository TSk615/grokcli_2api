#!/usr/bin/env bash
# Local development with uvicorn hot-reload.
# Usage:
#   ./dev.sh
#   GROK2API_PORT=40081 ./dev.sh
#
# Requires Redis + PostgreSQL (same as production hybrid mode).
# Example with docker only for stores:
#   docker compose up -d postgres redis
#   ./dev.sh
set -euo pipefail
cd "$(dirname "$0")"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

export GROK2API_RELOAD="${GROK2API_RELOAD:-1}"
export GROK2API_WORKERS="${GROK2API_WORKERS:-1}"
export GROK2API_OPEN_BROWSER="${GROK2API_OPEN_BROWSER:-0}"
export GROK2API_HOST="${GROK2API_HOST:-0.0.0.0}"
export GROK2API_PORT="${GROK2API_PORT:-3000}"
export GROK2API_STORE_BACKEND="${GROK2API_STORE_BACKEND:-hybrid}"
export REDIS_URL="${REDIS_URL:-${GROK2API_REDIS_URL:-redis://127.0.0.1:6379/0}}"
export DATABASE_URL="${DATABASE_URL:-${GROK2API_DATABASE_URL:-postgresql://grok2api:grok2api@127.0.0.1:5432/grok2api}}"
export GROK2API_REDIS_URL="${GROK2API_REDIS_URL:-$REDIS_URL}"
export GROK2API_DATABASE_URL="${GROK2API_DATABASE_URL:-$DATABASE_URL}"

# Prefer host Python for editable local sources (not the baked Docker image).
PY=python3
command -v python3 >/dev/null 2>&1 || PY=python

if ! $PY -c "import fastapi, uvicorn" 2>/dev/null; then
  echo "Installing core deps..."
  $PY -m pip install -r requirements.txt
fi
if ! $PY -c "import redis, psycopg" 2>/dev/null; then
  echo "Installing store deps..."
  $PY -m pip install -r requirements-store.txt
fi

# Rebuild admin static hashes when sources change (best-effort; non-fatal).
if [[ -f scripts/build_admin_assets.py ]]; then
  if [[ "${GROK2API_BUILD_ASSETS_ON_START:-1}" != "0" ]]; then
    $PY scripts/build_admin_assets.py || echo "WARN: admin asset build failed (continuing)" >&2
  fi
fi

echo "Dev hot-reload starting..."
echo "  GROK2API_RELOAD=1  workers=1"
echo "  Admin:  http://127.0.0.1:${GROK2API_PORT}/admin"
echo "  Health: http://127.0.0.1:${GROK2API_PORT}/health"
echo "  Edit .py / static/js / static/admin → auto restart"
echo ""

exec $PY app.py
