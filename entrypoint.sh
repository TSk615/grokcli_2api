#!/usr/bin/env bash
# Main container entrypoint: start grokcli-2api (app.py)
set -euo pipefail
cd /app

if [[ "$#" -gt 0 ]]; then
  exec "$@"
fi

echo "[entrypoint] starting app: python app.py"
exec python app.py
