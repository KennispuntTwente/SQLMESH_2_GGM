#!/usr/bin/env bash
set -euo pipefail

# Quick demo: start DBs, load synthetic data, run pipeline
# Usage: bash docker/run_demo.sh [destination]
# Example: bash docker/run_demo.sh postgres

DEST="${1:-postgres}"

echo "[demo] Starting Docker services (oracle + $DEST)..."
docker compose -f docker/docker-compose.yml up -d oracle "$DEST"

echo "[demo] Waiting for Oracle to be ready (can take 2-3 min)..."
until docker compose -f docker/docker-compose.yml exec -T oracle \
    sqlplus -L sys/ggm_dev_123@//localhost:1521/FREEPDB1 as sysdba <<< "SELECT 1 FROM DUAL;" > /dev/null 2>&1; do
    sleep 5
done
echo "[demo] Oracle is ready."

echo "[demo] Loading synthetic data to Oracle..."
uv run python synthetic/load_to_oracle.py

echo "[demo] Running dlt pipeline: Oracle -> $DEST raw layer..."
uv run python pipelines/source_to_raw.py --dest "$DEST"

echo "[demo] Running SQLMesh: raw -> stg -> silver..."
uv run sqlmesh plan --auto-apply

echo "[demo] Done! Run 'uv run sqlmesh ui' to explore."
