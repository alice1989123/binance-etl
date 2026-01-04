#!/usr/bin/env bash
set -euo pipefail

TIMEFRAME="${TIMEFRAME:-1d}"   # or 4h, 1h, etc.

echo "Running ETL runner timeframe=${TIMEFRAME}"
python -m src.etl_runner --timeframe "${TIMEFRAME}"