#!/usr/bin/env bash
set -euo pipefail

TIMEFRAME="${TIMEFRAME:-1d}"
EARLIEST_DATE="${EARLIEST_DATE:-1 Jan 2017}"
SLEEP="${SLEEP:-1}"

echo "Running BACKFILL runner timeframe=${TIMEFRAME} earliest_date=${EARLIEST_DATE} sleep=${SLEEP}"
python -m src.backfill_runner \
  --timeframe "${TIMEFRAME}" \
  --earliest-date "${EARLIEST_DATE}" \
  --sleep "${SLEEP}"
