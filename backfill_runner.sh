#!/bin/bash
set -e  # Exit immediately on error

echo "🟢 $(date '+%F %T') – Starting ETL script"

# Run Python module with logging streamed to both stdout and /tmp/etl.log
python -m src.backfill_runner --model_name=LSTMModel 2>&1 | tee /tmp/etl.log

echo "✅ $(date '+%F %T') – Script finished"
