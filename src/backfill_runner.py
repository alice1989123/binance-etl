"""
backfill_runner.py
──────────────────
Download historical Binance klines *before* the earliest row you
already have and insert them idempotently into PostgreSQL.

Environment variables are the same as your main ETL.
"""

from datetime import datetime
from src.backfill import backfill_symbol          
from src.modules.etl_klines import (                  
    get_db_engine,
    get_tracked_symbols,
)
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------------
#  CONFIG – tweak to taste
# -------------------------------------------------------------------------
TIMEFRAME       = "1h"            # '1h', '1d', etc.
EARLIEST_DATE   = "1 Jan 2017"    # stop here (Binance spot starts in 2017)
SLEEP_SECONDS   = 1               # respect API rate limits
# -------------------------------------------------------------------------


def main():
    engine  = get_db_engine()
    symbols = get_tracked_symbols(engine)

    for sym in symbols:
        try:
            backfill_symbol(
                sym,
                timeframe=TIMEFRAME,
                backfill_start=EARLIEST_DATE,
                sleep_seconds=SLEEP_SECONDS,
            )
        except Exception as exc:
            logger.error(f"Backfill failed for {sym}: {exc}")
            # Optional: time.sleep(10)  # cool-down on unexpected errors


if __name__ == "__main__":
    started = datetime.utcnow()
    print(f"🕒 Back-fill started at {started:%Y-%m-%d %H:%M:%S} UTC")
    logger.info(f"Back-fill started at {started:%Y-%m-%d %H:%M:%S} UTC")
    main()
    finished = datetime.utcnow()
    print(f"✅ All done ({finished - started})")
    logger.info(f"All done ({finished - started})")
