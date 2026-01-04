"""
backfill_runner.py
Download historical Binance klines before the earliest row you already have.
"""

from datetime import datetime
import argparse
import logging
from datetime import timezone
from src.backfill import backfill_symbol
from src.modules.DB import get_db_engine, get_tracked_symbols  # <- import from DB module (your functions live there)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--timeframe", default="1h", help="Binance interval (e.g. 1m, 5m, 1h, 1d)")
    p.add_argument("--earliest-date", default="1 Jan 2017", help='Stop date, format "1 Jan 2017"')
    p.add_argument("--sleep", type=int, default=1, help="Sleep seconds between API calls")
    p.add_argument("--symbol", default=None, help="Run only one symbol (e.g. BTCUSDT)")
    return p.parse_args()

def main():
    args = parse_args()

    engine = get_db_engine()
    #symbols = get_tracked_symbols(engine)
    symbols = [args.symbol.strip().upper()] if args.symbol else get_tracked_symbols(engine)
    if not symbols:
        logger.info("No tracked symbols found; exiting.")
        return
    if args.symbol and args.symbol.strip().upper() == "ALL":
        symbols = get_tracked_symbols(engine)

    for sym in symbols:
        try:
            backfill_symbol(
                sym,
                timeframe=args.timeframe,
                backfill_start=args.earliest_date,
                sleep_seconds=args.sleep,
            )
        except Exception as exc:
            logger.error(f"Backfill failed for {sym} ({args.timeframe}): {exc}")

if __name__ == "__main__":
    started = datetime.now(timezone.utc)
    logger.info(f"🕒 Back-fill started at {started:%Y-%m-%d %H:%M:%S} UTC")
    main()
    finished = datetime.now(timezone.utc)
    logger.info(f"✅ All done ({finished - started})")
