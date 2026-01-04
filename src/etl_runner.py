"""
etl_runner.py
─────────────
Forward-fill (incremental) Binance klines into Postgres for tracked symbols.

- Uses run_etl() (your incremental loader)
- Does NOT backfill historical data
- Safe to schedule as a CronJob / Airflow @daily / @hourly
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone

from src.modules.DB import get_db_engine, get_tracked_symbols
from src.modules.etl_klines import run_etl

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--timeframe", default="1d", help="Binance interval (e.g. 1h, 1d)")
    p.add_argument("--symbol", default=None, help="Run only one symbol (e.g. BTCUSDT)")
    p.add_argument("--end", default=None, help='End date, format "04 Jan 2026" (defaults to today UTC)')
    return p.parse_args()


def main():
    args = parse_args()

    engine = get_db_engine()
    symbols = [args.symbol.strip().upper()] if args.symbol else get_tracked_symbols(engine)

    # default end = today UTC in the same format your run_etl expects
    end = args.end or datetime.now(timezone.utc).strftime("%d %b %Y")

    logger.info(f"▶ ETL runner: timeframe={args.timeframe} symbols={len(symbols)} end={end}")

    for sym in symbols:
        try:
            run_etl(sym, args.timeframe, end=end)
        except Exception as exc:
            logger.error(f"ETL failed for {sym} ({args.timeframe}): {exc}")


if __name__ == "__main__":
    started = datetime.now(timezone.utc)
    logger.info(f"🕒 ETL started at {started:%Y-%m-%d %H:%M:%S} UTC")
    main()
    finished = datetime.now(timezone.utc)
    logger.info(f"✅ ETL done ({finished - started})")
