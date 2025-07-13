from datetime import timedelta
import os
import time
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import  text
from sqlalchemy.orm import sessionmaker
from binance.client import Client
from datetime import datetime, timedelta
from sqlalchemy import MetaData
from pandas import Timestamp
from src.modules.DB import get_db_engine, insert_to_postgres 
from src.modules.etl_klines import clean_klines, run_etl
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="logs/etl.log",            
    filemode="a"                   
)
logger = logging.getLogger(__name__)


#Load environment variables
load_dotenv("keys/.env")
TABLE_NAME = os.getenv("TABLE_NAME")
API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_SECRET_KEY")
client = Client(API_KEY, API_SECRET)



def backfill_symbol(
    symbol: str,
    timeframe: str = Client.KLINE_INTERVAL_1HOUR,
    backfill_start: str | datetime = "1 Jan 2017",
    sleep_seconds: int = 1,
) -> None:
    """
    Download *older* klines (before the earliest row in DB) until `backfill_start`
    and insert them idempotently.
    """
    engine = get_db_engine()

    # Normalise the date boundary -------------------------------------------
    if isinstance(backfill_start, str):
        backfill_start = datetime.strptime(backfill_start, "%d %b %Y")
    backfill_ts_ms = int(backfill_start.timestamp() * 1000)

    # Find earliest candle we already have -----------------------------------
    with engine.connect() as cn:
        earliest = cn.scalar(text(f"""
            SELECT MIN(open_time) FROM {TABLE_NAME}
            WHERE symbol = :symbol AND timeframe = :tf
        """), dict(symbol=symbol, tf=timeframe))

    if earliest is None:
        # No data at all → just call your normal run_etl and exit
        run_etl(symbol, timeframe)
        return

    earliest_ts_ms = int(earliest.timestamp() * 1000)

    logger.info(
        f"🔄 Back-filling {symbol} ({timeframe}) from "
        f"{datetime.utcfromtimestamp(backfill_ts_ms/1000):%Y-%m-%d} "
        f"to {earliest:%Y-%m-%d}"
    )

    while earliest_ts_ms > backfill_ts_ms:
        # Ask Binance for ≤1000 klines that end *just* before our earliest row
        batch = client.get_klines(
            symbol=symbol,
            interval=timeframe,
            startTime=max(backfill_ts_ms, earliest_ts_ms - 1000*3600*1000),
            endTime=earliest_ts_ms - 1,
            limit=1000,
        )
        if not batch:
            logger.warning("Binance returned empty batch — stopping.")
            break

        df = clean_klines(batch, symbol, timeframe)
        now_naive = pd.Timestamp.utcnow().replace(tzinfo=None)
        df = df[df["close_time"] <= now_naive]
        insert_to_postgres(df, engine)

        # Prepare next loop (older window)
        earliest_ts_ms = batch[0][0]  # open_time of first candle
        time.sleep(sleep_seconds)

    logger.info("✅ Backfill complete.")