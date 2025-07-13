import os
import time
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
from binance.client import Client
from datetime import datetime, timedelta
from pandas import Timestamp
from modules.DB import get_db_engine, insert_to_postgres, get_tracked_symbols , get_latest_open_time
import logging
# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
# Load environment variables
load_dotenv("keys/.env")

API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_SECRET_KEY")
client = Client(API_KEY, API_SECRET)

TABLE_NAME = os.getenv("TABLE_NAME")  




SLEEP_SECONDS = 1
INTERVAL = Client.KLINE_INTERVAL_1HOUR





def get_all_binance_klines(symbol: str, start_str: str, end_str: str, interval: str):

        start_ts = int(datetime.strptime(start_str, "%d %b %Y").timestamp() * 1000)
        end_ts = int(datetime.strptime(end_str, "%d %b %Y").timestamp() * 1000)
        
        limit = 1000
        klines = []

        while start_ts < end_ts:
            batch = client.get_klines(
                symbol=symbol,
                interval=interval,
                startTime=start_ts,
                endTime=end_ts,
                limit=limit
            )
            if not batch:
                break
            klines.extend(batch)
            start_ts = batch[-1][6] + 1  # move to next batch
            time.sleep(SLEEP_SECONDS)
        return klines
    


def clean_klines(raw_klines, symbol, timeframe):
    df = pd.DataFrame(raw_klines, columns=[
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_asset_volume", "number_of_trades",
    "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
    ])

    df["symbol"] = symbol
    df["timeframe"] = timeframe
    df["open_time"] = pd.to_datetime(df["open_time"], unit='ms')
    df["close_time"] = pd.to_datetime(df["close_time"], unit='ms')
    df = df.astype({
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "volume": float
    })
    if df.empty:
        logging.info("⚠️ Cleaned DataFrame is empty, skipping insert.")
        return

    return df[[
    "symbol", "timeframe", "open_time", "close_time", "open", "high", "low", "close",
    "volume", "quote_asset_volume", "number_of_trades",
    "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume"
    ]]


def run_etl(symbol: str, timeframe: str, end: str = "1 Jan 2026"):
    engine = get_db_engine()


    # Get latest timestamp from DB
    latest_time = get_latest_open_time(symbol, timeframe, engine)
    # Use the more recent of (latest_time, one_week_ago)
    if latest_time:
        start_time = latest_time + timedelta(milliseconds=1)
    else:
        start_time = datetime.utcnow() - timedelta(days=200)

    start_str = start_time.strftime("%d %b %Y")

    logging.info(f"🚀 Fetching {symbol} klines ({timeframe}) from {start_str} to {end}")

    raw = get_all_binance_klines(symbol, start_str, end, interval=timeframe)
    if not raw:
        logging.info("⚠️ No new data fetched.")
        return 

    df = clean_klines(raw, symbol, timeframe)
    now = pd.Timestamp.utcnow().replace(tzinfo=None)  # Make `now` naive
    logging.info(f"📊 Cleaned DataFrame with {len(df)} rows.")
    #This line ensure we dont insert klines that are not closed yet
    df = df[df["close_time"] <= now]
    logging.debug(f"📊 Filtered DataFrame with {len(df)} rows before inserting into DB.")
    if df.empty:
        logging.debug(f"⚠️ All rows filtered out (e.g. close_time > now), skipping insert.")
        return
    insert_to_postgres(df, engine)


if __name__ == "__main__":
    engine = get_db_engine()
    symbols = get_tracked_symbols(engine)
    if not symbols:
        logging.info("⚠️ No tracked symbols found, exiting.")
        exit(0)

    logging.info(f"🔄 Starting ETL for {len(symbols)} tracked symbols.")

    for symbol in symbols:
        try:
            run_etl(symbol, "1h")
        except Exception as e:
            logging.error(f"⚠️ ETL failed for {symbol}: {e}")
