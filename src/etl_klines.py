import os
import time
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert
from binance.client import Client
from datetime import datetime, timedelta
from sqlalchemy import MetaData
from pandas import Timestamp
# Load environment variables
load_dotenv("keys/.env")

API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_SECRET_KEY")
client = Client(API_KEY, API_SECRET)

# DB config
DB_USER = os.getenv("DBUSER")
DB_PASSWORD = os.getenv("DBPASSWORD")
DB_HOST = os.getenv("DBHOST")
DB_PORT = os.getenv("DBPORT")
DB_NAME = os.getenv("DBNAME")
TABLE_NAME = "binance_klines"

SLEEP_SECONDS = 1
INTERVAL = Client.KLINE_INTERVAL_1HOUR




def get_db_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


def get_latest_open_time(symbol: str, timeframe , engine) -> datetime | None:
    query = text(f"""
        SELECT MAX(open_time)
        FROM {TABLE_NAME}
        WHERE symbol = :symbol
        AND timeframe = :timeframe
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"symbol": symbol , "timeframe": timeframe})   
        latest = result.scalar()
    return latest


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

    return df[[
    "symbol", "timeframe", "open_time", "close_time", "open", "high", "low", "close",
    "volume", "quote_asset_volume", "number_of_trades",
    "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume"
    ]]


def insert_to_postgres(df, engine):
    metadata = MetaData()
    metadata.reflect(bind=engine)  # bind here, not in constructor
    table = metadata.tables[TABLE_NAME]

    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        records = df.to_dict(orient="records")
        stmt = pg_insert(table).values(records)
        stmt = stmt.on_conflict_do_nothing(index_elements=["symbol", "timeframe", "open_time"])
        session.execute(stmt)
        session.commit()
        print(f"✅ Inserted {len(records)} rows.")
    except Exception as e:
        session.rollback()
        print(f"❌ Insert failed: {e}")
    finally:
        session.close()

def get_tracked_symbols(engine) -> list[str]:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT symbol FROM coin_catalog WHERE tracked = TRUE"))
        return [row[0] for row in result.fetchall()]

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

    print(f"🚀 Fetching {symbol} klines ({timeframe}) from {start_str} to {end}")

    raw = get_all_binance_klines(symbol, start_str, end, interval=timeframe)
    if not raw:
        print("⚠️ No new data fetched.")
        return 

    df = clean_klines(raw, symbol, timeframe)
    now = pd.Timestamp.utcnow().replace(tzinfo=None)  # Make `now` naive
    df = df[df["close_time"] <= now]
    insert_to_postgres(df, engine)


if __name__ == "__main__":
    engine = get_db_engine()
    symbols = get_tracked_symbols(engine)

    for symbol in symbols:
        try:
            run_etl(symbol, "1h")
        except Exception as e:
            print(f"⚠️ ETL failed for {symbol}: {e}")
