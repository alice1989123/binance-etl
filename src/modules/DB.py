import os
from sqlalchemy import create_engine, text
from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert
import pandas as pd
from datetime import datetime
import dotenv
from psycopg2.extras import execute_values
# Load environment variables

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

dotenv.load_dotenv("keys/.env")


DB_USER = os.getenv("DBUSER")
DB_PASSWORD = os.getenv("DBPASSWORD")
DB_HOST = os.getenv("DBHOST")
DB_PORT = os.getenv("DBPORT")
DB_NAME = os.getenv("DBNAME")
TABLE_NAME = os.getenv("TABLE_NAME")  

def get_db_engine():
    
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


def insert_to_postgres(
    df: pd.DataFrame,
    engine,
    table_name: str | None = None,
    chunk_size: int = 10_000,
) -> int:
    """
    High-throughput UPSERT into Postgres using psycopg2 execute_values.
    On conflict (symbol,timeframe,open_time) it updates the candle fields.
    Returns rows attempted.
    """
    if df is None or df.empty:
        return 0

    table = table_name or TABLE_NAME  # e.g. "public.binance_klines"

    cols = [
        "symbol",
        "timeframe",
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
    ]

    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns for insert: {missing}")

    df2 = df[cols].copy()
    df2 = df2.where(pd.notnull(df2), None)  # NaN -> NULL

    # Quote all columns (safe for reserved words like "open"/"close")
    col_sql = ", ".join([f'"{c}"' for c in cols])

    # Update everything except the conflict key columns
    conflict_cols = {"symbol", "timeframe", "open_time"}
    set_sql = ", ".join(
        [f'"{c}" = EXCLUDED."{c}"' for c in cols if c not in conflict_cols]
    )

    sql = f"""
        INSERT INTO {table} ({col_sql})
        VALUES %s
        ON CONFLICT ("symbol","timeframe","open_time")
        DO UPDATE SET {set_sql}
    """

    total = len(df2)

    with engine.begin() as conn:
        raw = conn.connection
        with raw.cursor() as cur:
            for start in range(0, total, chunk_size):
                chunk = df2.iloc[start:start + chunk_size]
                values = [tuple(x) for x in chunk.itertuples(index=False, name=None)]
                execute_values(cur, sql, values, page_size=min(chunk_size, 5000))

    logger.info(f"Upserted (attempted) {total} rows into {table}.")
    return total

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

def get_tracked_symbols(engine) -> list[str]:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT symbol FROM coin_catalog WHERE tracked = TRUE"))
        return [row[0] for row in result.fetchall()]