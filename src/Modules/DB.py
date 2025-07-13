import os
from sqlalchemy import create_engine, text
from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime
import dotenv
# Load environment variables
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