# etl-exchange-data

A lightweight ETL pipeline to pull and store Binance kline (candlestick) data into PostgreSQL.

## Structure

- `src/` — ETL scripts
- `keys/.env` — Environment secrets (API keys, DB creds)
- `logs/` — Log files

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
python src/etl_klines.py
```
