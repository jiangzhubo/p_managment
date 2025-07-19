"""
US Stock Data Downloader to SQLite
----------------------------------

- Reads tickers from 'tickers.csv' (column: 'Ticker')
- Downloads 20 years of daily data for each ticker (incremental updates)
- Stores all data in one table: 'stock_data' in 'stock_data.db'
- Each row is uniquely identified by (ticker, date)
- Ready for adding features/signals for backtesting

Requires: yfinance, pandas, sqlalchemy
"""

import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text
import datetime
import os

# ---- CONFIG ----
tickers_csv_path = "tickers/manually_add_ticker.csv"
years = 20
db_path = "stock_data.db"

# ---- LOAD TICKERS ----
df_tickers = pd.read_csv(tickers_csv_path)
try:
    tickers = df_tickers['Ticker'].astype(str).str.strip().str.upper().dropna().unique().tolist()
except :
    tickers = df_tickers['symbol'].astype(str).str.strip().str.upper().dropna().unique().tolist()
print(f"Loaded {len(tickers)} tickers from {tickers_csv_path}")

# ---- DATABASE SETUP ----
engine = create_engine(f"sqlite:///{db_path}")
with engine.begin() as conn:
    # Create the table if not exists, with more columns ready for signals/features
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS stock_data (
        ticker TEXT,
        date TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        adj_close REAL,
        volume REAL,
        PRIMARY KEY (ticker, date)
        -- You can add more columns here for signals/features (e.g., ma_20, signal, etc)
    )
    """))

today = datetime.datetime.now().date()
start_full = today - datetime.timedelta(days=years * 365)

def get_latest_date(ticker):
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT MAX(date) FROM stock_data WHERE ticker=:ticker"
        ), {'ticker': ticker}).fetchone()
        return result[0] if result and result[0] else None

def upsert_to_db(df):
    if df.empty:
        return
    df = df.rename(columns={
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume"
    })
    expected_cols = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[expected_cols]
    df["date"] = df["date"].astype(str)
    df = df.where(pd.notnull(df), None)  # Convert all pd.NA/nan to None

    with engine.begin() as conn:
        for _, row in df.iterrows():
            # Convert to dict of native Python types
            params = {col: (row[col].item() if hasattr(row[col], "item") else row[col]) for col in expected_cols}
            # If value is still pd.NA or nan, set to None
            for k, v in params.items():
                if pd.isna(v):
                    params[k] = None
            conn.execute(text("""
                INSERT OR REPLACE INTO stock_data (ticker, date, open, high, low, close, adj_close, volume)
                VALUES (:ticker, :date, :open, :high, :low, :close, :adj_close, :volume)
            """), params)


abandon_days = 180  # Skip stocks not updated for 180+ days

for ticker in tickers:
    print(f"\n[{ticker}] Processing...")
    latest = get_latest_date(ticker)
    if latest:
        last_date = pd.to_datetime(latest).date()
        days_since_update = (today - last_date).days
        if days_since_update > abandon_days:
            print(f"Abandoned: no new data for {days_since_update} days. Skipping download.")
            continue
        start_date = pd.to_datetime(latest) + pd.Timedelta(days=1)
        if start_date.date() > today:
            print(f"Already up-to-date.")
            continue
    else:
        start_date = start_full

    print(f"Fetching data from {start_date} to {today}")
    # (download and insert as before...)

    df = yf.download(
        ticker,
        start=start_date,
        end=today + datetime.timedelta(days=1),
        interval='1d',
        progress=False
    )

    if df.empty:
        print(f"No new data for {ticker}.")
        continue

    df.reset_index(inplace=True)
    df['ticker'] = ticker
    upsert_to_db(df)
    print(f"Added/updated {len(df)} rows for {ticker}.")
