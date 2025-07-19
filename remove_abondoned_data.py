import pandas as pd
from sqlalchemy import create_engine
import datetime

engine = create_engine("sqlite:///stock_data.db")
today = datetime.date.today()
cutoff_days = 180  # e.g., 180 days

sql = """
SELECT ticker, MAX(date) AS last_date
FROM stock_data
GROUP BY ticker
"""
df = pd.read_sql(sql, engine)
df['last_date'] = pd.to_datetime(df['last_date']).dt.date
df['days_since_update'] = df['last_date'].apply(lambda x: (today - x).days)

abandoned = df[df['days_since_update'] > cutoff_days]
print("Abandoned stocks:")
print(abandoned)

with engine.connect() as conn:
    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)['name'].tolist()

# Only delete from 'tickers' table if it exists
if 'tickers' in tables:
    with engine.begin() as conn:
        for ticker in abandoned['ticker']:
            conn.execute(text("DELETE FROM tickers WHERE symbol = :ticker"), {"ticker": ticker})
            print(f"Deleted {ticker} from tickers table")
else:
    print("No 'tickers' table in database, skipping that step.")