"""Runs every historical puller in sequence over ALL tickers in
get_ticker_info/kalshi_tickers/all_tickers.txt (~4,065 tickers) — including
minute candles and trades, NOT just the focus universe.

WARNING: at full scale this is a multi-hour (potentially multi-day) job that
makes tens of thousands of API calls. For a bounded run, use the individual
pullers with --tickers / --limit / --since instead.
"""

from pull_historical.pull_daily import run as run_daily
from pull_historical.pull_hourly import run as run_hourly
from pull_historical.pull_minute import run as run_minute
from pull_historical.pull_trades import run as run_trades

TICKERS = "get_ticker_info/kalshi_tickers/all_tickers.txt"

print("=" * 60)
print("DAILY CANDLES")
print("=" * 60)
print(run_daily(TICKERS))

print("=" * 60)
print("HOURLY CANDLES")
print("=" * 60)
print(run_hourly(TICKERS))

print("=" * 60)
print("MINUTE CANDLES (focus universe only — last ~100 days)")
print("=" * 60)
print(run_minute(TICKERS))

print("=" * 60)
print("TRADES")
print("=" * 60)
print(run_trades(TICKERS))

print("\n✅ All pulls complete.")