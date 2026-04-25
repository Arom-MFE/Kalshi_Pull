"""Runs every historical puller in sequence. Just press run."""

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