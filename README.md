# Kalshi Macro Market Data Pipeline

Pure market-data pipeline for Kalshi prediction-market contracts on macro-economic events (CPI, Fed funds rate, unemployment, GDP, payrolls). Pulls and stores raw candles, trades, and orderbook snapshots as parquet files. Designed as a clean data layer for downstream research — no analysis, no derived metrics, just queryable market data.

## What it does

- **Historical candles** (daily, hourly, minute) for every ticker across 15 macro series (~4,164 tickers, 525 events)
- **Historical trades** (every individual fill) for a configurable focus universe of liquid contracts
- **Orderbook snapshots** via REST polling on configurable cadences
- **Idempotent pulls** — re-running any puller only fetches new data and appends to existing files

## Why it exists

Kalshi prediction markets price macro outcomes (Fed rate decisions, CPI prints, payroll numbers) in real time. This pipeline captures that pricing data at multiple frequencies for downstream SPX/VIX volatility research and macro signal extraction. The pipeline stores raw data only — all analysis happens in separate projects that consume the parquet output.

## Installation

```bash
# Clone the repo
git clone https://github.com/your-username/kalshi-pipeline.git
cd kalshi-pipeline

# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate

# Install in editable mode
pip install -e .
```

### Kalshi API setup

1. Generate an API key at [kalshi.com/account/api-keys](https://kalshi.com/account/api-keys)
2. Save your RSA private key:

```bash
mkdir -p ~/.kalshi && chmod 700 ~/.kalshi
# Save your private key PEM file as:
# ~/.kalshi/kalshi_key.pem
chmod 600 ~/.kalshi/kalshi_key.pem
```

3. Create a `.env` file at the repo root (see `.env.example`):

```
KALSHI_API_KEY_ID=your-api-key-id-here
KALSHI_KEY_PATH=/Users/yourname/.kalshi/kalshi_key.pem
```

## Directory layout

```
kalshi-pipeline/
├── kalshi_io/              # Shared library
│   ├── client.py           # Authenticated SDK + REST session
│   ├── config.py           # Paths, constants, focus universe, series list
│   ├── tickers.py          # Flexible ticker loading (file, series name, list)
│   ├── resolve.py          # Event/market/metadata resolution with fallbacks
│   ├── candles.py          # Candle fetching + normalization (live/historical schemas)
│   ├── trades.py           # Trade fetching with cursor pagination
│   ├── orderbook.py        # Orderbook snapshot → DataFrame
│   └── storage.py          # Parquet I/O: append, dedupe, resume
│
├── pull_historical/        # CLI scripts for historical backfill
│   ├── pull_daily.py       # Daily candles (period_interval=1440)
│   ├── pull_hourly.py      # Hourly candles (period_interval=60)
│   ├── pull_minute.py      # Minute candles (period_interval=1)
│   ├── pull_trades.py      # Historical trade tape
│   └── pull_audit.py       # Read-only coverage report (no API calls)
│
├── pull_live/
│   └── poll_focus.py       # Single-process scheduler for focus universe
│
├── get_ticker_info/        # Ticker discovery and series metadata
│   └── kalshi_tickers/     # Per-series JSON+TXT, plus all_tickers.*
│
└── kalshi_data/            # All output (gitignored)
    ├── candles/
    │   ├── daily/{series}/{ticker}.parquet
    │   ├── hourly/{series}/{year}/{ticker}.parquet
    │   └── minute/{series}/{year}/{month}/{ticker}.parquet
    ├── trades/{series}/{ticker}/{yyyy-mm}.parquet
    ├── orderbook/{ticker}/{yyyy-mm-dd}.parquet
    └── logs/
```

## Usage

### Historical backfill

```bash
# Daily candles for all tickers
python -m pull_historical.pull_daily

# Hourly candles for a specific series
python -m pull_historical.pull_hourly --tickers KXFED --limit 10

# Minute candles for focus universe
python -m pull_historical.pull_minute --tickers KXFED-26JUN-T4.25

# Historical trades
python -m pull_historical.pull_trades --tickers KXFEDDECISION

# Coverage audit (read-only, no API calls)
python -m pull_historical.pull_audit --tickers KXRECSSNBER
```

All pullers accept `--tickers` (path to `.txt`/`.json`, series name, or single ticker), `--limit`, and `--since YYYY-MM-DD`.

### Live polling

```bash
# Run with defaults (minute/trades/orderbook every 60s, hourly every 15min, daily every 6h)
python -m pull_live.poll_focus

# Override cadences
python -m pull_live.poll_focus --minute-interval 30 --trades-interval 30

# Disable specific pullers
python -m pull_live.poll_focus --no-daily --no-hourly

# Single iteration (useful for testing)
python -m pull_live.poll_focus --iterations 1
```

Ctrl+C for graceful shutdown. Logs to `kalshi_data/logs/poll_focus_{YYYYMMDD}.log`.

### Programmatic usage

```python
from pull_historical.pull_daily import run
result = run("KXFED", limit=5)
# {"processed": 5, "skipped": 0, "rows_written": 42, "elapsed_sec": 3.2}
```

## Reading the data

### pandas

```python
import pandas as pd

df = pd.read_parquet("kalshi_data/candles/daily/KXFED/KXFED-26JUN-T4.25.parquet")
df["timestamp"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
```

### DuckDB

```sql
SELECT market_ticker, COUNT(*) as rows, MIN(ts_ms) as first, MAX(ts_ms) as last
FROM read_parquet('kalshi_data/candles/daily/**/*.parquet')
GROUP BY market_ticker
ORDER BY rows DESC;
```

## Series covered

15 macro series, ~4,164 tickers across 525 events:

| Category | Series |
|----------|--------|
| Inflation | KXCPI, KXCPIYOY, KXACPI, KXCPICORE, KXPCECORE, KXCPICOREYOY |
| Labor | KXU3, KXJOBLESS, KXPAYROLLS |
| Growth | KXGDP, KXGDPYEAR, KXRECSSNBER |
| Fed | KXFEDDECISION, KXFED, KXFEDMEET |

## Known API quirks

- KX prefix migration (late 2024): `FED` → `KXFEDDECISION`, `CPIYOY` → `KXCPIYOY`, etc.
- Live vs historical candle schemas differ (`price.close` vs `price.close_dollars`)
- SDK `get_series_list()` is broken (Pydantic error on `tags: null`) — pipeline hits REST directly
- ~0.3% of tickers are malformed — automatically logged to skip-list and skipped
- `get_event`/`get_market` 404 for old events — pipeline has multi-layer fallbacks
- 5,000 candle cap per call — pipeline handles chunking and pagination automatically

## Maintenance

- **Weekly**: Re-run `get_ticker_info/get_tickers.py` to discover new events/tickers
- **Per macro cycle**: Update `FOCUS_UNIVERSE` in `kalshi_io/config.py` with the next month's active liquid tickers
- **Backfill new tickers**: `python -m pull_historical.pull_daily --tickers path/to/new_tickers.txt`

