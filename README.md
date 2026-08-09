# Kalshi Macro Market Data Pipeline

Kalshi_Pull is a standalone tool that collects market data from Kalshi prediction markets on US macro events. It pulls three kinds of data and nothing else: candles (daily, hourly, minute), individual trades, and orderbook snapshots. Every pull writes partitioned zstd Parquet under `kalshi_data/`, dedupes on write, and resumes from the last stored row, so re-running is safe. The repo contains no analysis code; downstream research lives in separate projects that read the output files. A committed ticker catalog covers 15 macro series, so there is no discovery step between cloning, installing, and the first pull.

## Quickstart

1. Clone and enter the repo:

   ```bash
   git clone https://github.com/Arom-MFE/Kalshi_Pull.git
   cd Kalshi_Pull
   ```

2. Create a virtual environment and install:

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -e .
   ```

3. Create an API key from your Kalshi account settings (see the [official key guide](https://docs.kalshi.com/getting_started/api_keys)) and save the private key it gives you:

   ```bash
   mkdir -p ~/.kalshi && chmod 700 ~/.kalshi
   # save the PEM as ~/.kalshi/kalshi_key.pem
   chmod 600 ~/.kalshi/kalshi_key.pem
   ```

4. Copy `.env.example` to `.env` at the repo root and fill in both values: `KALSHI_API_KEY_ID` and `KALSHI_KEY_PATH`.

5. Pull daily candles for one ticker:

   ```bash
   python -m pull_historical.pull_daily --tickers KXRECSSNBER-26
   ```

6. Read the result:

   ```python
   import pandas as pd
   df = pd.read_parquet("kalshi_data/candles/daily/KXRECSSNBER/KXRECSSNBER-26.parquet")
   df["date"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
   ```

## Example output

![Daily close of KXRECSSNBER-26, read as the market-implied probability of a 2026 US recession](docs/recession_probability.png)

Source: `kalshi_data/candles/daily/KXRECSSNBER/KXRECSSNBER-26.parquet`, 2025-07-15 through 2026-08-09. Price equals market-implied probability, so this contract repriced a 2026 recession from a first close of 0.42 to a latest close of 0.05.

## Tools

| Script | What it does | Needs API key |
|---|---|---|
| `pull_historical/pull_daily.py` | Daily candles, resuming from the last stored row | Yes |
| `pull_historical/pull_hourly.py` | Hourly candles, partitioned by year | Yes |
| `pull_historical/pull_minute.py` | Minute candles, partitioned by year and month | Yes |
| `pull_historical/pull_trades.py` | Every individual trade for a ticker | No |
| `pull_historical/pull_audit.py` | Coverage report over stored daily files; no API calls | No |
| `pull_historical/pull_all_freq.py` | All four pullers over every cataloged ticker | Yes |
| `pull_live/poll_focus.py` | Scheduled pulls plus orderbook snapshots over `FOCUS_UNIVERSE` | Yes |
| `get_ticker_info/get_tickers.py` | Rebuilds the ticker catalog for the 15 series | Yes |
| `get_ticker_info/get_Econ_Info.py` | Lists every series on Kalshi by category | No |

The trade endpoints are public today, so `pull_trades` runs without credentials; `kalshi_io/client.py` keeps them on an unauthenticated session.

## Finding what to pull

- `get_ticker_info/get_Econ_Info.py` prints every series on Kalshi grouped by category, then full details for the Economics ones.
- The committed files under `get_ticker_info/kalshi_tickers/` are the current catalog: one JSON and one TXT per series, plus `all_tickers.json` and `all_tickers.txt`.
- `get_ticker_info/get_tickers.py` rebuilds the catalog. `discover_series("KXCPIYOY")` works for any series ticker, and `build_combined()` regenerates the `all_tickers` files.

## Usage

```bash
python -m pull_historical.pull_daily --tickers KXRECSSNBER-26
python -m pull_historical.pull_hourly --tickers KXFED --limit 10
python -m pull_historical.pull_minute --tickers KXCPIYOY-26JUL-T3.5
python -m pull_historical.pull_trades --tickers KXCPIYOY-26JUL-T3.5
python -m pull_historical.pull_audit --tickers KXRECSSNBER
```

`--tickers` accepts a file path (`.txt` or `.json`), a series name, or a single ticker. `--limit` caps how many tickers a run processes. The candle and trade pullers also accept `--since YYYY-MM-DD`.

`pull_all_freq` runs daily, hourly, minute, and trade pulls over all 4,065 cataloged tickers in one shot. At that scale it is a multi-hour to multi-day job making tens of thousands of API calls; prefer the individual pullers with `--tickers` and `--limit` for bounded runs.

Live polling runs every puller on a cadence against the 37 tickers in `FOCUS_UNIVERSE`. Defaults: minute candles, trades, and orderbook snapshots every 60 seconds; hourly candles every 900; daily candles every 21600. Ctrl+C or SIGTERM finishes the current task, then exits cleanly.

```bash
python -m pull_live.poll_focus
python -m pull_live.poll_focus --minute-interval 30 --no-daily
python -m pull_live.poll_focus --iterations 1
```

Every puller is also importable:

```python
from pull_historical.pull_daily import run
result = run("KXRECSSNBER-26")
# {'processed': 1, 'skipped': 0, 'rows_written': 0, 'elapsed_sec': 0.7}
```

## Data and schemas

Candles (daily, hourly, minute):

| Column | Dtype | Meaning |
|---|---|---|
| `ts_ms` | int64 | Candle end time, UTC milliseconds |
| `open`, `high`, `low`, `close` | float64 | Trade prices in dollars, 0.0 to 1.0 |
| `mean` | float64 | Mean trade price for the period, as reported by the API |
| `volume` | float64 | Contracts traded during the period |
| `open_interest` | float64 | Contracts outstanding, as the API reports for the period |
| `market_ticker`, `event_ticker`, `series_ticker` | str | Kalshi identifiers |

Trades:

| Column | Dtype | Meaning |
|---|---|---|
| `trade_id` | str | Unique trade identifier, the dedupe key |
| `market_ticker` | str | Kalshi market identifier |
| `ts_ms` | int64 | Execution time, UTC milliseconds |
| `yes_price`, `no_price` | float64 | Fill prices in dollars, 0.0 to 1.0 |
| `count` | float64 | Contracts filled; fractional values are genuine |
| `taker_side` | str | Side the aggressor traded, `yes` or `no` |

Orderbook snapshots:

| Column | Dtype | Meaning |
|---|---|---|
| `ts_ms` | int64 | Snapshot time, UTC milliseconds |
| `market_ticker` | str | Kalshi market identifier |
| `side` | str | `YES` or `NO` bid book |
| `price` | float64 | Bid price in dollars |
| `quantity` | float64 | Contracts resting at this level |
| `cumulative_qty` | float64 | Running total from the best price down |
| `distance_from_top` | int64 | 0 at the best price, counting down the book |

The contract across every file: `ts_ms` is int64 UTC milliseconds. Prices are float64 dollars in [0.0, 1.0] and read directly as probabilities, because each contract settles at 1 dollar or 0. Volume, open interest, and count are float64 contract counts passed through unscaled; fractional values are genuine fractional contracts, not artifacts. NaN marks values the API did not provide; nothing is invented as 0.

## Directory layout

```
Kalshi_Pull/
├── kalshi_io/                    # shared library
│   ├── client.py                 # SDK client + REST session, credentials read lazily
│   ├── config.py                 # paths, chunk sizes, SERIES_LIST, FOCUS_UNIVERSE
│   ├── tickers.py                # ticker list loading (file, series name, single ticker)
│   ├── resolve.py                # event, market, and metadata resolution with fallbacks
│   ├── candles.py                # candle fetch + normalization for both API shapes
│   ├── trades.py                 # trade fetch with cursor pagination
│   ├── orderbook.py              # orderbook snapshot to DataFrame
│   └── storage.py                # parquet append, dedupe, resume, path routing
├── pull_historical/              # backfill CLIs (daily, hourly, minute, trades, audit, all_freq)
├── pull_live/
│   └── poll_focus.py             # cadence scheduler over FOCUS_UNIVERSE
├── get_ticker_info/
│   ├── get_Econ_Info.py          # list all series by category
│   ├── get_tickers.py            # per-series discovery, writes the catalog
│   └── kalshi_tickers/           # committed catalog: per-series JSON + TXT, all_tickers.*
├── examples/
│   └── plot_recession_probability.py
├── docs/
│   └── recession_probability.png
├── tests/                        # 9 offline tests
└── kalshi_data/                  # output, gitignored
    ├── candles/daily/{series}/{ticker}.parquet
    ├── candles/hourly/{series}/{year}/{ticker}.parquet
    ├── candles/minute/{series}/{year}/{month}/{ticker}.parquet
    ├── trades/{series}/{ticker}/{yyyy-mm}.parquet
    ├── orderbook/{ticker}/{yyyy-mm-dd}.parquet
    └── logs/
```

## Reading the data

```python
import pandas as pd
df = pd.read_parquet("kalshi_data/candles/daily/KXRECSSNBER/KXRECSSNBER-26.parquet")
df["date"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
```

```sql
SELECT market_ticker, COUNT(*) AS rows, MIN(ts_ms) AS first, MAX(ts_ms) AS last
FROM read_parquet('kalshi_data/candles/daily/**/*.parquet')
GROUP BY market_ticker
ORDER BY rows DESC;
```

## Series covered

15 series, 524 events, 4,065 unique tickers, per the committed `all_tickers.json`.

| Category | Series |
|---|---|
| Inflation | KXCPI, KXCPIYOY, KXACPI, KXCPICORE, KXPCECORE, KXCPICOREYOY |
| Labor | KXU3, KXJOBLESS, KXPAYROLLS |
| Growth | KXGDP, KXGDPYEAR, KXRECSSNBER |
| Fed | KXFEDDECISION, KXFED, KXFEDMEET |

## Tests

9 offline tests cover candle normalization, trade normalization, and the parquet append round trip. No credentials or network needed. Run with `pytest -q`.

## Known API quirks

- **KX prefix migration.** Older tickers have no KX prefix; newer ones do. The same series file holds both, for example `CPIYOY-22DEC` and `KXCPIYOY-26JUN` events. Discovery queries both variants of every series name.
- **Two response shapes for the same candle.** The live endpoint sends `price.close_dollars` and `volume_fp`; the historical endpoint sends `price.close` and `volume` for the same values. `kalshi_io/candles.py` normalizes both into one schema.
- **Old markets 404 on live endpoints.** Settled markets age out of the live API onto `/historical/` endpoints. The resolvers in `kalshi_io/resolve.py` fall back through up to four tiers, and the candle fetcher swaps to the historical endpoint when the live one returns 404.
- **5,000 candle cap per request.** The API rejects a request whose window spans more than 5,000 candles with the error `max candlesticks: 5000`. Chunk sizes in `kalshi_io/config.py` keep every window under the cap; a 3-day minute window is 4,320 candles.
- **Numbers arrive as decimal strings.** Prices, volumes, and counts are serialized as strings like `"0.6900"` and `"5247.00"`. Normalization casts them all to float64.
- **Fractional contracts are real.** The API reports contract counts as fixed-point values with two decimals, and fractional volumes such as `11747.08` are genuine fills, never rounding noise. They pass through unscaled.
- **Broken titles on some old markets.** 21 markets from the 2024 and 2025 KXACPI events carry unfilled template titles containing the literal text `above_below_between`. Titles are metadata only; prices are unaffected.
- **Unresolvable tickers get skipped, not fatal.** When a ticker cannot be resolved or fetched, the pullers log it to `kalshi_data/logs/skip_*.txt` and continue.

## Design notes

- **Trades refetch the full tape.** `fetch_trades` pulls the complete live plus historical tape and dedupes on `trade_id` instead of paginating incrementally. The largest stored tape here is 1,460 rows, so a full refetch is cheaper than cursor bookkeeping and cannot leave gaps.
- **Minute backfill starts at market open.** The `pull_minute` CLI defaults `--since` to 2025-01-01, but `pull_all_freq` calls the programmatic `run()` without it, so cold-start tickers pull minute candles from the market's open time.
- **The focus universe is hardcoded.** `FOCUS_UNIVERSE` in `kalshi_io/config.py` lists 37 tickers, each verified active on 2026-08-09. It is refreshed by hand once per macro cycle, so a polling run never depends on a discovery run.

## Maintenance

- **Weekly:** run `get_ticker_info/get_tickers.py` to pick up new events and tickers; it rewrites the per-series files and `all_tickers.*`.
- **Per macro cycle:** update `FOCUS_UNIVERSE` in `kalshi_io/config.py` with the next cycle's active tickers.
- **New tickers:** backfill with `python -m pull_historical.pull_daily --tickers path/to/new_tickers.txt`.

## API guide

[KalshiAPI.md](KalshiAPI.md) covers how Kalshi structures series, events, and markets, and which endpoints this repo calls.

## License

MIT, see [LICENSE](LICENSE).
