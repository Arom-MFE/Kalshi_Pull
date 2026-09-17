# Kalshi Market Data Pipeline

Kalshi_Pull turns Kalshi's prediction market API into clean, research-ready market data. Kalshi is an exchange where contracts trade on real-world events, so every price reads as a probability, and a full price history is a record of what the market believed, day by day. Getting that history out of the raw API is the hard part: data is split across live and historical endpoints that name the same fields differently, prices and volumes arrive as text rather than numbers, candle requests are capped at 5,000 per call, and settled markets drop off the live endpoints entirely. Kalshi_Pull handles all of it, pulling price history at three speeds (daily, hourly, minute) with the bid and ask quotes of every period, every individual trade, and order book snapshots into partitioned zstd Parquet that dedupes on write and resumes where it stopped. It ships with a committed catalog of 15 US macro series, from CPI and Fed decisions to payrolls and GDP, and it can pull any ticker on the exchange. A search CLI finds series, events and markets, and a roll command moves the catalog and the polled universe to the next event cycle. Every data endpoint it uses is public, so no API key is needed. There is no analysis code here: downstream research reads the Parquet output.

## Quickstart

1. Clone and enter the repo:

   ```bash
   git clone https://github.com/Arom-MFE/Kalshi_Pull.git
   cd Kalshi_Pull
   ```

2. Create a virtual environment and install. Python 3.13 or newer is required, because the Kalshi SDK requires it:

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -e .
   ```

3. Pull daily candles for one ticker. No API key is needed:

   ```bash
   python -m pull_historical.pull_daily --tickers KXRECSSNBER-26
   ```

4. Read the result:

   ```python
   import pandas as pd
   df = pd.read_parquet("kalshi_data/candles/daily/KXRECSSNBER/KXRECSSNBER-26.parquet")
   df["date"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
   ```

### Credentials are optional

Series, events, markets, candlesticks, trades and the orderbook all answer without a key today. If an endpoint ever answers 401 or 403, the same request is sent again with signed headers, and only then are credentials read. To prepare for that, create an API key in your Kalshi account settings (see the [official key guide](https://docs.kalshi.com/getting_started/api_keys)), save the private key, and fill in `.env`:

```bash
mkdir -p ~/.kalshi && chmod 700 ~/.kalshi
# save the PEM as ~/.kalshi/kalshi_key.pem
chmod 600 ~/.kalshi/kalshi_key.pem
cp .env.example .env    # then set KALSHI_API_KEY_ID and KALSHI_KEY_PATH
```

The SDK `kalshi-python-sync` is used only to sign requests. Version 3.30.0 or newer is required: older versions fail on every event call since Kalshi changed its event payload on 2026-09-10 (see [Known API changes](#known-api-changes)).

## Tools

| Script | What it does | Network |
|---|---|---|
| `get_ticker_info/find_events.py` | Search series, events and markets by category, tag, keyword and status | Keyless REST |
| `get_ticker_info/roll.py` | Refresh the catalog, report what changed, propose the next focus universe | Keyless REST |
| `get_ticker_info/get_tickers.py` | Rebuild the catalog without the report | Keyless REST |
| `get_ticker_info/get_Econ_Info.py` | List every series on Kalshi by category | Keyless REST |
| `pull_historical/pull_daily.py` | Daily candles, resuming from the last stored row | Keyless REST |
| `pull_historical/pull_hourly.py` | Hourly candles, partitioned by year | Keyless REST |
| `pull_historical/pull_minute.py` | Minute candles, partitioned by year and month | Keyless REST |
| `pull_historical/pull_trades.py` | Every individual trade for a ticker, resuming from the last stored trade | Keyless REST |
| `pull_historical/pull_all_freq.py` | All four pullers over every cataloged ticker | Keyless REST |
| `pull_historical/pull_audit.py` | Coverage report over stored daily files, with catalog age and focus universe status | None, offline |
| `pull_live/poll_focus.py` | Scheduled pulls plus orderbook snapshots over the derived focus universe | Keyless REST |

Every request goes through one helper, `kalshi_io/client.py`, which rate limits, retries and fails loudly. See [Design notes](#design-notes).

## Finding what to pull

`find_events.py` searches the exchange. Pick one mode:

```bash
python get_ticker_info/find_events.py --categories
python get_ticker_info/find_events.py --category Economics --keyword inflation
python get_ticker_info/find_events.py --keyword "fed funds"
python get_ticker_info/find_events.py --series KXCPIYOY --status open
python get_ticker_info/find_events.py --series KXCPIYOY --status open --markets
python get_ticker_info/find_events.py --event KXCPIYOY-26SEP --format py
```

```
SERIES_TICKER  EVENT_TICKER    LIVE_MARKETS  OPEN_MARKETS  CLOSES                TITLE
KXCPIYOY       KXCPIYOY-26SEP  21            21            2026-10-14T12:29:00Z  Inflation in September 2026 (CPI YoY)
KXCPIYOY       KXCPIYOY-26NOV  21            21            2026-12-10T13:29:00Z  Inflation in November 2026 (CPI YoY)
```

| Option | Meaning |
|---|---|
| `--status` | Events take `unopened`, `open`, `closed`, `settled`. Markets also take `paused`. Omit it for every status. The API rejects `all` |
| `--format table` | Aligned columns, the default |
| `--format tickers` | One ticker per line. For markets this is a valid `--tickers` file |
| `--format json` | The rows as JSON |
| `--format py` | A Python list literal, ready for `FOCUS_OVERRIDE` or `FOCUS_SERIES` |
| `--include-legacy` | Also list dead pre-KX series spellings such as `CPIYOY` |

Exit codes: 0 results, 1 nothing found, 2 usage error, 3 API error.

The committed files under `get_ticker_info/kalshi_tickers/` are the catalog: one JSON and one TXT per series, plus `all_tickers.json` and `all_tickers.txt`. The pullers resolve every ticker through it, and fall back to the API for a ticker it does not hold.

## Rolling forward

Events close and new ones open every month. `roll.py` moves the pipeline to the current cycle:

```bash
python get_ticker_info/roll.py --dry-run     # report only, write nothing
python get_ticker_info/roll.py               # refresh, write, report
```

It rediscovers the 15 series (about 125 requests, under a minute), rewrites the catalog, and derives the focus universe with the same rule `poll_focus` uses. The report lists the previous build and its age, totals before and after, new events, new markets in existing events, status changes per event, removed and carried-forward tickers, the proposed universe, sanity checks marked `[ok]` or `[FAIL]`, and the backfill commands to run next. The universe is written to `kalshi_tickers/focus_universe.json` and `.txt`, and every puller reads it as `--tickers focus`:

```bash
python -m pull_historical.pull_daily  --tickers focus
python -m pull_historical.pull_hourly --tickers focus
python -m pull_historical.pull_minute --tickers focus --since 2026-07-19
python -m pull_historical.pull_trades --tickers focus
```

Safety rules of a refresh: a series is written only if every request succeeded. An event the API lists as open must have an active market in the result, or nothing is written for that series. A ticker of the previous catalog that the API no longer returns is carried forward, so stored data always stays resolvable. A failed series keeps its previous files, the others are still written, and the exit code is 1. Other options: `--series`, `--events-per-series`, `--out-dir` (work on a catalog copy), `--no-focus`, `--full`.

## The focus universe

`poll_focus` polls a universe that is derived, not listed by hand:

| Setting in `kalshi_io/config.py` | Default | Meaning |
|---|---|---|
| `FOCUS_SERIES` | KXFED, KXFEDDECISION, KXCPIYOY, KXPAYROLLS, KXU3 | Series to follow |
| `FOCUS_EVENTS_PER_SERIES` | 1 | Nearest open events to take per series |
| `FOCUS_OVERRIDE` | empty | Market tickers. When set, replaces the rule and never rolls |
| `FOCUS_REFRESH_SECONDS` | 3600 | Seconds between refreshes while polling |

The rule: per series, one snapshot of the open markets is grouped by event. Events are ranked by the earliest `close_time` of their open markets, and the nearest ones are taken whole. Times come from the market payload, never from the ticker text. On 2026-09-17 this gave KXFED-26OCT, KXFEDDECISION-26OCT, KXCPIYOY-26SEP, KXPAYROLLS-26SEP and KXU3-26SEP: 64 tickers.

Before any pull, a preflight checks the universe. If it is empty, or nothing in it can trade, `poll_focus` prints the counts per status and exits with code 2. This matters because a settled market's orderbook answers 200 with empty books, so polling a stale universe would run clean and capture nothing.

While polling, the universe is refreshed every hour and 90 seconds after the nearest `close_time`. Tickers that left the selection are looked up before anything is dropped. What can still trade stays (a paused market, or an event pushed out by a nearer one). The rest get one final candle and trade sweep and are dropped, and the change is logged as `universe roll KXCPIYOY: KXCPIYOY-26SEP -> KXCPIYOY-26NOV`. A refresh that fails or finds nothing keeps the last good universe and logs an error. The polled universe is written to `kalshi_data/logs/focus_universe_live.json`.

One known limit: an event rolled to inside the loop starts its minute history cold, from market open, inside the loop. Running `roll.py --events-per-series 2` and the backfill ahead of a close avoids that stall.

## Usage

```bash
python -m pull_historical.pull_daily --tickers KXRECSSNBER-26
python -m pull_historical.pull_hourly --tickers KXFED --limit 10
python -m pull_historical.pull_minute --tickers KXCPIYOY-26SEP-T3.5 --since 2026-09-01
python -m pull_historical.pull_trades --tickers KXCPIYOY-26SEP-T3.4 KXCPIYOY-26SEP-T3.5
python -m pull_historical.pull_audit --tickers focus
```

`--tickers` accepts a file path (`.txt` or `.json`), a series name, `focus`, or market tickers. Several values may be given, separated by spaces or commas, also inside one quoted string. Each ticker is checked against the catalog and then the API. Unknown tickers are reported, recorded in the skip file and not fetched. `--limit` caps how many tickers a run processes. The candle and trade pullers also accept `--since YYYY-MM-DD`. A puller exits with code 1 when any ticker failed.

`pull_all_freq` runs daily, hourly, minute, and trade pulls over every cataloged ticker in one shot. At that scale it is a multi-hour to multi-day job making tens of thousands of API calls; prefer the individual pullers with `--tickers` and `--limit` for bounded runs.

Live polling runs every puller on a cadence against the focus universe. Defaults: minute candles, trades, and orderbook snapshots every 60 seconds; hourly candles every 900; daily candles every 21600. Ctrl+C or SIGTERM finishes the current task, then exits cleanly.

```bash
python -m pull_live.poll_focus --show-universe          # derive, check, print, exit
python -m pull_live.poll_focus
python -m pull_live.poll_focus --minute-interval 30 --no-daily
python -m pull_live.poll_focus --iterations 1
python -m pull_live.poll_focus --tickers KXCPIYOY-26SEP-T3.4 KXCPIYOY-26SEP-T3.5
```

| `poll_focus` exit code | Meaning |
|---|---|
| 0 | Clean stop |
| 1 | The universe could not be built (API failure, unreadable `--tickers` source) |
| 2 | Preflight refused to start: universe empty, or nothing in it can trade |
| 3 | An explicit universe (`--tickers`, `FOCUS_OVERRIDE`) ran out of tradable tickers during the run |

Every puller is also importable:

```python
from pull_historical.pull_daily import run
result = run("KXRECSSNBER-26")
# {'processed': 1, 'skipped': 0, 'failed': 0, 'aborted': False, 'unknown': [],
#  'rows_written': 0, 'elapsed_sec': 0.7}
```

### Settings from the environment

| Variable | Default | Meaning |
|---|---|---|
| `KALSHI_DATA_DIR` | `<repo>/kalshi_data` | Data root. Point it at a scratch directory for test runs. Every run logs the data root it uses |
| `KALSHI_MAX_RPS` | 10 | Client-side cap on requests per second, at most 20 |

Both are read from the process environment, never from `.env`.

## Data and schemas

Candles (daily, hourly, minute):

| Column | Dtype | Meaning |
|---|---|---|
| `ts_ms` | int64 | Candle end time, UTC milliseconds |
| `open`, `high`, `low`, `close` | float64 | Trade prices in dollars, 0.0 to 1.0. NaN when the period had no trade |
| `mean` | float64 | Mean trade price for the period, as reported by the API |
| `volume` | float64 | Contracts traded during the period |
| `open_interest` | float64 | Contracts outstanding, as the API reports for the period |
| `market_ticker`, `event_ticker`, `series_ticker` | str | Kalshi identifiers |
| `yes_bid_open`, `yes_bid_high`, `yes_bid_low`, `yes_bid_close` | float64 | Best YES bid over the period, in dollars |
| `yes_ask_open`, `yes_ask_high`, `yes_ask_low`, `yes_ask_close` | float64 | Best YES ask over the period, in dollars |

Every candle file has exactly these 19 columns in this order, with the same types, for daily, hourly and minute candles and for both API tiers.

The price columns hold trade prices only. A period without a trade has NaN in `open`, `high`, `low`, `close` and `mean`, and `volume` 0. The quote columns exist for every candle, traded or not. On a quiet strike most minute candles have quotes and no trade, so the quotes are the only price information there. An empty side of the book is quoted by the API as 0.0 (no bid) or 1.0 (no ask) and stored as sent. Until version 0.2.0 the historical tier copied the bid into the price columns of a period without trades; since 0.3.0 no price column ever holds a quote.

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

Catalog, `{SERIES}_tickers.json`, schema version 2:

| Key | Meaning |
|---|---|
| `schema_version`, `series`, `built_at` | Format version, series ticker, UTC build time |
| `historical_cutoff` | The API's live and historical boundary at build time |
| `status_counts` | Markets per API status |
| `events` | `event_ticker` and `title` |
| `markets` | `event_ticker`, `market_ticker`, `title`, `status`, `open_time`, `close_time`, `expected_expiration_time`, `latest_expiration_time`, `source` |
| `tickers` | Sorted market tickers, the same list as the TXT file |

Times are the API's ISO strings, null when the API sent none. `status` is the API status at build time. `source` is `live`, `historical`, or `carried_forward`. `all_tickers.json` adds totals, `status_counts` and the `built_at` of every series.

## Directory layout

```
Kalshi_Pull/
├── kalshi_io/                    # shared library
│   ├── client.py                 # REST helper: rate limit, retries, signed fallback
│   ├── config.py                 # paths, chunk sizes, SERIES_LIST, focus universe rule
│   ├── discovery.py              # series, event and market search
│   ├── catalog.py                # build, read and compare the ticker catalog
│   ├── universe.py               # derive, check and refresh the focus universe
│   ├── tickers.py                # ticker list loading and validation
│   ├── resolve.py                # event, market, and metadata resolution with fallbacks
│   ├── candles.py                # candle fetch + normalization for both API shapes
│   ├── trades.py                 # trade fetch with cursor pagination and resume
│   ├── orderbook.py              # orderbook snapshot to DataFrame
│   ├── runlog.py                 # per-run log files and skip files
│   └── storage.py                # parquet append, dedupe, resume, path routing
├── pull_historical/              # backfill CLIs (daily, hourly, minute, trades, audit, all_freq)
├── pull_live/
│   └── poll_focus.py             # cadence scheduler over the derived focus universe
├── get_ticker_info/
│   ├── find_events.py            # search series, events and markets
│   ├── roll.py                   # refresh the catalog, propose the focus universe
│   ├── get_tickers.py            # per-series discovery, writes the catalog
│   ├── get_Econ_Info.py          # list all series by category
│   └── kalshi_tickers/           # committed catalog: per-series JSON + TXT, all_tickers.*, focus_universe.*
├── tests/                        # offline tests against a fake exchange
└── kalshi_data/                  # output, gitignored
    ├── candles/daily/{series}/{ticker}.parquet
    ├── candles/hourly/{series}/{year}/{ticker}.parquet
    ├── candles/minute/{series}/{year}/{month}/{ticker}.parquet
    ├── trades/{series}/{ticker}/{yyyy-mm}.parquet
    ├── orderbook/{ticker}/{yyyy-mm-dd}.parquet
    └── logs/
        ├── pull_daily_{yyyymmdd_hhmm}.log      # one per run, same for the other pullers
        ├── poll_focus_{yyyymmdd}.log           # one per day; the pullers it calls log here
        ├── roll_{stamp}.log, roll_report_{stamp}.txt
        ├── skip_{kind}_{process start}.txt     # time, ticker, reason, tab separated
        ├── audit_{yyyymmdd}.csv
        └── focus_universe_live.json            # what poll_focus is polling
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

Files written before 2026-09-17 lack the quote columns. A query that names them across old and new files needs `union_by_name`, which fills the missing columns with NULL:

```sql
SELECT market_ticker, ts_ms, close, yes_bid_close, yes_ask_close
FROM read_parquet('kalshi_data/candles/minute/**/*.parquet', union_by_name = true);
```

## Series covered

15 series, 563 events, 4,840 unique tickers (611 of them open), per the committed `all_tickers.json`, built 2026-09-17.

| Category | Series |
|---|---|
| Inflation | KXCPI, KXCPIYOY, KXACPI, KXCPICORE, KXPCECORE, KXCPICOREYOY |
| Labor | KXU3, KXJOBLESS, KXPAYROLLS |
| Growth | KXGDP, KXGDPYEAR, KXRECSSNBER |
| Fed | KXFEDDECISION, KXFED, KXFEDMEET |

## Tests

238 offline tests run against an in-memory fake exchange that answers like the live API, including its error bodies. They cover normalization, the HTTP layer (retries, backoff, signed fallback), discovery, the catalog, the focus universe, every puller end to end, the poller, the CLIs, logging and the audit. Three guards are always on: data goes to a temp directory, opening a socket fails the test, and credentials cannot be read. Installing with `pip install -e ".[dev]"` provides pytest. Run with `pytest -q`.

## Known API changes

Changes on Kalshi's side that this repo follows. The two from September 2026 broke ticker discovery in version 0.1.0. Dates are from the [official changelog](https://docs.kalshi.com/changelog).

| Date | Change | Handling |
|---|---|---|
| 2025-10-13 | `tags` on `/series` splits on commas only | Tags are joined with commas |
| 2025-11-27 | `/markets` takes one `status` filter per request | One status per request is sent |
| 2025-12-13 | `paused` added to the `/markets` status filters | Accepted by discovery and `find_events`; a paused market stays in the focus universe |
| 2026-02-26 | Settled markets, their candles and older trades move to `/historical/` endpoints behind a moving cutoff | Live endpoint first, historical on 404; catalog and trades read both tiers |
| 2026-05 | `taker_side` on trades deprecated in favor of `taker_outcome_side` | `taker_outcome_side` is read when `taker_side` is missing |
| 2026-05-07 | Dedicated hosts announced, `https://external-api.kalshi.com/trade-api/v2` | The shared host `https://api.elections.kalshi.com/trade-api/v2` remains supported and is still used |
| 2026-09-10 | `available_on_brokers` removed from event payloads | `kalshi-python-sync` 3.27 and older raise a validation error on every event call. 3.30.0 or newer is required, and no SDK model sits in the data path anymore |
| 2026-09-17 | `category` on `/series` matches any entry of a series' `categories` list | A listed series can show a different primary category |
| Not in the changelog | `status=all` is rejected with HTTP 400 `invalid status filter`, although the quick start page still mentions it | The filter is omitted to get every status. Filters are checked before a request is made |
| Not in the changelog | Pre-KX series spellings (`CPIYOY`, `JOBLESS`) return nothing on list endpoints | The KX series returns the pre-KX events. The old spelling is still probed as insurance |

## Known API Nuances

- **KX prefix migration.** Older tickers have no KX prefix; newer ones do. The same series file holds both, for example `CPIYOY-22DEC` and `KXCPIYOY-26JUN` events. The API files pre-KX events under the KX series, so one query by the KX name returns them all.
- **Two response shapes for the same candle.** The live endpoint sends `price.close_dollars`, `yes_bid.close_dollars` and `volume_fp`; the historical endpoint sends `price.close`, `yes_bid.close` and `volume` for the same values. `kalshi_io/candles.py` normalizes both into one schema.
- **A period without trades has quotes but no trade prices.** The live endpoint then omits the trade price keys, the historical endpoint sends them as null. Both always send `yes_bid` and `yes_ask`.
- **Filter words are not status values.** Requests filter by `unopened`, `open`, `paused`, `closed`, `settled`. Responses carry `initialized`, `active`, `inactive`, `closed`, `determined`, `disputed`, `amended`, `finalized`.
- **Market payloads carry no series.** A market names its event, and the event names its series. Resolving an uncataloged ticker therefore takes two lookups.
- **Old markets 404 on live endpoints.** Settled markets age out of the live API onto `/historical/` endpoints. The two tiers overlap near the cutoff. The candle fetcher swaps to the historical endpoint when the live one returns 404.
- **5,000 candle cap per request.** The API rejects a request whose window spans more than 5,000 candles with the error `max candlesticks: 5000`. Chunk sizes in `kalshi_io/config.py` keep every window under the cap; a 3-day minute window is 4,320 candles.
- **Rate limit responses carry no Retry-After.** A 429 has the body `{"error": "too many requests"}` and no headers to read. Backoff is the mechanism. Keyless bursts on the candlestick endpoints were throttled at 10 requests per second during verification; every one succeeded on the first retry.
- **Numbers arrive as decimal strings.** Prices, volumes, and counts are serialized as strings like `"0.6900"` and `"5247.00"`. Normalization casts them all to float64.
- **Fractional contracts are real.** The API reports contract counts as fixed-point values with two decimals, and fractional volumes such as `11747.08` are genuine fills, never rounding noise. They pass through unscaled.
- **Broken titles on some old markets.** 21 markets from the 2024 and 2025 KXACPI events carry unfilled template titles containing the literal text `above_below_between`. Titles are metadata only; prices are unaffected.

## Design notes

- **One REST helper, keyless first.** `request_json` spaces requests (default 10 per second), retries 429, 5xx, timeouts, connection errors and a 200 that is not JSON, with exponential backoff and jitter, up to 6 attempts. Any other 4xx raises at once. A 401 or 403 repeats the call once with signed headers. When retries run out the failure is raised, logged at ERROR and recorded in the skip file. A run stops after 3 tickers in a row ran out of retries, because the API is down.
- **Failures are loud.** A ticker that fails is logged at ERROR, counted under `failed` in the run summary, and written to `logs/skip_{kind}_{process start}.txt` with a timestamp and the reason. The same problem is recorded once per 6 hours per process, so a long poller does not repeat it every minute.
- **Trades resume from the last stored trade.** The request carries `min_ts`, 60 seconds before the last stored trade. Stored `trade_id`s are dropped before the append, so a quiet cycle costs one request and writes nothing. The historical tier is asked only when the resume point lies before the cutoff. A trade fetch is all or nothing: pages arrive newest first, so saving a truncated fetch would move the resume point past trades that were never downloaded.
- **Candles may save a partial prefix.** Windows are fetched oldest first and are inclusive on both ends. If a window fails after earlier ones arrived, the rows before it are gap-free and are saved, the failure is recorded, and the next run resumes from the last saved candle.
- **Logging is configured once.** Every run attaches its own log file and detaches it when it ends. Pullers called by `poll_focus` log into its daily file, so a long run does not pile up handlers or duplicate lines.
- **Uncataloged tickers get their true event.** A ticker missing from the catalog is resolved through the API (market, then event, then series). The event is never guessed from the ticker text.
- **Minute backfill starts at market open.** The `pull_minute` CLI defaults `--since` to 2025-01-01, but `pull_all_freq` and `poll_focus` call the programmatic `run()` without it, so a cold-start ticker pulls minute candles from the market's open time and later runs resume from the last stored candle.

## Maintenance

- **Weekly:** run `python get_ticker_info/roll.py`, read the report, and commit the catalog. `pull_audit` warns when the catalog is older than 7 days.
- **Per macro cycle:** nothing to edit. `poll_focus` derives and rolls its universe. To backfill a new cycle ahead of time, run the pullers with `--tickers focus` after a roll.
- **New series:** add it to `SERIES_LIST` in `kalshi_io/config.py` (and to `FOCUS_SERIES` to poll it), then run `roll.py`. `find_events.py` finds the series ticker.

## API guide

[KalshiAPI.md](KalshiAPI.md) covers how Kalshi structures series, events, and markets, and which endpoints this repo calls. [CHANGELOG.md](CHANGELOG.md) lists what changed in each version.

## License

MIT, see [LICENSE](LICENSE).
