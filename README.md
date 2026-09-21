# Kalshi Market Data Pipeline

Kalshi_Pull turns Kalshi's prediction market API into clean, research-ready market data. Kalshi is an exchange where contracts trade on real-world events, so every price reads as a probability, and a full price history is a record of what the market believed, day by day. Getting that history out of the raw API is the hard part: data is split across live and historical endpoints that name the same fields differently, prices and volumes arrive as text rather than numbers, candle requests are capped at 5,000 per call, and settled markets drop off the live endpoints entirely. Kalshi_Pull handles all of it, pulling price history at three speeds (daily, hourly, minute) with the bid and ask quotes of every period, every individual trade, and order book snapshots into partitioned zstd Parquet that dedupes on write and resumes where it stopped. A metadata store keeps what each market was: its strike, its open and close, and once it settled, its result and the value it settled on. A bulk driver downloads the whole catalog in one resumable run, and a poller captures the order books of the markets that matter every few seconds around an economic release. It ships with a committed catalog of 15 US macro series, from CPI and Fed decisions to payrolls and GDP, and it can pull any ticker on the exchange. A search CLI finds series, events and markets, and a roll command moves the catalog and the polled universe to the next event cycle. Every data endpoint it uses is public, so no API key is needed. There is no analysis code here: downstream research reads the Parquet output.

## Quickstart

1. Clone and enter the repo:

   ```bash
   git clone https://github.com/Arom-MFE/Kalshi_Pull.git
   cd Kalshi_Pull
   ```

2. Create a virtual environment and install. A Python 3.13 or newer interpreter is required, because the Kalshi SDK requires it, so name it explicitly (`python3.14` works the same way):

   ```bash
   python3.13 -m venv .venv
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
| `get_ticker_info/roll.py` | Refresh the catalog and the market metadata store, report what changed, propose the next focus universe | Keyless REST |
| `get_ticker_info/get_tickers.py` | Rebuild the catalog without the report | Keyless REST |
| `get_ticker_info/get_Econ_Info.py` | List every series on Kalshi by category | Keyless REST |
| `pull_historical/backfill.py` | Bulk driver: metadata, daily, hourly, trades and minute candles over the catalog or a ticker list, with an estimate, priority order, a journal and failure lists; resumes with the same command | Keyless REST |
| `pull_historical/pull_daily.py` | Daily candles, from market open to close, resuming from the last stored row | Keyless REST |
| `pull_historical/pull_hourly.py` | Hourly candles, partitioned by year | Keyless REST |
| `pull_historical/pull_minute.py` | Minute candles, partitioned by year and month | Keyless REST |
| `pull_historical/pull_trades.py` | Every individual trade for a ticker, resuming from the last stored trade | Keyless REST |
| `pull_historical/pull_all_freq.py` | The driver over every cataloged ticker, without arguments | Keyless REST |
| `pull_historical/pull_audit.py` | Coverage report over stored daily files, catalog age and focus universe status, then the data-quality checks over the whole store (it stops before them when no audited ticker has a daily file) | None, offline |
| `pull_live/poll_focus.py` | Order books of the focus universe every minute, every 5 seconds around a release, plus scheduled candle and trade pulls; missing history is pulled in the background | Keyless REST |

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

It rediscovers the 15 series (about 125 requests, under a minute), rewrites the catalog, refreshes the market metadata store from the same payloads (so results fill in as markets settle, at no extra request), and derives the focus universe with the same rule `poll_focus` uses. The report lists the previous build and its age, totals before and after, new events, new markets in existing events, status changes per event, removed and carried-forward tickers, the metadata rows written, the proposed universe, sanity checks marked `[ok]` or `[FAIL]`, and the backfill command to run next. The universe is written to `kalshi_tickers/focus_universe.json` and `.txt`, and every puller reads it as `--tickers focus`:

```bash
python -m pull_historical.backfill --tickers focus    # every layer, from market open, resumable
```

Safety rules of a refresh: a series is written only if every request succeeded. An event the API lists as open must have an active market in the result, or nothing is written for that series. A ticker of the previous catalog that the API no longer returns is carried forward if it ever opened, so stored data always stays resolvable; one that never opened is dropped and listed as removed upstream. A failed series keeps its previous files, the others are still written, and the exit code is 1. Other options: `--series`, `--events-per-series`, `--out-dir` (work on a catalog copy; the metadata store still goes to the data root, because it is API truth, not catalog state), `--no-focus`, `--full`.

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

A ticker that joins the universe without stored daily or minute candles (a fresh store, or the event the poller just rolled to) is backfilled by a child process, `backfill.py --tickers ...` at `BACKGROUND_HISTORY_RPS` (3 requests per second), while the loop keeps capturing its books. Until that child has finished the loop pulls no candles or trades for the ticker, so the stored range stays contiguous from market open; a pull that fails is retried after 10 minutes, and after three failures the ticker rejoins the loop anyway. Together the two processes stay under the keyless rate limit. `--no-background-history` turns this off.

## Usage

```bash
python -m pull_historical.pull_daily --tickers KXRECSSNBER-26
python -m pull_historical.pull_hourly --tickers KXFED --limit 10
python -m pull_historical.pull_minute --tickers KXCPIYOY-26SEP-T3.5 --since 2026-09-01
python -m pull_historical.pull_trades --tickers KXCPIYOY-26SEP-T3.4 KXCPIYOY-26SEP-T3.5
python -m pull_historical.pull_audit --tickers focus
```

`--tickers` accepts a file path (`.txt` or `.json`), a series name, `focus`, or market tickers. Several values may be given, separated by spaces or commas, also inside one quoted string. A few old tickers hold a space or a comma themselves (`GDP-232022 Q4-T0.0`, `JOBLESS-22JUL23-C250,000`), so the rule has an order: a file is one ticker per line (or per element of `tickers` in a `.json` file) and a line is never split; an argument that is exactly a known ticker (cataloged, registered by the focus universe, or resolved through the API earlier in the process) is taken whole; only what is left is split on spaces and commas. Quote such a ticker as an argument of its own, `--tickers "GDP-232022 Q4-T0.0"`, or put it in a file: inside a quoted string next to other tickers it is split, and one that the catalog does not hold must come from a file. One limit remains for the poller: an explicit universe (`poll_focus --tickers`, `FOCUS_OVERRIDE`) is not registered, so there a ticker with a space or a comma must be cataloged. An event ticker is not expanded to its markets: write them to a file with `find_events.py --event E --format tickers` and pass the file. Each ticker is checked against the catalog and then the API. Unknown tickers are reported, recorded in the skip file and not fetched. `--limit` caps how many tickers a run processes. Without `--tickers` the candle pullers and the audit take every cataloged ticker; `pull_trades` requires it. The candle and trade pullers also accept `--since YYYY-MM-DD`; without it a pull starts at the market's open time on a cold start and resumes from the last stored row afterwards. A market that can no longer trade is pulled up to its close and never again past it. A puller exits with code 1 when any ticker failed.

### The bulk driver

`backfill.py` pulls every layer over the whole catalog, or over a ticker list, in one resumable run:

```bash
python -m pull_historical.backfill --estimate-only                 # requests, runtime, rows and disk; no request made
python -m pull_historical.backfill                                 # the whole catalog (pull_all_freq.py does the same)
python -m pull_historical.backfill --tickers focus
python get_ticker_info/find_events.py --event KXCPIYOY-26JUL --format tickers > /tmp/cpi_jul.txt
python -m pull_historical.backfill --tickers /tmp/cpi_jul.txt --layers daily,trades   # one event: list its markets first
python -m pull_historical.backfill --retry-failed                  # only what the newest failure lists hold
```

The layers run one after the other: metadata (a lookup of every ticker in batches of 100, which also refreshes status and close times so nothing below relies on a stale catalog), daily, hourly, trades, minute. Within a layer the tickers go event by event, events that can still trade first (nearest close first), then settled events, newest first. The estimate is printed before anything starts: on the 2026-09-17 catalog the whole download is 245,134 requests, at least 13.6 hours at the default rate (17 hours at 80 percent of it), 0.6 to 1.1 GB. A journal, `kalshi_data/state/backfill_journal.jsonl`, records every (ticker, layer) pair pulled completely while the market was finalized; such a pair can never change again, so a rerun skips it without a request, and everything else resumes from the last stored row. A ticker that fails is retried once at the end of its layer; what still fails goes to `logs/backfill_failed_{stamp}_{layer}.txt`. When the API is down (three tickers in a row ran out of retries) the run waits 1, 2, 4, 8 and 16 minutes between attempts, then exits with code 2; the same command resumes. Ctrl+C finishes the current ticker. The summary lists rows, requests, the share of 429 answers and the store's counts, then the data-quality checks. `--no-audit` skips those checks, which write `logs/quality_{date}.csv`; the coverage CSV `logs/audit_{date}.csv` comes from `pull_audit` only, never from the driver. A run is complete only when every ticker it was asked for has an outcome: a ticker for which a puller returned none is a failure like any other (retried at the end of the layer, written to the failure list), and `logs/backfill_summary_{stamp}.json` names the tickers behind every count (`complete`, `unknown`, and per layer `failed`, `other`, `not_attempted_tickers`, `unexpected`). A run over the whole catalog holds a lock, so a second one exits with code 75 instead of competing for the rate limit.

Exit codes: 0 complete (every ticker of every layer has an outcome), 1 some tickers failed or were left without an outcome although the run was neither interrupted nor stopped by an outage, 2 the API stayed down, 75 another run holds the lock, 130 interrupted. A usage error such as an unknown layer also exits with 2, as argparse does.

### Live polling

`poll_focus` polls the focus universe. Every cycle starts with one batch request for the order books of the whole universe, so every book of a sweep carries the same timestamp, once a minute. Then the scheduled pulls: minute candles and trades every 60 seconds, hourly candles every 900, daily candles every 21600. Ctrl+C or SIGTERM finishes the current task, then exits cleanly.

Around a release the books are polled every 5 seconds. A release window runs from 5 minutes before to 15 minutes after a `close_time`: Kalshi closes a market one to five minutes before the number it settles on comes out (12:29Z for a 12:30Z BLS print, 17:55Z or 17:59Z for the 18:00Z Fed statement), so the close is the anchor. Windows come from the polled events, from every cataloged event that can still trade (a CPI print moves the Fed strikes too), and from `RELEASE_CALENDAR` in `config.py`; windows that overlap merge. Inside a window only the books are polled: candle and trade pulls, and the final sweep of an event that just closed, wait for the window to end, because they can be pulled from the API afterwards and the book cannot. Upcoming windows are logged at startup.

```bash
python -m pull_live.poll_focus --show-universe          # derive, check, print, exit
python -m pull_live.poll_focus
python -m pull_live.poll_focus --minute-interval 30 --no-daily
python -m pull_live.poll_focus --iterations 1
python -m pull_live.poll_focus --tickers KXCPIYOY-26SEP-T3.4 KXCPIYOY-26SEP-T3.5
python -m pull_live.poll_focus --release-interval 2 --release-after 1800
python -m pull_live.poll_focus --no-daily --no-hourly --no-minute --no-trades   # books only
```

Flags: the cadences `--minute-interval`, `--trades-interval`, `--orderbook-interval`, `--hourly-interval`, `--daily-interval` and their switches `--no-minute`, `--no-trades`, `--no-orderbook`, `--no-hourly`, `--no-daily`; the universe flags `--tickers`, `--series`, `--events-per-series`, `--universe-refresh`, `--show-universe`; the release flags `--release-interval`, `--release-before`, `--release-after`, `--no-release-windows`; and `--iterations`, `--no-background-history`. The books-only form is the one to run next to a full download: books cannot be backfilled, and it costs one batch request per sweep (per 100 tickers), so one a minute and one every 5 seconds inside a release window, plus the hourly universe refresh of two list calls per focus series.

| `poll_focus` exit code | Meaning |
|---|---|
| 0 | Clean stop |
| 1 | The universe could not be built (API failure, unreadable `--tickers` source) |
| 2 | Preflight refused to start: universe empty, or nothing in it can trade. A bad flag also exits with 2, as argparse does |
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
| `KALSHI_MAX_RPS` | 5 | Client-side cap on requests per second, at most 20. Without a key the candlestick endpoints sustain about 4 to 5; the poller's background history pull runs at 3 |

Both are read from the process environment, never from `.env`.

## Data and schemas

Candles (daily, hourly, minute):

| Column | Dtype | Meaning |
|---|---|---|
| `ts_ms` | int64 | Candle end time, UTC milliseconds |
| `open`, `high`, `low`, `close` | float64 | Trade prices in dollars, 0.0 to 1.0. NaN when the period had no trade |
| `mean` | float64 | Mean trade price for the period, as reported by the API |
| `volume` | float64 | Contracts traded during the period |
| `open_interest` | float64 | Contracts outstanding at the end of the period, as the API reports it |
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
| `taker_side` | str | `yes` or `no`: the outcome the aggressor positioned for (buying YES and selling NO are both `yes`) |

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

Market metadata, `kalshi_data/metadata/markets.parquet`, one row per market. The candle and trade files say what a market traded at; this file says what the market was. It is refreshed from API market payloads by every `roll.py` and by the driver's metadata layer, never from ticker text, and a refresh replaces the rows of the markets it saw and keeps every other row. 33 columns:

| Column | Dtype | Meaning |
|---|---|---|
| `market_ticker`, `event_ticker`, `series_ticker` | str | Kalshi identifiers |
| `title`, `yes_sub_title`, `no_sub_title`, `market_type` | str | Display text; `market_type` is `binary` or `scalar` |
| `strike_type` | str | `greater`, `greater_or_equal`, `less`, `less_or_equal`, `between`, `functional`, `custom`, `structured`; null only when neither the API nor the subtitle gives one (single-outcome markets) |
| `floor_strike`, `cap_strike` | float64 | The threshold(s); null when not applicable. A `greater` market asks whether the value exceeds `floor_strike`, a `less` market whether it stays below `cap_strike`, a `between` market both |
| `custom_strike` | str | JSON text of the API object, for example `{"Cut": "25"}` on a Fed decision |
| `functional_strike` | str | Formula text for `functional` markets |
| `strike_source` | str | `api` when the API sent the `strike_type`; `subtitle` when `strike_type` and `floor_strike` were derived from `yes_sub_title`; null when there is neither |
| `mutually_exclusive` | boolean | The event's flag: at most one market of the event resolves yes. It does not mean exhaustive |
| `open_ts_ms`, `close_ts_ms`, `expected_expiration_ts_ms`, `expiration_ts_ms`, `latest_expiration_ts_ms`, `settlement_ts_ms` | Int64 | UTC milliseconds, like every `ts_ms`; null when the API sent none |
| `status` | str | API status when the row was read: `initialized`, `active`, `inactive`, `closed`, `determined`, `disputed`, `amended`, `finalized` |
| `result` | str | `yes`, `no` or `scalar`; null until determined |
| `settlement_value` | float64 | Dollars paid per YES contract; null until determined |
| `expiration_value` | str | The value the market settled on, as text (`3.4` for July CPI); null until known |
| `can_close_early`, `early_close_condition` | boolean, str | Early close rules |
| `rules_primary`, `rules_secondary` | str | The rules text |
| `volume`, `open_interest`, `last_price` | float64 | As of `built_at`; for a finalized market `volume` is its lifetime volume |
| `tier`, `built_at` | str | `live`, `historical` or `carried_forward`; the UTC time the row was read |

One value in the store is derived, and `strike_source` flags it. The exchange sends no `strike_type`, `floor_strike` or `cap_strike` for 531 finalized threshold markets of 2021 to 2025 (in the catalog of 2026-09-19), although their `yes_sub_title` reads `Above 0.4%`. For a row without a strike type and without a floor or cap strike whose `yes_sub_title` is `Above N` and nothing else, the store holds `strike_type = greater`, `floor_strike = N` and `strike_source = subtitle`. Nothing else is derived (no `less`, no `between`, no `cap_strike`, nothing from the ticker text), and a value the API sent is never changed, also where it disagrees with the subtitle. Every write of the store applies the rule to every row; `python -m kalshi_io.metadata --rederive` is that write without a request. Filter `strike_source = 'api'` for the API's numbers only.

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
│   ├── resolve.py                # event, market and window resolution: catalog first, API for the rest
│   ├── candles.py                # candle fetch + normalization for both API shapes
│   ├── trades.py                 # trade fetch with cursor pagination and resume
│   ├── orderbook.py              # orderbook snapshots, one market or the whole universe in one request
│   ├── metadata.py               # the market metadata store
│   ├── releases.py               # release windows from close times
│   ├── quality.py                # data-quality checks over the store, counts only
│   ├── runlog.py                 # per-run log files and skip files
│   └── storage.py                # parquet append, dedupe, resume, path routing, file locks
├── pull_historical/              # backfill.py (the driver), pull_daily/hourly/minute/trades, pull_audit, pull_all_freq
├── pull_live/
│   └── poll_focus.py             # books first, fast around releases, scheduled pulls over the focus universe
├── get_ticker_info/
│   ├── find_events.py            # search series, events and markets
│   ├── roll.py                   # refresh the catalog and the metadata store, propose the focus universe
│   ├── get_tickers.py            # per-series discovery, writes the catalog
│   ├── get_Econ_Info.py          # list all series by category
│   └── kalshi_tickers/           # committed catalog: per-series JSON + TXT, all_tickers.*, focus_universe.*
├── tests/                        # offline tests against a fake exchange
└── kalshi_data/                  # output, gitignored (so is kalshi_data_old_*/, a previous store kept for reference)
    ├── candles/daily/{series}/{ticker}.parquet
    ├── candles/hourly/{series}/{year}/{ticker}.parquet
    ├── candles/minute/{series}/{year}/{month}/{ticker}.parquet
    ├── trades/{series}/{ticker}/{yyyy-mm}.parquet
    ├── orderbook/{ticker}/{yyyy-mm-dd}.parquet
    ├── metadata/markets.parquet                # one row per market
    ├── state/backfill_journal.jsonl            # (ticker, layer) pairs the driver will never pull again
    ├── .locks/                                 # 256 striped lock files plus named locks; never deleted
    └── logs/
        ├── pull_daily_{yyyymmdd_hhmm}.log      # one per run, same for the other pullers
        ├── backfill_{stamp}.log, backfill_summary_{stamp}.json, backfill_failed_{stamp}_{layer}.txt
        ├── backfill_history_{stamp}.log        # the poller's background history pull
        ├── poll_focus_{yyyymmdd}.log           # named after the UTC day the poller started, never rotated; the pullers it calls log here
        ├── roll_{stamp}.log, roll_report_{stamp}.txt
        ├── skip_{kind}_{process start}.txt     # time, ticker, reason, tab separated
        ├── audit_{yyyymmdd}.csv                # coverage per ticker, written by pull_audit
        ├── quality_{yyyymmdd}.csv              # data-quality findings, written by pull_audit and by the driver
        └── focus_universe_live.json            # what poll_focus is polling
```

A data file is named after its ticker, so a file or directory name may hold a space, a comma or `>` (`GDP-232022 Q4-T0.0.parquet`, `FEDDECISION-23JUN-C>25.parquet`); quote such paths in a shell. A parquet file is written to `{name}.parquet.{pid}.tmp` and renamed into place, under a lock on the file, so a poller and a backfill can write the same store at once and a reader never sees a half-written file.

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

Every candle file has the same 19 columns, so a glob over a whole layer needs no `union_by_name`. The metadata store joins on `market_ticker`, for example the daily closes of one CPI ladder next to its strikes and result:

```sql
SELECT d.market_ticker, m.floor_strike, m.strike_source, m.result, m.expiration_value, d.ts_ms, d.close, d.yes_bid_close, d.yes_ask_close
FROM read_parquet('kalshi_data/candles/daily/KXCPIYOY/*.parquet') d
JOIN read_parquet('kalshi_data/metadata/markets.parquet') m USING (market_ticker)
WHERE m.event_ticker = 'KXCPIYOY-26JUL'
ORDER BY m.floor_strike, d.ts_ms;
```

`pull_audit` runs the data-quality checks of `kalshi_io/quality.py` over the whole store and writes `logs/quality_{date}.csv`, one row per finding; it stops before the checks when none of the audited tickers has a stored daily file. The checks are counts, never repairs: a schema pass over every file (column set, order, dtypes), duplicate and out-of-order rows per file, bars whose volume and close disagree, `taker_side` per month, daily volume sums against the exchange's lifetime volume of finalized markets (with the number of flagged markets that have no daily file at all, which is a gap in the store and not the exchange's doing), threshold ladders whose adjacent strikes have inverted mids or strictly crossed quotes on the same day, mutually exclusive events whose mids sum to more than 1.05 or to less than 0.95 (counted separately: only a sum above the band speaks against the flag, which does not promise that the outcomes are exhaustive; a flagged event made of threshold strikes is a ladder, is left out of the sums and is listed on its own), tradable markets without a fresh bar, listed strikes without a bar per day, finer layers that start after the first daily bar, and coverage per series and layer. Several of these are natural for a thin market rather than a defect, and the report says which. A daily bar is labelled with the day it covers everywhere in the report, the coverage table included (a bar ends at midnight Eastern time, 04:00Z or 05:00Z, which is the next UTC date); hourly, minute, trade and book rows keep the UTC date of their instant.

## Series covered

15 series, 570 events, 5,027 unique tickers (798 of them open), per the committed `all_tickers.json`, built 2026-09-19.

| Category | Series |
|---|---|
| Inflation | KXCPI, KXCPIYOY, KXACPI, KXCPICORE, KXPCECORE, KXCPICOREYOY |
| Labor | KXU3, KXJOBLESS, KXPAYROLLS |
| Growth | KXGDP, KXGDPYEAR, KXRECSSNBER |
| Fed | KXFEDDECISION, KXFED, KXFEDMEET |

## Tests

365 offline tests run against an in-memory fake exchange that answers like the live API, including its error bodies. They cover normalization, the HTTP layer (retries, backoff, signed fallback), discovery, the catalog, the metadata store, the focus universe, every puller end to end, the driver (estimate, order, journal, outage, failure lists), the file lock, release windows, the poller, the CLIs, logging, the audit and the data-quality checks. Four guards are always on: data goes to a temp directory, opening a socket fails the test, credentials cannot be read, and a test that would start a real background history pull fails. Installing with `pip install -e ".[dev]"` provides pytest. Run with `pytest -q`.

## Known API changes

Changes on Kalshi's side that this repo follows. The two from September 2026 broke ticker discovery in version 0.1.0. Dates are from the [official changelog](https://docs.kalshi.com/changelog).

| Date | Change | Handling |
|---|---|---|
| 2025-10-13 | `tags` on `/series` splits on commas only | Tags are joined with commas |
| 2025-11-27 | `/markets` takes one `status` filter per request | One status per request is sent |
| 2025-12-13 | `paused` added to the `/markets` status filters | Accepted by discovery and `find_events`; a paused market stays in the focus universe |
| 2026-02-19 | Settled markets, their candles and older trades move to `/historical/` endpoints behind a moving cutoff (`GET /historical/trades` followed on 2026-03-06) | Candles ask the tier the catalog recorded and swap on 404; catalog, metadata and trades read both tiers |
| 2026-05 | `taker_side` on trades deprecated in favor of `taker_outcome_side` and `taker_book_side`; removal "not before" 2026-05-14 (spec) or 2026-05-28 (changelog), still sent on 2026-09-17 | The stored `taker_side` is the first of the three the API sent; `pull_audit` counts it per month so a removal shows up |
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
- **Old markets 404 on live endpoints.** Settled markets age out of the live API onto `/historical/` endpoints. The two tiers overlap near the cutoff. The candle fetcher asks the tier the catalog recorded for the market (3,932 of the 5,027 cataloged markets are historical) and swaps to the other on 404.
- **Tickers with a space, a comma or `>`.** 12 cataloged tickers hold a space or a comma (ten `GDP-232022 Q4-T*`, `JOBLESS-22JUL23-C250,000`, `JOBLESS-22SEP10-C220,000`) and six hold `>` (`FEDDECISION-23JUN-C>25` and siblings). The single-market, candle and trade routes serve them once the ticker is URL-quoted in the path, which the client always does. The `tickers=` list form of `/markets` and `/historical/markets` cannot: it splits on commas and returns nothing for a ticker with a space (observed 2026-09-21), so `lookup_markets` asks the single-market routes for those.
- **No strike fields on 531 old threshold markets.** For 531 finalized markets of nine series (2021 to 2025) the API sends no `strike_type`, `floor_strike` or `cap_strike`; the number is only in `yes_sub_title` (`Above 0.4%`). The metadata store derives it and flags it with `strike_source = subtitle`; see Data and schemas.
- **The night daylight saving ends.** On the daily bar that ends 04:00Z after that night the exchange's daily aggregation loses trades while open interest still moves, so the daily volume sum of some markets stays below the exchange's lifetime volume. `KalshiAPI.md` has the numbers.
- **5,000 candle cap per request.** The API rejects a request whose window spans more than 5,000 candles with the error `max candlesticks: 5000`. Chunk sizes in `kalshi_io/config.py` keep every window under the cap; a 3-day minute window is 4,320 candles.
- **Rate limit responses carry no Retry-After.** A 429 has the body `{"error": "too many requests"}` and no headers to read. Backoff is the mechanism. Without a key the candlestick endpoints sustain about 4 to 5 requests per second (measured 2026-09-17: one 429 in 1,344 requests at 4 per second, about 5 percent at 6, 8 to 12 percent at 10); every one succeeded on the first retry. The default rate is 5.
- **One request for every book.** `GET /markets/orderbooks` returns the books of up to 100 markets (repeated `tickers` parameters; a comma-joined value is read as one ticker) and is throttled as one request. The poller's minute sweep of 64 books is one request with one timestamp.
- **The closing candle.** The period that contains `close_time` has one last candle and nothing after it. A window that ends exactly at `close_time` loses it, so a settled market is pulled to `close_time` plus two periods (two, because the ET day on which daylight saving ends is 25 hours long). Daily candles end at midnight Eastern time, 04:00Z or 05:00Z.
- **Numbers arrive as decimal strings.** Prices, volumes, and counts are serialized as strings like `"0.6900"` and `"5247.00"`. Normalization casts them all to float64.
- **Fractional contracts are real.** The API reports contract counts as fixed-point values with two decimals, and fractional volumes such as `11747.08` are genuine fills, never rounding noise. They pass through unscaled.
- **Broken titles on some old markets.** 21 markets from the 2024 and 2025 KXACPI events carry unfilled template titles containing the literal text `above_below_between`. Titles are metadata only; prices are unaffected.

## Design notes

- **One REST helper, keyless first.** `request_json` spaces requests (default 5 per second), retries 429, 5xx, timeouts, connection errors and a 200 that is not JSON, with exponential backoff and jitter, up to 6 attempts. Any other 4xx raises at once. A 401 or 403 repeats the call once with signed headers. When retries run out the failure is raised, logged at ERROR and recorded in the skip file. A run stops after 3 tickers in a row ran out of retries, because the API is down.
- **Failures are loud.** A ticker that fails is logged at ERROR, counted under `failed` in the run summary, and written to `logs/skip_{kind}_{process start}.txt` with a timestamp and the reason. In the driver a ticker without an outcome fails the run too: `Result: complete` and exit code 0 mean that every ticker of every layer has an outcome, not that nothing raised. The same problem is recorded once per 6 hours per process, so a long poller does not repeat it every minute.
- **Trades resume from the last stored trade.** The request carries `min_ts`, 60 seconds before the last stored trade. Stored `trade_id`s are dropped before the append, so a quiet cycle costs one request and writes nothing. The historical tier is asked only when the resume point lies before the cutoff. A trade fetch is all or nothing: pages arrive newest first, so saving a truncated fetch would move the resume point past trades that were never downloaded.
- **Candles may save a partial prefix.** Windows are fetched oldest first and are inclusive on both ends. If a window fails after earlier ones arrived, the rows before it are gap-free and are saved, the failure is recorded, and the next run resumes from the last saved candle.
- **Logging is configured once.** Every run attaches its own log file and detaches it when it ends. Pullers called by `poll_focus` log into its daily file, so a long run does not pile up handlers or duplicate lines.
- **Uncataloged tickers get their true event.** A ticker missing from the catalog is resolved through the API (market, then event, then series). The event is never guessed from the ticker text.
- **Every candle pull starts at market open and stops at close.** The open and close come from the catalog, and from the API only for an uncataloged ticker. A market that can no longer trade (`finalized`) is pulled up to `close_time` plus two periods, which keeps its closing candle; a `closed` or `determined` market is pulled up to now, because it can be reopened with a later close. Minute candles have no depth limit on either API tier (observed back to 2022-11), so the minute layer starts at open like the others. The `pull_minute` CLI no longer defaults `--since`.
- **One invariant everything relies on: a ticker's stored range is contiguous from market open.** Resume starts at the newest stored row, so a poller that stored today's bars for a ticker without history would make the driver skip that history. Hence the poller pulls candles and trades only for tickers whose history is in (the background pull), the driver never uses `--since`, and during a full download only a books-only poller should run next to it. The `history start` check of the audit compares each ticker's first minute, hourly and daily bar.
- **Concurrent writers are safe.** An append is read, merge, write, rename under `flock` on one of 256 striped lock files in `kalshi_data/.locks`; the kernel releases a lock when its holder dies, so there are no stale locks. A writer waits up to 120 seconds (5 for an orderbook snapshot, which is perishable) and then fails the ticker for that cycle. A full-catalog driver run holds a named lock so it cannot be started twice.
- **Books first, and only books around a release.** A 64-ticker candle and trade sweep takes about 26 seconds at 5 requests per second; running it inside a release window would punch holes into the 5-second book series. Candles and trades can be pulled from the API afterwards, the book cannot, so they wait.

## Maintenance

- **Weekly:** run `python get_ticker_info/roll.py`, read the report, and commit the catalog. The same run refreshes the metadata store, so settled results fill in. `pull_audit` warns when the catalog is older than 7 days.
- **Per macro cycle:** nothing to edit. `poll_focus` derives and rolls its universe and pulls the history of a new event in the background. To have it in before the close, run `roll.py --events-per-series 2` and `python -m pull_historical.backfill --tickers focus`.
- **Catching up the whole store:** `python -m pull_historical.backfill`, resumable with the same command; `--estimate-only` first. Run only a books-only poller next to it.
- **New series:** add it to `SERIES_LIST` in `kalshi_io/config.py` (and to `FOCUS_SERIES` to poll it), then run `roll.py`. `find_events.py` finds the series ticker.
- **Extra release times:** add ISO-8601 UTC times to `RELEASE_CALENDAR` in `kalshi_io/config.py` for a release whose markets are not cataloged.

## API guide

[KalshiAPI.md](KalshiAPI.md) covers how Kalshi structures series, events, and markets, and which endpoints this repo calls. [CHANGELOG.md](CHANGELOG.md) lists what changed in each version.

## License

MIT, see [LICENSE](LICENSE).
