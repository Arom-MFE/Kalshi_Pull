# Kalshi_Pull skill guide

Read this before you work in this repo. It is written for a language model, or a person, that has been given a clone of Kalshi_Pull and is asked to set it up, pull whatever Kalshi market data someone wants, explain the result, or build on top. It is guidance, not a rule book. It was checked against version 0.3.0 on 2026-09-18: every signature and option below against the installed package (`inspect.signature`, the argparse parsers), every column list against the constants in the code, every API statement against the official OpenAPI specification (version 3.30.0) and documentation pages as they stood that day, and every dated observation by a request or a command run that day against the live API, without a key, into a scratch data directory. A statement that rests on one observation says "observed on" with its date. The exchange changes its API and this file will age: the installed source and the live API outrank it, and the last section says how to check. The guide is task agnostic: the patterns are building blocks, not the only permitted shapes.

What the repo is: a pipeline that turns Kalshi's public REST API into partitioned zstd Parquet files. It pulls candles at three intervals (daily, hourly, minute) with the bid and ask quotes of every period, every individual trade, order book snapshots, and one metadata row per market. It ships a committed catalog of 15 US macro series and can pull any ticker on the exchange. There is no analysis code: research reads the Parquet output.

## Onboarding

1. Clone and enter the repo: `git clone https://github.com/Arom-MFE/Kalshi_Pull.git` then `cd Kalshi_Pull`. Run every command below from the repo root.
2. Create the environment with a Python 3.13 or newer interpreter (`pyproject.toml` has `requires-python = ">=3.13"`): `python3.13 -m venv .venv`, `source .venv/bin/activate`, `pip install -e ".[dev]"`. Name the interpreter explicitly (`python3.14` works the same way): a plain `python` may be older or absent. The `dev` extra adds pytest. If your shell keeps no state between commands, skip the activation and call `.venv/bin/python` and `.venv/bin/pytest` directly. Dependencies: `pandas>=3.0,<4`, `pyarrow>=18`, `requests>=2.32`, `python-dotenv>=1.0`, `kalshi-python-sync>=3.30.0,<4`, `duckdb>=1.0`.
3. Run `pytest -q`. At 0.3.0 it reports `340 passed` in about ten seconds. The tests are offline: they use a fake exchange, a temp data directory, and fail if a socket opens. They ignore `KALSHI_DATA_DIR` and `KALSHI_MAX_RPS` from your shell.
4. You need no API key. Every endpoint the repo calls answers without one. `.env` is optional: `KALSHI_API_KEY_ID` and `KALSHI_KEY_PATH` are read only if an endpoint answers 401 or 403. Never print, commit or copy a `.env` or a key file.
5. Choose where data goes before the first pull. The default data root is `<repo>/kalshi_data`. For experiments point `KALSHI_DATA_DIR` at a scratch directory: `export KALSHI_DATA_DIR=/tmp/kalshi_scratch`. The variable has to be set in the shell that starts the command: if your shell keeps no state between commands, put it on the command line (`KALSHI_DATA_DIR=/tmp/kalshi_scratch python -m pull_historical.pull_daily ...`), because a command started without it writes to the default data root without asking. Every run logs the data root it uses in its first line. If someone else's `kalshi_data` already exists, treat it as production data: append only, and see "Never do".
6. First pull: `python -m pull_historical.pull_daily --tickers KXRECSSNBER-26`. It prints one summary dict to stdout, for example `{'processed': 1, 'skipped': 0, 'failed': 0, 'aborted': False, 'unknown': [], 'rows_written': <n>, 'elapsed_sec': <s>}`, and exits with code 0. `KXRECSSNBER-26` is a market ticker: in that series an event has a single market whose ticker equals the event's. Everywhere else a market ticker extends its event ticker, for example `KXCPIYOY-26JUL-T3.5` in the event `KXCPIYOY-26JUL`, and only market tickers can be pulled.
7. Read the result:

   ```python
   import os, pandas as pd
   root = os.environ.get("KALSHI_DATA_DIR", "kalshi_data")
   df = pd.read_parquet(f"{root}/candles/daily/KXRECSSNBER/KXRECSSNBER-26.parquet")
   df["end"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
   ```

8. Two kinds of entry points. `kalshi_io`, `pull_historical` and `pull_live` are installed packages: run them as modules (`python -m pull_historical.backfill`) or import them. `get_ticker_info` is not installed and has no `__init__.py`: run its scripts as files from the repo root (`python get_ticker_info/find_events.py ...`).
9. Where things live: settings in `kalshi_io/config.py`; the committed catalog in `get_ticker_info/kalshi_tickers/`; data under the data root; logs under `{data root}/logs/`; the user-facing docs are `README.md`, `KalshiAPI.md` (how the exchange's API behaves) and `CHANGELOG.md`.
10. Official references: https://docs.kalshi.com , the OpenAPI specification at https://docs.kalshi.com/openapi.yaml (the exchange names it the source of truth), and the changelog at https://docs.kalshi.com/changelog .

## The surface, typed

Signatures are exactly what `inspect.signature` prints for the installed package on Python 3.14. Python 3.13 prints the class `pathlib.Path` as `pathlib._local.Path` and nothing else differs (checked on 2026-09-18 with 3.13.5 and 3.14.0). A parameter without a default is required. `DATA` means the data root, `CAT` means `get_ticker_info/kalshi_tickers/`. Times named `ts_ms` are int64 UTC milliseconds; times named `*_ts` in function arguments are Unix seconds.

### Environment variables and config defaults

| Name | Default | Meaning |
|-|-|-|
| `KALSHI_DATA_DIR` | `<repo>/kalshi_data` | Data root. Read from the process environment at import time, never from `.env`. Blank means the default. `~` is expanded and the path resolved |
| `KALSHI_MAX_RPS` | 5 | Client-side cap on request starts per second. A float; values above 20 are cut to 20; zero, negative or unparsable values fall back to 5. Read from the process environment at import time |
| `KALSHI_API_KEY_ID`, `KALSHI_KEY_PATH` | unset | Credentials, read from the environment or `.env` only when an endpoint answered 401 or 403 and the request is repeated signed |

Constants in `kalshi_io/config.py` (edit the file to change them; they are not flags):

| Constant | Value | Meaning |
|-|-|-|
| `CHUNK_SECONDS` | `{1: 259200, 60: 2592000, 1440: 31536000}` | Seconds per candle request window: 3 days of minute candles, 30 days of hourly, 365 days of daily |
| `MAX_CANDLES_PER_CALL` | 5000 | The exchange's cap; the chunk sizes stay below it |
| `SERIES_LIST` | 15 series: `KXCPI`, `KXCPIYOY`, `KXACPI`, `KXCPICORE`, `KXPCECORE`, `KXCPICOREYOY`, `KXU3`, `KXJOBLESS`, `KXPAYROLLS`, `KXGDP`, `KXGDPYEAR`, `KXRECSSNBER`, `KXFEDDECISION`, `KXFED`, `KXFEDMEET` | The series a roll refreshes |
| `FOCUS_SERIES` | `["KXFED", "KXFEDDECISION", "KXCPIYOY", "KXPAYROLLS", "KXU3"]` | Series the poller follows |
| `FOCUS_EVENTS_PER_SERIES` | 1 | Nearest open events taken per focus series |
| `FOCUS_OVERRIDE` | `[]` | Market tickers; when set they replace the derived universe and never roll |
| `FOCUS_REFRESH_SECONDS` | 3600 | Seconds between universe refreshes in the poller; 0 means never |
| `RELEASE_POLL_SECONDS` | 5 | Seconds between book sweeps inside a release window |
| `RELEASE_WINDOW_BEFORE_S`, `RELEASE_WINDOW_AFTER_S` | 300, 900 | A release window runs from 300 s before to 900 s after an event's `close_time` |
| `RELEASE_CALENDAR` | `[]` | Extra release times, ISO-8601 UTC strings such as `"2030-01-31T13:30:00Z"` |
| `BACKGROUND_HISTORY_RPS` | 3 | Request rate of the poller's background history pull |
| `TS_COL` | `"ts_ms"` | The timestamp column of every parquet file |
| `DEDUPE_COLS_CANDLES`, `DEDUPE_COLS_TRADES` | `["ts_ms", "market_ticker"]`, `["trade_id"]` | Dedupe keys on write |
| `MAX_REQUESTS_PER_SECOND`, `RATE_LIMIT_SECONDS` | 5.0, 0.2 | Resolved from `KALSHI_MAX_RPS`; the second is the minimum spacing of request starts |
| `HTTP_TIMEOUT` | `(5.0, 30.0)` | Connect and read timeout in seconds per attempt |
| `HTTP_MAX_ATTEMPTS` | 6 | Attempts per request, the first included |
| `HTTP_BACKOFF_BASE_S`, `HTTP_BACKOFF_CAP_S`, `HTTP_RETRY_AFTER_CAP_S` | 0.5, 30.0, 120.0 | Backoff `uniform(b/2, b)` with `b = min(30, 0.5 * 2^n)`; at least 1 s after a 429; a `Retry-After` header is honored up to 120 s |
| `MAX_CONSECUTIVE_OUTAGES` | 3 | A puller run stops after this many tickers in a row ran out of retries |
| `LOCK_TIMEOUT_S`, `ORDERBOOK_LOCK_TIMEOUT_S` | 120.0, 5.0 | How long a writer waits for another writer of the same file |
| `TRADES_RESUME_OVERLAP_S` | 60 | A trade resume asks again for the last 60 s and drops the overlap by `trade_id` |

Other constants that shape behaviour: `kalshi_io.client.BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"`; `kalshi_io.resolve.CLOSE_PAD_PERIODS = 2`; `kalshi_io.storage.LOCK_STRIPES = 256`; `kalshi_io.runlog.SKIP_DEDUPE_SECONDS = 21600`; `kalshi_io.trades.CUTOFF_TTL_SECONDS = 3600`; `kalshi_io.orderbook.BATCH_SIZE = 100`; `kalshi_io.releases.HORIZON_S = 7776000` (90 days); `kalshi_io.universe.CLOSE_REFRESH_DELAY_S = 90`; `kalshi_io.quality.MUTEX_SUM_LOW, MUTEX_SUM_HIGH = 0.95, 1.05` and `VOLUME_TOLERANCE = 0.005`; `pull_historical.backfill.OUTAGE_WAITS_S = (60, 120, 240, 480, 960)`, `METADATA_CHUNK = 500`, `FULL_RUN_LOCK = "backfill_full"`; `pull_historical.pull_audit.STALE_CATALOG_DAYS = 7`; `pull_live.poll_focus.WINDOW_GUARD_S = 60`, `HISTORY_RETRY_S = 600`, `HISTORY_MAX_FAILURES = 3`, `HISTORY_LOCK_NAME = "focus_history"`, `FAST_SWEEP_ATTEMPTS = 2`, `FAST_SWEEP_TIMEOUT = (5.0, 10.0)`.

`kalshi_io` re-exports (its `__all__`): `get_client`, `get_session`, `BASE_URL`, `PROJECT_ROOT`, `DATA_DIR`, `TICKERS_DIR`, `CHUNK_SECONDS`, `MAX_CANDLES_PER_CALL`, `SERIES_LIST`, `FOCUS_SERIES`, `FOCUS_EVENTS_PER_SERIES`, `FOCUS_OVERRIDE`, `RELEASE_POLL_SECONDS`, `RELEASE_WINDOW_BEFORE_S`, `RELEASE_WINDOW_AFTER_S`, `RELEASE_CALENDAR`, `BACKGROUND_HISTORY_RPS`, `TS_COL`, `DEDUPE_COLS_CANDLES`, `DEDUPE_COLS_TRADES`, `MAX_REQUESTS_PER_SECOND`, `RATE_LIMIT_SECONDS`, `LOCK_TIMEOUT_S`, `CANDLE_COLUMNS`, `candles_frame`, `fetch_candles`, `resolve_ticker_meta`, `fetch_trades`, `snapshot_orderbook`, `snapshot_orderbooks`, `append_orderbook_snapshot`, `METADATA_COLUMNS`, `load_market_metadata`, `refresh_market_metadata`, `LockTimeout`, `append_parquet`, `duckdb_connect`, `file_lock`, `named_lock`, `format_report`, `run_checks`.

`DATA_DIR` is resolved when `kalshi_io.config` is first imported and other modules bind it by value: set `KALSHI_DATA_DIR` before starting Python, not from inside a running process.

### kalshi_io.client: the only HTTP path

- `kalshi_io.client.request_json(path: str, params: dict | None = None, *, timeout: tuple[float, float] = (5.0, 30.0), max_attempts: int = 6) -> dict`. GET `BASE_URL + path`, decoded JSON. Drops `None` params. Spaces request starts by `RATE_LIMIT_SECONDS`. Retries 429, 5xx, connection errors, timeouts and a 200 whose body is not JSON; logs a WARNING per retry and one ERROR when it gives up. Raises `KalshiNotFound` on 404, `KalshiAPIError` on any other 4xx at once, `RetriesExhausted` when every attempt failed. On 401 or 403 it repeats the call once with signed headers; signing then stays on for the process; without usable credentials that is a `KalshiAPIError` with status 401 whose text says the endpoint requires authentication.
- `kalshi_io.client.paginate(path: str, params: dict | None = None, *, key: str, limit: int = 1000, max_pages: int = 10000) -> Iterator[dict]`. Follows the response `cursor` until it is empty. Raises whatever `request_json` raises, so a truncated listing never looks complete; raises `KalshiAPIError` on a repeated cursor or more than `max_pages` pages.
- `kalshi_io.client.KalshiAPIError(status: int | None, url: str, details: str = '')`: carries `.status`, `.url`, `.details`. `kalshi_io.client.KalshiNotFound(status: int | None, url: str, details: str = '')` is its 404 subclass. `kalshi_io.client.RetriesExhausted(status: int | None, url: str, details: str, attempts: int)` adds `.attempts`.
- `kalshi_io.client.is_outage(exc: BaseException) -> bool`. True when `exc` is, or was caused by, `RetriesExhausted`.
- `kalshi_io.client.path_part(value: str) -> str`. URL-quotes a ticker for one path segment.
- `kalshi_io.client.get_session() -> requests.sessions.Session` (cached, keyless) and `kalshi_io.client.get_client()` (the SDK client, built on first use, reads `.env`; raises `RuntimeError` when `KALSHI_KEY_PATH` or `KALSHI_API_KEY_ID` is missing). Prefer `request_json`.
- `kalshi_io.client.stats` is a dict `{"requests", "http_429", "retries"}` counting HTTP attempts of the process.

### kalshi_io.discovery: find series, events, markets

Stateless, keyless. Filter words (`unopened`, `open`, `paused`, `closed`, `settled`) are not response statuses (`initialized`, `active`, `inactive`, `closed`, `determined`, `disputed`, `amended`, `finalized`). An invalid filter raises `ValueError` before any request; `all` is invalid: omit the status to get every status.

- `kalshi_io.discovery.status_bucket(status: str | None) -> str`. Response status to filter word; unmapped or missing gives `"unknown"`. `STATUS_BUCKET` holds the map; `POLLABLE_BUCKETS` is `{"open", "paused", "unopened"}`; `MARKET_STATUS_FILTERS` has five words, `EVENT_STATUS_FILTERS` four (no `paused`).
- `kalshi_io.discovery.list_categories() -> dict[str, list[str]]`. `{category: [tags]}`.
- `kalshi_io.discovery.list_series(category: str | None = None, tags: str | list[str] | None = None) -> list[dict]`. Series dicts with `tags` as a list and an added `legacy_twin` flag (a dead pre-KX spelling whose KX series is in the same listing). The category is exact and case sensitive; an unknown one raises `ValueError` with a suggestion.
- `kalshi_io.discovery.search_series(keyword: str, category: str | None = None, tags: str | list[str] | None = None, include_legacy: bool = False) -> list[dict]`. Client-side match: every word must occur in the ticker, the title or a tag. Without a category it fetches the full series list.
- `kalshi_io.discovery.list_events(series_ticker: str, status: str | None = None, with_nested_markets: bool = False) -> list[dict]`. Every event of a series, however old. With nesting, `markets` is always a list and holds live-tier markets only.
- `kalshi_io.discovery.get_event(event_ticker: str) -> dict | None`. One event with `markets` as a list; `None` on 404.
- `kalshi_io.discovery.list_markets(event_ticker: str | None = None, series_ticker: str | None = None, status: str | None = None, tickers: list[str] | None = None) -> list[dict]`. Live tier; exactly one selector or `ValueError`. Market payloads carry `event_ticker` and no `series_ticker`.
- `kalshi_io.discovery.list_historical_markets(event_ticker: str | None = None, series_ticker: str | None = None, tickers: list[str] | None = None) -> list[dict]`. Markets settled before the cutoff; one selector.
- `kalshi_io.discovery.find_markets(event_ticker: str | None = None, series_ticker: str | None = None, status: str | None = None) -> list[dict]`. Both tiers, each dict with `tier`; the live record wins where the tiers overlap; the historical tier is asked only for status `None` or `settled`.
- `kalshi_io.discovery.get_market(ticker: str) -> dict | None`. Live tier, then historical; adds `tier`; `None` when neither knows it.
- `kalshi_io.discovery.lookup_markets(tickers: list[str]) -> dict[str, dict]`. Batch lookup in both tiers, 100 tickers per request; unknown tickers are simply absent.
- `kalshi_io.discovery.resolve_market_meta(ticker: str) -> tuple[str | None, str] | None`. `(series_ticker, event_ticker)` through the API (market, then event); cached per process, "not found" included.
- `kalshi_io.discovery.get_historical_cutoff() -> dict`. The cutoff timestamps as ISO strings, among them `market_settled_ts` and `trades_created_ts`.

### kalshi_io.candles: resolution and candle fetch

- `kalshi_io.candles.fetch_candles(market_ticker: str, interval: int, start_ts: int, end_ts: int) -> list[dict]`. `interval` is 1, 60 or 1440 minutes. Walks the range in `CHUNK_SECONDS` windows oldest first, starts on the tier the catalog recorded and swaps to the other tier on 404, returns ascending rows with the three identifiers, one per candle. Writes nothing. Raises `PartialCandlesError` when a later window failed (its `.rows` are a gap-free prefix that is safe to store, `.next_start_ts` is where it stopped), the original `KalshiAPIError` when the first window failed, `UnknownTickerError` for a ticker nobody knows.
- `kalshi_io.candles.parse_candle(raw: object, is_historical: bool) -> dict`. One raw candle to `ts_ms` plus 15 floats. The live tier's keys end in `_dollars` and `_fp`, the historical tier's are bare; that is the only difference it handles. Missing values stay `None`, never 0.
- `kalshi_io.candles.candles_frame(rows: list[dict]) -> pandas.DataFrame`. The stored frame: `CANDLE_COLUMNS` in order, numerics forced to float64, `ts_ms` to int64.
- `kalshi_io.candles.resolve_ticker_meta(market_ticker: str, allow_api: bool = True) -> tuple[str, str | None]`. `(series, event)`: catalog first, then tickers registered by the focus universe, then the API. With `allow_api=False` an uncataloged ticker gives a prefix-derived series and `None`. Raises `UnknownTickerError` (a `LookupError`). The event is never guessed from the ticker text.
- `kalshi_io.candles.is_cataloged(market_ticker: str) -> bool`, `kalshi_io.candles.known_market_window(market_ticker: str) -> kalshi_io.candles.MarketWindow | None`, `kalshi_io.candles.register_ticker_meta(mapping: dict[str, tuple[str, str]]) -> None` (never overrides the catalog), `kalshi_io.candles.register_market_windows(windows: dict[str, kalshi_io.candles.MarketWindow]) -> None` (overrides the catalog: an API answer is newer), `kalshi_io.candles.canonical_series(series_ticker: str) -> str` (`CPIYOY` becomes `KXCPIYOY` for a configured series), `kalshi_io.candles.iso_to_ts(value) -> int | None`.
- `kalshi_io.candles.MarketWindow(open_ts: int | None, close_ts: int | None, status: str, tier: str = '')` is a named tuple. `kalshi_io.candles.PartialCandlesError(rows: list[dict], next_start_ts: int, cause: Exception)`. Column constants: `CANDLE_COLUMNS` (19), `QUOTE_COLUMNS` (8), `CANDLE_FLOAT_COLUMNS` (15).

### kalshi_io.resolve: a market's life

- `kalshi_io.resolve.market_window(market_ticker: str, allow_api: bool = True) -> dict`. `{"open_ts", "close_ts", "status", "tier", "source"}` in Unix seconds; `source` is `catalog`, `api` or `unknown`. The catalog answers without a request. Raises `KalshiAPIError` when a needed lookup failed for a reason other than not found.
- `kalshi_io.resolve.candle_end_ts(window: dict, interval: int, now_ts: int) -> int`. Where a candle pull may stop: `close_ts + 2 * interval * 60` for status `finalized` (never later than now), otherwise now. `closed` and `determined` markets are pulled to now because a closed market can be reopened.
- Older helpers, still exported: `kalshi_io.resolve.resolve_event(event_ticker: str) -> types.SimpleNamespace`, `kalshi_io.resolve.resolve_market(event: types.SimpleNamespace, event_ticker: str) -> str | None`, `kalshi_io.resolve.get_market_metadata(market_ticker: str) -> dict` (`{"open_ts_ms", "expiration_time", "status"}`).

### kalshi_io.tickers: what `--tickers` accepts

- `kalshi_io.tickers.load_tickers(source: str | list[str]) -> list[str]`. No network. Each element may be a path to a `.txt` file (one ticker per line, `#` lines ignored), a path to a `.json` file (its `tickers` key), a series name (any word for which `CAT/{word}_tickers.txt` exists; it expands to that file, whether or not the series is in `SERIES_LIST`), the word `focus` (expands to `CAT/focus_universe.txt`), a market ticker, or several of these separated by spaces, commas or newlines. An existing path is taken whole, spaces included. Returns a sorted, deduped list. Raises `FileNotFoundError` for a missing `.txt` or `.json`, and for `focus` when `CAT/focus_universe.txt` does not exist (the committed catalog ships one; a roll without `--dry-run` and without `--no-focus` rewrites it). It does not expand an event ticker: an event name is taken as a market ticker and ends up unknown.
- `kalshi_io.tickers.validate_tickers(tickers: list[str], allow_api: bool = True) -> tuple[list[str], list[str]]`. `(known, unknown)`. Catalog first, then the API. A lookup that fails for a reason other than not found keeps the ticker in `known`: an outage is not a typo.

### kalshi_io.trades

- `kalshi_io.trades.fetch_trades(market_ticker: str, since_trade_id: str | None = None, *, min_ts: int | None = None) -> pandas.DataFrame`. Columns `TRADE_COLUMNS`, sorted by `ts_ms`. `min_ts=None` pulls the complete tape from both tiers; with a `min_ts` at or after the trades cutoff only the live tier is asked. Writes nothing. Raises `KalshiAPIError` on any failed page and returns nothing partial, because pages arrive newest first. `taker_side` is the first of the wire fields `taker_side`, `taker_outcome_side`, `taker_book_side` (`bid` is `yes`, `ask` is `no`) that the API sent; nothing is inferred from prices.
- `kalshi_io.trades.get_trades_cutoff_ts() -> int | None`. The trades cutoff in Unix seconds, cached for an hour; `None` when unavailable, and then both tiers are queried.

### kalshi_io.orderbook

- `kalshi_io.orderbook.snapshot_orderbook(market_ticker: str) -> pandas.DataFrame`. All levels of both bid books with `BOOK_COLUMNS`. An empty frame for a market without resting orders. A settled market answers 200 with empty books, and so does a ticker the exchange does not know (both observed on 2026-09-18, on this route and on the batch route), so an empty frame is not an error and does not prove that the ticker exists. Raises `KeyError` when the body lacks `orderbook_fp`.
- `kalshi_io.orderbook.snapshot_orderbooks(tickers: list[str], *, max_attempts: int = 6, timeout: tuple[float, float] = (5.0, 30.0)) -> dict[str, pandas.DataFrame]`. One request per 100 tickers; every book of one response shares one `ts_ms`. A ticker missing from the response is absent from the result; do not count on that to spot a bad ticker, because on 2026-09-18 the exchange answered a made-up ticker with an entry holding empty books.
- `kalshi_io.orderbook.append_orderbook_snapshot(market_ticker: str, df_book: pandas.DataFrame) -> int`. Writes to `DATA/orderbook/{ticker}/{yyyy-mm-dd}.parquet`, the UTC day of the snapshot's own `ts_ms`; dedupes on `ts_ms, side, price`; waits at most 5 s for the file lock. Returns new rows; 0 for an empty frame.

### kalshi_io.metadata: one row per market

- `kalshi_io.metadata.refresh_market_metadata(tickers: list[str], *, now: datetime.datetime | None = None, event_flags: dict[str, bool | None] | None = None) -> dict`. Reads the markets from both tiers in batches of 100, reads each event's `mutually_exclusive` flag, merges into the store. Returns `{"requested", "found", "missing", "markets", "series", "path", "rows", "added", "updated", "kept"}`. Logs a WARNING naming tickers found in neither tier. Raises `KalshiAPIError`; then nothing is written. The committed catalog is not touched.
- `kalshi_io.metadata.upsert_market_metadata(rows: list[dict], *, keep_existing: list[dict] | None = None) -> dict`. Replaces stored rows of the same `market_ticker`, keeps every other row, writes atomically under the file lock. Returns `{"path", "rows", "added", "updated", "kept"}`.
- `kalshi_io.metadata.load_market_metadata() -> pandas.DataFrame | None`. The store with fixed dtypes, or `None` if it does not exist.
- Row builders and helpers: `kalshi_io.metadata.market_row(payload: dict, *, series: str | None, tier: str, built_at: str, mutually_exclusive: bool | None = None) -> dict`, `kalshi_io.metadata.catalog_row(record: dict, *, series: str, built_at: str) -> dict`, `kalshi_io.metadata.metadata_frame(rows: list[dict] | pandas.DataFrame) -> pandas.DataFrame`, `kalshi_io.metadata.metadata_path() -> pathlib.Path`, `kalshi_io.metadata.iso_to_ms(value) -> int | None`, `kalshi_io.metadata.utc_stamp(now: datetime.datetime | None = None) -> str`. `METADATA_COLUMNS` has 32 names.

### kalshi_io.catalog: the committed ticker catalog

- `kalshi_io.catalog.refresh_catalog(series_list: list[str] | None = None, *, out_dir: pathlib.Path | None = None, dry_run: bool = False, now: datetime.datetime | None = None, write_metadata: bool = True) -> dict`. Rediscovers the series (default `SERIES_LIST`), rebuilds the combined files, feeds the metadata store from the same payloads. Returns `{"built_at", "previous_built_at", "series_ok", "series_failed", "diff", "combined", "removed_upstream", "metadata", "checks", "api_requests", "elapsed_sec"}`. A series that fails keeps its previous file and is reported; nothing raises. With `dry_run` nothing is written.
- `kalshi_io.catalog.discover_series(series: str, *, save: bool = True, out_dir: pathlib.Path | None = None, legacy_probe: bool = True, now: datetime.datetime | None = None) -> dict`. Unions `/events`, `/historical/markets` and `/markets` of the series by ticker (the live record wins). Writes `CAT/{series}_tickers.json` and `.txt` atomically only if every request succeeded. Raises `CatalogValidationError` when an event the API lists as open has no active market in the result, and `KalshiAPIError`; in both cases nothing is written. A ticker of the previous file that the API no longer returns is carried forward if it ever opened, otherwise reported as removed upstream.
- `kalshi_io.catalog.build_combined(out_dir: pathlib.Path | None = None, *, save: bool = True, now: datetime.datetime | None = None, catalog: dict[str, dict] | None = None) -> dict`, `kalshi_io.catalog.write_series(result: dict, out_dir: pathlib.Path | None = None) -> None`, `kalshi_io.catalog.diff_catalog(old: dict[str, dict], new: dict[str, dict]) -> dict`.
- Readers: `kalshi_io.catalog.load_series(series: str, out_dir: pathlib.Path | None = None) -> dict | None`, `kalshi_io.catalog.load_catalog(out_dir: pathlib.Path | None = None) -> dict[str, dict]`, `kalshi_io.catalog.market_index(out_dir: pathlib.Path | None = None) -> dict[str, dict]` (`{market_ticker: record + "series"}`), `kalshi_io.catalog.catalog_built_at(out_dir: pathlib.Path | None = None) -> tuple[datetime.datetime | None, str]` (basis `built_at`, `mtime` or `none`), `kalshi_io.catalog.status_counts(markets: list[dict]) -> dict[str, int]`. `kalshi_io.catalog.CatalogValidationError` is a `RuntimeError`.

### kalshi_io.universe: what the poller polls

A universe is a dict: `schema_version`, `derived_at`, `source` (`derived`, `--tickers` or `FOCUS_OVERRIDE`), `rule`, `series`, `events_per_series`, `groups` (each `series`, `event_ticker`, `title`, `close_time`, `tickers`), `tickers`, `statuses`, `warnings`; `check_universe` adds `status_counts` and `dropped`.

- `kalshi_io.universe.derive_universe(series_list: list[str] | None = None, events_per_series: int | None = None, *, now: datetime.datetime | None = None) -> dict`. Per series, one snapshot of the open markets grouped by event; events ranked by the earliest `close_time` of their open markets (then `expected_expiration_time`, then `latest_expiration_time`; no time ranks last; ties break on the event ticker); the nearest N taken whole. Two list calls per series. Raises `ValueError` for N below 1 and `KalshiAPIError`.
- `kalshi_io.universe.explicit_universe(tickers: list[str], source: str, *, now: datetime.datetime | None = None) -> dict`. Exactly these tickers, statuses looked up in both tiers; never rolls.
- `kalshi_io.universe.build_universe(cli_tickers: str | list[str] | None = None, series_list: list[str] | None = None, events_per_series: int | None = None) -> dict`. Precedence: `--tickers`, then `FOCUS_OVERRIDE`, then the derived rule.
- `kalshi_io.universe.check_universe(universe: dict) -> dict`. Keeps tickers that are open, paused or unopened. Raises `UniverseError` (a `RuntimeError` with `.status_counts` and `.dead`) when the universe is empty or nothing in it can trade. Makes no request.
- `kalshi_io.universe.refresh_universe(current: dict) -> tuple[dict, dict]`. Returns the new universe and `{"added", "dead", "kept", "rolls"}`. Paused tickers and live tickers pushed out by a nearer event stay.
- `kalshi_io.universe.register(universe: dict) -> None`, `kalshi_io.universe.next_refresh_at(universe: dict, last_refresh_ts: float, interval_s: float) -> float | None`, `kalshi_io.universe.write_universe(universe: dict, json_path: pathlib.Path, txt_path: pathlib.Path | None = None) -> None`, `kalshi_io.universe.load_universe(json_path: pathlib.Path) -> dict | None`, `kalshi_io.universe.format_universe(universe: dict) -> str`. `kalshi_io.universe.UniverseError(message: str, status_counts: dict[str, int] | None = None)`. File names: `LIVE_SNAPSHOT = "focus_universe_live.json"`, `FOCUS_JSON = "focus_universe.json"`, `FOCUS_TXT = "focus_universe.txt"`.

### kalshi_io.releases: release windows

Pure functions. A window is `(start, end, label)` in Unix seconds.

- `kalshi_io.releases.release_times(universe: dict | None, catalog_index: dict[str, dict] | None, calendar: list[str] | None, now_ts: float, after_s: float) -> list[tuple[float, str]]`. Times from the polled events, from every cataloged market that can still trade (one time per event, the earliest close), and from the calendar; from `now - after_s` to 90 days ahead. Raises `ValueError` for a calendar entry that is not ISO-8601.
- `kalshi_io.releases.windows(times: list[tuple[float, str]], before_s: float, after_s: float) -> list[tuple[float, float, str]]` (overlaps merged), `kalshi_io.releases.merge_windows(existing: list[tuple[float, float, str]], fresh: list[tuple[float, float, str]], now_ts: float) -> list[tuple[float, float, str]]` (an open window never disappears), `kalshi_io.releases.active_window(wins: list[tuple[float, float, str]], now_ts: float) -> tuple[float, float, str] | None`, `kalshi_io.releases.next_window(wins: list[tuple[float, float, str]], now_ts: float) -> tuple[float, float, str] | None`, `kalshi_io.releases.describe(wins: list[tuple[float, float, str]], now_ts: float, limit: int = 5) -> list[str]`, `kalshi_io.releases.default_calendar() -> list[str]`.

### kalshi_io.storage: parquet appends, locks, paths

Every candle, trade and book file is written by `append_parquet` and by nothing else. The metadata store is the one other parquet file: `upsert_market_metadata` writes it itself, under the same `file_lock`, through the same temp file and rename.

- `kalshi_io.storage.append_parquet(df: pandas.DataFrame, path: pathlib.Path, dedupe_on: list[str], sort_by: str | None = None, lock_timeout: float | None = None) -> int`. Under the file's lock: read the existing file, concatenate, drop duplicates keeping the last, sort, write to a temp file, rename. Returns the number of new rows after dedupe. An empty frame returns 0 and touches nothing. Raises `LockTimeout` (a `TimeoutError`); then nothing was written.
- `kalshi_io.storage.file_lock(target: pathlib.Path | str, timeout: float | None = None)` and `kalshi_io.storage.named_lock(name: str, timeout: float = 0.0)`: context managers over `flock` on files in `DATA/.locks`; re-entrant within a thread; the kernel releases a lock when its holder dies. Where `flock` is unavailable writes proceed unlocked after one WARNING.
- `kalshi_io.storage.get_output_path(kind: str, interval: int | None, series: str, ticker: str, ts: pandas.Timestamp | None = None) -> pathlib.Path`. `kind` is `candles`, `trades` or `orderbook`; raises `ValueError` for an unknown kind or interval, or when `ts` is missing for a partitioned kind.
- `kalshi_io.storage.get_last_timestamp(path: pathlib.Path, col: str = 'ts_ms') -> int | None`, `kalshi_io.storage.read_parquet_safe(path: pathlib.Path) -> pandas.DataFrame | None`, `kalshi_io.storage.lock_path_for(target: pathlib.Path | str) -> pathlib.Path`, `kalshi_io.storage.temp_path_for(path: pathlib.Path) -> pathlib.Path`, `kalshi_io.storage.atomic_write_text(path: pathlib.Path, text: str) -> None`, `kalshi_io.storage.duckdb_connect()` (in-memory DuckDB, extension download off).

### kalshi_io.runlog: logs and skip files

- `kalshi_io.runlog.run_logging(name: str, stamp_fmt: str = '%Y%m%d_%H%M')`. Context manager: logs one run to `DATA/logs/{name}_{utc stamp}.log`, yields the path. A run started inside another run logs into the outer file.
- `kalshi_io.runlog.configure_logging(level: int = 20) -> logging.Logger` (one stderr handler on the parent logger `kalshi`, idempotent), `kalshi_io.runlog.get_logger(name: str) -> logging.Logger`.
- `kalshi_io.runlog.SkipRecorder(kind: str)` with property `path` and `record(self, ticker: str, reason: str, code: str | None = None) -> bool`; `kalshi_io.runlog.get_skip_recorder(kind: str) -> kalshi_io.runlog.SkipRecorder`.
- `kalshi_io.runlog.note_result(results: dict | None, ticker: str, status: str, *, rows: int = 0, error: str | None = None, outage: bool = False) -> None`. Fills a caller's `results` dict with `{ticker: {"status", "rows", "error", "outage"}}`.

### kalshi_io.quality: counts, never repairs

- `kalshi_io.quality.run_checks(now: datetime.datetime | None = None, write_csv: bool = True) -> kalshi_io.quality.Report`. Read-only over the whole data root; writes only `DATA/logs/quality_{yyyymmdd}.csv`.
- `kalshi_io.quality.format_report(report: kalshi_io.quality.Report) -> str`. One line per check, the coverage table, the CSV path.
- The pieces, each taking the DuckDB connection and the set of views present: `kalshi_io.quality.check_duplicates(con, present: set[str], root: pathlib.Path) -> kalshi_io.quality.Check`, `kalshi_io.quality.check_order(con, present: set[str], root: pathlib.Path) -> kalshi_io.quality.Check`, `kalshi_io.quality.check_volume_close(con, present: set[str]) -> kalshi_io.quality.Check`, `kalshi_io.quality.check_taker_side(con, present: set[str]) -> kalshi_io.quality.Check`, `kalshi_io.quality.check_volume_exchange(con, present: set[str]) -> kalshi_io.quality.Check`, `kalshi_io.quality.check_ladder(con, present: set[str]) -> kalshi_io.quality.Check`, `kalshi_io.quality.check_mutex(con, present: set[str]) -> kalshi_io.quality.Check`, `kalshi_io.quality.check_stale(con, present: set[str], now_ms: int) -> kalshi_io.quality.Check`, `kalshi_io.quality.check_missing(con, present: set[str]) -> kalshi_io.quality.Check`, `kalshi_io.quality.check_history_start(con, present: set[str]) -> kalshi_io.quality.Check`, `kalshi_io.quality.coverage(con, present: set[str]) -> list[dict]`.
- File pass: `kalshi_io.quality.scan_files(root: pathlib.Path) -> dict[str, list[pathlib.Path]]`, `kalshi_io.quality.schema_problem(path: pathlib.Path, expected: dict[str, str]) -> str | None`, `kalshi_io.quality.check_schema(found: dict[str, list[pathlib.Path]], root: pathlib.Path) -> tuple[kalshi_io.quality.Check, dict[str, list[pathlib.Path]]]`, `kalshi_io.quality.write_report_csv(report: kalshi_io.quality.Report, path: pathlib.Path) -> pathlib.Path`. `SCHEMAS` maps each file kind to `{column: parquet type}` in stored order. `Check` and `Report` are dataclasses; `Report.check(key)` returns one check, `Report.csv_rows()` the CSV rows.

### pull_historical: the four pullers

One signature for all four:

- `pull_historical.pull_daily.run(tickers: str | list[str], since: str | None = None, limit: int | None = None, *, results: dict | None = None, should_stop=None) -> dict`
- `pull_historical.pull_hourly.run(tickers: str | list[str], since: str | None = None, limit: int | None = None, *, results: dict | None = None, should_stop=None) -> dict`
- `pull_historical.pull_minute.run(tickers: str | list[str], since: str | None = None, limit: int | None = None, *, results: dict | None = None, should_stop=None) -> dict`
- `pull_historical.pull_trades.run(tickers: str | list[str], since: str | None = None, limit: int | None = None, *, results: dict | None = None, should_stop=None) -> dict`

`tickers` is anything `load_tickers` accepts. `since` is `"YYYY-MM-DD"`. `limit` cuts the sorted ticker list before validation. Returns the summary dict `{"processed", "skipped", "failed", "aborted", "unknown", "rows_written", "elapsed_sec"}`. Writes the layer's parquet files, a log file `DATA/logs/pull_{layer}_{yyyymmdd_hhmm}.log` (unless called inside another run) and, on the first problem, `DATA/logs/skip_{layer}_{process stamp}.txt`. Logs one line per ticker. Raises nothing per ticker: every exception is caught, logged at ERROR, counted under `failed` and written to the skip file; a missing ticker file raises `FileNotFoundError` from `load_tickers` before the loop. A caller that passes `results={}` gets one outcome per ticker; `should_stop` is a callable checked before every ticker.

Candle pullers: without `since`, a ticker without stored data starts at the market's open time (from the catalog, or from the API for an uncataloged ticker) and a ticker with data resumes at its last stored candle; the end is `candle_end_ts`. If a later window fails, the gap-free prefix is saved, the ticker counts as failed, and the next run resumes. A `since` later than market open leaves the earlier history unpulled for good, because later runs resume from the newest candle. Trades puller: a ticker without stored trades gets the complete tape from both tiers; a ticker with data asks from 60 s before its last stored trade and writes nothing when nothing is new; a failed page writes nothing at all.

CLI, the same three options for `python -m pull_historical.pull_daily`, `pull_hourly`, `pull_minute` and `pull_trades`:

| Option | Default | Meaning |
|-|-|-|
| `--tickers` | `CAT/all_tickers.txt` for the three candle pullers; required for `pull_trades` | One or more ticker sources |
| `--limit` | `None` | Maximum number of tickers |
| `--since` | `None` | Start date override `YYYY-MM-DD` |

Each prints the summary dict and exits 1 when `failed` is non-zero, else 0. `pull_minute` without `--tickers` and without `--limit` prints a warning to stderr that it is about to pull every cataloged ticker from market open, and then does it.

### pull_historical.backfill: the bulk driver

`python -m pull_historical.backfill` runs the layers `metadata`, `daily`, `hourly`, `trades`, `minute` one after the other over the catalog or a ticker list. Within a layer it walks event by event: events with a market that can still trade first, nearest close first, then settled events, newest close first. It prints an estimate before it starts. `python -m pull_historical.pull_all_freq` is the same driver without arguments.

| Option | Default | Meaning |
|-|-|-|
| `--tickers` | `None`: every cataloged ticker | Ticker sources, as for the pullers |
| `--layers` | `metadata,daily,hourly,trades,minute` | Comma-separated subset; they always run in that order |
| `--estimate-only` | off | Print the estimate and stop; no request is made, nothing is created |
| `--retry-failed` | off | Run only the tickers of the newest failure lists, each in the layer it failed in |
| `--ignore-journal` | off | Do not skip pairs the journal calls final |
| `--no-audit` | off | Skip the data-quality checks after the run |
| `--log-name` | `backfill` | Prefix of the log file in `DATA/logs` |
| `--lock-name` | `None` | Hold `DATA/.locks/NAME.lock` for the run and exit 75 if another process holds it |

Writes: data files through the pullers; `DATA/logs/{log-name}_{yyyymmdd_hhmmss}.log`; `DATA/logs/backfill_summary_{stamp}.json`; `DATA/logs/backfill_failed_{stamp}_{layer}.txt` per layer with failures; `DATA/state/backfill_journal.jsonl`; the quality CSV unless `--no-audit`. A run without `--tickers` and without `--retry-failed` holds the lock `backfill_full`. The journal records every (ticker, layer) pair pulled completely while the market was `finalized`; a rerun skips such a pair without a request while the files on disk still agree with the journal. A failed ticker is retried once at the end of its layer. When three tickers in a row ran out of retries the driver waits 1, 2, 4, 8, 16 minutes between attempts and then gives up. The first Ctrl+C or SIGTERM stops after the current ticker; a second one stops at once. The committed catalog is read, never written.

Importable pieces, for building on top: `pull_historical.backfill.main(argv: list[str] | None = None) -> int`; `pull_historical.backfill.build_items(tickers: list[str]) -> tuple[list[pull_historical.backfill.Item], list[str]]`; `pull_historical.backfill.order_batches(items: list[pull_historical.backfill.Item]) -> list[list[pull_historical.backfill.Item]]`; `pull_historical.backfill.estimate(items: list[pull_historical.backfill.Item], journal: pull_historical.backfill.Journal, layers: tuple[str, ...], now_ts: int) -> dict`; `pull_historical.backfill.format_estimate(est: dict, n_items: int, n_uncataloged: int, rps: float) -> str`; `pull_historical.backfill.stored_files(layer: str, item: pull_historical.backfill.Item) -> list[pathlib.Path]`; `pull_historical.backfill.last_stored_ts(layer: str, item: pull_historical.backfill.Item) -> int | None`; `pull_historical.backfill.settled_in_store() -> set[str]`; `pull_historical.backfill.store_counts() -> dict[str, dict]`; `pull_historical.backfill.newest_failure_lists() -> dict[str, pathlib.Path]`; `pull_historical.backfill.write_failure_lists(reports: list[pull_historical.backfill.LayerReport], stamp: str) -> list[pathlib.Path]`; `pull_historical.backfill.format_summary(reports: list[pull_historical.backfill.LayerReport], counts: dict, stats: dict, elapsed: float, exit_code: int) -> str`; `pull_historical.backfill.pull_batch(layer: str, tickers: list[str], state: pull_historical.backfill.State, report: pull_historical.backfill.LayerReport, what: str) -> None`; `pull_historical.backfill.run_metadata_layer(state: pull_historical.backfill.State, report: pull_historical.backfill.LayerReport, tickers: list[str]) -> None`; `pull_historical.backfill.run_layer(layer: str, batches: list[list[pull_historical.backfill.Item]], state: pull_historical.backfill.State, est_requests: int) -> pull_historical.backfill.LayerReport`. Classes: `Item`, `Journal` (`Journal.load(ignore=False)`, `is_done`, `add_final`, `add_run`), `LayerReport`, `State`, and `ApiDown` (a `RuntimeError`).

### pull_historical.pull_audit: offline coverage report and quality checks

`python -m pull_historical.pull_audit` never calls the API. It prints the catalog's age and the audited tickers by catalog status, the focus universe on record (the newer of the poller's snapshot and the roll's proposal), a per-series summary of the stored daily files, the tickers without a daily file, and then the data-quality checks over the whole data root.

| Option | Default | Meaning |
|-|-|-|
| `--tickers` | `CAT/all_tickers.txt` | Ticker sources |
| `--limit` | `None` | Maximum number of tickers |
| `--no-checks` | off | Skip the data-quality checks |

Writes `DATA/logs/audit_{yyyymmdd}.csv` and `DATA/logs/quality_{yyyymmdd}.csv`. Always exits 0. When none of the audited tickers has a stored daily file it prints `No daily parquet files found.` and stops there, before both CSV files and before the checks (observed on 2026-09-18). To run the checks alone, call `run_checks()` yourself (see the patterns). Importable: `pull_historical.pull_audit.catalog_header(tickers: list[str], index: dict[str, dict], now: datetime.datetime | None = None) -> list[str]`, `pull_historical.pull_audit.focus_header(index: dict[str, dict], now: datetime.datetime | None = None) -> list[str]`, `pull_historical.pull_audit.main(argv: list[str] | None = None) -> int`.

### pull_live.poll_focus: the poller

`python -m pull_live.poll_focus` derives the focus universe, refuses to start when nothing in it can trade, and then loops. Every pass starts with one batch order book request for the whole universe (one per 100 tickers), so every book of a sweep carries one timestamp. Then, when due, the daily, hourly, minute and trade pullers. Inside a release window only the books are polled, every `--release-interval` seconds; candle and trade pulls, and the final sweep of tickers that just stopped trading, wait for the window to end; no candle or trade sweep starts within 60 s before a window. The universe is refreshed every `--universe-refresh` seconds and once 90 s after the nearest `close_time`; tickers that stopped trading get one final candle and trade sweep and are dropped; a roll is logged as `universe roll {series}: {old event} -> {new event}`. A ticker without a stored daily file or without a stored minute file is handed to a child process (`python -m pull_historical.backfill --tickers ... --layers metadata,<enabled layers> --no-audit --log-name backfill_history --lock-name focus_history` with `KALSHI_MAX_RPS=3`) and the loop pulls no candles or trades for it until the child has exited; a failed child is retried after 10 minutes and after three failures the tickers rejoin anyway. With only the book sweep enabled no child is ever started. With `--no-daily` or `--no-minute` the file that would mark a ticker as having history is never written, so every start hands the whole universe to the child again: it only resumes, which is cheap, but the loop pulls no candles or trades until it has exited.

| Option | Default | Meaning |
|-|-|-|
| `--minute-interval`, `--trades-interval`, `--orderbook-interval` | 60 | Seconds between minute pulls, trade pulls, book sweeps |
| `--hourly-interval` | 900 | Seconds between hourly pulls |
| `--daily-interval` | 21600 | Seconds between daily pulls |
| `--no-minute`, `--no-trades`, `--no-orderbook`, `--no-hourly`, `--no-daily` | off | Disable that task |
| `--iterations` | `None`: forever | Number of loop passes |
| `--tickers` | `None` | Poll exactly these tickers; never rolls |
| `--series` | `None`: `FOCUS_SERIES` | Series to derive the universe from |
| `--events-per-series` | `None`: `FOCUS_EVENTS_PER_SERIES` | Nearest open events per series |
| `--universe-refresh` | 3600 | Seconds between refreshes; 0 means never |
| `--release-interval` | 5 | Seconds between book sweeps inside a window |
| `--release-before`, `--release-after` | 300, 900 | Window bounds around a `close_time`, in seconds |
| `--no-release-windows` | off | One cadence everywhere |
| `--no-background-history` | off | Pull missing history inside the loop instead of in a child |
| `--show-universe` | off | Derive, check, print, exit; no log file |

Writes `DATA/logs/poll_focus_{yyyymmdd}.log` (named after the UTC day the process started; it is never rotated, so a long run keeps writing to that one file, and the pullers it calls log into it), `DATA/logs/focus_universe_live.json`, the book files, the candle and trade files through the pullers, `DATA/logs/skip_orderbook_{process stamp}.txt` on a book problem, and through the child `DATA/logs/backfill_history_{stamp}.log`. `pull_live.poll_focus.main(argv: list[str] | None = None) -> int` is the only public function.

### get_ticker_info: search and catalog scripts

`python get_ticker_info/find_events.py` searches the exchange. Pick exactly one mode.

| Option | Meaning |
|-|-|
| `--categories` | Every category with its tags |
| `--category C`, `--tag T [T ...]`, `--keyword K` | Series of a category, with any of the tags, whose ticker, title or tags contain every word; combinable with each other |
| `--include-legacy` | Also show dead pre-KX spellings |
| `--series S [S ...]` | Events of those series; with `--markets`, their markets |
| `--event E` | Markets of one event from both tiers |
| `--status X` | With `--series` or `--event`. Events take `unopened`, `open`, `closed`, `settled`; markets also `paused`. Omit it for every status |
| `--format` | `table` (default), `tickers` (one per line; for markets a valid `--tickers` file), `json`, `py` (a Python list literal) |

Table columns: categories `CATEGORY TAGS`; series `TICKER CATEGORY FREQUENCY TAGS TITLE`; events `SERIES_TICKER EVENT_TICKER LIVE_MARKETS OPEN_MARKETS CLOSES TITLE` (market counts cover the live tier only); markets `TICKER EVENT_TICKER STATUS TIER CLOSE_TIME TITLE`. Importable when the repo root is on `sys.path`: `get_ticker_info.find_events.main(argv: list[str] | None = None) -> int`, `get_ticker_info.find_events.event_rows(series_list: list[str], status=None) -> list[dict]`, `get_ticker_info.find_events.market_rows(series_list=None, event_ticker=None, status=None) -> list[dict]`, `get_ticker_info.find_events.series_rows(category=None, tags=None, keyword=None, include_legacy=False) -> list[dict]`, `get_ticker_info.find_events.category_rows() -> list[dict]`, `get_ticker_info.find_events.render(kind: str, rows: list[dict], fmt: str) -> str`. `get_ticker_info.find_events.UsageError` (a `ValueError`) marks an invalid combination of modes inside the CLI and becomes exit code 2.

`python get_ticker_info/roll.py` refreshes the catalog and the metadata store, reports what changed and proposes the focus universe.

| Option | Default | Meaning |
|-|-|-|
| `--series` | `None`: all of `SERIES_LIST` | Series to refresh |
| `--events-per-series` | `None` | Nearest open events per focus series to propose |
| `--dry-run` | off | Report only; nothing is written, not even a log file |
| `--out-dir` | `None`: `CAT` | Catalog directory to read and write; the metadata store still goes to the data root |
| `--no-focus` | off | Propose no focus universe |
| `--full` | off | Do not shorten report sections (40 lines each otherwise) |

Writes `CAT/{SERIES}_tickers.json` and `.txt`, `CAT/all_tickers.json` and `.txt`, `CAT/focus_universe.json` and `.txt`, `DATA/metadata/markets.parquet`, `DATA/logs/roll_{yyyymmdd_hhmmss}.log` and `DATA/logs/roll_report_{same stamp}.txt`. Importable: `get_ticker_info.roll.main(argv: list[str] | None = None) -> int`, `get_ticker_info.roll.focus_checks(report: dict, focus: dict | None, focus_error: str | None) -> list[dict]`, `get_ticker_info.roll.format_report(report: dict, focus: dict | None, checks: list[dict], *, out_dir: pathlib.Path, dry_run: bool, limit: int | None = 40, now: datetime.datetime | None = None) -> str`.

`python get_ticker_info/get_tickers.py` rebuilds the catalog of every series in `SERIES_LIST` without a report and without touching the metadata store (`get_ticker_info.get_tickers.discover_series(series, verbose=True, save=True)`, `get_ticker_info.get_tickers.build_combined(verbose=True)`, `get_ticker_info.get_tickers.load_tickers(series, key='tickers')`, `get_ticker_info.get_tickers.main() -> int`). `python get_ticker_info/get_Econ_Info.py` prints every series on the exchange by category and the Economics listing; it writes nothing (`get_ticker_info.get_Econ_Info.main() -> None`).

## Golden patterns

Run every block from the repo root with the environment active, and with `KALSHI_DATA_DIR` pointing at a scratch directory until you mean to write to the real data root. Placeholders in angle brackets are yours to fill; the text says where the value comes from. Every command below was run that way on 2026-09-18 from a fresh clone of the public repo, on Python 3.13.5, without credentials, at `KALSHI_MAX_RPS=1`, with three exceptions that would have started a long download: the whole-catalog run was run with `--estimate-only` (its detached form was run for one series instead), `backfill --tickers focus` with `--estimate-only`, and the bare `poll_focus`, with its detached form, as the `--tickers` form with `--iterations 1`.

### Set up from a clone

```bash
git clone https://github.com/Arom-MFE/Kalshi_Pull.git && cd Kalshi_Pull
python3.13 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest -q                                      # 340 passed at 0.3.0, offline
export KALSHI_DATA_DIR=/tmp/kalshi_scratch     # leave unset to write to <repo>/kalshi_data
python -m pull_historical.pull_daily --tickers KXRECSSNBER-26
```

### Find what trades for a series

```bash
python get_ticker_info/find_events.py --categories
python get_ticker_info/find_events.py --category Economics --keyword inflation
python get_ticker_info/find_events.py --series KXCPIYOY --status open
python get_ticker_info/find_events.py --series KXCPIYOY --status open --markets --format tickers
python get_ticker_info/find_events.py --event <EVENT> --format json
```

Take `<EVENT>` from the `EVENT_TICKER` column of the third command. A category is case sensitive (`Economics`, not `economics`; the error names the valid ones). `--status all` is a usage error: omit `--status`. Exit code 1 means nothing was found, not a failure.

### Add a series to the catalog

1. Find its ticker: `python get_ticker_info/find_events.py --category <CATEGORY> --keyword <WORD>`.
2. Preview: `python get_ticker_info/roll.py --series <SERIES> --dry-run`. Nothing is written.
3. Add `"<SERIES>"` to `SERIES_LIST` in `kalshi_io/config.py`, so that every later roll refreshes it; add it to `FOCUS_SERIES` too if the poller should follow it.
4. Write it: `python get_ticker_info/roll.py --series <SERIES>` refreshes that series alone and rebuilds `all_tickers.*` from every per-series file in the directory; `python get_ticker_info/roll.py` refreshes all of them.
5. Pull it: `python -m pull_historical.backfill --tickers <SERIES> --estimate-only`, then without `--estimate-only`.

Step 2 works before step 3, because a roll takes any series name; step 3 is what makes every later roll without `--series` refresh the new series. The catalog files are committed files: review `git status get_ticker_info/kalshi_tickers` (a new series appears as two untracked files, which `git diff` does not list) and `git diff --stat get_ticker_info/kalshi_tickers`, and let the repo's owner commit them.

### Roll to a new cycle

```bash
python get_ticker_info/roll.py --dry-run        # report only; makes requests, writes nothing
python get_ticker_info/roll.py                  # refresh, write, report
python -m pull_historical.backfill --tickers focus
```

Read the report's `CHECKS` block: every line must start with `[ok]`; exit code 1 means a series failed or a check did. `NEW EVENTS`, `NEW MARKETS IN EXISTING EVENTS` and `STATUS CHANGES BY EVENT` say what moved, and each is printed only when it has something to list; `PROPOSED FOCUS UNIVERSE` is what `--tickers focus` means from now on. The third command pulls every layer of every focus ticker from market open, which is a long download on a data root without their history: add `--estimate-only` first and read the minute layer's line. The poller needs no roll: it derives and rolls its universe itself. `python get_ticker_info/roll.py --events-per-series 2` proposes the next cycle as well, so its history can be in before the current one closes.

### Backfill one event at every frequency

`--tickers` does not expand an event ticker, so list the event's markets into a file first:

```bash
python get_ticker_info/find_events.py --event <EVENT> --format tickers > /tmp/event_tickers.txt
python -m pull_historical.backfill --tickers /tmp/event_tickers.txt --estimate-only
python -m pull_historical.backfill --tickers /tmp/event_tickers.txt
```

For a cheap first try use a short-lived settled single-market event such as `RECSSNBER-22`. The summary table has one row per layer; `Result: complete` and exit code 0 mean every ticker went through. Run the same command again: a settled event costs no request the second time (`final, skipped` in the summary).

### Backfill the whole catalog

```bash
python -m pull_historical.backfill --estimate-only
mkdir -p "${KALSHI_DATA_DIR:-kalshi_data}/logs"
caffeinate -i nohup python -m pull_historical.backfill >> "${KALSHI_DATA_DIR:-kalshi_data}/logs/backfill_stdout.log" 2>&1 &
grep -hE "\] event |=== layer" "$(ls -t "${KALSHI_DATA_DIR:-kalshi_data}"/logs/backfill_2*.log | head -1)" | tail -1
```

The estimate makes no request and prints requests, runtime at the configured rate, rows and disk per layer; the minute layer dominates, at one request per 3 days of every market's life. `caffeinate` is a macOS utility that keeps the machine from idle sleep while the command runs; leave it out where it does not exist. The last line prints the newest progress line of the newest driver log: layer, event k of n, tickers done, requests used of the estimate, rate, ETA (at a layer boundary it prints a layer banner instead). It picks the newest log by name because a `grep` over several files returns their lines in no fixed order when the shell wraps `grep` in a parallel search tool. To stop a detached run, signal the Python process, not the `caffeinate` helper: `pgrep -fl "pull_historical.backfill"` lists both, and on macOS the process id the shell printed for the `nohup` line is the Python process, with the helper as its child. `kill -INT <pid>` (or `-TERM`) lets it finish the current ticker, print its summary with `Result: interrupted; run the same command again to resume`, and exit with code 130; a signal to the helper leaves the run untouched (both observed on 2026-09-18). The same command resumes after any stop. A second full-catalog run exits with code 75 while the first holds the lock. While it runs, run at most a books-only poller next to it, and give any other process `KALSHI_MAX_RPS=1`: all processes on one machine share one keyless rate limit. Observed on 2026-09-18: a full-catalog run wrote the metadata store first and then the daily files of the open events with the nearest close.

### Run the poller: normally, books only, through a release window

```bash
python -m pull_live.poll_focus --show-universe
python -m pull_live.poll_focus --iterations 1 --no-daily --no-hourly --no-minute --no-trades
python -m pull_live.poll_focus --tickers <TICKER> [<TICKER> ...] --iterations 1
python -m pull_live.poll_focus
mkdir -p "${KALSHI_DATA_DIR:-kalshi_data}/logs"
caffeinate -i nohup python -m pull_live.poll_focus >> "${KALSHI_DATA_DIR:-kalshi_data}/logs/poll_focus_stdout.log" 2>&1 &
```

The first prints the derived universe and exits (code 2 with the counts per status if nothing in it can trade). The second is one books-only pass: the universe is derived, one batch request takes every book, and each ticker whose book holds an order gets a book file. The third is the normal form for tickers of your choice: books first, then the daily, hourly, minute and trade pullers once each; backfill those tickers first (`python -m pull_historical.backfill --tickers <TICKER> [<TICKER> ...]`), otherwise they are handed to the background history pull. The bare form runs until Ctrl+C or SIGTERM, which finish the current task and exit with code 0; the last two lines start it detached from the terminal, with its console output appended to a file. On a data root without history the normal forms hand every ticker of the universe to the background history pull, which is by design but is a large download: on a first try use `--show-universe` and the books-only pass. The books-only form is the one to run next to a full download: books cannot be backfilled, and a poller that stored today's bars for a ticker without history would make a later backfill skip that history.

Release windows need no action: the startup line `Release windows: ... Next: ...` lists them, `release window open until ...` and `release window closed (...)` mark them in the log. To rehearse one without waiting for a release, widen the window until it contains now; observed on 2026-09-18, this entered the window at once and swept the books every 5 s:

```bash
python -m pull_live.poll_focus --iterations 3 --release-before 3000000 --no-daily --no-hourly --no-minute --no-trades
```

For a release whose markets are not cataloged, add its time to `RELEASE_CALENDAR` in `kalshi_io/config.py`.

### Read the store with pandas or DuckDB

```python
import os, duckdb, pandas as pd
root = os.environ.get("KALSHI_DATA_DIR", "kalshi_data")
daily = pd.read_parquet(f"{root}/candles/daily/KXRECSSNBER/KXRECSSNBER-26.parquet")
con = duckdb.connect()
per_ticker = con.execute(f"""
    SELECT market_ticker, count(*) AS bars, min(ts_ms) AS first_ts, max(ts_ms) AS last_ts
    FROM read_parquet('{root}/candles/daily/**/*.parquet')
    GROUP BY 1 ORDER BY 1""").fetchdf()
```

Every candle file has the same 19 columns, so a glob over a layer needs no `union_by_name`. Missing prices are stored as nulls: in SQL test `close IS NULL`, in pandas `df["close"].isna()`. The quality checks alone, without the coverage report:

```python
from kalshi_io import quality
print(quality.format_report(quality.run_checks()))
```

### Assemble the strike-ladder inputs for an implied distribution

The inputs are one event's strikes from the metadata store joined with each strike's quotes on one bar. Pull the two layers that hold them, then join. Nothing below computes a probability, a mid or a distribution: that is the caller's work.

```bash
python get_ticker_info/find_events.py --event <EVENT> --format tickers > /tmp/ladder.txt
python -m pull_historical.backfill --tickers /tmp/ladder.txt --layers metadata,daily --no-audit
```

```python
import os, duckdb
root = os.environ.get("KALSHI_DATA_DIR", "kalshi_data")
ladder = duckdb.connect().execute(f"""
    SELECT m.market_ticker, m.strike_type, m.floor_strike, m.cap_strike, m.status, m.result, m.expiration_value,
           d.ts_ms, d.close, d.volume, d.open_interest, d.yes_bid_close, d.yes_ask_close
    FROM read_parquet('{root}/metadata/markets.parquet') m
    JOIN read_parquet('{root}/candles/daily/**/*.parquet') d USING (market_ticker)
    WHERE m.event_ticker = '<EVENT>'
    ORDER BY d.ts_ms, coalesce(m.floor_strike, m.cap_strike)""").fetchdf()
```

Reading rules for whoever computes on it: a `greater` strike asks whether the value ends above `floor_strike` (`greater_or_equal`: at or above), a `less` strike whether it ends below `cap_strike` (`less_or_equal`: at or below), a `between` strike both; the market's `rules_primary` text is the binding wording. Take strikes from these columns, never from the ticker text. `close` is the last trade of the period and is null on a day without a trade; `yes_bid_close` and `yes_ask_close` exist on every bar. A bid of 0.0 with an ask of 1.0 is an empty book, not a quote. Only strikes with a bar on the same `ts_ms` are comparable. `mutually_exclusive` means at most one market of the event resolves yes; it does not mean the outcomes are exhaustive. Intraday ladders work the same way from the hourly or minute layer, or from book snapshots.

### Resume after a crash

Run the same command again. Every puller resumes from the last stored row; the driver also skips finalized pairs through its journal; `python -m pull_historical.backfill --retry-failed` reruns only what the newest failure lists hold. What a crash can leave behind is harmless: a `*.tmp` file next to a parquet file (it never matches a `*.parquet` glob; the quality report counts it as a stray temp file), a lock file (the kernel released the lock when the process died; lock files are never deleted), a torn last journal line (ignored, and repaired on the next append). A candle layer may hold a saved prefix of a failed ticker; trades are all or nothing per ticker. Do not delete anything to "clean up".

### Diagnose a rate-limit or an empty result

- Rate limit: look for `WARNING HTTP 429: too many requests on GET ... retrying in ...s` in the log and for `429 answers: N (p%)` in a driver summary. Isolated ones are normal and retried. If they pile up, count the processes that share the machine's keyless limit and lower `KALSHI_MAX_RPS`. `giving up on GET ... after 6 attempts` followed by `run aborted: 3 tickers in a row failed after all retries` means the API was down or throttling hard: wait and rerun.
- `0 candles returned` or `0 trades returned`: the API had nothing in that window. Candles exist only for periods in which something happened, so this is normal for a quiet market; `up-to-date` means the stored data already reaches the end of the window.
- `unknown` in the summary, `SKIP` in the log: the ticker is in neither the catalog nor the API. Check the spelling; check that it is a market ticker and not an event or series name; list the event's markets with `find_events.py --event`.
- No book file for a ticker: an empty snapshot writes nothing, and the exchange answers 200 with empty books for a settled market and even for a ticker it does not know (observed on 2026-09-18), so a wrong ticker list fails silently on the book routes. `python -m pull_live.poll_focus --show-universe` shows what can trade; the poller's preflight refuses a universe in which nothing can.
- `find_events` exit code 1: nothing matched. Check the category's case, remember that filter words are not response statuses, and that `--status settled` on `--series` without `--markets` lists events.
- A market missing from a listing: markets settled before the cutoff live on the `/historical/` tier; `find_markets`, `get_market` and `lookup_markets` look in both.
- A new event missing from the catalog: the catalog is a snapshot. `python -m pull_historical.pull_audit --tickers focus` prints its age and warns after 7 days; `roll.py` refreshes it.

## Never do

- Never delete, move, rewrite, "repair" or re-sort an existing parquet file, journal or lock file in a data root that holds real pulls. The pipeline only appends to candle, trade and book files, through `append_parquet`, and replaces rows of the metadata store through `upsert_market_metadata`. If a store is wrong, the repo's answer is to rename the whole directory (the pattern `kalshi_data_old_*/` is git-ignored) and download afresh.
- Never write parquet into the store with your own code path. Two writers are safe only because every write goes through `append_parquet` or `upsert_market_metadata`, both under the file lock; a direct `to_parquet` can lose another process's rows.
- Never run two pollers on one data root: they spend the shared rate limit twice on the same data. Never run a full poller next to a backfill of the same tickers. One invariant holds everything together: a ticker's stored range is contiguous from market open, and resume starts at the newest stored row. A poller that stores today's bars for a ticker without history makes a later backfill skip that history. Next to a download run only the books-only poller.
- Never pass `--since` to a backfill you want complete: later runs resume from the newest candle, so the earlier history stays unpulled.
- Never commit data, logs, `.env` or a key file. `.gitignore` covers `.env`, `.venv/`, `kalshi_data/` and `kalshi_data_old_*/`; keep scratch data roots outside the repo. Never print or read a `.env` or a private key: keyless access needs neither.
- Never point tests or experiments at a real data root: set `KALSHI_DATA_DIR` to a scratch directory first. The test suite does this itself and refuses to run inside the real one.
- Never send `status=all`, a response status as a filter, or two filters at once; never treat a 404 on a live endpoint as "does not exist" before asking the `/historical/` twin.
- Never guess an event or a strike from the ticker text. Events come from the catalog or the API, strikes from the metadata store.
- Never read a price column as a quote or a quote as a trade. `open`, `high`, `low`, `close`, `mean` are trade prices and null without a trade; the quotes are in the `yes_bid_*` and `yes_ask_*` columns.
- Never invent a 0 for a missing value, round a contract count or rescale a price. Fractional contract counts are genuine; prices are already dollars.
- Never raise `KALSHI_MAX_RPS` to speed up a keyless download: above what the endpoints sustain it only buys retries. Never run several downloaders in parallel for the same reason.
- Never read a log line's clock as UTC: log lines carry the machine's local time, while file names and stored timestamps are UTC (observed on 2026-09-18).
- Never start a second full-catalog driver to "make sure": it exits with code 75, and killing the first loses nothing but time.
- Never edit the catalog JSON by hand: a roll rewrites it, and a hand edit hides a change the report would have shown.

## Decode table

Log line format: `{asctime} {LEVEL} {message}`, for example `2030-01-31 09:15:02,123 INFO pull_daily starting (data root: /tmp/kalshi_scratch)`. Several messages carry an em dash between a marker and its reason; the signals below quote the text before it.

| Signal | Where | Meaning | Action |
|-|-|-|-|
| `pull_{layer} starting (data root: ...)` | puller log | First line of a run; names the data root in use | Check it is the root you meant |
| `Tickers: {n} (limit={limit}, unknown={k})` | puller log | Tickers that passed validation | |
| `[i/n] {ticker}: {k} new rows ({m} fetched)` | candle pullers | Stored; k is new rows after dedupe, m what the API returned (a resume re-fetches the last stored candle) | |
| `[i/n] {ticker}: {k} new trades ({m} fetched)`, `resuming from min_ts=...` | trades puller | Same for trades; the second line shows the resume point in Unix seconds | |
| `[i/n] {ticker}: up-to-date` | candle pullers | Stored data reaches the end of the window; no request | |
| `[i/n] {ticker}: 0 candles returned`, `0 trades returned` | pullers | The API had nothing in the window | Normal for a quiet market |
| `{ticker}: SKIP` with `unknown ticker (not in the catalog, not found on the API)` | WARNING | Not fetched | Fix the ticker |
| `[i/n] {ticker}: SKIP` with `could not resolve open_ts_ms` | WARNING | No open time from catalog or API | Look the market up with `find_events.py` |
| `[i/n] {ticker}: FAILED` with `{ExceptionType}: {message}` | ERROR | The ticker raised; counted under `failed`; also in the skip file | Rerun; read the exception |
| `... FAILED` with `PartialCandlesError` and `fetch stopped at {iso} after {n} candles` | ERROR | A later window failed; the candles before it were saved | Rerun; it resumes there |
| `{problem} on GET {path} (attempt {i}/{n}); retrying in {d}s` | WARNING | One retry: `HTTP 429: too many requests`, `HTTP 5xx: ...`, a timeout or a connection error | None unless frequent |
| `giving up on GET {path} after {n} attempts: {problem}` | ERROR | `RetriesExhausted` for that request; n is 6 by default | Expect a `FAILED` line next |
| `run aborted: {n} tickers in a row failed after all retries (API down or throttling); {m} tickers not attempted` | ERROR, skip file ticker `*` | The outage breaker, at 3 tickers in a row; summary has `aborted: True` | Wait, rerun |
| `HTTP {status} on GET {path}: endpoint wants authentication, retrying signed` | INFO, status 401 or 403 | An endpoint stopped answering keyless | Provide credentials in `.env` |
| `{ticker}: not in the catalog; API says event ..., series ...` | INFO | An uncataloged ticker was resolved through the API | Consider a roll |
| `historical cutoff unavailable (...); querying both trade tiers` | WARNING | Correct, just slower | |
| `pull_{layer} done: {summary}` | puller log | Last line of a run | Read the summary keys below |
| `=== layer {layer}: {n} tickers in {m} events, about {r} requests ===`, `=== layer {layer} done: {a} pulled, {b} final and skipped, {c} failed, {d} new rows, {e} requests, {t} ===` | driver log | Layer banners | |
| `[{layer}] event {k}/{n} {event}: {t} tickers, {r} new rows, {f} failed`, then `tickers {seen}/{total}`, `requests {used} of about {estimate}`, `{rate} req/s`, `ETA {x}`, separated by vertical bars | driver log | One line per event batch | Progress |
| `API outage at {what}: {n} tickers in a row ran out of retries; waiting {w} min before the next attempt` | driver, ERROR | Outage wait 1, 2, 4, 8, 16 min | Leave it; after the last wait it exits 2 |
| `signal {n}: stopping after the current ticker (again to stop at once)` | driver | Graceful stop requested | |
| `Result: complete` / `some tickers failed; retry with --retry-failed` / `stopped because the API was down; ...` / `interrupted; ...` | driver summary | One per exit code 0, 1, 2, 130 | As the line says |
| `final, skipped` column | estimate and summary | Pairs the journal holds as final; no request was made | |
| `FAILED {layer} {ticker}: {reason}` | driver summary | Still failed after the end-of-layer retry; listed in `backfill_failed_{stamp}_{layer}.txt` | `--retry-failed` |
| `Unknown tickers, left out: ...` | driver summary | In neither the catalog nor the API | Fix the input |
| `poll_focus starting` with `{n} tickers, universe {source}` | poller log | Start (source is `derived`, `--tickers` or `FOCUS_OVERRIDE`); then the rule, one line per event with its close, the schedule, the release windows | |
| `poll_focus refusing to start: {reason}` | ERROR, exit 2 | Nothing in the universe can trade; the reason carries the counts per status, for example settled and closed | Drop stale `--tickers` or `FOCUS_OVERRIDE` |
| `orderbook: {'processed': N, 'skipped': ..., 'failed': ..., 'rows_written': ...}` | poller | One book sweep. Inside a release window a sweep is logged only when it failed, as `orderbook (release window): {...}` at WARNING | |
| `release window open until {iso} ({events}): books every {s} s; candle and trade pulls resume after the window`, `release window closed ({events}); back to the normal cadence` | poller | Fast book polling on and off | |
| `universe roll {series}: {old} -> {new}`, `universe: {n} tickers stopped trading, final sweep then dropped: {tickers}`, `universe: {n} tickers added: {tickers}` | poller | The universe moved to the next event | |
| `universe refresh failed; keeping the last universe`, `universe refresh found nothing to poll; keeping the last universe` | ERROR | The poller keeps polling what it had | Check the API; it retries at the next refresh |
| `history: {n} tickers have no stored history and wait for a background pull: {tickers}`, `history: background pull started for {n} tickers (pid {p}, {r} requests/s); polling their books meanwhile`, `history: background pull finished (exit {code}) for {n} tickers; they rejoin the loop` | poller | The history child | Its own log is `backfill_history_{stamp}.log` |
| `batch orderbook request failed (...); falling back to one request per ticker` | WARNING | A plain error on the batch endpoint | |
| `poll_focus exiting cleanly` / `poll_focus exiting with code {code}` | poller | End of the run | |

Summary keys of a puller: `processed` handled without an exception (new rows, up to date, or nothing returned); `skipped` every ticker not processed (unknown, no open time, failed, not attempted); `failed` the subset that raised; `aborted` True when the outage breaker stopped the run; `unknown` list of tickers nobody knows; `rows_written` new rows after dedupe, a saved prefix included; `elapsed_sec`. Per-ticker statuses in `results=`: `ok`, `up_to_date`, `empty`, `failed`, `skipped`, `unknown`, `not_attempted`.

Exit codes:

| Command | Codes |
|-|-|
| `pull_daily`, `pull_hourly`, `pull_minute`, `pull_trades` | 0 no ticker failed (an unknown ticker is not a failure: read `unknown` in the summary); 1 at least one failed, and also an uncaught error such as a missing ticker file, which ends in a traceback; 2 usage error (argparse) |
| `backfill`, `pull_all_freq` | 0 complete (also `--estimate-only`, and `--retry-failed` with nothing to retry); 1 some tickers failed; 2 the API stayed down through every wait, and also a usage error such as an unknown layer; 75 another run holds the lock; 130 interrupted |
| `pull_audit` | 0 always |
| `poll_focus` | 0 clean stop, `--iterations` reached, `--show-universe`; 1 the universe could not be built; 2 preflight refused, and also a usage error; 3 an explicit universe ran out of tradable tickers |
| `find_events.py` | 0 results; 1 nothing found; 2 usage error, unknown category included; 3 API error |
| `roll.py` | 0 every series refreshed and every check `[ok]`; 1 otherwise; 2 bad arguments |
| `get_tickers.py` | 0; 1 when a series failed |

Skip file `DATA/logs/skip_{kind}_{process stamp}.txt`, kinds `daily`, `hourly`, `minute`, `trades`, `orderbook`: one line per problem, `{YYYY-MM-DDTHH:MM:SSZ}<TAB>{ticker}<TAB>{reason}`, UTC. The same ticker and cause is written once per 6 hours per process. A ticker `*` marks a run-level line. Reasons: `unknown ticker: not in the catalog and not found on the API`; `could not resolve open_ts_ms`; `run aborted: ...`; `{ExceptionType}: {message}`; `orderbook sweep failed: ...`; `orderbook: the API did not return this ticker`.

Quality report, one line per check; each is a count, and several are natural for a thin market:

| Check (`check` in the CSV) | Counts | How to read it |
|-|-|-|
| `schema` | Files whose column set, order or types differ from the fixed schema | Must be 0 on a store written by 0.3.0; a rejected file is left out of every other check |
| `duplicates` | Rows repeated on `(ts_ms, market_ticker)` in a candle file, on `trade_id` in a trade file | Must be 0 |
| `order` | Rows whose `ts_ms` is below the row before | Must be 0 |
| `volume_close` | Bars with a close but volume 0, with volume but no close, without a volume, without a quote | The first two must be 0 |
| `taker_side` | Trades per month by `yes`, `no`, null, other | A month with nulls would mean the exchange dropped its direction fields |
| `volume_exchange` | Finalized markets whose daily volume sum differs from the exchange's lifetime volume by more than 0.005 | A difference means missing or extra daily bars |
| `ladder` | Adjacent threshold strikes with inverted mids, and strictly crossed pairs, per event and ET day | Natural between wide books; crossed pairs deserve a look; events without strike metadata are listed as skipped |
| `mutex` | Mutually exclusive events whose mids sum outside 0.95 to 1.05 on days when every listed market is quoted; incomplete days | A sum below 1 can be right: the flag does not mean exhaustive |
| `stale` | Tradable markets, open for more than a day, without a candle newer than a day, or without any | Natural for a quiet market; it is also what a store looks like when nothing has pulled for a day |
| `missing` | Listed markets without a daily bar, per event and day | Natural: a bar exists only when something happened |
| `history_start` | Tickers whose minute or hourly history starts after their first daily bar | Deserves a look: the finer layer may have been started late (a `--since`, or a poller that stored bars before the history was in). It can also be natural, because a daily candle can exist for a period that has no hourly or minute candle (observed at a quiet close on 2026-09-18) |
| `coverage` | Per series and layer: events, markets, files, rows, first and last day | What the store holds |

The printed report labels six of the checks in words instead of their key: `volume vs close` (`volume_close`), `volume vs exchange` (`volume_exchange`), `mutex sum` (`mutex`), `stale tradable` (`stale`), `missing strikes` (`missing`), `history start` (`history_start`). A skipped check says why: `no candle files`, `no daily candle files`, `no trade files`, `no metadata store (metadata/markets.parquet); a roll or a backfill writes it`, `no parquet files passed the schema pass`. The header line also counts stray temp files and legacy skip files.

## API limits and quirks, as observed

All of this is the exchange's behaviour on the stated date and can change without notice.

- No key is needed for series, events, markets, candlesticks, trades and order books (on 2026-09-18 every request made while checking this file came from a clone without credentials, and none was refused). The specification declares authentication on both order book routes while the exchange's order book guide says none is required; the client asks keyless first and signs only after a 401 or 403.
- Rate limits are documented only for authenticated accounts: token buckets, 10 tokens for a default request, a Basic read budget of 200 tokens per second. Limits for requests without a key are not documented. A 429 has the body `{"error": "too many requests"}`, carries no `Retry-After` header, and the documentation says there is no penalty: back off and retry. The source records a measurement of 2026-09-17: the keyless candlestick endpoints sustained about 4 to 5 requests per second, which is why the default is 5. All processes on one machine share that limit (observed on 2026-09-18: next to a download running at 5 requests per second, a second process held to 1 request per second still drew occasional 429 answers, and every one of them succeeded on the first retry).
- A candlestick request whose window spans more than 5,000 candles is rejected with HTTP 400 `max candlesticks: 5000` on both tiers (observed on 2026-09-18; the specification does not state this cap for the per-market endpoints). `CHUNK_SECONDS` keeps every window below it.
- Candle windows are inclusive on both ends (specification: candlesticks ending on or after `start_ts` and on or before `end_ts`), so a candle on a window boundary arrives twice and is deduped.
- `period_interval` is 1, 60 or 1440 minutes. `end_period_ts` is the inclusive end of the period; the stored `ts_ms` is that value in milliseconds.
- Status filters: `/markets` takes one of `unopened`, `open`, `paused`, `closed`, `settled`; `/events` the same without `paused`; one per request; no filter means every status. `status=all` answers HTTP 400 (observed on 2026-09-18: `invalid status filter` on `/markets`, `bad request` on `/events`) although the exchange's quick start page still mentions it.
- Two tiers: markets settled before `market_settled_ts`, their candles, and trades before `trades_created_ts` are served only by the `/historical/` endpoints; `GET /historical/cutoff` returns the boundaries, which advance. Events and series stay on the live side. With nested markets, an event's historical markets are not included.
- The closing candle (observed on 2026-09-18): the period that contains `close_time` has one last candle and nothing after it. A market that traded into its close had a minute candle ending at the next full minute, an hourly one at the next full hour and a daily one at the next midnight Eastern time (04:00Z in summer, 05:00Z in winter). A market with no activity in its closing period, on the historical tier, had the closing daily candle, without a trade price, but no closing hourly or minute candle: its last hourly candle lay weeks before the close. That is its complete history, not a gap.
- Only completed periods were served (observed on 2026-09-18: for three open markets, asked with a window that ended two minutes in the future, the newest minute candle ended before the answer arrived).
- Market payloads carry `event_ticker` and no `series_ticker`; the series comes from the event. Older events have tickers without the `KX` prefix and are filed under the `KX` series.
- `/markets?tickers=` and `/historical/markets?tickers=` take a comma-separated list; the repo sends 100 per request; the specification states no maximum (observed on 2026-09-18: 150 in one request were answered in full). `GET /markets/orderbooks` takes repeated `tickers` parameters, at most 100.
- Both order book routes answer HTTP 200 with empty books for a settled market, on either tier, and also for a ticker that does not exist (observed on 2026-09-18), whereas the market routes answer 404 for a ticker neither tier knows. An empty book therefore says nothing about whether a ticker is valid.
- Numbers arrive as decimal strings; contract counts are fixed-point with two decimals and can be fractional. The repo casts to float64 and never rescales.
- `taker_side` on trades is deprecated in favour of `taker_outcome_side` and `taker_book_side`; the specification says it will not be removed before 2026-05-14, the changelog says 2026-05-28. On 2026-09-18 both trade tiers still sent all three fields, and they agreed on every trade looked at: `yes` with `bid`, `no` with `ask`. The specification defines the value as the outcome the taker is positioned for: buying YES and selling NO both give `yes`.
- The SDK `kalshi-python-sync` is used for one thing: signing a request after a 401 or 403 (`KalshiAuth.create_auth_headers`). It is imported only then. No SDK response model is in the data path; every call is plain REST parsed into dicts. Version 3.30.0 or newer is required, and it is the reason for the Python 3.13 floor.
- The base URL is `https://api.elections.kalshi.com/trade-api/v2`. The exchange now lists `https://external-api.kalshi.com/trade-api/v2` first; both are in the specification's server list.

## Return shapes

Every data file is zstd Parquet: candles, trades and books are written through `append_parquet`, the metadata store through `upsert_market_metadata`. The types in the tables are named as `kalshi_io.quality.SCHEMAS` names them. Other tools spell them differently (observed on 2026-09-18): `pyarrow.parquet.read_schema` prints `float64` as `double` and `string` as `large_string`; pandas 3 reads the strings as its string dtype and, in the metadata store, the times as nullable `Int64` and the flags as nullable `boolean`. Units: every price is dollars between 0.0 and 1.0 and reads directly as a probability, because a contract pays 1 dollar or nothing; volumes, open interest, counts and quantities are contracts, float64, possibly fractional; every `ts_ms` and `*_ts_ms` is UTC milliseconds. A null (NaN in pandas) always means "the API did not provide it", never zero.

### Candles: `DATA/candles/daily/{series}/{ticker}.parquet`, `DATA/candles/hourly/{series}/{yyyy}/{ticker}.parquet`, `DATA/candles/minute/{series}/{yyyy}/{mm}/{ticker}.parquet`

Partitioned by the UTC year (hourly) or year and month (minute) of `ts_ms`. `{series}` is the catalog spelling (the `KX` name). Dedupe key `ts_ms, market_ticker`; sorted by `ts_ms`. The same 19 columns in this order for all three layers and both tiers:

| # | Column | Type | Meaning |
|-|-|-|-|
| 1 | `ts_ms` | int64 | End of the candle period |
| 2 to 5 | `open`, `high`, `low`, `close` | float64 | Trade prices of the period; null when the period had no trade |
| 6 | `mean` | float64 | Mean trade price as the API reports it; null without a trade |
| 7 | `volume` | float64 | Contracts traded in the period; 0 without a trade |
| 8 | `open_interest` | float64 | Contracts outstanding at the end of the period |
| 9 to 11 | `market_ticker`, `event_ticker`, `series_ticker` | string | Identifiers |
| 12 to 15 | `yes_bid_open`, `yes_bid_high`, `yes_bid_low`, `yes_bid_close` | float64 | Best YES bid over the period; 0.0 means no bid |
| 16 to 19 | `yes_ask_open`, `yes_ask_high`, `yes_ask_low`, `yes_ask_close` | float64 | Best YES ask over the period; 1.0 means no ask |

Candles are sparse: a candle exists only for a period in which something happened, so missing periods are not gaps in the download. Daily candles end at midnight Eastern time.

### Trades: `DATA/trades/{series}/{ticker}/{yyyy-mm}.parquet`

Partitioned by the UTC month of `ts_ms`. Dedupe key `trade_id`; sorted by `ts_ms`. The file has no series column: the series is the directory.

| # | Column | Type | Meaning |
|-|-|-|-|
| 1 | `trade_id` | string | The exchange's trade identifier |
| 2 | `market_ticker` | string | |
| 3 | `ts_ms` | int64 | Execution time |
| 4, 5 | `yes_price`, `no_price` | float64 | Price of the YES and of the NO side of the same trade |
| 6 | `count` | float64 | Contracts filled |
| 7 | `taker_side` | string | `yes` or `no`: the outcome the aggressor positioned for; null if the API sent no direction field |

### Order book snapshots: `DATA/orderbook/{ticker}/{yyyy-mm-dd}.parquet`

Partitioned by the UTC day of the snapshot. Dedupe key `ts_ms, side, price`. One row per price level per side per snapshot; all rows of one sweep share one `ts_ms`.

| # | Column | Type | Meaning |
|-|-|-|-|
| 1 | `ts_ms` | int64 | When the response arrived |
| 2 | `market_ticker` | string | |
| 3 | `side` | string | `YES` or `NO`: both are bid books; the exchange returns no asks |
| 4 | `price` | float64 | Bid price |
| 5 | `quantity` | float64 | Contracts resting at that price |
| 6 | `cumulative_qty` | float64 | Running total from the best price down |
| 7 | `distance_from_top` | int64 | 0 at the best bid of that side |

The best YES ask is 1 minus the best NO bid, and the other way round (the exchange's order book guide). An empty book writes no row.

### Market metadata: `DATA/metadata/markets.parquet`

One row per market, replaced when the market is read again, kept when the API stops returning it. Sorted by series, event, market. 32 columns in this order:

| Columns | Type | Meaning |
|-|-|-|
| `market_ticker`, `event_ticker`, `series_ticker` | string | Identifiers |
| `title`, `yes_sub_title`, `no_sub_title`, `market_type` | string | Display text; `market_type` is `binary` or `scalar` |
| `strike_type` | string | `greater`, `greater_or_equal`, `less`, `less_or_equal`, `between`, `functional`, `custom`, `structured`; null when the API sends none |
| `floor_strike`, `cap_strike` | float64 | The thresholds; null when not applicable |
| `custom_strike` | string | JSON text of the API's object |
| `functional_strike` | string | Formula text |
| `mutually_exclusive` | bool | The event's flag |
| `open_ts_ms`, `close_ts_ms`, `expected_expiration_ts_ms`, `expiration_ts_ms`, `latest_expiration_ts_ms`, `settlement_ts_ms` | int64 | Null when the API sent none |
| `status` | string | API status when the row was read |
| `result` | string | `yes`, `no` or `scalar`; null until determined |
| `settlement_value` | float64 | Dollars paid per YES contract; null until determined; 0.0 is a value |
| `expiration_value` | string | The value the market settled on, as text; null until known |
| `can_close_early`, `early_close_condition` | bool, string | |
| `rules_primary`, `rules_secondary` | string | The rules text |
| `volume`, `open_interest`, `last_price` | float64 | As of `built_at`; for a finalized market `volume` is its lifetime volume |
| `tier` | string | `live`, `historical` or `carried_forward` |
| `built_at` | string | UTC time the row was read, `YYYY-MM-DDTHH:MM:SSZ` |

### Other files

| File | Shape |
|-|-|
| `DATA/state/backfill_journal.jsonl` | JSON lines. `{"v": 1, "type": "final", "ticker", "layer", "at", "rows", "has_files"}` per finished pair of a finalized market (`has_files` false for a market that never traded); `{"v": 1, "type": "run", ...}` per driver run |
| `DATA/logs/backfill_summary_{stamp}.json` | `stamp`, `exit_code`, `elapsed_sec`, `layers`, `requests`, `http_429`, `retries`, `reports` (per layer: `layer`, `done`, `skipped_final`, `failed`, `other`, `not_attempted`, `rows`, `requests`, `elapsed_sec`), `store` (per kind: `files`, `rows`) |
| `DATA/logs/backfill_failed_{stamp}_{layer}.txt` | Two `#` lines, then one ticker per line; a valid `--tickers` file |
| `DATA/logs/audit_{yyyymmdd}.csv` | `ticker, series, daily_rows, first_ts_ms, last_ts_ms, total_volume, recent_volume_30d, hourly_expectation, minute_expectation, status, close_time` |
| `DATA/logs/quality_{yyyymmdd}.csv` | `check, series, event, market, period, count, detail`; `period` holds the layer, the ET day, the month, the status or `skipped`, depending on the check |
| `DATA/logs/focus_universe_live.json`, `CAT/focus_universe.json` | A universe dict (see `kalshi_io.universe`) |
| `DATA/.locks/{00..ff}.lock`, `DATA/.locks/{name}.lock` | 256 striped lock files plus named locks; empty; never deleted |
| `{file}.{pid}.tmp` | A write in progress, or the leftover of a killed writer; never ends in `.parquet` |
| `CAT/{SERIES}_tickers.json` | `schema_version` (2), `series`, `built_at`, `historical_cutoff`, `status_counts`, `events` (`event_ticker`, `title`), `markets` (`event_ticker`, `market_ticker`, `title`, `status`, `open_time`, `close_time`, `expected_expiration_time`, `latest_expiration_time`, `source`), `tickers`. Times are the API's ISO strings; `source` is `live`, `historical` or `carried_forward` |
| `CAT/all_tickers.json` | `schema_version`, `built_at`, `oldest_series_built_at`, `series`, `total_events`, `total_markets`, `status_counts`, `tickers`, `by_series` |
| `CAT/{SERIES}_tickers.txt`, `CAT/all_tickers.txt`, `CAT/focus_universe.txt` | One market ticker per line |

## How to explain results to a person

- A candle: "Over this period the last trade was at `close`, the best bid ended at `yes_bid_close` and the best ask at `yes_ask_close`; `volume` contracts changed hands." A price of 0.27 means the market priced the outcome at about 27 percent at that moment, because a YES contract pays 1 dollar if it happens. Say which of the two you quote: a trade price or a quote.
- A trade: one fill at `yes_price` for `count` contracts at `ts_ms`; `taker_side` says which outcome the aggressor positioned for. Both parties traded at the same price.
- A book snapshot: the resting bids for YES and for NO at one instant. The spread is between the best YES bid and 1 minus the best NO bid. Depth is `quantity` per level; `cumulative_qty` answers "how many contracts to move the price this far".
- A metadata row: what the market was: its threshold, when it opened and closed, and after settlement its `result`, its `settlement_value` and the `expiration_value` it settled on.
- A settled ladder: the strikes of one event with their results. For threshold strikes the results flip from yes to no, or the reverse, around the `expiration_value`; the last prices before the close show what the market believed just before the release.
- What the pipeline guarantees: what it stores is what the API sent, cast to numbers and nothing else; no duplicates on the dedupe key; rows sorted by time; a ticker's candle history contiguous from market open when it was pulled by the driver or a puller without `--since`; files never half-written.
- What it does not guarantee: that a period has a candle (sparse by nature); that the catalog, the metadata store or the focus universe is current (each carries its build time: `built_at`, `derived_at`); that books exist for times when no poller ran (they cannot be backfilled); that `volume` in the metadata of an unsettled market is final; that a market's status in the catalog is still true.
- Phrase caveats plainly. Gaps: "Between a ticker's first and newest stored candle, a period without a candle is a period in which nothing happened; after the newest stored candle the period has not been pulled yet; the `history_start` check flags a layer whose history was started late." Staleness: "This row was read from the exchange at `built_at`." Quotes versus trades: "On a quiet strike most minute candles have quotes and no trade, so `close` is empty there; a quote is an offer, not a transaction; a bid of 0 with an ask of 1 means the book was empty." Thin markets: "An inverted ladder or a sum of mids away from 1 is common between wide books and is not an error in the data."
- Never present a number you did not read from the store, and say which file and which `ts_ms` it came from.

## Pre-flight self check

1. Which data root will this write to? Is `KALSHI_DATA_DIR` set as I intend, and does the run's first log line agree?
2. Am I about to append to a real store? Then only through the pullers, the driver or the poller, never my own writer, and never with `--since` unless a partial history is what is wanted.
3. Is another process pulling right now (`pgrep -fl "pull_historical|pull_live"`)? Then lower my rate (`KALSHI_MAX_RPS=1`) and do not pull the same tickers.
4. Are my tickers market tickers? Events and series need `find_events.py --event ... --format tickers` or a series name.
5. Did I run `--estimate-only` before a large backfill, and is the minute layer what I expect?
6. Is the catalog fresh enough for the question (`pull_audit` prints its age; `roll.py --dry-run` shows what changed)?
7. For the poller: did `--show-universe` show tradable tickers, and do I know whether it will start a background history pull?
8. After a run: exit code, `failed`, `unknown`, `aborted`, the skip file, the failure lists.
9. Before I explain a number: is it a trade price or a quote, which period end does `ts_ms` mark, and how old is the row?
10. Before I state anything about the exchange's API: did I check the specification or make the request, and did I date the observation?

## Not built, and why

- No analysis code: no probabilities, mids, distributions, returns or charts. The repo ends at clean Parquet.
- No repair or rewrite of existing files: the quality checks count and never fix; a store written under older conventions is replaced by a fresh download, not patched.
- No expansion of event tickers in `--tickers`: `find_events.py --event ... --format tickers` writes the file.
- No batch candlestick requests: `GET /markets/candlesticks` exists on the exchange (up to 100 tickers and 10,000 candles per request) and is not called; each ticker and interval is its own request.
- No signed reads: requests are keyless and signed only after a 401 or 403, so the authenticated rate budget is not used.
- No WebSocket: the exchange's quick start says a WebSocket connection requires authentication; books are captured by REST polling.
- No backfill of order books: the API serves only the current book.
- No log rotation: the poller writes one file per process start day.
- No file locking where `flock` is unavailable (Windows, some network file systems): writes proceed unlocked after one warning, so run one writer there.
- No scheduler: nothing starts the poller or a weekly roll for you; use `nohup`, `caffeinate`, cron or a service manager.
- No event-level candlesticks, no fills, orders or positions: only public market data.

## Trust and verify

This guide is evidence based and dated, not exhaustive. It was checked on one machine, without a key, on 2026-09-18, against version 0.3.0. Some statements will age wrong as the exchange or the code changes, and some may be wrong already.

Precedence: the installed source and the live API outrank this file. If what you observe contradicts a line here, trust the observation, do not bend code to match the guide, and tell the repo's owner so the line gets corrected.

How to check a claim yourself:

- A signature or default: `python -c "import inspect, kalshi_io.candles as m; print(inspect.signature(m.fetch_candles))"`. On Python 3.13 an annotation of `pathlib.Path` prints as `pathlib._local.Path`; it is the same class.
- A CLI option: `python -m pull_historical.backfill --help`, `python get_ticker_info/roll.py --help`.
- A column list: `python -c "from kalshi_io.quality import SCHEMAS; print(SCHEMAS['daily'])"`, or `pyarrow.parquet.read_schema(path)` on a stored file.
- A constant: `python -c "from kalshi_io import config; print(config.CHUNK_SECONDS)"`.
- Intended behaviour: the tests under `tests/` encode it and run offline in seconds; `tests/fakes.py` documents how the exchange answered when they were written.
- An API statement: read https://docs.kalshi.com/openapi.yaml , or make one keyless request through `kalshi_io.client.request_json` and look at the answer. Date what you observe.
