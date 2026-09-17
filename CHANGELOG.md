# Changelog

## 0.2.0, 2026-09-17

Two changes on Kalshi's side broke ticker discovery, and the hand-maintained focus universe had fully settled. This release fixes both, makes every data path keyless REST with retries, and adds the tools to move to a new event cycle without editing code.

### Upgrade notes

- Run `pip install -e .` again. `kalshi-python-sync` 3.30.0 or newer is now required.
- `FOCUS_UNIVERSE` is gone from `kalshi_io/config.py`. The universe is derived from `FOCUS_SERIES`. Put tickers in `FOCUS_OVERRIDE` only to pin a universe by hand.
- Candle files gain eight quote columns. Existing files are not rewritten; they gain the columns on their next append, with NaN for the old rows. DuckDB queries that name the new columns across old and new files need `union_by_name = true`.
- `poll_focus` writes one log file per day and the pullers it calls log into that file. Skip files are now `logs/skip_{kind}_{process start}.txt`; the old `skip_daily.txt` and `skip_hourly.txt` are no longer written or read.
- No API key is needed for anything. Credentials are read only if an endpoint answers 401 or 403.

### Fixed

- **Discovery dropped every active market.** `status=all` is now rejected by the API with HTTP 400, and `kalshi-python-sync` 3.27 raised a validation error on every event call after Kalshi removed `available_on_brokers` from event payloads on 2026-09-10. Both failures were swallowed, so a catalog rebuild would have kept settled markets only. Discovery now omits the status filter to get every status, checks filter values before a request, and uses no SDK response model.
- **The focus universe was stale.** All 37 hardcoded tickers had settled, and polling them ran clean and captured nothing. The universe is now derived from the API, and `poll_focus` refuses to start when nothing in it can trade.
- **Uncataloged tickers stored the wrong `event_ticker`.** The fallback used the market ticker as the event ticker. It now asks the API for the market, then for the event's series.
- **Log handlers piled up.** Every `run()` call added another file handler and stream handler, so `poll_focus` duplicated lines and leaked file handles. Logging is configured once, and each run attaches its file for exactly as long as it runs.
- **Non-200 responses ended pagination silently.** A 429 became a gap without a trace. Requests are now retried with exponential backoff and jitter, and a failure that survives the retries is raised, logged at ERROR and recorded in the skip file.
- **The poller refetched the full trade tape every minute.** Trades now resume from the last stored trade with `min_ts`. A quiet cycle costs one request and writes nothing.
- **A space-separated ticker string was one ticker.** Ticker sources are split on whitespace and commas, validated against the catalog and the API, and unknown tickers are reported.
- **Skip files never cleared.** There is one skip file per process, with a timestamp on every line and the same problem recorded once per 6 hours.
- **A candle batch without any trade was stored untyped.** Price columns that held only missing values reached Parquet as null-typed columns. Every numeric candle column is now float64.
- `build_combined` crashed on its second run because it read `all_tickers.json` back as a series file.
- Orderbook snapshots could straddle two daily files. The file is now chosen from the snapshot's own timestamp.
- `pull_trades` and `pull_minute` reported a run over an unparsed nine-ticker string as processed with zero rows.

### Added

- **Bid and ask OHLC on every candle.** `yes_bid_open`, `yes_bid_high`, `yes_bid_low`, `yes_bid_close` and the same four for `yes_ask`. The API sends them with every candle, traded or not, and the pipeline used to drop them. On a quiet strike most minute candles have quotes and no trade.
- `kalshi_io/discovery.py`: list categories, series by category and tags, keyword search across series, events by status, markets by event, series or tickers and status, in both tiers.
- `kalshi_io/catalog.py`: catalog format version 2 with `status`, `open_time`, `close_time`, expiration times and `built_at`; an oracle check that refuses a result in which an open event has no active market; carry-forward of tickers the API stopped returning; atomic writes; a diff and a refresh report.
- `kalshi_io/universe.py`: the derived focus universe, its preflight check, and the refresh that rolls to the next event while polling.
- `get_ticker_info/find_events.py`: search CLI with `table`, `tickers`, `json` and `py` output.
- `get_ticker_info/roll.py`: refresh the catalog, report what changed, propose the next focus universe, and write it where `--tickers focus` reads it.
- `poll_focus` flags `--tickers`, `--series`, `--events-per-series`, `--universe-refresh` and `--show-universe`, and exit codes 1, 2 and 3.
- `pull_audit` reports the catalog's age, the audited tickers by catalog status, and the state of the focus universe. Its CSV gains `status` and `close_time`.
- `KALSHI_DATA_DIR` moves the data root, for scratch runs and tests. `KALSHI_MAX_RPS` caps the request rate.
- Run summaries gain `failed`, `aborted` and `unknown`. Puller CLIs exit with code 1 when a ticker failed. A run stops after 3 tickers in a row ran out of retries.
- 229 new offline tests (238 in total) against a fake exchange that answers like the live API, with guards that keep tests away from the real data directory, the network and the credentials.
- `CHANGELOG.md`.

### Changed

- Every data call is plain REST through `client.request_json`, without a key. The SDK is only the request signer for a 401 or 403 fallback, and it is imported lazily.
- `get_ticker_info/get_tickers.py` is a thin wrapper around `kalshi_io/catalog.py`. A rebuild of all 15 series takes about 125 requests instead of more than 1,000.
- `get_ticker_info/get_Econ_Info.py` lists series through the discovery module.
- Packaging: version 0.2.0, `pandas>=3.0,<4`, `pyarrow>=18`, `requests>=2.32`, `python-dotenv>=1.0`, `duckdb>=1.0`, `kalshi-python-sync>=3.30.0,<4`. `requires-python >=3.13` stays, because the SDK requires it.

### Removed

- `FOCUS_UNIVERSE`.
- The dead `adjustedEndTs` loop in the candle fetcher. Neither per-market endpoint truncates; both reject a window over 5,000 candles.

## 0.1.0, 2026-08-09

First public version: daily, hourly and minute candles, trades, orderbook snapshots, the polling scheduler, and the committed catalog of 15 macro series.
