# Changelog

## 0.3.1, 2026-09-21

The first full daily download (5,027 tickers, 2026-09-19) ended with `Result: complete` and exit code 0 while 12 traded markets had never been requested: their tickers hold a space or a comma, and the pullers split the driver's list a second time. This release fixes the loader, makes the driver fail when a ticker is left without an outcome, fills the strike of 531 old threshold markets for which the exchange sends no strike fields, and corrects what the data-quality report says.

### Upgrade notes

- Run `python -m kalshi_io.metadata --rederive` once. It makes no request, rewrites only `metadata/markets.parquet`, and adds the column `strike_source`. Until the metadata store has been written once by 0.3.1, the schema pass of the data-quality checks rejects the 32-column file and the checks that need it are skipped.
- Run the driver again with the same command. The 12 tickers are not in the journal, so every layer picks them up; everything final is skipped without a request.
- `backfill` exits with code 1 when a ticker was left without an outcome. A caller that read exit code 0 as "complete" was right only by luck before.
- No candle, trade or book file is touched by this release.

### Fixed

- **A list of tickers was split a second time.** `load_tickers` split every element on whitespace and commas, also the elements of a list that the driver or the poller had already resolved, so `GDP-232022 Q4-T0.0` reached the puller as `GDP-232022` and `Q4-T0.0`. A known ticker is now taken whole before anything is split; a file is one ticker per line and is never split further; only a typed argument is split. 12 cataloged tickers were affected (92,196 contracts). One limit remains: an explicit poller universe (`poll_focus --tickers`, `FOCUS_OVERRIDE`) is not registered, so a ticker with a space or a comma has to be cataloged there.
- **The driver reported a run as complete with tickers not tried.** A ticker for which the puller returned no outcome now fails: it is retried at the end of the layer, written to the failure list, and the run exits with code 1. A finished run that still holds tickers not tried exits with code 1 as well and lists them (`NOT TRIED {layer} {ticker}`). `backfill_summary_{stamp}.json` gains `complete`, `unknown`, and per layer `not_attempted_tickers` and `unexpected`.
- **The batch market lookup could not find a ticker with a space or a comma.** `GET /markets?tickers=` and its historical twin return nothing for such a ticker; `lookup_markets` asks the single-market routes for them, live then historical.
- **The coverage table printed the UTC date of a daily bar's end**, one day after the day the bar covers. Daily bars are labelled with the day they cover everywhere in the report.
- **The `mutex sum` count varied between runs over the same files**, by about ten event-days on the 2026-09-19 store. Mids sit on a half-cent grid, so many days sum to exactly 1.05 or 0.95, and the float sum landed on either side of the band with the order in which it was added. The sum is rounded to six decimals before it is compared, so a day on the edge of the inclusive band is inside on every run. Present in 0.3.0.

### Added

- **`strike_source` in the metadata store** (33 columns). The exchange sends no `strike_type`, `floor_strike` or `cap_strike` for 531 finalized threshold markets of 2021 to 2025, although `yes_sub_title` reads `Above 0.4%`. For a market without a `strike_type` whose subtitle has exactly that form, `strike_type` is `greater`, `floor_strike` the number and `strike_source` `subtitle`; everywhere else `strike_source` is `api` or null. A value the API sent is never changed. The ladder check covers 53 more events.
- `python -m kalshi_io.metadata --rederive`.
- 25 new offline tests (365 in total), with a ticker that holds a space and one that holds a comma in every layer. The fake exchange URL-decodes path segments and answers the `tickers=` list form like the live API.

### Changed

- `volume vs exchange` says how many of the flagged markets have no daily file at all. `mutex sum` counts the days above and below the band separately and leaves out, and lists, a flagged event whose markets are threshold strikes. Thresholds and tolerances are unchanged.
- An element of the `tickers` key of a `.json` ticker file is stripped like a line of a `.txt` file.
- Docs: what `--tickers` accepts; which command writes `audit_{date}.csv` and which `quality_{date}.csv`; the catalog of 2026-09-19 (570 events, 5,027 tickers); new observations in `KalshiAPI.md`.
- Packaging: version 0.3.1.

## 0.3.0, 2026-09-17

Version 0.2.0 fixed discovery but left the store with mixed conventions: historical no-trade bars carried the bid in the price columns, files written before the quote columns lacked them, minute history began on an arbitrary date, and a full-catalog minute pull would have scanned empty windows up to today. This release makes the schema uniform, adds a market metadata store, a resumable bulk driver, safe concurrent writers, a release-aware poller and data-quality checks, so that the whole catalog can be downloaded once, cleanly, and kept current.

### Upgrade notes

- **Rebuild the store.** A store written by 0.2.0 or earlier mixes conventions that no code path repairs (existing files are never rewritten). Rename it and download afresh: `mv kalshi_data kalshi_data_old_2026-09-17` (the pattern `kalshi_data_old_*/` is gitignored), then `python -m pull_historical.backfill --estimate-only` and `python -m pull_historical.backfill`. The run resumes with the same command after any stop.
- `KALSHI_MAX_RPS` defaults to 5, the measured keyless limit, instead of 10.
- `pull_minute --since` no longer defaults to 2025-01-01: without it a cold start pulls from market open and a later run resumes. `roll.py` no longer prints a `--since`.
- `pull_all_freq.py` is the driver without arguments: it now prints an estimate first and pulls the metadata layer too.
- Only one writer convention: a parquet file is written to `{name}.parquet.{pid}.tmp` and renamed under a lock. Lock files live in `kalshi_data/.locks` and are never deleted.
- `poll_focus` no longer pulls candles or trades for a ticker whose history is not stored; a child `backfill.py` pulls it while the loop polls the books. During a full download run only a books-only poller (`--no-daily --no-hourly --no-minute --no-trades`) next to the driver.

### Fixed

- **Historical no-trade bars carried the bid in the price columns.** `parse_candle` is one code path for both tiers; `open`, `high`, `low`, `close` and `mean` hold trade prices only and are NaN for a period without a trade, on both tiers. The bid and ask of every period are in the quote columns.
- **Minute history started 60 days back.** `roll.py` printed `--since <today minus 60 days>` from a repo constant and the backfill followed it. Neither API tier limits candle depth (observed back to 2022-11), so minute pulls start at market open.
- **A settled market was pulled up to today.** Every candle pull now stops at `close_time` plus two periods once a market is finalized, which keeps the closing candle and saves about 1.1 million empty minute windows on the full catalog. A cold start takes `open_time` from the catalog instead of one metadata request per ticker.
- **The candle fetcher cost one live 404 per settled market and interval.** It asks the tier the catalog recorded first (3,932 of 4,840 markets are historical) and swaps on 404.
- **Two writers could lose rows.** An append is read, merge, write, rename; two processes doing that to one file lost the rows of whoever renamed first. Appends, the metadata upsert and the driver's journal now run under `flock`.
- **A settled ticker in `pull_minute` walked every empty 3-day window to today** (82 seconds for a 2023 market). See the close cap above.

### Added

- **Market metadata store**, `kalshi_data/metadata/markets.parquet`, one row per market: strike type and strikes, `mutually_exclusive`, the six times as UTC milliseconds, status, result, settlement value, the value the market settled on, the rules, volume, open interest and last price. Filled by every `roll.py` from the payloads it fetches anyway and by the driver's metadata layer.
- **Bulk driver**, `pull_historical/backfill.py`: metadata, daily, hourly, trades and minute layers over the catalog or a ticker list; events that can still trade first, then settled events newest first; an estimate of requests, runtime, rows and disk before the first request (`--estimate-only`); a journal of pairs that are final, so a rerun costs zero requests for them; failure lists and `--retry-failed`; outage waits of 1, 2, 4, 8 and 16 minutes then exit 2; Ctrl+C after the current ticker; a summary with the share of 429 answers and the store's counts; a lock against a second full-catalog run. The pullers gain a per-ticker outcome collector (`results=`) and a stop callback.
- **Release-aware poller.** `GET /markets/orderbooks` takes every book of the universe in one request with one timestamp; the sweep runs first in each cycle. Around a release (5 minutes before to 15 minutes after the `close_time` of any polled or cataloged tradable event, plus `RELEASE_CALENDAR`) only the books are polled, every 5 seconds; candle and trade pulls wait for the window to end. Missing history is pulled by a background child process at 3 requests per second. Flags `--release-interval`, `--release-before`, `--release-after`, `--no-release-windows`, `--no-background-history`.
- **Data-quality checks**, `kalshi_io/quality.py`, run by `pull_audit` (`--no-checks` skips them) and at the end of a driver run (`--no-audit`): a schema pass over every file, duplicate and out-of-order rows, volume against close, `taker_side` by month, daily volume against the exchange's lifetime volume, threshold ladders (inverted mids, strictly crossed quotes), mutually exclusive events, stale tradable markets, listed strikes without a bar, finer layers that start after the first daily bar, coverage per series and layer. Counts only, with `logs/quality_{date}.csv`.
- `trades.py`: `taker_side` falls back to `taker_outcome_side`, then `taker_book_side` (bid is yes, ask is no). The exchange still sends all three; nothing is inferred.
- `resolve.market_window()` and `resolve.candle_end_ts()`; `storage.file_lock()`, `named_lock()` and `LockTimeout`; `orderbook.snapshot_orderbooks()`; `kalshi_io/releases.py`.
- 102 new offline tests (340 in total). A fourth guard fails any test that would start a real background history pull.
- `.gitignore`: `kalshi_data_old_*/`.

### Changed

- `KALSHI_MAX_RPS` default 5. `client.stats` counts 429 answers.
- Every candle file has the same 19 columns, in the same order and with the same types, for daily, hourly and minute candles and both tiers; `union_by_name` is no longer needed on a fresh store.
- Metadata times are int64 UTC milliseconds (`open_ts_ms`, `close_ts_ms`, ...) like every `ts_ms`; the API's `""` result and expiration value are stored as null.
- `roll.py` reports the metadata rows written and suggests the driver as the next step; `roll --out-dir` still writes the metadata store under the data root.
- `pull_audit` writes `quality_{date}.csv` next to `audit_{date}.csv`.
- Packaging: version 0.3.0.

### Removed

- `MINUTE_BACKFILL_DAYS` and the historical tier's bid fallback in the price columns.

## 0.2.0, 2026-09-17

Two changes on Kalshi's side broke ticker discovery, and the hand-maintained focus universe had fully settled. This release fixes both, makes every data path keyless REST with retries, and adds the tools to move to a new event cycle without editing code.

### Upgrade notes

- Run `pip install -e .` again. `kalshi-python-sync` 3.30.0 or newer is now required.
- `FOCUS_UNIVERSE` is gone from `kalshi_io/config.py`. The universe is derived from `FOCUS_SERIES`. Put tickers in `FOCUS_OVERRIDE` only to pin a universe by hand.
- Candle files gain eight quote columns. Existing files are not rewritten; they gain the columns on their next append, with NaN for the old rows. DuckDB queries that name the new columns across old and new files need `union_by_name = true`.
- `poll_focus` writes one log file per process, named after the UTC day it started, and the pullers it calls log into that file. Skip files are now `logs/skip_{kind}_{process start}.txt`; the old `skip_daily.txt` and `skip_hourly.txt` are no longer written or read.
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
- The committed catalog was rebuilt on 2026-09-17 in format version 2: 524 to 563 events and 4,065 to 4,840 markets, 611 of them open. All 29 events that had no market in the old catalog now have theirs, and no cataloged ticker was lost. `focus_universe.json` and `.txt` hold the proposed universe.
- Packaging: version 0.2.0, `pandas>=3.0,<4`, `pyarrow>=18`, `requests>=2.32`, `python-dotenv>=1.0`, `duckdb>=1.0`, `kalshi-python-sync>=3.30.0,<4`. `requires-python >=3.13` stays, because the SDK requires it.

### Removed

- `FOCUS_UNIVERSE`.
- The dead `adjustedEndTs` loop in the candle fetcher. Neither per-market endpoint truncates; both reject a window over 5,000 candles.

## 0.1.0, 2026-08-09

First public version: daily, hourly and minute candles, trades, orderbook snapshots, the polling scheduler, and the committed catalog of 15 macro series.
