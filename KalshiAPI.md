# Kalshi API guide

How Kalshi structures its markets and API, in the order you need it to pull data. Everything here is verified against the official documentation and against the code in this repo. Facts that the documentation does not state are marked as observed, with the date they were seen.

## The hierarchy

Kalshi organizes everything in three levels.

- **Series:** a recurring question template. `KXCPIYOY` is the series for year-over-year CPI inflation; Kalshi titles it "Inflation".
- **Event:** one occurrence of that question. `KXCPIYOY-26JUL` is "Inflation in July 2026 (CPI YoY)": the July 2026 running of the series.
- **Market:** one tradable yes-or-no contract inside an event. `KXCPIYOY-26JUL-T3.5` asks whether CPI inflation for the year ending July 2026 comes in above 3.5 percent.

An event usually holds a ladder of markets, one per threshold, so the T3.5 market sits alongside T3.4, T3.6, and the rest.

Ticker names nest. A market ticker begins with its event ticker, and an event ticker begins with its series ticker, so `KXCPIYOY-26JUL-T3.5` parses left to right. The payloads do not nest the same way: a market payload carries `event_ticker` and no `series_ticker`, and the event payload carries `series_ticker`. Resolving a market to its series takes two lookups. This repo never derives an event from the ticker text.

## Prices are probabilities

Each contract settles at 1 dollar if the outcome happens and 0 if it does not. A yes contract trading at 0.04 therefore means the market prices a 4 percent chance. No conversion is needed: the stored `close` column reads directly as a probability.

## Status values

Requests and responses use two different vocabularies.

| Filter word in a request | Response `status` values it selects |
|---|---|
| `unopened` | `initialized` |
| `open` | `active` |
| `paused` (markets only) | `inactive` |
| `closed` | `closed`, `determined`, `disputed`, `amended` |
| `settled` | `finalized` |

- `GET /markets` accepts `unopened`, `open`, `paused`, `closed`, `settled`. `GET /events` accepts `unopened`, `open`, `closed`, `settled`. (The prose description of `GET /markets` in the OpenAPI spec lists four status filters; the parameter's enum has five, with `paused`.)
- One status per request. Leave the parameter out to get every status.
- An event has no status of its own. The `/events` filter matches an event when any of its markets matches.
- `all` is not a valid value. Both endpoints answer it with HTTP 400, `/markets` with the detail `invalid status filter`. The quick start page still mentions `status=all`; the OpenAPI spec and the live API reject it (observed 2026-09-17). Sending several values answers 400 `only one status filter may be supplied`.
- On `/markets`, timestamp filters exclude each other and some status filters. This repo sends none of them.

## Pagination

List endpoints are cursor based. The response carries a `cursor`; send it back as `cursor` to get the next page. An empty cursor means there are no more pages. Cursors are opaque and only valid within one listing, so this repo never stores one.

| Endpoint | Default `limit` | Maximum |
|---|---|---|
| `/markets`, `/historical/markets` | 100 | 1000 |
| `/markets/trades`, `/historical/trades` | 100 | 1000 |
| `/events` | 200 | 200 |
| `/series` | no pagination, one response | |

`/series` takes `category` (exact and case sensitive; a lower-case value answers 200 with nothing), `tags` (comma separated) and no keyword parameter. Keyword search in this repo is done on the client.

## Live and historical

Kalshi runs two API surfaces. Settled markets, their candles, and their trades age out of the live endpoints onto `/historical/` endpoints; events and series stay on the live side. `GET /historical/cutoff` returns the boundary: `market_settled_ts` for markets and their candles, `trades_created_ts` for trades. The cutoff advances over time. The historical-data page used to say the target window for live data is about three months; on 2026-09-17 it says that each data type has its own cutoff and that the windows differ, without a number. On 2026-09-17 the cutoff stood at 2026-07-19 for both.

- A market settled before the cutoff is missing from `/markets`, from `/markets/{ticker}` (404), and from the nested markets of `/events`. It is on `/historical/markets`.
- The two tiers overlap near the cutoff (observed). This repo unions them by ticker and lets the live record win, because its status is current.
- `/historical/markets` takes `tickers`, `event_ticker` or `series_ticker`, one at a time. Both `/markets?tickers=` and `/historical/markets?tickers=` take a comma separated list; this repo sends 100 tickers per request, which is how the metadata store is refreshed. The specification states no maximum, and 150 tickers in one request were answered in full on 2026-09-18.
- With `with_nested_markets=true`, an event whose markets are all historical has no `markets` key at all, not an empty list (observed). `GET /events/{ticker}` puts the markets at the top level or inside the event depending on that flag.
- The two surfaces name the same values differently: a live candle carries `price.close_dollars` and `volume_fp`, while the historical version of that candle carries `price.close` and `volume`. The market payloads have the same schema on both tiers. This repo asks the tier the catalog recorded for a market first, falls back to the other on 404, and normalizes both candle shapes into one schema.
- The OpenAPI description of `trades_created_ts` says trades before it must be read from `GET /historical/fills`, an authenticated endpoint for the caller's own fills; the historical-data guide, and the live API, serve the public tape before the cutoff on `GET /historical/trades`.

A ticker that worked against the live API last quarter may need the historical endpoints today.

## Market metadata

`GET /markets/{ticker}`, `GET /markets?tickers=` and their `/historical/` twins return the same `Market` schema. The fields this repo keeps in `kalshi_data/metadata/markets.parquet`:

| Field | Meaning |
|---|---|
| `strike_type` | `greater`, `greater_or_equal`, `less`, `less_or_equal`, `between`, `functional`, `custom`, `structured`. Absent on single-outcome markets and on some 2022 markets (`FED-22DEC` has no strike fields at all) |
| `floor_strike`, `cap_strike` | The threshold(s) as numbers. A `greater` market asks whether the value exceeds `floor_strike`, a `less` market whether it stays below `cap_strike`, a `between` market both |
| `custom_strike` | An object for markets such as a Fed decision, for example `{"Cut": "25"}` |
| `functional_strike` | Formula text |
| `result` | `yes`, `no` or `scalar` once determined; `""` until then |
| `settlement_value_dollars`, `settlement_ts` | Present once the market is determined |
| `expiration_value` | The underlying print the market settled on, as text: `"3.4"` for July 2026 CPI; `""` until known |
| `open_time`, `close_time`, `expected_expiration_time`, `latest_expiration_time` | ISO times. `expiration_time` is deprecated and may disappear |
| `rules_primary`, `rules_secondary`, `can_close_early`, `early_close_condition` | The rules |
| `volume_fp`, `open_interest_fp`, `last_price_dollars` | As of the request |

`mutually_exclusive` is a field of the event, not of the market: at most one market of the event resolves yes. It does not say the outcomes are exhaustive. `GET /events?series_ticker=` lists it for every event of a series in one request.

Observed on 2026-09-17: `close_time` is the release anchor. The BLS series close at 12:29Z for a 12:30Z print, the Fed series at 17:55Z or 17:59Z for the 18:00Z statement. `GET /milestones` has milestones for these releases, but their `start_date` is 30 to 60 minutes before the release (12:00Z for a 12:30Z print, 17:00Z for 18:00Z), and `Market.occurrence_datetime` equals the expected expiration, not the release. `close_time` is updated by the exchange on an early close; `finalized` is terminal; a `closed` market can be reopened with a later close.

## Candlesticks

`GET /series/{series_ticker}/markets/{ticker}/candlesticks` and `GET /historical/markets/{ticker}/candlesticks` take `start_ts` and `end_ts` in Unix seconds and `period_interval` of 1, 60 or 1440 minutes.

| Field | Live name | Historical name | Notes |
|---|---|---|---|
| Period end | `end_period_ts` | `end_period_ts` | Inclusive end of the period, Unix seconds |
| Trade prices | `price.open_dollars`, `low_dollars`, `high_dollars`, `close_dollars`, `mean_dollars` | `price.open`, `low`, `high`, `close`, `mean` | Null when the period had no trade |
| Best YES bid | `yes_bid.open_dollars`, `low_dollars`, `high_dollars`, `close_dollars` | `yes_bid.open`, `low`, `high`, `close` | Required, present on every candle |
| Best YES ask | `yes_ask.*_dollars` | `yes_ask.*` | Required, present on every candle |
| Volume | `volume_fp` | `volume` | Fixed-point contracts, two decimals |
| Open interest | `open_interest_fp` | `open_interest` | Fixed-point contracts, two decimals |

Observed on 2026-09-17:

- A period without trades: the live endpoint omits the trade price keys, the historical endpoint sends them as null. Both send the quotes. On one live CPI strike, 219 of 233 minute candles over two days had quotes and no trade.
- An empty side of the book is quoted as `0.0000` (no bid) or `1.0000` (no ask).
- Candles are sparse at every interval: a candle exists only for a period in which something happened, a trade or a quote change. That strike had 233 minute candles in 2,880 minutes. Across the catalog a market has roughly 0.3 to 1 daily, 1 to 13 hourly and 1 to 85 minute candles per day of its life. Only completed periods are served.
- **No depth limit for minute candles.** Neither the specification nor any guide page limits how far back a `period_interval` reaches or how long candles are kept, on either tier. Observed: `/historical/` served minute candles from 2022-11 for `RECSSNBER-23`, the live tier served them from market open (2025-07) for `KXRECSSNBER-26`, and for a market that both tiers hold (`KXCPIYOY-26JUN-T3.5`) the two returned the same 543 minute candles for the same window. Minute pulls in this repo therefore start at market open. (Version 0.2.0 suggested a 60 day window; that was a default of this repo, not a limit of the API.)
- Windows are inclusive on both ends, so a candle on a window boundary arrives twice. The specification says as much: a request returns the candlesticks ending on or after `start_ts` and on or before `end_ts`.
- **The closing candle.** The period that contains `close_time` has one last candle and nothing after it: for a 12:29:00Z close the last minute candle ends 12:30:00Z, the last hourly candle 13:00Z, the last daily candle at the next midnight Eastern time. A window that ends exactly at `close_time` loses it. This repo pulls a finalized market to `close_time` plus two periods (the ET day on which daylight saving ends is 25 hours long, so one daily period is not enough).
- **A quiet close has no closing hourly or minute candle.** On the historical tier a market with no activity in its closing period gets a closing daily candle but no closing hourly or minute candle; a market that traded into its close has all three. Observed on 2026-09-18: `RECSSNBER-23` (historical tier, closed 2024-01-25 13:25Z) has a closing daily candle that ends 2024-01-26 05:00Z and carries no trade price, its last hourly candle ends 2023-12-21 19:00Z, and the three days before its close hold no minute candle. `KXCPIYOY-26JUL-T3.5` (closed 2026-08-12 12:29Z, settled after the cutoff and therefore still on the live tier) has a minute candle ending 12:30Z, an hourly one ending 13:00Z and a daily one ending 2026-08-13 04:00Z; the hourly and daily ones carry a trade price, the closing minute candle carries quotes only. The last hourly or minute candle of a settled market can therefore lie weeks before its close, and that is its complete history, not a gap.
- **Daily candles end at midnight Eastern time**, 04:00Z in summer and 05:00Z in winter, not at 00:00Z.
- **Batch candlesticks.** `GET /markets/candlesticks` returns candles for several markets in one request (10,000 candles in total). Its spec declares `period_interval` as `minimum: 1` without the 1, 60, 1440 enum the per-market endpoints have. This repo does not use it yet; it would turn the poller's minute sweep of 64 requests into one.
- **5,000 candle cap.** A request whose window spans more than 5,000 candles is rejected with HTTP 400 `max candlesticks: 5000`, on both tiers. The documentation describes a cap only for event candlesticks (the response carries `adjusted_end_ts` when a request is too large) and for the batch endpoint (10,000 candlesticks in total). For the per-market endpoints the cap is observed behavior, and the API rejects the request instead of truncating it. This repo sizes windows to stay under it: 3 days of minute candles, 30 days of hourly, 365 days of daily.
- The live URL does not validate its series segment.

## Trades

`GET /markets/trades` and `GET /historical/trades` take `ticker`, `min_ts`, `max_ts` (Unix seconds), `limit` and `cursor`. Fields: `trade_id`, `ticker`, `count_fp`, `yes_price_dollars`, `no_price_dollars`, `taker_side`, `taker_outcome_side`, `taker_book_side`, `created_time`, `is_block_trade`.

- `taker_side` is deprecated in favor of `taker_outcome_side` (same `yes` or `no` value) and `taker_book_side` (`bid` is yes, `ask` is no). In the spec `taker_side` is no longer a required field, while the other two are. The removal date is stated twice and differently: not before 2026-05-14 in the spec, not before 2026-05-28 in the changelog and the order direction guide. Both dates have passed and the field is still there: on 2026-09-17 both trade tiers sent all three fields, and all 80,110 trades stored by this repo between 2022-07 and 2026-09 carry a `taker_side` (44,953 yes, 35,157 no, none missing).
- The stored `taker_side` column is the first of the three fields the API sent, so it stays an exchange field. Nothing is inferred from prices. `pull_audit` counts missing, yes and no per month, so a removal would show up there.
- A taker who buys YES, or sells NO, is `yes` and `bid`. The direction does not change the price: both sides of a trade see the same `yes_price`.
- Observed: pages arrive newest first. `min_ts` is inclusive at second granularity. Several trades can share one microsecond. The end of pagination is an empty cursor string.
- This repo resumes with `min_ts` set 60 seconds before the last stored trade and drops the overlap by `trade_id`.

## Orderbook

`GET /markets/{ticker}/orderbook` returns `orderbook_fp` with `yes_dollars` and `no_dollars`, each a list of `[price, count]` string pairs. Both are bid books; there is no separate ask side. This repo stores both books under a `side` column.

`GET /markets/orderbooks` returns the books of several markets in one request, up to 100 per call. The tickers must be repeated parameters (`?tickers=A&tickers=B`); a comma-joined value is read as one ticker name and answers with nothing (observed 2026-09-17). All 64 books of the focus universe come back in one 0.12 second response, and the response is throttled as one request (12 calls at one per second drew no 429), although the rate-limits page says batch endpoints cost per item. `poll_focus` takes every sweep this way, so every book of a sweep carries one timestamp, and falls back to per-ticker requests on an error.

The OpenAPI spec declares authentication on both orderbook routes, the orderbook guide and the quick start page list them as public, and both answered without a key on 2026-09-17. A settled market answers 200 with empty books, so an empty snapshot is not an error and a stale ticker list fails silently. Observed on 2026-09-18: a ticker that does not exist is answered the same way on both routes (200 with empty books; the batch route returns an entry for it), so an empty book does not show that a ticker is valid. That is why `poll_focus` checks its universe before it starts. There is no keyless WebSocket, so REST polling is the only way to a book history.

## Rate limits and retries

Documented, for authenticated accounts:

- Token buckets. Most requests cost 10 tokens. `GET /account/endpoint_costs` lists the exceptions, and `GET /account/limits` shows your tier.
- Read budgets in tokens per second: Basic 200, Advanced 300, Expert 600, Premier 1,000, Paragon 2,000, Prime 4,000, Prestige 10,000. Basic is therefore 20 default-cost requests per second, and its read bucket holds two seconds of budget.
- A limited request answers 429 with the body `{"error": "too many requests"}`. "429 responses do not currently include Retry-After or X-RateLimit-* headers. There is no penalty or cooldown." The documented advice is exponential backoff.
- Limits for requests without a key are not documented.
- `GET /account/endpoint_costs` (public) lists no market-data endpoint, so every read this repo makes costs the default 10 tokens.

Observed on 2026-09-17, without a key: the candlestick endpoints sustain about 4 to 5 requests per second. One 429 in 1,344 requests at 4 per second, about 5 percent at 6, 8 to 12 percent at 10 (59 of about 470 in the longest burst on `/historical/`). Every one succeeded on the first retry. The keyless limit is therefore lower than the Basic tier's 20. A batch orderbook request for 64 tickers counts as one request.

What this repo does, in `kalshi_io/client.py`:

| Case | Handling |
|---|---|
| Pacing | Requests start at least 1 / `KALSHI_MAX_RPS` seconds apart (default 5 per second, at most 20) |
| 429, 5xx, timeout, connection error, a 200 that is not JSON | Retried, up to 6 attempts. Backoff `uniform(b/2, b)` with `b = min(30, 0.5 * 2^n)` seconds, at least 1 second after a 429. A `Retry-After` header is honored up to 120 seconds if one ever appears |
| Any other 4xx | Raised at once. A 404 is `KalshiNotFound`, which callers use to switch to `/historical/` |
| 401 or 403 | The same call is repeated once with signed headers. Signing then stays on for the process |
| Retries used up | `RetriesExhausted` is raised after one ERROR log line. Pullers record the ticker in the skip file and stop the run after 3 such tickers in a row |

The default of 5 is the measured keyless limit, so a full-catalog download draws almost no 429s. Two processes share one keyless limit: the poller's background history pull runs at 3, and a backfill next to a full poller should too. Signed requests would raise the budget to the documented 20 per second; this repo does not sign reads today.

## The SDK

`kalshi-python-sync` 3.27.0 declared `available_on_brokers` as a required field of its event model. Kalshi deprecated that field on 2026-08-27 and removed it from event payloads on 2026-09-10. From then on every `get_event` and `get_events` call raised a pydantic validation error. The old discovery code caught the error and fell through to settled markets only, so a catalog rebuild would have dropped every active market without a visible failure.

The fix has two parts. `pyproject.toml` requires `kalshi-python-sync>=3.30.0`, whose event model has no such field (3.30.0, released 2026-09-15, requires Python 3.13 or newer). And no SDK response model sits in the data path anymore: every call is plain REST that parses JSON into dicts. The documentation says that SDKs "may lag the API" and names the OpenAPI specification as the source of truth. The SDK remains only as the request signer, `KalshiAuth.create_auth_headers(method, url)`, which signs timestamp, method and path, without host and query.

## Endpoints this repo calls

All are plain REST and answer without a key today. The base URL is `https://api.elections.kalshi.com/trade-api/v2`. Kalshi announced dedicated hosts on 2026-05-07 and now recommends `https://external-api.kalshi.com/trade-api/v2`; the shared host remains supported.

| Endpoint | Purpose | Called from |
|---|---|---|
| `GET /series` | List series, by category and tags | `kalshi_io/discovery.py` |
| `GET /search/tags_by_categories` | Categories with their tags | `kalshi_io/discovery.py` |
| `GET /events` | List events of a series, by status | `kalshi_io/discovery.py` (catalog, universe, `find_events`) |
| `GET /events/{event_ticker}` | One event with its markets, its series and its `mutually_exclusive` flag | `kalshi_io/discovery.py` (resolving uncataloged tickers, events the catalog sees only on markets, the event flag of the metadata store) |
| `GET /markets` | List markets by series, event, tickers and status; 100 tickers per request for the metadata store | `kalshi_io/discovery.py` (catalog, universe, `find_events`, `kalshi_io/metadata.py`) |
| `GET /markets/{ticker}` | One market's metadata | `kalshi_io/discovery.py`, `kalshi_io/resolve.py` |
| `GET /series/{series}/markets/{ticker}/candlesticks` | Live candles | `kalshi_io/candles.py` |
| `GET /markets/trades` | Live trade tape | `kalshi_io/trades.py` |
| `GET /markets/{ticker}/orderbook` | One orderbook (the poller's fallback) | `kalshi_io/orderbook.py` |
| `GET /markets/orderbooks` | The orderbooks of up to 100 markets in one request | `kalshi_io/orderbook.py` (`poll_focus`) |
| `GET /historical/cutoff` | Live and historical boundary | `kalshi_io/discovery.py`, `kalshi_io/trades.py` |
| `GET /historical/markets` | List settled markets; 100 tickers per request for the metadata store | `kalshi_io/discovery.py`, `kalshi_io/metadata.py` |
| `GET /historical/markets/{ticker}` | One settled market's metadata | `kalshi_io/discovery.py` |
| `GET /historical/markets/{ticker}/candlesticks` | Candles for settled markets | `kalshi_io/candles.py` |
| `GET /historical/trades` | Trade tape before the cutoff | `kalshi_io/trades.py` |

## Wire format

- **Numbers are strings.** Prices, volumes, and counts arrive as decimal strings like `"0.6900"` and `"5247.00"`. This repo casts them all to float64 during normalization.
- **Fractional contracts are real.** Contract counts are fixed-point values with two decimals; a volume of `11747.08` is a genuine fractional fill, not an error. The minimum granularity is 0.01 contracts.
- **Two timestamp conventions.** Candle requests take `start_ts` and `end_ts` in Unix seconds, and trades arrive with an ISO 8601 `created_time`. Stored data uses one convention everywhere: `ts_ms`, int64 UTC milliseconds.
- **Two error shapes.** Most errors are `{"error": {"code", "message", "details"}}`. The 429 body is `{"error": "too many requests"}`, a plain string.

## Nuances

- **KX prefix migration.** Older tickers lack the KX prefix; newer ones have it. The `KXCPIYOY` series contains both `CPIYOY-22DEC` and `KXCPIYOY-26JUN` events. The API files the pre-KX events under the KX series, and a query by the pre-KX spelling returns nothing on list endpoints (observed 2026-09-17).
- **Delisted duplicates.** `/events?series_ticker=KXGDP` omits `KXGDP-27JAN30` while its markets still show on `/markets` (observed). The catalog adds events it sees only on markets.
- **Old markets 404.** A market that settled long ago returns 404 from the live endpoints even though it exists; retry the `/historical/` equivalent before concluding a ticker is invalid. An unknown ticker answers 404 on both.
- **Broken titles on some old markets.** A few 2024 and 2025 markets in the `KXACPI` series carry unfilled template titles containing the literal text `above_below_between`. Treat titles as display metadata, not as data. The market fields `title` and `subtitle` are deprecated.

## Official documentation

The canonical reference is [docs.kalshi.com](https://docs.kalshi.com/). The [OpenAPI specification](https://docs.kalshi.com/openapi.yaml) is the source of truth for fields and enums, and the [changelog](https://docs.kalshi.com/changelog) dates every change. The changelog is not in date order and, read on 2026-09-17, already carried an entry dated 2026-09-24; it has no entry for `status=all` being rejected. Where the pages disagree with each other or with the live API, this guide says so and marks what was observed and when.
