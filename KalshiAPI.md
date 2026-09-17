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

- `GET /markets` accepts `unopened`, `open`, `paused`, `closed`, `settled`. `GET /events` accepts `unopened`, `open`, `closed`, `settled`.
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

Kalshi runs two API surfaces. Settled markets, their candles, and their trades age out of the live endpoints onto `/historical/` endpoints; events and series stay on the live side. `GET /historical/cutoff` returns the boundary: `market_settled_ts` for markets and their candles, `trades_created_ts` for trades. The cutoff advances over time, and the documented target is about three months of live data. On 2026-09-17 it stood at 2026-07-19.

- A market settled before the cutoff is missing from `/markets`, from `/markets/{ticker}` (404), and from the nested markets of `/events`. It is on `/historical/markets`.
- The two tiers overlap near the cutoff (observed). This repo unions them by ticker and lets the live record win, because its status is current.
- `/historical/markets` takes `tickers`, `event_ticker` or `series_ticker`, one at a time.
- With `with_nested_markets=true`, an event whose markets are all historical has no `markets` key at all, not an empty list (observed). `GET /events/{ticker}` puts the markets at the top level or inside the event depending on that flag.
- The two surfaces name the same values differently: a live candle carries `price.close_dollars` and `volume_fp`, while the historical version of that candle carries `price.close` and `volume`. This repo tries the live endpoint first and falls back to the historical one on 404, then normalizes both shapes into one schema.

A ticker that worked against the live API last quarter may need the historical endpoints today.

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
- Minute candles are sparse: most minutes of a quiet market are absent. That strike had 233 minute candles in 2,880 minutes.
- Windows are inclusive on both ends, so a candle on a window boundary arrives twice.
- **5,000 candle cap.** A request whose window spans more than 5,000 candles is rejected with HTTP 400 `max candlesticks: 5000`, on both tiers. The documentation describes a cap only for event candlesticks (the response carries `adjusted_end_ts` when a request is too large) and for the batch endpoint (10,000 candlesticks in total). For the per-market endpoints the cap is observed behavior, and the API rejects the request instead of truncating it. This repo sizes windows to stay under it: 3 days of minute candles, 30 days of hourly, 365 days of daily.
- The live URL does not validate its series segment.

## Trades

`GET /markets/trades` and `GET /historical/trades` take `ticker`, `min_ts`, `max_ts` (Unix seconds), `limit` and `cursor`. Fields: `trade_id`, `ticker`, `count_fp`, `yes_price_dollars`, `no_price_dollars`, `taker_side`, `taker_outcome_side`, `taker_book_side`, `created_time`, `is_block_trade`.

- `taker_side` is deprecated in favor of `taker_outcome_side` (same `yes` or `no` value) and `taker_book_side`. The spec says it will not be removed before 2026-05-14. It was still sent on 2026-09-17. This repo reads `taker_outcome_side` when `taker_side` is missing.
- Observed: pages arrive newest first. `min_ts` is inclusive at second granularity. Several trades can share one microsecond. The end of pagination is an empty cursor string.
- This repo resumes with `min_ts` set 60 seconds before the last stored trade and drops the overlap by `trade_id`.

## Orderbook

`GET /markets/{ticker}/orderbook` returns `orderbook_fp` with `yes_dollars` and `no_dollars`, each a list of `[price, count]` string pairs. Both are bid books; there is no separate ask side. This repo stores both books under a `side` column.

The OpenAPI spec declares authentication on this endpoint, the quick start page lists it as public, and it answered without a key on 2026-09-17. A settled market answers 200 with empty books, so an empty snapshot is not an error and a stale ticker list fails silently. That is why `poll_focus` checks its universe before it starts.

## Rate limits and retries

Documented, for authenticated accounts:

- Token buckets. Most requests cost 10 tokens. `GET /account/endpoint_costs` lists the exceptions, and `GET /account/limits` shows your tier.
- Read budgets in tokens per second: Basic 200, Advanced 300, Expert 600, Premier 1,000, Paragon 2,000, Prime 4,000, Prestige 10,000. Basic is therefore 20 default-cost requests per second, and its read bucket holds two seconds of budget.
- A limited request answers 429 with the body `{"error": "too many requests"}`. "429 responses do not currently include Retry-After or X-RateLimit-* headers. There is no penalty or cooldown." The documented advice is exponential backoff.
- Limits for requests without a key are not documented.

Observed on 2026-09-17, without a key: sustained bursts at 10 requests per second drew 429s on both candlestick endpoints, roughly one request in ten on `/historical/` (59 of about 470 in the longest burst). Every one succeeded on the first retry.

What this repo does, in `kalshi_io/client.py`:

| Case | Handling |
|---|---|
| Pacing | Requests start at least 1 / `KALSHI_MAX_RPS` seconds apart (default 10 per second, at most 20) |
| 429, 5xx, timeout, connection error, a 200 that is not JSON | Retried, up to 6 attempts. Backoff `uniform(b/2, b)` with `b = min(30, 0.5 * 2^n)` seconds, at least 1 second after a 429. A `Retry-After` header is honored up to 120 seconds if one ever appears |
| Any other 4xx | Raised at once. A 404 is `KalshiNotFound`, which callers use to switch to `/historical/` |
| 401 or 403 | The same call is repeated once with signed headers. Signing then stays on for the process |
| Retries used up | `RetriesExhausted` is raised after one ERROR log line. Pullers record the ticker in the skip file and stop the run after 3 such tickers in a row |

For a long historical backfill, `KALSHI_MAX_RPS=5` avoids most 429s.

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
| `GET /events/{event_ticker}` | One event with its markets and its series | `kalshi_io/discovery.py` (resolving uncataloged tickers) |
| `GET /markets` | List markets by series, event, tickers and status | `kalshi_io/discovery.py` (catalog, universe, `find_events`) |
| `GET /markets/{ticker}` | One market's metadata | `kalshi_io/discovery.py`, `kalshi_io/resolve.py` |
| `GET /series/{series}/markets/{ticker}/candlesticks` | Live candles | `kalshi_io/candles.py` |
| `GET /markets/trades` | Live trade tape | `kalshi_io/trades.py` |
| `GET /markets/{ticker}/orderbook` | Current orderbook | `kalshi_io/orderbook.py` |
| `GET /historical/cutoff` | Live and historical boundary | `kalshi_io/discovery.py`, `kalshi_io/trades.py` |
| `GET /historical/markets` | List settled markets | `kalshi_io/discovery.py` |
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

The canonical reference is [docs.kalshi.com](https://docs.kalshi.com/). The [OpenAPI specification](https://docs.kalshi.com/openapi.yaml) is the source of truth for fields and enums, and the [changelog](https://docs.kalshi.com/changelog) dates every change.
