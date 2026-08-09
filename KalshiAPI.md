# Kalshi API field guide

How Kalshi structures its markets and API, in the order you need it to pull data. Everything here is verified against the official documentation and against the code in this repo.

## The hierarchy

Kalshi organizes everything in three levels.

- **Series:** a recurring question template. `KXCPIYOY` is the series for year-over-year CPI inflation; Kalshi titles it "Inflation".
- **Event:** one occurrence of that question. `KXCPIYOY-26JUL` is "Inflation in July 2026 (CPI YoY)": the July 2026 running of the series.
- **Market:** one tradable yes-or-no contract inside an event. `KXCPIYOY-26JUL-T3.5` asks whether CPI inflation for the year ending July 2026 comes in above 3.5 percent.

An event usually holds a ladder of markets, one per threshold, so the T3.5 market sits alongside T3.4, T3.6, and the rest.

Ticker names nest. A market ticker begins with its event ticker, and an event ticker begins with its series ticker, so `KXCPIYOY-26JUL-T3.5` parses left to right.

## Prices are probabilities

Each contract settles at 1 dollar if the outcome happens and 0 if it does not. A yes contract trading at 0.04 therefore means the market prices a 4 percent chance. No conversion is needed: the stored `close` column reads directly as a probability.

## Live and historical

Kalshi runs two API surfaces. Settled markets, their candles, and their trades age out of the live endpoints onto `/historical/` endpoints after a few months; events and series stay on the live side. The two surfaces name the same values differently: a live candle carries `price.close_dollars` and `volume_fp`, while the historical version of that candle carries `price.close` and `volume`. This repo tries the live endpoint first and falls back to the historical one automatically, then normalizes both shapes into one schema.

The cutoff advances over time. A ticker that worked against the live API last quarter may need the historical endpoints today.

## Endpoints this repo calls

| Endpoint | Purpose | Called from |
|---|---|---|
| `GET /series` | List every series on Kalshi | `get_ticker_info/get_Econ_Info.py` |
| `GET /events` | List events for a series | `get_ticker_info/get_tickers.py` (SDK) |
| `GET /events/{event_ticker}` | One event with its markets | `kalshi_io/resolve.py`, `get_ticker_info/get_tickers.py` |
| `GET /markets` | List markets by series or event | `kalshi_io/resolve.py`, `get_ticker_info/get_tickers.py` |
| `GET /markets/{ticker}` | One market's metadata | `kalshi_io/resolve.py` (SDK) |
| `GET /series/{series}/markets/{ticker}/candlesticks` | Live candles | `kalshi_io/candles.py` (SDK) |
| `GET /markets/trades` | Live trade tape | `kalshi_io/trades.py` |
| `GET /markets/{ticker}/orderbook` | Current orderbook | `kalshi_io/orderbook.py` (SDK) |
| `GET /historical/markets` | List settled markets | `kalshi_io/resolve.py`, `get_ticker_info/get_tickers.py` |
| `GET /historical/markets/{ticker}` | One settled market's metadata | `kalshi_io/resolve.py` |
| `GET /historical/markets/{ticker}/candlesticks` | Candles for settled markets | `kalshi_io/candles.py` |
| `GET /historical/trades` | Trade tape for settled markets | `kalshi_io/trades.py` |

Rows marked (SDK) go through the `kalshi-python-sync` client; the rest are plain REST calls. The base URL is `https://api.elections.kalshi.com/trade-api/v2`.

## Wire format

- **Numbers are strings.** Prices, volumes, and counts arrive as decimal strings like `"0.6900"` and `"5247.00"`. This repo casts them all to float64 during normalization.
- **Fractional contracts are real.** Contract counts are fixed-point values with two decimals; a volume of `11747.08` is a genuine fractional fill, not an error.
- **Two timestamp conventions.** Candle requests take `start_ts` and `end_ts` in Unix seconds, and trades arrive with an ISO 8601 `created_time`. Stored data uses one convention everywhere: `ts_ms`, int64 UTC milliseconds.
- **The orderbook is two bid books.** The endpoint returns YES bids and NO bids with no separate ask side. This repo stores both books under a `side` column.

## Quirks

- **KX prefix migration.** Older tickers lack the KX prefix; newer ones have it. The `KXCPIYOY` series contains both `CPIYOY-22DEC` and `KXCPIYOY-26JUN` events. Query both spellings when you look up a series by name.
- **5,000 candle cap.** A candle request whose window spans more than 5,000 candles is rejected with `max candlesticks: 5000`. Size request windows to stay under it: for minute candles that means about 3 days per request.
- **Old markets 404.** A market that settled long ago returns 404 from the live endpoints even though it exists; retry the `/historical/` equivalent before concluding a ticker is invalid.
- **Broken titles on some old markets.** A few 2024 and 2025 markets in the `KXACPI` series carry unfilled template titles containing the literal text `above_below_between`. Treat titles as display metadata, not as data.

## Official documentation

The canonical reference is [docs.kalshi.com](https://docs.kalshi.com/).
