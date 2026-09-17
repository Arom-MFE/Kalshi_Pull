"""
kalshi_io/candles.py — Candle fetching and normalization, plus the
market_ticker → (series_ticker, event_ticker) lookup every puller uses.
"""

import json
from datetime import datetime, timezone

import pandas as pd
import requests

from kalshi_io import client, discovery
from kalshi_io.client import KalshiAPIError, KalshiNotFound, path_part
from kalshi_io.config import CHUNK_SECONDS, SERIES_LIST, TICKERS_DIR
from kalshi_io.runlog import get_logger

logger = get_logger("candles")

# ============================================================
# Reverse lookup: market_ticker → (series_ticker, event_ticker)
# Catalog first (loaded lazily, once), then tickers registered by the focus
# universe, then the API. Never guessed from the ticker text.
# ============================================================
_ticker_meta: dict[str, tuple[str, str]] = {}
_unknown_tickers: set[str] = set()
_catalog_loaded = False


class UnknownTickerError(LookupError):
    """The ticker is not in the catalog and the API does not know it either."""


def _ensure_ticker_meta() -> None:
    """Load market→(series, event) mapping from the per-series catalog files once."""
    global _catalog_loaded
    if _catalog_loaded:
        return
    for series_json in sorted(TICKERS_DIR.glob("*_tickers.json")):
        data = json.loads(series_json.read_text())
        series = data.get("series")
        if not isinstance(series, str):
            continue    # all_tickers.json: "series" is a list there
        for m in data.get("markets", []):
            _ticker_meta[m["market_ticker"]] = (series, m["event_ticker"])
    _catalog_loaded = True


def canonical_series(series_ticker: str) -> str:
    """Spell a series the way the catalog and the data directories do: the KX
    name for a configured series ("CPIYOY" → "KXCPIYOY"), unchanged otherwise."""
    if series_ticker not in SERIES_LIST and "KX" + series_ticker in SERIES_LIST:
        return "KX" + series_ticker
    return series_ticker


def _derive_series(market_ticker: str) -> str:
    """Offline fallback: series from the market_ticker prefix."""
    for s in SERIES_LIST:
        if market_ticker.startswith(s + "-") or market_ticker == s:
            return s
    return canonical_series(market_ticker.split("-")[0])


def is_cataloged(market_ticker: str) -> bool:
    """True if the ticker's event and series are known without asking the API
    (committed catalog, or registered by the focus universe)."""
    _ensure_ticker_meta()
    return market_ticker in _ticker_meta


def register_ticker_meta(mapping: dict[str, tuple[str, str]]) -> None:
    """
    Pre-register {market_ticker: (series_ticker, event_ticker)} for tickers
    whose event and series are already known (the derived focus universe), so
    resolving them costs no API call. Catalog entries are never overwritten.
    """
    _ensure_ticker_meta()
    for ticker, (series, event) in mapping.items():
        _ticker_meta.setdefault(ticker, (canonical_series(series), event))
        _unknown_tickers.discard(ticker)


def resolve_ticker_meta(market_ticker: str, allow_api: bool = True) -> tuple[str, str | None]:
    """
    Return (series_ticker, event_ticker) for a market_ticker.

    Order: committed catalog, registered tickers, then the API (market →
    event → series; market payloads carry no series_ticker). API answers,
    including "unknown", are cached for the life of the process.

    Args:
        market_ticker: e.g. "KXCPIYOY-26SEP-T3.0"
        allow_api:     False keeps the call offline: an uncataloged ticker
                       then gets a series derived from its prefix and
                       event_ticker None (enough to locate its files).

    Raises:
        UnknownTickerError: allow_api is True and the ticker exists in
        neither tier of the API.
    """
    _ensure_ticker_meta()
    if market_ticker in _ticker_meta:
        return _ticker_meta[market_ticker]
    if not allow_api:
        return (_derive_series(market_ticker), None)
    if market_ticker in _unknown_tickers:
        raise UnknownTickerError(f"{market_ticker}: not in the catalog and not found on the API")

    meta = discovery.resolve_market_meta(market_ticker)
    if meta is None:
        _unknown_tickers.add(market_ticker)
        raise UnknownTickerError(f"{market_ticker}: not in the catalog and not found on the API")

    api_series, event_ticker = meta
    if api_series:
        series = canonical_series(api_series)
    else:
        series = _derive_series(market_ticker)
        logger.warning(f"{market_ticker}: event {event_ticker} not found; series {series} derived from the ticker prefix")
    _ticker_meta[market_ticker] = (series, event_ticker)
    logger.info(f"{market_ticker}: not in the catalog; API says event {event_ticker}, series {series}")
    return _ticker_meta[market_ticker]


def _reset_state() -> None:
    """Forget the loaded catalog and API answers (test isolation)."""
    global _catalog_loaded
    _ticker_meta.clear()
    _unknown_tickers.clear()
    _catalog_loaded = False


# ============================================================
# parse_candle
# ============================================================

_OHLC = ("open", "high", "low", "close")

# Best YES bid and YES ask over the period, from the API's yes_bid / yes_ask
# objects. Unlike trade prices they exist for every candle, traded or not.
QUOTE_COLUMNS: tuple[str, ...] = tuple(f"{side}_{k}" for side in ("yes_bid", "yes_ask") for k in _OHLC)

# Stored column order, the same for daily, hourly and minute files and for both
# API tiers: timestamp, trade prices, volume and open interest, the three
# identifiers, then the quote columns.
CANDLE_COLUMNS: tuple[str, ...] = (
    "ts_ms", "open", "high", "low", "close", "mean", "volume", "open_interest",
    "market_ticker", "event_ticker", "series_ticker",
    *QUOTE_COLUMNS,
)
CANDLE_FLOAT_COLUMNS: tuple[str, ...] = (
    "open", "high", "low", "close", "mean", "volume", "open_interest", *QUOTE_COLUMNS,
)


def _to_float(v) -> float | None:
    """Cast an API decimal string (or number) to float. None stays None (→ NaN)."""
    return None if v is None else float(v)


def _get(obj, key):
    """Read key from a wire dict, or the attribute of that name from an object."""
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def _quotes(raw: object, suffix: str) -> dict:
    """The eight QUOTE_COLUMNS of one raw candle. The OHLC keys of yes_bid and
    yes_ask are bare on the historical endpoint and end in "_dollars" on the
    live one."""
    out = {}
    for side in ("yes_bid", "yes_ask"):
        book = _get(raw, side) or {}
        for k in _OHLC:
            out[f"{side}_{k}"] = _to_float(_get(book, k + suffix))
    return out


def parse_candle(raw: object, is_historical: bool) -> dict:
    """
    Normalize one candle from the historical or the live endpoint.

    Both endpoints serialize numerics as decimal strings; everything is cast
    to float here. Returns dict with ts_ms (int64 UTC ms) and float values:
    open/high/low/close/mean are TRADE prices in dollars in [0, 1]; volume and
    open_interest are contract counts exactly as the API reports them
    (fractional on markets with fractional-contract support), unscaled;
    yes_bid_* and yes_ask_* (QUOTE_COLUMNS) are the OHLC of the best YES bid
    and YES ask in dollars. None (→ NaN) marks values the API did not
    provide, never an invented 0.

    Quotes exist for every candle. Trade prices exist only when the period
    had a trade: the live endpoint then omits the OHLC keys, the historical
    one sends them as null, and both come out as None. A price column never
    holds a quote; the quote of a period without trades is in yes_bid_* and
    yes_ask_*. An empty side of the book is quoted by the API as "0.0000"
    (no bid) or "1.0000" (no ask) and is stored as sent.

    The two tiers send the same candle under different key names, and that
    is the only difference handled here:
        historical   price.close, yes_bid.close, volume, open_interest
        live         price.close_dollars, yes_bid.close_dollars, volume_fp,
                     open_interest_fp
    Accepts the REST dict as well as an object with the same attribute names.
    """
    suffix, count_suffix = ("", "") if is_historical else ("_dollars", "_fp")
    price = _get(raw, "price") or {}
    return {
        "ts_ms":         int(_get(raw, "end_period_ts") * 1000),
        "open":          _to_float(_get(price, "open" + suffix)),
        "high":          _to_float(_get(price, "high" + suffix)),
        "low":           _to_float(_get(price, "low" + suffix)),
        "close":         _to_float(_get(price, "close" + suffix)),
        "mean":          _to_float(_get(price, "mean" + suffix)),
        "volume":        _to_float(_get(raw, "volume" + count_suffix)),
        "open_interest": _to_float(_get(raw, "open_interest" + count_suffix)),
        **_quotes(raw, suffix),
    }


def candles_frame(rows: list[dict]) -> pd.DataFrame:
    """
    Build the DataFrame the candle pullers store from fetch_candles() rows.

    Columns come out in CANDLE_COLUMNS order. Every numeric column is forced
    to float64 and ts_ms to int64: a batch in which no candle had a trade
    holds only None prices, and would otherwise reach parquet as an untyped
    all-null column instead of float64 NaN.
    """
    df = pd.DataFrame(rows, columns=list(CANDLE_COLUMNS))
    floats = list(CANDLE_FLOAT_COLUMNS)
    df[floats] = df[floats].astype("float64")
    df["ts_ms"] = df["ts_ms"].astype("int64")
    return df


# ============================================================
# fetch_candles
# ============================================================

class PartialCandlesError(Exception):
    """
    A fetch failed after some chunks had already arrived.

    Chunks are fetched oldest first and windows are inclusive, so .rows is a
    gap-free prefix starting at the requested start_ts. Saving it is safe:
    the next run resumes from its last candle. .next_start_ts is where the
    fetch stopped. The original error is the __cause__.
    """

    def __init__(self, rows: list[dict], next_start_ts: int, cause: Exception):
        self.rows = rows
        self.next_start_ts = next_start_ts
        stopped = datetime.fromtimestamp(next_start_ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        super().__init__(
            f"fetch stopped at {stopped} after {len(rows)} candles ({type(cause).__name__}: {cause}); "
            f"the candles before that point were saved, the rest is retried on the next run"
        )


def _fetch_chunk(
    market_ticker: str,
    series_ticker: str,
    start_ts: int,
    end_ts: int,
    interval: int,
    use_historical: bool,
) -> tuple[list[dict], bool]:
    """
    Fetch one window of raw candles. Windows must span at most 5,000 candles:
    both tiers reject larger ones with HTTP 400 (there is no truncation and
    no continuation token), which CHUNK_SECONDS guarantees.

    Returns:
        (raw candles, is_historical). A 404 from the live endpoint means the
        market settled before the historical cutoff; the historical endpoint
        is used from then on.
    """
    params = {"start_ts": start_ts, "end_ts": end_ts, "period_interval": interval}
    if not use_historical:
        try:
            data = client.request_json(
                f"/series/{path_part(series_ticker)}/markets/{path_part(market_ticker)}/candlesticks",
                params,
            )
            return data.get("candlesticks") or [], False
        except KalshiNotFound:
            pass
    data = client.request_json(f"/historical/markets/{path_part(market_ticker)}/candlesticks", params)
    return data.get("candlesticks") or [], True


def fetch_candles(
    market_ticker: str,
    interval: int,
    start_ts: int,
    end_ts: int,
) -> list[dict]:
    """
    Fetch candles for a single market ticker at a given interval. Keyless REST.

    Args:
        market_ticker: market ticker (e.g. "KXRECSSNBER-26")
        interval:      period_interval in minutes (1, 60, or 1440)
        start_ts:      start timestamp in UTC seconds (API convention)
        end_ts:        end timestamp in UTC seconds (API convention)

    Returns:
        List of dicts, each with ts_ms (int64 UTC ms), OHLCMV fields, the
        yes_bid_* / yes_ask_* quote OHLC, and market_ticker/event_ticker/
        series_ticker metadata (see parse_candle; candles_frame() turns the
        list into the stored DataFrame). Ascending, one row per candle
        (window edges are inclusive on both sides, so a candle on a chunk
        boundary arrives twice and is kept once).

    Raises:
        PartialCandlesError: a chunk failed after earlier chunks succeeded;
                             carries the contiguous rows fetched so far.
        kalshi_io.client.KalshiAPIError: the very first chunk failed.
    """
    series_ticker, event_ticker = resolve_ticker_meta(market_ticker)
    chunk_seconds = CHUNK_SECONDS[interval]

    rows: dict[int, dict] = {}
    chunk_start = start_ts
    use_historical = False

    while chunk_start < end_ts:
        chunk_end = min(chunk_start + chunk_seconds, end_ts)

        try:
            raw, use_historical = _fetch_chunk(
                market_ticker, series_ticker, chunk_start, chunk_end, interval, use_historical
            )
        except (KalshiAPIError, requests.RequestException) as e:
            if rows:
                raise PartialCandlesError(list(rows.values()), chunk_start, e) from e
            raise

        for c in raw:
            candle = parse_candle(c, is_historical=use_historical)
            candle["market_ticker"] = market_ticker
            candle["event_ticker"] = event_ticker
            candle["series_ticker"] = series_ticker
            rows[candle["ts_ms"]] = candle

        chunk_start = chunk_end

    return list(rows.values())
