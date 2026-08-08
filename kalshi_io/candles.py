"""
kalshi_io/candles.py — Candle fetching and normalization.

Ported from reference_scripts/prediction_hourly_data_hist.py:
    parse_candle   ← parse_candle              (lines 201-234)
    fetch_candles  ← chunk loop + fallback      (lines 282-302)
                   + fetch_historical_chunk     (lines 237-242)
                   + attach_meta               (lines 245-246)
"""

import json
import time

from kalshi_python_sync.exceptions import NotFoundException

from kalshi_io.client import BASE_URL, client, session
from kalshi_io.config import (
    CHUNK_SECONDS,
    RATE_LIMIT_SECONDS,
    SERIES_LIST,
    TICKERS_DIR,
)

# ============================================================
# Reverse lookup: market_ticker → (series_ticker, event_ticker)
# Built lazily from per-series JSON files on first use.
# ============================================================
_ticker_meta: dict[str, tuple[str, str]] = {}


def _ensure_ticker_meta() -> None:
    """Load market→(series, event) mapping from ticker JSON files once."""
    if _ticker_meta:
        return
    for series_json in sorted(TICKERS_DIR.glob("*_tickers.json")):
        if series_json.name == "all_tickers.json":
            continue
        data = json.loads(series_json.read_text())
        series = data["series"]
        for m in data.get("markets", []):
            _ticker_meta[m["market_ticker"]] = (series, m["event_ticker"])


def _derive_series(market_ticker: str) -> str:
    """Fallback: derive series from market_ticker prefix."""
    for s in SERIES_LIST:
        if market_ticker.startswith(s + "-") or market_ticker == s:
            return s
    return market_ticker.split("-")[0]


def resolve_ticker_meta(market_ticker: str) -> tuple[str, str]:
    """Return (series_ticker, event_ticker) for a market_ticker."""
    _ensure_ticker_meta()
    if market_ticker in _ticker_meta:
        return _ticker_meta[market_ticker]
    return (_derive_series(market_ticker), market_ticker)


# ============================================================
# parse_candle — ported from lines 201-234
# ============================================================

def _to_float(v) -> float | None:
    """Cast an API decimal string (or number) to float. None stays None (→ NaN)."""
    return None if v is None else float(v)


def _first_not_none(*vals):
    """Return the first non-None value (unlike `or`, keeps legitimate 0 prices)."""
    for v in vals:
        if v is not None:
            return v
    return None


def parse_candle(raw: object, is_historical: bool) -> dict:
    """
    Normalize one candle from either historical REST (dict) or live SDK (object).

    Both API paths serialize numerics as decimal strings; everything is cast
    to float here. Returns dict with ts_ms (int64 UTC ms) and float values:
    open/high/low/close/mean are dollar prices in [0, 1]; volume and
    open_interest are contract counts exactly as the API reports them
    (fractional on markets with fractional-contract support), unscaled.
    None (→ NaN) marks values the API did not provide.

    Historical path falls back to yes_bid.* when price.* is null.
    """
    if is_historical:
        price = raw.get("price", {})
        yes_bid = raw.get("yes_bid", {})

        return {
            "ts_ms":         int(raw["end_period_ts"] * 1000),
            "open":          _to_float(_first_not_none(price.get("open"), yes_bid.get("open"))),
            "high":          _to_float(_first_not_none(price.get("high"), yes_bid.get("high"))),
            "low":           _to_float(_first_not_none(price.get("low"), yes_bid.get("low"))),
            "close":         _to_float(_first_not_none(price.get("close"), yes_bid.get("close"))),
            "mean":          _to_float(price.get("mean")),
            "volume":        _to_float(raw.get("volume")),
            "open_interest": _to_float(raw.get("open_interest")),
        }

    # Live SDK object
    p = raw.price
    return {
        "ts_ms":         int(raw.end_period_ts * 1000),
        "open":          _to_float(p.open_dollars),
        "high":          _to_float(p.high_dollars),
        "low":           _to_float(p.low_dollars),
        "close":         _to_float(p.close_dollars),
        "mean":          _to_float(p.mean_dollars),
        "volume":        _to_float(raw.volume_fp),
        "open_interest": _to_float(raw.open_interest_fp),
    }


# ============================================================
# fetch_candles — ported from chunk loop (lines 282-302)
# ============================================================

def _fetch_historical_chunk(
    market_ticker: str,
    start_ts: int,
    end_ts: int,
    interval: int,
) -> list[dict]:
    """
    Fetch candles from the historical REST endpoint.

    Handles pagination via adjustedEndTs when response hits 5000 candles.
    """
    all_candles: list[dict] = []
    chunk_start = start_ts

    while chunk_start < end_ts:
        resp = session.get(
            f"{BASE_URL}/historical/markets/{market_ticker}/candlesticks",
            params={
                "start_ts": chunk_start,
                "end_ts": end_ts,
                "period_interval": interval,
            },
        )
        if resp.status_code != 200:
            break

        data = resp.json()
        candles = data.get("candlesticks", [])
        all_candles.extend(candles)

        if len(candles) < 5000:
            break

        # Pagination: API returns adjustedEndTs when truncated
        adjusted = data.get("adjustedEndTs")
        if adjusted and adjusted > chunk_start:
            chunk_start = adjusted
            time.sleep(RATE_LIMIT_SECONDS)
        else:
            break

    return all_candles


def fetch_candles(
    market_ticker: str,
    interval: int,
    start_ts: int,
    end_ts: int,
) -> list[dict]:
    """
    Fetch candles for a single market ticker at a given interval.

    Args:
        market_ticker: market ticker (e.g. "KXRECSSNBER-26")
        interval:      period_interval in minutes (1, 60, or 1440)
        start_ts:      start timestamp in UTC seconds (API convention)
        end_ts:        end timestamp in UTC seconds (API convention)

    Returns:
        List of dicts, each with ts_ms (int64 UTC ms), OHLCMV fields,
        and market_ticker/event_ticker/series_ticker metadata.
    """
    series_ticker, event_ticker = resolve_ticker_meta(market_ticker)
    chunk_seconds = CHUNK_SECONDS[interval]

    rows: list[dict] = []
    chunk_start = start_ts
    use_historical = False

    while chunk_start < end_ts:
        chunk_end = min(chunk_start + chunk_seconds, end_ts)

        if use_historical:
            for c in _fetch_historical_chunk(
                market_ticker, chunk_start, chunk_end, interval
            ):
                candle = parse_candle(c, is_historical=True)
                candle["market_ticker"] = market_ticker
                candle["event_ticker"] = event_ticker
                candle["series_ticker"] = series_ticker
                rows.append(candle)
        else:
            try:
                result = client.get_market_candlesticks(
                    series_ticker=series_ticker,
                    ticker=market_ticker,
                    start_ts=chunk_start,
                    end_ts=chunk_end,
                    period_interval=interval,
                )
                for c in result.candlesticks:
                    candle = parse_candle(c, is_historical=False)
                    candle["market_ticker"] = market_ticker
                    candle["event_ticker"] = event_ticker
                    candle["series_ticker"] = series_ticker
                    rows.append(candle)
            except NotFoundException:
                use_historical = True
                for c in _fetch_historical_chunk(
                    market_ticker, chunk_start, chunk_end, interval
                ):
                    candle = parse_candle(c, is_historical=True)
                    candle["market_ticker"] = market_ticker
                    candle["event_ticker"] = event_ticker
                    candle["series_ticker"] = series_ticker
                    rows.append(candle)

        chunk_start = chunk_end
        time.sleep(RATE_LIMIT_SECONDS)

    return rows
