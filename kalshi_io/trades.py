"""
kalshi_io/trades.py — Trade fetching and normalization.

Trades live in two tiers: /markets/trades serves everything since the
historical cutoff, /historical/trades everything before it. Both take
min_ts (Unix seconds, inclusive) and return pages newest-first.
"""

import time
from datetime import datetime

import pandas as pd

from kalshi_io import client, discovery
from kalshi_io.runlog import get_logger

logger = get_logger("trades")

TRADE_COLUMNS = [
    "trade_id", "market_ticker", "ts_ms",
    "yes_price", "no_price", "count", "taker_side",
]

# Order direction in the API's two vocabularies: a taker who buys YES (or sells
# NO) is "yes" and "bid"; a taker who buys NO (or sells YES) is "no" and "ask".
TAKER_BOOK_TO_OUTCOME = {"bid": "yes", "ask": "no"}

# The cutoff only moves forward, slowly; a poller runs for weeks
CUTOFF_TTL_SECONDS = 3600

_monotonic = time.monotonic
_cutoff_cache: tuple[float, int] | None = None    # (fetched at, cutoff Unix seconds)


def _iso_to_ts(iso: str) -> float:
    return datetime.fromisoformat(str(iso).replace("Z", "+00:00")).timestamp()


def get_trades_cutoff_ts() -> int | None:
    """
    Unix seconds of the live/historical boundary for trades
    (GET /historical/cutoff → trades_created_ts), cached for an hour.

    Returns:
        The cutoff, or None if it cannot be determined right now (callers
        then query both tiers, which is always correct, just slower).
    """
    global _cutoff_cache
    now = _monotonic()
    if _cutoff_cache is not None and now - _cutoff_cache[0] < CUTOFF_TTL_SECONDS:
        return _cutoff_cache[1]
    try:
        cutoff = int(_iso_to_ts(discovery.get_historical_cutoff()["trades_created_ts"]))
    except Exception as e:
        logger.warning(f"historical cutoff unavailable ({type(e).__name__}: {e}); querying both trade tiers")
        return None
    _cutoff_cache = (now, cutoff)
    return cutoff


def _paginate_trades(endpoint: str, ticker: str, min_ts: int | None = None) -> list[dict]:
    """
    Pull all pages from a trade endpoint via cursor pagination.

    Raises on any failed page (after the client's retries): a truncated tape
    must never pass for a complete one.
    """
    return list(client.paginate(endpoint, {"ticker": ticker, "min_ts": min_ts}, key="trades"))


def fetch_trades(
    market_ticker: str,
    since_trade_id: str | None = None,
    *,
    min_ts: int | None = None,
) -> pd.DataFrame:
    """
    Fetch trades for a market ticker.

    Args:
        market_ticker:   market ticker (e.g. "KXRECSSNBER-26")
        since_trade_id:  legacy client-side filter: keep trades at or after
                         this trade's timestamp, except the trade itself.
                         Prefer min_ts, which narrows the request.
        min_ts:          only request trades at or after this Unix second.
                         None pulls the complete tape from both tiers. With a
                         min_ts at or after the historical cutoff only the
                         live tier is queried.

    Returns:
        DataFrame with columns: trade_id, market_ticker, ts_ms (int64 UTC ms),
        yes_price, no_price, count, taker_side. Sorted by ts_ms ascending.
        yes_price/no_price are float64 dollars in [0, 1]; count is float64
        contracts exactly as the API reports it (fractional on markets with
        fractional-contract support), unscaled. taker_side ("yes" or "no")
        comes from the deprecated wire field of that name, else from
        taker_outcome_side, else from taker_book_side (bid = yes, ask = no):
        the first one the API sent.

    Raises:
        kalshi_io.client.KalshiAPIError on any failed page. Nothing partial is
        returned: pages arrive newest-first, so storing a truncated fetch
        would move the resume point past trades that were never downloaded.
    """
    all_trades = _paginate_trades("/markets/trades", market_ticker, min_ts)

    cutoff = get_trades_cutoff_ts() if min_ts is not None else None
    if min_ts is None or cutoff is None or min_ts < cutoff:
        all_trades += _paginate_trades("/historical/trades", market_ticker, min_ts)

    if not all_trades:
        return pd.DataFrame(columns=TRADE_COLUMNS)

    df = pd.DataFrame(all_trades)

    # Normalize columns
    df["ts_ms"] = df["created_time"].apply(lambda ct: int(_iso_to_ts(ct) * 1000))
    df = df.rename(columns={
        "ticker": "market_ticker",
        "yes_price_dollars": "yes_price",
        "no_price_dollars": "no_price",
        "count_fp": "count",
    })

    # taker_side is deprecated on the wire (still sent on 2026-09-17). The spec
    # makes taker_outcome_side (same yes/no value) and taker_book_side ("bid"
    # is yes, "ask" is no) the required fields. All three are exchange data,
    # nothing is inferred. None-aware: a present value is never overwritten.
    if "taker_side" not in df.columns:
        df["taker_side"] = None
    if "taker_outcome_side" in df.columns:
        df["taker_side"] = df["taker_side"].where(df["taker_side"].notna(), df["taker_outcome_side"])
    if "taker_book_side" in df.columns:
        from_book = df["taker_book_side"].map(TAKER_BOOK_TO_OUTCOME)
        df["taker_side"] = df["taker_side"].where(df["taker_side"].notna(), from_book)

    # Keep only the columns we need
    df = df[TRADE_COLUMNS]

    # Wire JSON serializes prices and counts as decimal strings — cast to
    # float64. count is fixed-point contracts; fractional values are genuine
    # (fractional-contract support), never rounded or rescaled.
    for col in ("yes_price", "no_price", "count"):
        df[col] = pd.to_numeric(df[col]).astype("float64")

    # Dedupe and sort (the two tiers overlap near the cutoff)
    df = (
        df
        .drop_duplicates(subset="trade_id", keep="last")
        .sort_values("ts_ms")
        .reset_index(drop=True)
    )

    # Legacy post-fetch filter. Not strict: several trades can share one
    # timestamp, and the siblings of since_trade_id must survive.
    if since_trade_id is not None:
        match = df[df["trade_id"] == since_trade_id]
        if not match.empty:
            cutoff_ts = int(match["ts_ms"].iloc[0])
            df = df[(df["ts_ms"] >= cutoff_ts) & (df["trade_id"] != since_trade_id)].reset_index(drop=True)
        # If trade_id not found, all rows are new — keep everything

    return df


def _reset_state() -> None:
    """Forget the cached cutoff (test isolation)."""
    global _cutoff_cache
    _cutoff_cache = None
