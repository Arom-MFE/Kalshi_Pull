"""
kalshi_io/orderbook.py — Orderbook snapshots and persistence.

GET /markets/{ticker}/orderbook and GET /markets/orderbooks (up to 100
tickers in one request) answer without a key today, although the OpenAPI
spec declares them authenticated. request_json asks keyless first and repeats
the call signed if the API answers 401/403.

Books cannot be backfilled: what is not captured while a market trades is
gone. poll_focus captures the whole universe in one batch request per sweep,
so every book of a sweep carries the same timestamp.
"""

import time

import pandas as pd

from kalshi_io import client
from kalshi_io.client import path_part
from kalshi_io.config import HTTP_MAX_ATTEMPTS, HTTP_TIMEOUT, ORDERBOOK_LOCK_TIMEOUT_S
from kalshi_io.storage import append_parquet, get_output_path

BOOK_COLUMNS = ["ts_ms", "market_ticker", "side", "price", "quantity", "cumulative_qty", "distance_from_top"]

# The batch endpoint takes at most this many tickers per request
BATCH_SIZE = 100


def _book_rows(market_ticker: str, ob: dict, ts_ms: int) -> list[dict]:
    """Rows of the stored schema for one orderbook_fp object: both bid books,
    best price first, with the running total from the top."""
    rows = []
    for side, key in (("YES", "yes_dollars"), ("NO", "no_dollars")):
        levels = sorted(((float(p), float(q)) for p, q in (ob.get(key) or [])), key=lambda x: -x[0])
        cumulative = 0.0
        for i, (price, qty) in enumerate(levels):
            cumulative += qty
            rows.append({
                "ts_ms": ts_ms,
                "market_ticker": market_ticker,
                "side": side,
                "price": price,
                "quantity": qty,
                "cumulative_qty": cumulative,
                "distance_from_top": i,
            })
    return rows


def _frame(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=BOOK_COLUMNS) if rows else pd.DataFrame(rows)


def snapshot_orderbook(market_ticker: str) -> pd.DataFrame:
    """
    Take a full orderbook snapshot for a market.

    Returns:
        df_book: all levels with ts_ms, market_ticker, side, price, quantity,
        cumulative_qty, distance_from_top. Top-of-book (best bids, implied
        asks, spread, mid) is derivable from the stored full-depth rows.
        Empty for a market with no resting orders; a settled market answers
        200 with empty books, so an empty frame is not an error.
    """
    data = client.request_json(f"/markets/{path_part(market_ticker)}/orderbook")
    ob = data["orderbook_fp"]
    return _frame(_book_rows(market_ticker, ob, int(time.time() * 1000)))


def snapshot_orderbooks(
    tickers: list[str],
    *,
    max_attempts: int = HTTP_MAX_ATTEMPTS,
    timeout: tuple[float, float] = HTTP_TIMEOUT,
) -> dict[str, pd.DataFrame]:
    """
    Snapshot many books in one request per 100 tickers (GET /markets/orderbooks).

    The tickers go as repeated `tickers` parameters; a comma-separated value
    is not understood by the API. All books of one response share one ts_ms.

    Args:
        max_attempts, timeout: per request; a poller inside a release window
                               passes small values so a failing sweep cannot
                               eat the window

    Returns:
        {ticker: df_book} with the columns of snapshot_orderbook(). A settled
        market comes back with empty books (an empty frame); a ticker the API
        does not know is simply absent.
    """
    books: dict[str, pd.DataFrame] = {}
    wanted = list(dict.fromkeys(tickers))
    for start in range(0, len(wanted), BATCH_SIZE):
        chunk = wanted[start:start + BATCH_SIZE]
        data = client.request_json("/markets/orderbooks", {"tickers": chunk},
                                   max_attempts=max_attempts, timeout=timeout)
        ts_ms = int(time.time() * 1000)
        for entry in data.get("orderbooks") or []:
            ticker = entry.get("ticker")
            if ticker in chunk:
                books[ticker] = _frame(_book_rows(ticker, entry.get("orderbook_fp") or {}, ts_ms))
    return books


def append_orderbook_snapshot(market_ticker: str, df_book: pd.DataFrame) -> int:
    """
    Write an orderbook snapshot to the daily parquet file.

    Path: orderbook/{ticker}/{yyyy-mm-dd}.parquet, the UTC day of the
    snapshot's own ts_ms (so one snapshot never straddles two files).
    Dedupes on [ts_ms, side, price], sorts by ts_ms.

    Returns:
        Number of new rows written.
    """
    if df_book.empty:
        return 0
    ts = pd.Timestamp(int(df_book["ts_ms"].iloc[0]), unit="ms", tz="UTC")
    path = get_output_path("orderbook", None, "", market_ticker, ts=ts)
    # A snapshot is perishable and the next one is seconds away: wait briefly for a
    # backfill that holds the same lock stripe, never the full default
    return append_parquet(df_book, path, ["ts_ms", "side", "price"], sort_by="ts_ms",
                          lock_timeout=ORDERBOOK_LOCK_TIMEOUT_S)
