"""
kalshi_io/orderbook.py — Orderbook snapshot and persistence.
"""

import time

import pandas as pd

from kalshi_io.client import get_client
from kalshi_io.storage import append_parquet, get_output_path


def snapshot_orderbook(market_ticker: str) -> pd.DataFrame:
    """
    Take a full orderbook snapshot for a market.

    Returns:
        df_book: all levels with ts_ms, market_ticker, side, price, quantity,
        cumulative_qty, distance_from_top. Top-of-book (best bids, implied
        asks, spread, mid) is derivable from the stored full-depth rows.
    """
    orderbook = get_client().get_market_orderbook(ticker=market_ticker)
    ob = orderbook.orderbook_fp

    yes_bids = sorted(
        [(float(p), float(q)) for p, q in (ob.yes_dollars or [])],
        key=lambda x: -x[0],
    )
    no_bids = sorted(
        [(float(p), float(q)) for p, q in (ob.no_dollars or [])],
        key=lambda x: -x[0],
    )

    ts_ms = int(time.time() * 1000)

    # Build df_book
    rows = []
    yes_cum = 0.0
    for i, (price, qty) in enumerate(yes_bids):
        yes_cum += qty
        rows.append({
            "ts_ms": ts_ms,
            "market_ticker": market_ticker,
            "side": "YES",
            "price": price,
            "quantity": qty,
            "cumulative_qty": yes_cum,
            "distance_from_top": i,
        })

    no_cum = 0.0
    for i, (price, qty) in enumerate(no_bids):
        no_cum += qty
        rows.append({
            "ts_ms": ts_ms,
            "market_ticker": market_ticker,
            "side": "NO",
            "price": price,
            "quantity": qty,
            "cumulative_qty": no_cum,
            "distance_from_top": i,
        })

    return pd.DataFrame(rows)


def append_orderbook_snapshot(market_ticker: str, df_book: pd.DataFrame) -> int:
    """
    Write an orderbook snapshot to the daily parquet file.

    Path: orderbook/{ticker}/{yyyy-mm-dd}.parquet
    Dedupes on [ts_ms, side, price], sorts by ts_ms.

    Returns:
        Number of new rows written.
    """
    ts = pd.Timestamp.now(tz="UTC")
    path = get_output_path("orderbook", None, "", market_ticker, ts=ts)
    return append_parquet(df_book, path, ["ts_ms", "side", "price"], sort_by="ts_ms")
