"""
kalshi_io/orderbook.py — Orderbook snapshot and persistence.

GET /markets/{ticker}/orderbook answers without a key today, although the
OpenAPI spec declares it authenticated. request_json asks keyless first and
repeats the call signed if the API answers 401/403.
"""

import time

import pandas as pd

from kalshi_io import client
from kalshi_io.client import path_part
from kalshi_io.storage import append_parquet, get_output_path


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

    yes_bids = sorted(
        [(float(p), float(q)) for p, q in (ob.get("yes_dollars") or [])],
        key=lambda x: -x[0],
    )
    no_bids = sorted(
        [(float(p), float(q)) for p, q in (ob.get("no_dollars") or [])],
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
    return append_parquet(df_book, path, ["ts_ms", "side", "price"], sort_by="ts_ms")
