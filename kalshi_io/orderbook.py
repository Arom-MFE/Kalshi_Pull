"""
kalshi_io/orderbook.py — Orderbook snapshot and persistence.

Ported from reference_scripts/prediction_data_live.py:
    snapshot_orderbook       ← orderbook section (lines 53-102)
    append_orderbook_snapshot ← new (writes to parquet)
"""

import time

import pandas as pd

from kalshi_io.candles import resolve_ticker_meta
from kalshi_io.client import client
from kalshi_io.storage import append_parquet, get_output_path


def snapshot_orderbook(market_ticker: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Take a full orderbook snapshot for a market.

    Ported from prediction_data_live.py lines 53-102.

    Returns:
        (df_book, df_top) where:
        - df_book: all levels with ts_ms, market_ticker, side, price, quantity,
          cumulative_qty, distance_from_top
        - df_top: single-row summary with ts_ms, market_ticker, yes_bid,
          yes_bid_qty, yes_ask, yes_ask_qty, no_bid, no_bid_qty, no_ask,
          no_ask_qty, spread, mid
    """
    orderbook = client.get_market_orderbook(ticker=market_ticker)
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

    df_book = pd.DataFrame(rows)

    # Build df_top
    best_yes_price = yes_bids[0][0] if yes_bids else None
    best_yes_qty = yes_bids[0][1] if yes_bids else None
    best_no_price = no_bids[0][0] if no_bids else None
    best_no_qty = no_bids[0][1] if no_bids else None

    yes_ask = round(1 - best_no_price, 4) if best_no_price is not None else None
    no_ask = round(1 - best_yes_price, 4) if best_yes_price is not None else None
    spread = (
        round(yes_ask - best_yes_price, 4)
        if yes_ask is not None and best_yes_price is not None
        else None
    )
    mid = (
        round((best_yes_price + yes_ask) / 2, 4)
        if yes_ask is not None and best_yes_price is not None
        else None
    )

    df_top = pd.DataFrame([{
        "ts_ms": ts_ms,
        "market_ticker": market_ticker,
        "yes_bid": best_yes_price,
        "yes_bid_qty": best_yes_qty,
        "yes_ask": yes_ask,
        "yes_ask_qty": best_no_qty,
        "no_bid": best_no_price,
        "no_bid_qty": best_no_qty,
        "no_ask": no_ask,
        "no_ask_qty": best_yes_qty,
        "spread": spread,
        "mid": mid,
    }])

    return df_book, df_top


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
