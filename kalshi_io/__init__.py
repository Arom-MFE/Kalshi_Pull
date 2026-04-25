"""
kalshi_io — Shared library for the Kalshi macro market data pipeline.
"""

from kalshi_io.client import client, session, BASE_URL
from kalshi_io.candles import resolve_ticker_meta
from kalshi_io.trades import fetch_trades
from kalshi_io.orderbook import snapshot_orderbook, append_orderbook_snapshot
from kalshi_io.config import (
    PROJECT_ROOT,
    DATA_DIR,
    TICKERS_DIR,
    EXAMPLES_DIR,
    CHUNK_SECONDS,
    MAX_CANDLES_PER_CALL,
    SERIES_LIST,
    FOCUS_UNIVERSE,
    TS_COL,
    DEDUPE_COLS_CANDLES,
    DEDUPE_COLS_TRADES,
    RATE_LIMIT_SECONDS,
)


__all__ = [
    "client",
    "session",
    "BASE_URL",
    "PROJECT_ROOT",
    "DATA_DIR",
    "TICKERS_DIR",
    "EXAMPLES_DIR",
    "CHUNK_SECONDS",
    "MAX_CANDLES_PER_CALL",
    "SERIES_LIST",
    "FOCUS_UNIVERSE",
    "TS_COL",
    "DEDUPE_COLS_CANDLES",
    "DEDUPE_COLS_TRADES",
    "RATE_LIMIT_SECONDS",
    "resolve_ticker_meta",
    "fetch_trades",
    "snapshot_orderbook",
    "append_orderbook_snapshot",
]
