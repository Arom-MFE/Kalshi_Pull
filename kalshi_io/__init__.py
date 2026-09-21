"""
kalshi_io — Shared library for the Kalshi macro market data pipeline.
"""

import warnings

# `python -m kalshi_io.metadata --rederive` executes a module this package has already imported, and runpy
# warns about exactly that. The module holds no state of its own, so running it twice is harmless; the
# message would only worry whoever runs the command. Nothing else is filtered.
warnings.filterwarnings("ignore", category=RuntimeWarning,
                        message=r"'kalshi_io\.metadata' found in sys\.modules after import of package")

from kalshi_io.client import get_client, get_session, BASE_URL
from kalshi_io.candles import CANDLE_COLUMNS, candles_frame, fetch_candles, resolve_ticker_meta
from kalshi_io.trades import fetch_trades
from kalshi_io.orderbook import snapshot_orderbook, snapshot_orderbooks, append_orderbook_snapshot
from kalshi_io.metadata import METADATA_COLUMNS, load_market_metadata, refresh_market_metadata
from kalshi_io.storage import LockTimeout, append_parquet, duckdb_connect, file_lock, named_lock
from kalshi_io.quality import format_report, run_checks
from kalshi_io.config import (
    PROJECT_ROOT,
    DATA_DIR,
    TICKERS_DIR,
    CHUNK_SECONDS,
    MAX_CANDLES_PER_CALL,
    SERIES_LIST,
    FOCUS_SERIES,
    FOCUS_EVENTS_PER_SERIES,
    FOCUS_OVERRIDE,
    RELEASE_POLL_SECONDS,
    RELEASE_WINDOW_BEFORE_S,
    RELEASE_WINDOW_AFTER_S,
    RELEASE_CALENDAR,
    BACKGROUND_HISTORY_RPS,
    TS_COL,
    DEDUPE_COLS_CANDLES,
    DEDUPE_COLS_TRADES,
    MAX_REQUESTS_PER_SECOND,
    RATE_LIMIT_SECONDS,
    LOCK_TIMEOUT_S,
)


__all__ = [
    "get_client",
    "get_session",
    "BASE_URL",
    "PROJECT_ROOT",
    "DATA_DIR",
    "TICKERS_DIR",
    "CHUNK_SECONDS",
    "MAX_CANDLES_PER_CALL",
    "SERIES_LIST",
    "FOCUS_SERIES",
    "FOCUS_EVENTS_PER_SERIES",
    "FOCUS_OVERRIDE",
    "RELEASE_POLL_SECONDS",
    "RELEASE_WINDOW_BEFORE_S",
    "RELEASE_WINDOW_AFTER_S",
    "RELEASE_CALENDAR",
    "BACKGROUND_HISTORY_RPS",
    "TS_COL",
    "DEDUPE_COLS_CANDLES",
    "DEDUPE_COLS_TRADES",
    "MAX_REQUESTS_PER_SECOND",
    "RATE_LIMIT_SECONDS",
    "LOCK_TIMEOUT_S",
    "CANDLE_COLUMNS",
    "candles_frame",
    "fetch_candles",
    "resolve_ticker_meta",
    "fetch_trades",
    "snapshot_orderbook",
    "snapshot_orderbooks",
    "append_orderbook_snapshot",
    "METADATA_COLUMNS",
    "load_market_metadata",
    "refresh_market_metadata",
    "LockTimeout",
    "append_parquet",
    "duckdb_connect",
    "file_lock",
    "named_lock",
    "format_report",
    "run_checks",
]
