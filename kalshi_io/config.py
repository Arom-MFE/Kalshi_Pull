"""
kalshi_io/config.py — Paths, constants, and the focus universe rule.

Two settings come from the process environment (never from .env, which is
only read lazily for credentials):
    KALSHI_DATA_DIR  — data root override (default: <repo>/kalshi_data)
    KALSHI_MAX_RPS   — client-side request rate cap (default 5, max 20)
"""

import os
from pathlib import Path

# Anchor all paths to the repo root so they work regardless of CWD
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _resolve_data_dir(env=os.environ) -> Path:
    """Data root: KALSHI_DATA_DIR if set and non-empty, else <repo>/kalshi_data."""
    override = (env.get("KALSHI_DATA_DIR") or "").strip()
    if override:
        return Path(override).expanduser().resolve()
    return PROJECT_ROOT / "kalshi_data"


DEFAULT_MAX_RPS = 5.0


def _resolve_max_rps(env=os.environ) -> float:
    """Request rate cap: KALSHI_MAX_RPS, at most 20; default 5 when unset or invalid."""
    try:
        rps = float(env.get("KALSHI_MAX_RPS") or DEFAULT_MAX_RPS)
    except ValueError:
        return DEFAULT_MAX_RPS
    if rps <= 0:
        return DEFAULT_MAX_RPS
    return min(rps, 20.0)


# Resolved at import time: modules bind DATA_DIR by value
DATA_DIR     = _resolve_data_dir()
TICKERS_DIR  = PROJECT_ROOT / "get_ticker_info" / "kalshi_tickers"

# Chunk sizes per period_interval (seconds per API call window)
CHUNK_SECONDS: dict[int, int] = {
    1:    3 * 86_400,      # minute: 3 days
    60:   30 * 86_400,     # hourly: 30 days
    1440: 365 * 86_400,    # daily:  365 days
}

MAX_CANDLES_PER_CALL = 5_000

SERIES_LIST: list[str] = [
    # Inflation (6)
    "KXCPI", "KXCPIYOY", "KXACPI",
    "KXCPICORE", "KXPCECORE", "KXCPICOREYOY",
    # Labor (3)
    "KXU3", "KXJOBLESS", "KXPAYROLLS",
    # Growth (3)
    "KXGDP", "KXGDPYEAR", "KXRECSSNBER",
    # Fed (3)
    "KXFEDDECISION", "KXFED", "KXFEDMEET",
]

# ============================================================
# Focus universe (kalshi_io/universe.py, pull_live/poll_focus.py)
# ============================================================
# The tickers poll_focus polls are derived, not listed: for every series below,
# the nearest FOCUS_EVENTS_PER_SERIES events that still have open markets,
# ranked by the earliest close_time of those markets. The universe is resolved
# at startup and refreshed while polling, so it moves to the next event cycle
# on its own. Nothing here needs editing per cycle.
FOCUS_SERIES: list[str] = ["KXFED", "KXFEDDECISION", "KXCPIYOY", "KXPAYROLLS", "KXU3"]
FOCUS_EVENTS_PER_SERIES = 1

# Manual override: market tickers. A non-empty list replaces the derived
# universe entirely and is never rolled forward; tickers that stopped trading
# are dropped, and poll_focus refuses to start once none is left.
FOCUS_OVERRIDE: list[str] = []

# Seconds between universe refreshes inside poll_focus (0 = never refresh)
FOCUS_REFRESH_SECONDS = 3600

# ============================================================
# Release windows (kalshi_io/releases.py, pull_live/poll_focus.py)
# ============================================================
# Around a release the books are polled every RELEASE_POLL_SECONDS instead
# of every minute (one batch request per sweep). The window runs from
# RELEASE_WINDOW_BEFORE_S before to RELEASE_WINDOW_AFTER_S after the release,
# whose anchor is the close_time of the event (Kalshi closes a market one to
# five minutes before the number it settles on comes out). Release times come
# from the polled events, from every cataloged event that can still trade, and
# from RELEASE_CALENDAR: extra ISO-8601 UTC times such as "2026-10-30T12:30:00Z".
RELEASE_POLL_SECONDS = 5
RELEASE_WINDOW_BEFORE_S = 300
RELEASE_WINDOW_AFTER_S = 900
RELEASE_CALENDAR: list[str] = []

# A ticker that joins the universe without stored history is backfilled by a
# child process (pull_historical/backfill.py) at this request rate, while the
# poller keeps capturing its books; together they stay under the keyless limit.
BACKGROUND_HISTORY_RPS = 3

# Canonical timestamp column across all parquet files (int64 UTC milliseconds)
TS_COL = "ts_ms"

DEDUPE_COLS_CANDLES: list[str] = ["ts_ms", "market_ticker"]
DEDUPE_COLS_TRADES: list[str] = ["trade_id"]

# ============================================================
# HTTP behavior (kalshi_io/client.py)
# ============================================================
# Kalshi documents rate limits as token buckets per authenticated account:
# 10 tokens per request, Basic tier read budget 200 tokens/s = 20 requests/s.
# Limits for keyless requests are undocumented. Measured on 2026-09-17, the
# candlestick endpoints sustain 4 to 5 requests/s without a key (one 429 in
# 1,344 requests at 4/s, about 5 percent at 6/s, 8 to 12 percent at 10/s), so
# the default is 5.
MAX_REQUESTS_PER_SECOND: float = _resolve_max_rps()

# Minimum spacing between requests, enforced centrally in client.request_json
RATE_LIMIT_SECONDS = 1.0 / MAX_REQUESTS_PER_SECOND

HTTP_TIMEOUT: tuple[float, float] = (5.0, 30.0)   # (connect, read) seconds
HTTP_MAX_ATTEMPTS = 6
HTTP_BACKOFF_BASE_S = 0.5
HTTP_BACKOFF_CAP_S = 30.0
HTTP_RETRY_AFTER_CAP_S = 120.0

# A run stops after this many tickers in a row exhausted their retries:
# the API is down or throttling, and grinding on helps nobody.
MAX_CONSECUTIVE_OUTAGES = 3

# ============================================================
# Concurrent writers (kalshi_io/storage.py)
# ============================================================
# The poller and a backfill may run at the same time. A writer waits this long
# for another process to finish with the same parquet file, then fails the
# ticker (it is retried on the next cycle or run). Holds last milliseconds.
LOCK_TIMEOUT_S: float = 120.0
# Orderbook snapshots are perishable and frequent: rather lose one than stall the sweep
ORDERBOOK_LOCK_TIMEOUT_S: float = 5.0

# Trades resume re-requests this many seconds before the last stored trade;
# the overlap is dropped again by trade_id, so it can never leave a gap.
TRADES_RESUME_OVERLAP_S = 60
