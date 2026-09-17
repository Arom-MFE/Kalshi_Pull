"""
kalshi_io/config.py — Paths, constants, and universe definitions.

Two settings come from the process environment (never from .env, which is
only read lazily for credentials):
    KALSHI_DATA_DIR  — data root override (default: <repo>/kalshi_data)
    KALSHI_MAX_RPS   — client-side request rate cap (default 10, max 20)
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


def _resolve_max_rps(env=os.environ) -> float:
    """Request rate cap: KALSHI_MAX_RPS, at most 20; default 10 when unset or invalid."""
    try:
        rps = float(env.get("KALSHI_MAX_RPS") or 10)
    except ValueError:
        return 10.0
    if rps <= 0:
        return 10.0
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

# Refreshed 2026-08-09; every ticker verified active via GET /markets/{ticker}.
# Update per macro cycle (see README Maintenance).
FOCUS_UNIVERSE: list[str] = [
    # Fed funds rate level — September 2026 FOMC (next meeting), liquid core
    "KXFED-26SEP-T2.75",
    "KXFED-26SEP-T3.00",
    "KXFED-26SEP-T3.25",
    "KXFED-26SEP-T3.50",
    "KXFED-26SEP-T3.75",
    "KXFED-26SEP-T4.00",
    "KXFED-26SEP-T4.25",
    "KXFED-26SEP-T4.50",

    # Fed decision action — September 2026 FOMC
    "KXFEDDECISION-26SEP-C25",
    "KXFEDDECISION-26SEP-C26",
    "KXFEDDECISION-26SEP-H0",
    "KXFEDDECISION-26SEP-H25",
    "KXFEDDECISION-26SEP-H26",

    # CPI YoY — July 2026 data, releases 2026-08-12 (liquid core of ladder)
    "KXCPIYOY-26JUL-T3.2",
    "KXCPIYOY-26JUL-T3.3",
    "KXCPIYOY-26JUL-T3.4",
    "KXCPIYOY-26JUL-T3.5",
    "KXCPIYOY-26JUL-T3.6",
    "KXCPIYOY-26JUL-T3.7",
    "KXCPIYOY-26JUL-T3.8",
    "KXCPIYOY-26JUL-T3.9",
    "KXCPIYOY-26JUL-T4.0",

    # Payrolls — August 2026 data, releases 2026-09-04
    "KXPAYROLLS-26AUG-T-25000",
    "KXPAYROLLS-26AUG-T0",
    "KXPAYROLLS-26AUG-T50000",
    "KXPAYROLLS-26AUG-T60000",
    "KXPAYROLLS-26AUG-T70000",
    "KXPAYROLLS-26AUG-T80000",
    "KXPAYROLLS-26AUG-T90000",
    "KXPAYROLLS-26AUG-T100000",

    # Unemployment — August 2026 data, releases 2026-09-04
    "KXU3-26AUG-T3.9",
    "KXU3-26AUG-T4.0",
    "KXU3-26AUG-T4.1",
    "KXU3-26AUG-T4.2",
    "KXU3-26AUG-T4.3",
    "KXU3-26AUG-T4.4",
    "KXU3-26AUG-T4.5",
]
# Canonical timestamp column across all parquet files (int64 UTC milliseconds)
TS_COL = "ts_ms"

DEDUPE_COLS_CANDLES: list[str] = ["ts_ms", "market_ticker"]
DEDUPE_COLS_TRADES: list[str] = ["trade_id"]

# ============================================================
# HTTP behavior (kalshi_io/client.py)
# ============================================================
# Kalshi documents rate limits as token buckets per authenticated account:
# 10 tokens per request, Basic tier read budget 200 tokens/s = 20 requests/s.
# Limits for keyless requests are undocumented, so the default is half of Basic.
MAX_REQUESTS_PER_SECOND: float = _resolve_max_rps()

# Minimum spacing between requests, enforced centrally in client.request_json
RATE_LIMIT_SECONDS = 1.0 / MAX_REQUESTS_PER_SECOND

HTTP_TIMEOUT: tuple[float, float] = (5.0, 30.0)   # (connect, read) seconds
HTTP_MAX_ATTEMPTS = 6
HTTP_BACKOFF_BASE_S = 0.5
HTTP_BACKOFF_CAP_S = 30.0
HTTP_RETRY_AFTER_CAP_S = 120.0

# Trades resume re-requests this many seconds before the last stored trade;
# the overlap is dropped again by trade_id, so it can never leave a gap.
TRADES_RESUME_OVERLAP_S = 60
