"""
kalshi_io/config.py — Paths, constants, and universe definitions.
"""

from pathlib import Path

# Anchor all paths to prediction_market/ so they work regardless of CWD
PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR     = PROJECT_ROOT / "kalshi_data"
TICKERS_DIR  = PROJECT_ROOT / "get_ticker_info" / "kalshi_tickers"
EXAMPLES_DIR = PROJECT_ROOT / "reference_scripts"

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

FOCUS_UNIVERSE: list[str] = [
    # Fed funds rate level — June 2026 FOMC (next meeting after April)
    "KXFED-26JUN-T5.25",
    "KXFED-26JUN-T5.00",
    "KXFED-26JUN-T4.75",
    "KXFED-26JUN-T4.50",
    "KXFED-26JUN-T4.25",
    "KXFED-26JUN-T4.00",
    "KXFED-26JUN-T3.75",
    "KXFED-26JUN-T3.50",

    # Fed decision action — June 2026 FOMC
    "KXFEDDECISION-26JUN-C25",
    "KXFEDDECISION-26JUN-C26",
    "KXFEDDECISION-26JUN-H0",
    "KXFEDDECISION-26JUN-H25",
    "KXFEDDECISION-26JUN-H26",

    # CPI YoY — May 2026 release (full ladder)
    "KXCPIYOY-26MAY-T4.0",
    "KXCPIYOY-26MAY-T3.5",
    "KXCPIYOY-26MAY-T3.0",
    "KXCPIYOY-26MAY-T2.9",
    "KXCPIYOY-26MAY-T2.8",
    "KXCPIYOY-26MAY-T2.7",
    "KXCPIYOY-26MAY-T2.6",
    "KXCPIYOY-26MAY-T2.5",
    "KXCPIYOY-26MAY-T2.4",

    # Payrolls — May 2026 release
    "KXPAYROLLS-26MAY-T-25000",
    "KXPAYROLLS-26MAY-T0",
    "KXPAYROLLS-26MAY-T50000",
    "KXPAYROLLS-26MAY-T60000",
    "KXPAYROLLS-26MAY-T70000",
    "KXPAYROLLS-26MAY-T80000",
    "KXPAYROLLS-26MAY-T90000",
    "KXPAYROLLS-26MAY-T100000",

    # Unemployment — April 2026 release (most recent available)
    "KXU3-26APR-T3.9",
    "KXU3-26APR-T4.0",
    "KXU3-26APR-T4.1",
    "KXU3-26APR-T4.2",
    "KXU3-26APR-T4.3",
    "KXU3-26APR-T4.4",
    "KXU3-26APR-T4.5",
]
# Canonical timestamp column across all parquet files (int64 UTC milliseconds)
TS_COL = "ts_ms"

DEDUPE_COLS_CANDLES: list[str] = ["ts_ms", "market_ticker"]
DEDUPE_COLS_TRADES: list[str] = ["trade_id"]

RATE_LIMIT_SECONDS = 0.2
