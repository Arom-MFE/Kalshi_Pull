"""
kalshi_io/trades.py — Trade fetching and normalization.

Ported from reference_scripts/prediction_data_live.py:
    _paginate_trades  ← paginate_trades  (lines 222-238)
    fetch_trades      ← dedup block      (lines 248-257)
"""

import time
from datetime import datetime, timezone

import pandas as pd

from kalshi_io.client import BASE_URL, session
from kalshi_io.config import RATE_LIMIT_SECONDS

TRADE_COLUMNS = [
    "trade_id", "market_ticker", "ts_ms",
    "yes_price", "no_price", "count", "taker_side",
]


def _paginate_trades(endpoint: str, ticker: str) -> list[dict]:
    """
    Pull all pages from a trade endpoint.

    Ported from prediction_data_live.py lines 222-238.
    Uses session.get instead of bare requests.get.
    Sleeps RATE_LIMIT_SECONDS between paginated calls.
    """
    rows: list[dict] = []
    cursor = None
    while True:
        params = {"ticker": ticker, "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        resp = session.get(f"{BASE_URL}{endpoint}", params=params)
        if resp.status_code != 200:
            break
        data = resp.json()
        rows.extend(data.get("trades", []))
        cursor = data.get("cursor")
        if not cursor:
            break
        time.sleep(RATE_LIMIT_SECONDS)
    return rows


def fetch_trades(
    market_ticker: str,
    since_trade_id: str | None = None,
) -> pd.DataFrame:
    """
    Fetch all trades for a market ticker from live + historical endpoints.

    Args:
        market_ticker:   market ticker (e.g. "KXRECSSNBER-26")
        since_trade_id:  if provided, filter to trades after this trade_id's timestamp

    Returns:
        DataFrame with columns: trade_id, market_ticker, ts_ms (int64 UTC ms),
        yes_price, no_price, count, taker_side. Sorted by ts_ms ascending.
    """
    live = _paginate_trades("/markets/trades", market_ticker)
    hist = _paginate_trades("/historical/trades", market_ticker)
    all_trades = live + hist

    if not all_trades:
        return pd.DataFrame(columns=TRADE_COLUMNS)

    df = pd.DataFrame(all_trades)

    # Normalize columns
    df["ts_ms"] = df["created_time"].apply(
        lambda ct: int(
            datetime.fromisoformat(
                str(ct).replace("Z", "+00:00")
            ).timestamp() * 1000
        )
    )
    df = df.rename(columns={
        "ticker": "market_ticker",
        "yes_price_dollars": "yes_price",
        "no_price_dollars": "no_price",
        "count_fp": "count",
    })

    # Keep only the columns we need
    df = df[TRADE_COLUMNS]

    # Dedupe and sort
    df = (
        df
        .drop_duplicates(subset="trade_id", keep="last")
        .sort_values("ts_ms")
        .reset_index(drop=True)
    )

    # Post-fetch filter: keep only trades after since_trade_id
    if since_trade_id is not None:
        match = df[df["trade_id"] == since_trade_id]
        if not match.empty:
            cutoff_ts = int(match["ts_ms"].iloc[0])
            df = df[df["ts_ms"] > cutoff_ts].reset_index(drop=True)
        # If trade_id not found, all rows are new — keep everything

    return df
