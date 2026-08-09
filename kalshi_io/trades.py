"""
kalshi_io/trades.py — Trade fetching and normalization.
"""

import time
from datetime import datetime

import pandas as pd

from kalshi_io.client import BASE_URL, get_session
from kalshi_io.config import RATE_LIMIT_SECONDS

TRADE_COLUMNS = [
    "trade_id", "market_ticker", "ts_ms",
    "yes_price", "no_price", "count", "taker_side",
]


def _paginate_trades(endpoint: str, ticker: str) -> list[dict]:
    """
    Pull all pages from a trade endpoint via cursor pagination.

    Sleeps RATE_LIMIT_SECONDS between paginated calls.
    """
    rows: list[dict] = []
    cursor = None
    while True:
        params = {"ticker": ticker, "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        resp = get_session().get(f"{BASE_URL}{endpoint}", params=params)
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
        yes_price/no_price are float64 dollars in [0, 1]; count is float64
        contracts exactly as the API reports it (fractional on markets with
        fractional-contract support), unscaled.
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

    # Wire JSON serializes prices and counts as decimal strings — cast to
    # float64. count is fixed-point contracts; fractional values are genuine
    # (fractional-contract support), never rounded or rescaled.
    for col in ("yes_price", "no_price", "count"):
        df[col] = pd.to_numeric(df[col]).astype("float64")

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
