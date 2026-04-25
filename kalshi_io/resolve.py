"""
kalshi_io/resolve.py — Event, market, and metadata resolution.

Ported from reference_scripts/prediction_hourly_data_hist.py:
    resolve_event      ← safe_get_event       (lines 112-143)
    resolve_market     ← get_market_ticker     (lines 146-167)
    get_market_metadata ← get_market_metadata  (lines 170-198)
"""

from datetime import datetime
from types import SimpleNamespace

from kalshi_python_sync.exceptions import NotFoundException

from kalshi_io.client import BASE_URL, client, session


def resolve_event(event_ticker: str) -> SimpleNamespace:
    """
    Resolve an event ticker to its series_ticker and market list.

    3-tier fallback:
        1. SDK get_event
        2. REST GET /events/{event_ticker}
        3. Derive series_ticker from prefix

    Returns:
        SimpleNamespace with .event.series_ticker and .markets (list).
    """
    # Tier 1: SDK
    try:
        return client.get_event(event_ticker=event_ticker)
    except NotFoundException:
        pass

    # Tier 2: REST
    resp = session.get(f"{BASE_URL}/events/{event_ticker}")
    if resp.status_code == 200:
        data = resp.json()
        return SimpleNamespace(
            event=SimpleNamespace(series_ticker=data["event"]["series_ticker"]),
            markets=[
                SimpleNamespace(ticker=m["ticker"])
                for m in data.get("markets", [])
            ],
        )

    # Tier 3: derive from prefix
    series_ticker = event_ticker.rsplit("-", 1)[0]
    if not series_ticker.startswith("KX"):
        series_ticker = "KX" + series_ticker

    return SimpleNamespace(
        event=SimpleNamespace(series_ticker=series_ticker),
        markets=[],
    )


def resolve_market(event: SimpleNamespace, event_ticker: str) -> str | None:
    """
    Resolve a market ticker from an event.

    4-tier fallback:
        1. event.markets[0].ticker
        2. REST GET /markets?event_ticker=...&status=all
        3. REST GET /historical/markets?event_ticker=...
        4. Try event_ticker as market_ticker via /historical/markets/{ticker}

    Returns:
        Market ticker string, or None if all methods fail.
    """
    # Tier 1: from event object
    if event.markets:
        return event.markets[0].ticker

    # Tier 2: live markets endpoint
    resp = session.get(
        f"{BASE_URL}/markets",
        params={"event_ticker": event_ticker, "status": "all"},
    )
    markets = resp.json().get("markets", [])
    if markets:
        return markets[0]["ticker"]

    # Tier 3: historical markets endpoint
    resp = session.get(
        f"{BASE_URL}/historical/markets",
        params={"event_ticker": event_ticker},
    )
    markets = resp.json().get("markets", [])
    if markets:
        return markets[0]["ticker"]

    # Tier 4: event_ticker == market_ticker for very old contracts
    resp = session.get(f"{BASE_URL}/historical/markets/{event_ticker}")
    if resp.status_code == 200 and resp.json().get("market"):
        return event_ticker

    return None


def get_market_metadata(market_ticker: str) -> dict:
    """
    Get open_ts_ms, expiration_time, and status for a market.

    2-tier fallback:
        1. SDK get_market
        2. REST GET /historical/markets/{ticker}

    Returns:
        {"open_ts_ms": int | None, "expiration_time": str, "status": str}
        open_ts_ms is int64 UTC milliseconds.
    """
    # Tier 1: SDK
    try:
        market = client.get_market(ticker=market_ticker)
        m = market.market
        open_time = m.open_time
        if hasattr(open_time, "timestamp"):
            open_ts_ms = int(open_time.timestamp() * 1000)
        else:
            open_ts_ms = int(
                datetime.fromisoformat(str(open_time)).timestamp() * 1000
            )
        return {
            "open_ts_ms": open_ts_ms,
            "expiration_time": str(
                getattr(m, "expiration_time", None)
                or getattr(m, "latest_expiration_time", "unknown")
            ),
            "status": m.status,
        }
    except NotFoundException:
        pass
    except Exception as e:
        print(f"  Warning: unexpected SDK error: {e}")

    # Tier 2: REST historical
    resp = session.get(f"{BASE_URL}/historical/markets/{market_ticker}")
    if resp.status_code == 200:
        m = resp.json().get("market", {})
        open_time_str = m.get("open_time", "")
        if open_time_str:
            open_ts_ms = int(
                datetime.fromisoformat(
                    open_time_str.replace("Z", "+00:00")
                ).timestamp()
                * 1000
            )
        else:
            open_ts_ms = None
        return {
            "open_ts_ms": open_ts_ms,
            "expiration_time": (
                m.get("expiration_time")
                or m.get("latest_expiration_time")
                or "unknown"
            ),
            "status": m.get("status", "unknown"),
        }

    return {"open_ts_ms": None, "expiration_time": "unknown", "status": "unknown"}
