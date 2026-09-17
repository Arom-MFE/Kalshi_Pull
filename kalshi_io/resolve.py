"""
kalshi_io/resolve.py — Event, market, and metadata resolution.

All lookups are keyless REST calls through kalshi_io.discovery, so HTTP
failures are retried and then raised; only a 404 counts as "not there".
"""

from datetime import datetime
from types import SimpleNamespace

from kalshi_io import discovery
from kalshi_io.runlog import get_logger

logger = get_logger("resolve")


def resolve_event(event_ticker: str) -> SimpleNamespace:
    """
    Resolve an event ticker to its series_ticker and market list.

    2-tier fallback:
        1. REST GET /events/{event_ticker}
        2. Derive series_ticker from the prefix (event unknown to the API)

    Returns:
        SimpleNamespace with .event.series_ticker and .markets (list of
        objects with .ticker).
    """
    # Tier 1: REST
    event = discovery.get_event(event_ticker)
    if event is not None:
        return SimpleNamespace(
            event=SimpleNamespace(series_ticker=event["series_ticker"]),
            markets=[SimpleNamespace(ticker=m["ticker"]) for m in event["markets"]],
        )

    # Tier 2: derive from prefix
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
        2. REST GET /markets?event_ticker=... (no status filter = every
           status; the API rejects status=all with HTTP 400)
        3. REST GET /historical/markets?event_ticker=...
        4. Try event_ticker as market_ticker (very old single-market events)

    Returns:
        Market ticker string, or None if all methods fail.
    """
    # Tier 1: from event object
    if event.markets:
        return event.markets[0].ticker

    # Tier 2: live markets endpoint
    markets = discovery.list_markets(event_ticker=event_ticker)
    if markets:
        return markets[0]["ticker"]

    # Tier 3: historical markets endpoint
    markets = discovery.list_historical_markets(event_ticker=event_ticker)
    if markets:
        return markets[0]["ticker"]

    # Tier 4: event_ticker == market_ticker for very old contracts
    if discovery.get_market(event_ticker) is not None:
        return event_ticker

    return None


def get_market_metadata(market_ticker: str) -> dict:
    """
    Get open_ts_ms, expiration_time, and status for a market.

    Looks the market up on the live tier, then on /historical/ (markets
    settled before the cutoff 404 on the live endpoint).

    Returns:
        {"open_ts_ms": int | None, "expiration_time": str, "status": str}
        open_ts_ms is int64 UTC milliseconds. A market found in neither tier
        gives open_ts_ms None and "unknown" for the rest.

    Raises:
        kalshi_io.client.KalshiAPIError: the lookup failed for a reason other
        than "not found" (exhausted retries included), so a cold start is
        never silently skipped because the API was down.
    """
    m = discovery.get_market(market_ticker)
    if m is None:
        return {"open_ts_ms": None, "expiration_time": "unknown", "status": "unknown"}

    open_time_str = m.get("open_time") or ""
    if open_time_str:
        open_ts_ms = int(
            datetime.fromisoformat(open_time_str.replace("Z", "+00:00")).timestamp() * 1000
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
        "status": m.get("status") or "unknown",
    }
