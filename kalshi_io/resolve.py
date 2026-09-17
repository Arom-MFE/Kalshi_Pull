"""
kalshi_io/resolve.py — Event, market, and metadata resolution.

All lookups are keyless REST calls through kalshi_io.discovery, so HTTP
failures are retried and then raised; only a 404 counts as "not there".
"""

from datetime import datetime
from types import SimpleNamespace

from kalshi_io import candles, discovery
from kalshi_io.discovery import status_bucket
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


# ============================================================
# A market's life: where a candle pull starts and where it may stop
# ============================================================

# A pull for a settled market stops this many periods after close_time. The
# period that contains close_time has one last candle and nothing follows it.
# One period would do on most days; the second covers the 25 hour day on which
# daylight saving time ends (daily candles end at midnight Eastern time).
CLOSE_PAD_PERIODS = 2


def market_window(market_ticker: str, allow_api: bool = True) -> dict:
    """
    When a market opened, when it closed, its status and its API tier.

    The committed catalog (format v2) answers without a request; an API
    answer registered earlier in the process is preferred because it is
    newer. Only a ticker that neither knows, or a v1 catalog record without
    times, costs a lookup (live tier, then /historical/), and that answer is
    remembered for the process.

    Args:
        market_ticker: e.g. "KXCPIYOY-26JUL-T3.5"
        allow_api:     False never makes a request; an unknown ticker then
                       gives open_ts None and status "unknown". The pullers
                       pass False on every resume, so a long-running poller
                       never pays a request per ticker and cycle for this.

    Returns:
        {"open_ts": int | None, "close_ts": int | None, "status": str,
         "tier": "live" | "historical" | "", "source": "catalog" | "api" | "unknown"},
        times in Unix seconds.

    Raises:
        kalshi_io.client.KalshiAPIError: the lookup failed for a reason other
        than "not found", so a cold start is never silently skipped because
        the API was down.
    """
    known = candles.known_market_window(market_ticker)
    if known is not None and (known.open_ts is not None or not allow_api):
        return {**known._asdict(), "source": "catalog"}
    if not allow_api:
        return {"open_ts": None, "close_ts": None, "status": "unknown", "tier": "", "source": "unknown"}

    m = discovery.get_market(market_ticker)
    if m is None:
        return {"open_ts": None, "close_ts": None, "status": "unknown", "tier": "", "source": "unknown"}
    window = candles.MarketWindow(candles.iso_to_ts(m.get("open_time")), candles.iso_to_ts(m.get("close_time")),
                                  m.get("status") or "", m.get("tier") or "")
    candles.register_market_windows({market_ticker: window})
    return {**window._asdict(), "source": "api"}


def candle_end_ts(window: dict, interval: int, now_ts: int) -> int:
    """
    Where a candle pull for this market may stop (Unix seconds).

    A settled market (status finalized, which is terminal) is pulled up to
    close_time plus CLOSE_PAD_PERIODS periods: the period that contains
    close_time has one last candle (a 12:29:00 close has a minute candle at
    12:30:00, an hourly one at 13:00, a daily one at the next midnight
    Eastern time) and nothing follows it, so every later window would come
    back empty. A window that ends exactly at close_time would lose that last
    candle.

    Every other status is pulled up to now, also closed and determined: a
    closed market can be reopened with a later close_time, and a recorded
    status may be stale. Erring on that side costs a few empty windows;
    erring on the other side would lose data without a trace.

    Args:
        window:   market_window() result
        interval: period_interval in minutes (1, 60, 1440)
        now_ts:   the current Unix second
    """
    close_ts = window.get("close_ts")
    if close_ts is None or status_bucket(window.get("status")) != "settled":
        return now_ts
    return min(now_ts, close_ts + CLOSE_PAD_PERIODS * interval * 60)
