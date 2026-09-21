"""
kalshi_io/discovery.py — Find series, events and markets. Keyless, stateless.

All calls are public REST endpoints (no API key) and go through
client.request_json / client.paginate, so they are rate limited, retried, and
fail loudly. Nothing here reads or writes files; the catalog and the focus
universe are built on top of these functions.

Status vocabulary (verified against the OpenAPI spec and the live API):
    filter values    /markets: unopened, open, paused, closed, settled
                     /events:  unopened, open, closed, settled
                     One per request. No filter = every status. "all" is
                     rejected with HTTP 400.
    response values  initialized, inactive, active, closed, determined,
                     disputed, amended, finalized
    STATUS_BUCKET maps a response status to the filter word that selects it.
    An event has no status of its own: the /events filter matches when ANY of
    its markets matches.
"""

import re

from kalshi_io import client
from kalshi_io.client import KalshiNotFound, path_part
from kalshi_io.runlog import get_logger

logger = get_logger("discovery")

MARKET_STATUS_FILTERS = ("unopened", "open", "paused", "closed", "settled")
EVENT_STATUS_FILTERS = ("unopened", "open", "closed", "settled")

STATUS_BUCKET = {
    "initialized": "unopened",
    "active": "open",
    "inactive": "paused",
    "closed": "closed",
    "determined": "closed",
    "disputed": "closed",
    "amended": "closed",
    "finalized": "settled",
}

# Buckets in which a market can still trade, now or later
POLLABLE_BUCKETS = frozenset({"open", "paused", "unopened"})

_TICKERS_PER_REQUEST = 100

# The `tickers=` list form joins tickers with commas, so it cannot name a ticker
# that holds one, and it returns nothing for a ticker that holds a space
# ("GDP-232022 Q4-T0.0", "JOBLESS-22JUL23-C250,000"; observed 2026-09-21). The
# single-market routes serve both once the ticker is URL-quoted in the path.
_NOT_LISTABLE = re.compile(r"[\s,]")

_event_series_cache: dict[str, str | None] = {}
_market_meta_cache: dict[str, tuple[str | None, str] | None] = {}


def status_bucket(status: str | None) -> str:
    """Map a response status ("active", "finalized", ...) to its filter word
    ("open", "settled", ...). Unmapped or missing statuses give "unknown"."""
    return STATUS_BUCKET.get(status or "", "unknown")


def _check_status(status: str | None, allowed: tuple[str, ...], endpoint: str) -> None:
    """Reject an invalid status filter before any request is made."""
    if status is None:
        return
    if status not in allowed:
        hint = " Omit status to get every status." if status == "all" else ""
        raise ValueError(
            f"invalid status filter {status!r} for {endpoint}: use one of "
            f"{', '.join(allowed)} (one per request).{hint}"
        )


def _chunks(items: list[str], size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


# ============================================================
# Series
# ============================================================

def list_categories() -> dict[str, list[str]]:
    """Return {category: [tags]} for every series category on Kalshi."""
    data = client.request_json("/search/tags_by_categories")
    return {cat: list(tags or []) for cat, tags in (data.get("tags_by_categories") or {}).items()}


def list_series(category: str | None = None, tags: str | list[str] | None = None) -> list[dict]:
    """
    List series, optionally filtered by category and/or tags.

    Args:
        category: exact, case-sensitive category name ("Economics",
                  "Financials", ...). The API matches it against each series'
                  `categories` list, so a returned series can have a different
                  primary `category`. An unknown name raises ValueError with a
                  suggestion (the API itself would silently return nothing).
        tags:     one tag or a list; a series matches if it has any of them.

    Returns:
        Series dicts as sent by the API, with `tags` normalized to a list and
        one added key: `legacy_twin` is True for a pre-KX spelling ("CPIYOY")
        whose KX series ("KXCPIYOY") is in the same listing. Twins are dead:
        their events moved to the KX series.
    """
    params: dict = {}
    if category:
        params["category"] = category
    if tags:
        params["tags"] = tags if isinstance(tags, str) else ",".join(tags)

    rows = client.request_json("/series", params).get("series") or []

    if category and not rows:
        known = list_categories()
        if category not in known:
            close = [c for c in known if c.lower() == category.lower()]
            hint = f" Did you mean {close[0]!r}?" if close else ""
            raise ValueError(
                f"unknown category {category!r}.{hint} Valid categories: {', '.join(sorted(known))}"
            )

    tickers = {s.get("ticker") for s in rows}
    out = []
    for s in rows:
        s = dict(s)
        s["tags"] = list(s.get("tags") or [])
        ticker = s.get("ticker") or ""
        s["legacy_twin"] = (not ticker.startswith("KX")) and ("KX" + ticker) in tickers
        out.append(s)
    return out


def search_series(
    keyword: str,
    category: str | None = None,
    tags: str | list[str] | None = None,
    include_legacy: bool = False,
) -> list[dict]:
    """
    Keyword search across series tickers, titles and tags.

    The API has no search parameter, so matching is client-side: every
    whitespace-separated term must occur (case-insensitive) in the ticker,
    the title or a tag. Without a category the full series list (several MB)
    is fetched once per call.

    Returns:
        Matching series dicts (see list_series), legacy twins excluded unless
        include_legacy is True.
    """
    terms = keyword.lower().split()
    matches = []
    for s in list_series(category=category, tags=tags):
        if s["legacy_twin"] and not include_legacy:
            continue
        haystack = " ".join([s.get("ticker") or "", s.get("title") or "", *s["tags"]]).lower()
        if all(term in haystack for term in terms):
            matches.append(s)
    return matches


# ============================================================
# Events
# ============================================================

def list_events(
    series_ticker: str,
    status: str | None = None,
    with_nested_markets: bool = False,
) -> list[dict]:
    """
    List the events of a series.

    Args:
        series_ticker:       e.g. "KXCPIYOY". The KX series also returns its
                             pre-KX events (CPIYOY-22DEC, ...).
        status:              None for every event, or one of
                             EVENT_STATUS_FILTERS. "open" = at least one
                             market is open.
        with_nested_markets: include each event's live-tier markets under
                             "markets" (always a list here; the API omits the
                             key when an event has none).

    Returns:
        Event dicts. All events are listed, however old; only their markets
        move to the historical tier.
    """
    _check_status(status, EVENT_STATUS_FILTERS, "/events")
    params: dict = {"series_ticker": series_ticker, "status": status}
    if with_nested_markets:
        params["with_nested_markets"] = "true"
    events = list(client.paginate("/events", params, key="events", limit=200))
    if with_nested_markets:
        for e in events:
            e["markets"] = list(e.get("markets") or [])
    return events


def get_event(event_ticker: str) -> dict | None:
    """
    Fetch one event with its live-tier markets.

    Returns:
        The event dict with "markets" always present as a list (the API puts
        them at the top level or inside the event depending on a flag), or
        None if the event does not exist.
    """
    try:
        data = client.request_json(f"/events/{path_part(event_ticker)}", {"with_nested_markets": "true"})
    except KalshiNotFound:
        return None
    event = dict(data.get("event") or {})
    event["markets"] = list(event.get("markets") or data.get("markets") or [])
    return event


# ============================================================
# Markets
# ============================================================

def _one_selector(event_ticker, series_ticker, tickers) -> None:
    given = [name for name, value in (
        ("event_ticker", event_ticker), ("series_ticker", series_ticker), ("tickers", tickers),
    ) if value]
    if len(given) != 1:
        raise ValueError(
            "pass exactly one of event_ticker, series_ticker, tickers"
            + (f" (got {', '.join(given)})" if given else "")
        )


def _list_markets(path: str, event_ticker, series_ticker, tickers, status=None) -> list[dict]:
    if tickers:
        out: list[dict] = []
        for chunk in _chunks(list(tickers), _TICKERS_PER_REQUEST):
            out.extend(client.paginate(path, {"tickers": ",".join(chunk), "status": status}, key="markets"))
        return out
    params = {"event_ticker": event_ticker, "series_ticker": series_ticker, "status": status}
    return list(client.paginate(path, params, key="markets"))


def list_markets(
    event_ticker: str | None = None,
    series_ticker: str | None = None,
    status: str | None = None,
    tickers: list[str] | None = None,
) -> list[dict]:
    """
    List live-tier markets for one event, one series, or a list of tickers.

    Args:
        event_ticker / series_ticker / tickers: exactly one selector
        status: None for every status, or one of MARKET_STATUS_FILTERS

    Returns:
        Market dicts. Markets settled before the historical cutoff are not
        here; see list_historical_markets. Market payloads carry event_ticker
        but no series_ticker.
    """
    _check_status(status, MARKET_STATUS_FILTERS, "/markets")
    _one_selector(event_ticker, series_ticker, tickers)
    return _list_markets("/markets", event_ticker, series_ticker, tickers, status)


def list_historical_markets(
    event_ticker: str | None = None,
    series_ticker: str | None = None,
    tickers: list[str] | None = None,
) -> list[dict]:
    """List historical-tier markets (settled before the cutoff). The endpoint's
    filters are mutually exclusive, so exactly one selector is required."""
    _one_selector(event_ticker, series_ticker, tickers)
    return _list_markets("/historical/markets", event_ticker, series_ticker, tickers)


def find_markets(
    event_ticker: str | None = None,
    series_ticker: str | None = None,
    status: str | None = None,
) -> list[dict]:
    """
    List the markets of one event or one series across both tiers.

    The live tier is asked with the status filter. The historical tier has no
    status filter and holds settled markets only, so it is added when status
    is None or "settled". The tiers overlap near the cutoff; the live record
    wins.

    Returns:
        Market dicts, each with "tier" ("live" or "historical"), sorted by
        event ticker and market ticker.
    """
    _check_status(status, MARKET_STATUS_FILTERS, "/markets")
    _one_selector(event_ticker, series_ticker, None)
    found: dict[str, dict] = {}
    if status in (None, "settled"):
        for m in list_historical_markets(event_ticker=event_ticker, series_ticker=series_ticker):
            found[m["ticker"]] = {**m, "tier": "historical"}
    for m in list_markets(event_ticker=event_ticker, series_ticker=series_ticker, status=status):
        found[m["ticker"]] = {**m, "tier": "live"}
    return sorted(found.values(), key=lambda m: (m.get("event_ticker") or "", m["ticker"]))


def get_market(ticker: str) -> dict | None:
    """
    Fetch one market, trying the live tier and then the historical tier.

    Returns:
        The market dict plus "tier" ("live" or "historical"), or None if the
        ticker exists in neither tier.
    """
    for tier, path in (("live", "/markets/"), ("historical", "/historical/markets/")):
        try:
            data = client.request_json(f"{path}{path_part(ticker)}")
        except KalshiNotFound:
            continue
        market = dict(data.get("market") or {})
        market["tier"] = tier
        return market
    return None


def lookup_markets(tickers: list[str]) -> dict[str, dict]:
    """
    Batch lookup across both tiers.

    Tickers go through the `tickers=` list form, 100 per request: the live
    tier, then the historical tier for what the live tier did not return. A
    ticker that holds whitespace or a comma cannot be found that way (the
    list form answers 200 without it) and is looked up on its own through
    get_market(): at most two requests for each such ticker.

    Returns:
        {ticker: market dict with "tier"}. Tickers that exist nowhere are
        simply absent (the API omits them without an error).
    """
    wanted = list(dict.fromkeys(tickers))
    single = [t for t in wanted if _NOT_LISTABLE.search(t)]
    listable = [t for t in wanted if not _NOT_LISTABLE.search(t)]
    found: dict[str, dict] = {}
    for tier, lister in (("live", list_markets), ("historical", list_historical_markets)):
        missing = [t for t in listable if t not in found]
        if not missing:
            break
        for market in lister(tickers=missing):
            market = dict(market)
            market["tier"] = tier
            found.setdefault(market["ticker"], market)
    for ticker in single:
        market = get_market(ticker)
        if market is not None:
            found[ticker] = market
    return found


def resolve_market_meta(ticker: str) -> tuple[str | None, str] | None:
    """
    Resolve a market ticker to (series_ticker, event_ticker) through the API.

    Market payloads have no series_ticker, so this is two steps: the market
    gives the event, the event gives the series (the KX spelling, also for
    pre-KX events). Results, including "not found", are cached per process.

    Returns:
        (series_ticker, event_ticker); series_ticker is None when the event
        lookup 404s. None when the market exists in neither tier.
    """
    if ticker in _market_meta_cache:
        return _market_meta_cache[ticker]

    market = get_market(ticker)
    if market is None or not market.get("event_ticker"):
        _market_meta_cache[ticker] = None
        return None

    event_ticker = market["event_ticker"]
    if event_ticker not in _event_series_cache:
        event = get_event(event_ticker)
        _event_series_cache[event_ticker] = (event or {}).get("series_ticker") or None
    _market_meta_cache[ticker] = (_event_series_cache[event_ticker], event_ticker)
    return _market_meta_cache[ticker]


def get_historical_cutoff() -> dict:
    """Return the live/historical boundary timestamps (ISO strings), e.g.
    {"market_settled_ts": ..., "trades_created_ts": ...}."""
    return client.request_json("/historical/cutoff")


def _reset_state() -> None:
    """Drop lookup caches (test isolation)."""
    _event_series_cache.clear()
    _market_meta_cache.clear()
