"""Offline stand-in for the Kalshi REST API. Synthetic data only.

FakeKalshi is a drop-in for the requests.Session that kalshi_io.client uses.
Its behavior mirrors what was verified against the live API on 2026-09-17:

- status=all (or any value outside the documented enum) answers the real 400
  bodies; /markets accepts only one status per request
- filter values and response statuses are different vocabularies
  (open -> active, settled -> finalized, closed -> closed/determined/...)
- markets live in a "live" and a "historical" tier that can overlap; the live
  single-market and candle endpoints 404 for historical-only markets
- /events with nested markets omits the "markets" key (it is not an empty
  list) when an event has no live-tier market; GET /events/{t} moves the
  markets between the top level and the event depending on the flag
- candle windows are inclusive on both ends and windows over 5,000 candles
  are rejected with 400, on both tiers
- trades come back newest-first, min_ts is inclusive at second granularity,
  and the end of pagination is an empty cursor string
"""

import json
import re
from datetime import datetime

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

MARKET_FILTER_TO_STATUSES = {
    "unopened": {"initialized"},
    "open": {"active"},
    "paused": {"inactive"},
    "closed": {"closed", "determined", "disputed", "amended"},
    "settled": {"finalized"},
}
EVENT_FILTERS = ("unopened", "open", "closed", "settled")

NOT_FOUND = {"error": {"code": "not_found", "message": "not found"}}
HIST_NOT_FOUND = {"error": {
    "code": "failed_to_get_market_by_ticker:_not_found",
    "message": "failed to get market by ticker: not found",
}}
BAD_STATUS_MARKETS = {"error": {
    "code": "bad_request", "message": "bad request", "details": "invalid status filter",
}}
BAD_STATUS_EVENTS = {"error": {"code": "bad_request", "message": "bad request"}}
MULTI_STATUS = {"error": {
    "code": "bad_request", "message": "bad request",
    "details": "only one status filter may be supplied",
}}
TOO_MANY_REQUESTS = {"error": "too many requests"}


def iso_to_ts(iso: str) -> float:
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()


class FakeResponse:
    """Just enough of requests.Response for kalshi_io.client."""

    def __init__(self, status_code=200, payload=None, headers=None, text=None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = text if text is not None else json.dumps(payload)

    def json(self):
        if self._payload is None:
            raise ValueError("response body is not JSON")
        return self._payload


def make_market(ticker, event_ticker, status="active", tier="live",
                open_time="2026-06-09T22:25:00Z", close_time="2026-10-14T12:29:00Z",
                title=None, **extra):
    """Market payload with the keys this repo reads. No series_ticker, as on the wire."""
    market = {
        "ticker": ticker,
        "event_ticker": event_ticker,
        "title": title or f"Will {ticker} resolve yes?",
        "status": status,
        "open_time": open_time,
        "close_time": close_time,
        "expected_expiration_time": "2026-10-14T14:00:00Z",
        "latest_expiration_time": "2027-01-13T14:00:00Z",
        "expiration_time": "2027-01-13T14:00:00Z",
        "result": "" if status not in ("determined", "finalized") else "no",
        "_tier": tier,
    }
    market.update(extra)
    return market


def make_event(event_ticker, series_ticker, title=None, listed=True, **extra):
    """Event payload as sent since 2026-09-10: no available_on_brokers key."""
    event = {
        "event_ticker": event_ticker,
        "series_ticker": series_ticker,
        "title": title or f"Event {event_ticker}",
        "sub_title": "",
        "category": "Economics",
        "collateral_return_type": "binary",
        "mutually_exclusive": False,
        "strike_period": "",
        "settlement_sources": [{"name": "BLS", "url": "https://www.bls.gov/"}],
        "last_updated_ts": "2026-09-16T14:02:11Z",
        "exchange_index": 0,
        "_listed": listed,
    }
    event.update(extra)
    return event


def make_trade(trade_id, ticker, created_time, yes_price="0.5000", count="10.00",
               taker_side="yes", **extra):
    trade = {
        "trade_id": trade_id,
        "ticker": ticker,
        "created_time": created_time,
        "yes_price_dollars": yes_price,
        "no_price_dollars": f"{1 - float(yes_price):.4f}",
        "count_fp": count,
        "taker_side": taker_side,
        "taker_outcome_side": taker_side,
        "taker_book_side": "bid" if taker_side == "yes" else "ask",
        "is_block_trade": False,
    }
    trade.update(extra)
    return trade


def make_candle(end_period_ts, close="0.5000", volume="10.00", open_interest="100.00",
                traded=True):
    """Canonical candle; rendered into the live or historical wire shape on serve."""
    return {
        "end_period_ts": end_period_ts,
        "close": close,
        "volume": volume,
        "open_interest": open_interest,
        "traded": traded,
    }


def _public(record: dict) -> dict:
    return {k: v for k, v in record.items() if not k.startswith("_")}


class FakeKalshi:
    """Semantic fake exchange + recorder. Use it as the HTTP session."""

    def __init__(self):
        self.series: list[dict] = []
        self.tags_by_categories: dict[str, list[str]] = {}
        self.events: dict[str, dict] = {}
        self.markets: dict[str, dict] = {}
        self.candles: dict[str, dict[int, list[dict]]] = {}
        self.trades: dict[str, list[dict]] = {}
        self.orderbooks: dict[str, dict] = {}
        self.cutoff = "2026-07-19T00:00:00Z"
        self.page_size: int | None = None        # force small pages in tests
        self.auth_required: set[str] = set()      # path prefixes that answer 401 keyless
        self.valid_key_ids: set[str] = {"test-key"}
        self.calls: list[tuple[str, dict, dict]] = []
        self._injected: list[tuple[re.Pattern, list]] = []

    # ---------------------------------------------------------- setup helpers
    def add_event(self, event: dict, markets=()):
        self.events[event["event_ticker"]] = event
        for m in markets:
            self.markets[m["ticker"]] = m
        return event

    def add_markets(self, *markets: dict):
        for m in markets:
            self.markets[m["ticker"]] = m

    def inject(self, path_regex: str, responses: list):
        """Queue forced outcomes for matching paths: FakeResponse or Exception
        instances, consumed one per matching request, then normal service."""
        self._injected.append((re.compile(path_regex), list(responses)))

    def requests_to(self, path_prefix: str) -> list[tuple[str, dict, dict]]:
        return [c for c in self.calls if c[0].startswith(path_prefix)]

    # ---------------------------------------------------------- session API
    def get(self, url, params=None, headers=None, timeout=None):
        assert url.startswith(BASE_URL), f"unexpected host in {url}"
        path = url[len(BASE_URL):]
        params = {k: v for k, v in (params or {}).items() if v is not None}
        headers = dict(headers or {})
        self.calls.append((path, dict(params), headers))

        for pattern, queue in self._injected:
            if queue and pattern.search(path):
                outcome = queue.pop(0)
                if isinstance(outcome, BaseException):
                    raise outcome
                return outcome

        if any(path.startswith(p) for p in self.auth_required):
            if headers.get("KALSHI-ACCESS-KEY") not in self.valid_key_ids:
                return FakeResponse(401, {"error": {
                    "code": "unauthorized", "message": "authentication required"}})

        return self._route(path, params)

    # ---------------------------------------------------------- routing
    def _route(self, path: str, p: dict) -> FakeResponse:
        if path == "/series":
            return self._series(p)
        if path == "/search/tags_by_categories":
            return FakeResponse(200, {"tags_by_categories": self.tags_by_categories})
        if path == "/historical/cutoff":
            return FakeResponse(200, {
                "market_settled_ts": self.cutoff,
                "trades_created_ts": self.cutoff,
                "orders_updated_ts": self.cutoff,
                "market_positions_last_updated_ts": self.cutoff,
            })
        if path == "/events":
            return self._events(p)
        if path == "/markets":
            return self._markets(p, tier="live")
        if path == "/historical/markets":
            return self._markets(p, tier="historical")
        if path == "/markets/trades":
            return self._trades(p, tier="live")
        if path == "/historical/trades":
            return self._trades(p, tier="historical")

        m = re.fullmatch(r"/events/([^/]+)", path)
        if m:
            return self._event(m.group(1), p)
        m = re.fullmatch(r"/markets/([^/]+)/orderbook", path)
        if m:
            return self._orderbook(m.group(1))
        m = re.fullmatch(r"/markets/([^/]+)", path)
        if m:
            return self._market(m.group(1), tier="live")
        m = re.fullmatch(r"/historical/markets/([^/]+)/candlesticks", path)
        if m:
            return self._candles(m.group(1), p, tier="historical")
        m = re.fullmatch(r"/historical/markets/([^/]+)", path)
        if m:
            return self._market(m.group(1), tier="historical")
        m = re.fullmatch(r"/series/([^/]+)/markets/([^/]+)/candlesticks", path)
        if m:
            return self._candles(m.group(2), p, tier="live")
        return FakeResponse(404, NOT_FOUND)

    # ---------------------------------------------------------- helpers
    def _in_tier(self, market: dict, tier: str) -> bool:
        return market["_tier"] in (tier, "both")

    def _page(self, items: list, p: dict, key: str, max_limit: int, extra=None):
        limit = int(p.get("limit") or 100)
        if limit > max_limit or limit < 0:
            return FakeResponse(400, {"error": {
                "code": "bad_request", "message": "bad request",
                "details": f"limit must be <= {max_limit}"}})
        if self.page_size:
            limit = min(limit, self.page_size)
        start = int(p.get("cursor") or 0)
        chunk = items[start:start + limit]
        nxt = start + limit
        body = {key: chunk, "cursor": str(nxt) if nxt < len(items) else ""}
        body.update(extra or {})
        return FakeResponse(200, body)

    def _series_of(self, market: dict) -> str | None:
        event = self.events.get(market["event_ticker"])
        return event["series_ticker"] if event else None

    # ---------------------------------------------------------- endpoints
    def _series(self, p):
        rows = self.series
        if "category" in p:
            rows = [s for s in rows
                    if p["category"] == s.get("category") or p["category"] in (s.get("categories") or [])]
        if "tags" in p:
            wanted = set(p["tags"].split(","))
            rows = [s for s in rows if wanted & set(s.get("tags") or [])]
        return FakeResponse(200, {"series": rows})

    def _events(self, p):
        status = p.get("status")
        if status is not None and status not in EVENT_FILTERS:
            return FakeResponse(400, BAD_STATUS_EVENTS)
        nested = str(p.get("with_nested_markets", "false")).lower() == "true"
        rows = []
        for ev in self.events.values():
            if not ev["_listed"]:
                continue
            if "series_ticker" in p and ev["series_ticker"] != p["series_ticker"]:
                continue
            children = [m for m in self.markets.values() if m["event_ticker"] == ev["event_ticker"]]
            if status is not None:
                wanted = MARKET_FILTER_TO_STATUSES[status]
                if not any(m["status"] in wanted for m in children):
                    continue
            out = _public(ev)
            if nested:
                live = [_public(m) for m in children if self._in_tier(m, "live")]
                if live:                      # key is absent, not [], when nothing is live
                    out["markets"] = live
            rows.append(out)
        return self._page(rows, p, "events", max_limit=200, extra={"milestones": []})

    def _event(self, event_ticker, p):
        ev = self.events.get(event_ticker)
        if ev is None:
            return FakeResponse(404, NOT_FOUND)
        nested = str(p.get("with_nested_markets", "false")).lower() == "true"
        live = [_public(m) for m in self.markets.values()
                if m["event_ticker"] == event_ticker and self._in_tier(m, "live")]
        out = _public(ev)
        if nested:
            if live:
                out["markets"] = live
            return FakeResponse(200, {"event": out, "markets": []})
        return FakeResponse(200, {"event": out, "markets": live})

    def _markets(self, p, tier):
        status = p.get("status")
        if tier == "live" and status is not None:
            if "," in status:
                return FakeResponse(400, MULTI_STATUS)
            if status not in MARKET_FILTER_TO_STATUSES:
                return FakeResponse(400, BAD_STATUS_MARKETS)
        if tier == "historical":
            if sum(k in p for k in ("tickers", "event_ticker", "series_ticker")) > 1:
                return FakeResponse(400, {"error": {
                    "code": "bad_request", "message": "bad request",
                    "details": "filters are mutually exclusive"}})
        rows = [m for m in self.markets.values() if self._in_tier(m, tier)]
        if "event_ticker" in p:
            rows = [m for m in rows if m["event_ticker"] == p["event_ticker"]]
        if "series_ticker" in p:
            rows = [m for m in rows if self._series_of(m) == p["series_ticker"]]
        if "tickers" in p:
            wanted = set(p["tickers"].split(","))
            rows = [m for m in rows if m["ticker"] in wanted]
        if tier == "live" and status is not None:
            rows = [m for m in rows if m["status"] in MARKET_FILTER_TO_STATUSES[status]]
        return self._page([_public(m) for m in rows], p, "markets", max_limit=1000)

    def _market(self, ticker, tier):
        m = self.markets.get(ticker)
        if m is None or not self._in_tier(m, tier):
            return FakeResponse(404, NOT_FOUND if tier == "live" else HIST_NOT_FOUND)
        return FakeResponse(200, {"market": _public(m)})

    def _orderbook(self, ticker):
        if ticker not in self.markets:
            return FakeResponse(404, NOT_FOUND)
        book = self.orderbooks.get(ticker, {"yes_dollars": [], "no_dollars": []})
        return FakeResponse(200, {"orderbook_fp": book})

    def _candles(self, ticker, p, tier):
        m = self.markets.get(ticker)
        if m is None or not self._in_tier(m, tier):
            return FakeResponse(404, NOT_FOUND if tier == "live" else HIST_NOT_FOUND)
        start, end, interval = int(p["start_ts"]), int(p["end_ts"]), int(p["period_interval"])
        if interval not in (1, 60, 1440):
            return FakeResponse(400, {"error": {"code": "bad_request", "message": "bad request"}})
        n = (end - start) / (interval * 60)
        if n > 5000:
            return FakeResponse(400, {"error": {
                "code": "bad_request", "message": "bad request",
                "details": f"requested time range with candlesticks: {n:.6f}, max candlesticks: 5000"}})
        rows = [c for c in self.candles.get(ticker, {}).get(interval, [])
                if start <= c["end_period_ts"] <= end]
        render = self._live_candle if tier == "live" else self._hist_candle
        return FakeResponse(200, {"ticker": ticker, "candlesticks": [render(c) for c in rows]})

    @staticmethod
    def _live_candle(c):
        price = {"previous_dollars": c["close"]}
        if c["traded"]:                       # no-trade candles omit the OHLC keys
            price.update({k + "_dollars": c["close"] for k in ("open", "high", "low", "close", "mean")})
        quote = {k + "_dollars": c["close"] for k in ("open", "high", "low", "close")}
        return {"end_period_ts": c["end_period_ts"], "price": price, "yes_bid": quote,
                "yes_ask": quote, "volume_fp": c["volume"], "open_interest_fp": c["open_interest"]}

    @staticmethod
    def _hist_candle(c):
        price = {"previous": c["close"]}
        if c["traded"]:
            price.update({k: c["close"] for k in ("open", "high", "low", "close", "mean")})
        quote = {k: c["close"] for k in ("open", "high", "low", "close")}
        return {"end_period_ts": c["end_period_ts"], "price": price, "yes_bid": quote,
                "yes_ask": quote, "volume": c["volume"], "open_interest": c["open_interest"]}

    def _trades(self, p, tier):
        cutoff = iso_to_ts(self.cutoff)
        rows = list(self.trades.get(p.get("ticker"), []))
        if tier == "live":
            rows = [t for t in rows if iso_to_ts(t["created_time"]) >= cutoff]
        else:
            rows = [t for t in rows if iso_to_ts(t["created_time"]) < cutoff]
        if "min_ts" in p:                      # inclusive, second granularity
            rows = [t for t in rows if int(iso_to_ts(t["created_time"])) >= int(p["min_ts"])]
        if "max_ts" in p:
            rows = [t for t in rows if int(iso_to_ts(t["created_time"])) <= int(p["max_ts"])]
        rows.sort(key=lambda t: iso_to_ts(t["created_time"]), reverse=True)   # newest first
        return self._page(rows, p, "trades", max_limit=1000)
