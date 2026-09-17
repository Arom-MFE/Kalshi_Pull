"""Offline tests for ticker → (series, event) resolution and kalshi_io.resolve."""

import json

import pytest

import kalshi_io.candles as candles
from kalshi_io import resolve
from kalshi_io.candles import UnknownTickerError, canonical_series, register_ticker_meta, resolve_ticker_meta
from kalshi_io.client import RetriesExhausted
from fakes import FakeResponse, make_event, make_market


@pytest.fixture
def catalog(catalog_dir):
    """Committed-catalog stand-in: one series file plus the combined file."""
    (catalog_dir / "KXCPIYOY_tickers.json").write_text(json.dumps({
        "series": "KXCPIYOY",
        "events": [{"event_ticker": "KXCPIYOY-26JUN", "title": "Inflation in June 2026"}],
        "markets": [{"event_ticker": "KXCPIYOY-26JUN", "market_ticker": "KXCPIYOY-26JUN-T3.5",
                     "title": "", "status": "finalized", "source": "historical"}],
        "tickers": ["KXCPIYOY-26JUN-T3.5"],
    }))
    # all_tickers.json has "series" as a list and no "markets": it must be skipped, not crash
    (catalog_dir / "all_tickers.json").write_text(json.dumps({
        "series": ["KXCPIYOY"], "total_events": 1, "total_markets": 1,
        "tickers": ["KXCPIYOY-26JUN-T3.5"], "by_series": {},
    }))
    return catalog_dir


@pytest.fixture
def exchange(fake_api):
    fake_api.add_event(make_event("KXCPIYOY-26SEP", "KXCPIYOY"), [
        make_market("KXCPIYOY-26SEP-T3.0", "KXCPIYOY-26SEP", open_time="2026-06-09T22:25:00Z"),
        make_market("KXCPIYOY-26SEP-T3.1", "KXCPIYOY-26SEP"),
    ])
    fake_api.add_event(make_event("RECSSNBER-23", "KXRECSSNBER"), [
        make_market("RECSSNBER-23", "RECSSNBER-23", status="finalized", tier="historical",
                    open_time="2022-11-08T15:00:00Z"),
    ])
    return fake_api


# ------------------------------------------------------------------ resolve_ticker_meta (fix 4)

def test_cataloged_ticker_resolves_without_any_request(catalog, exchange):
    assert resolve_ticker_meta("KXCPIYOY-26JUN-T3.5") == ("KXCPIYOY", "KXCPIYOY-26JUN")
    assert exchange.calls == []


def test_uncataloged_ticker_gets_its_real_event_and_series_from_the_api(catalog, exchange):
    """The old fallback returned the market ticker as the event ticker."""
    series, event = resolve_ticker_meta("KXCPIYOY-26SEP-T3.0")
    assert (series, event) == ("KXCPIYOY", "KXCPIYOY-26SEP")
    assert event != "KXCPIYOY-26SEP-T3.0"
    # market -> event -> series: two requests, then cached
    assert [c[0] for c in exchange.calls] == ["/markets/KXCPIYOY-26SEP-T3.0", "/events/KXCPIYOY-26SEP"]
    resolve_ticker_meta("KXCPIYOY-26SEP-T3.0")
    assert len(exchange.calls) == 2


def test_single_market_event_keeps_event_equal_to_market_when_the_api_says_so(catalog, exchange):
    # Legitimate case: RECSSNBER-23 is both the event and its only market
    assert resolve_ticker_meta("RECSSNBER-23") == ("KXRECSSNBER", "RECSSNBER-23")


def test_unknown_ticker_raises_once_and_is_remembered(catalog, exchange):
    with pytest.raises(UnknownTickerError, match="KXNOPE-99-T1"):
        resolve_ticker_meta("KXNOPE-99-T1")
    n = len(exchange.calls)
    with pytest.raises(UnknownTickerError):
        resolve_ticker_meta("KXNOPE-99-T1")
    assert len(exchange.calls) == n


def test_api_outage_is_not_mistaken_for_an_unknown_ticker(catalog, exchange):
    exchange.inject(r"^/markets/KXCPIYOY-26SEP-T3\.0$", [FakeResponse(503, {"error": {"message": "down"}})] * 6)
    with pytest.raises(RetriesExhausted):
        resolve_ticker_meta("KXCPIYOY-26SEP-T3.0")
    # Not cached as unknown: the next call asks again and succeeds
    assert resolve_ticker_meta("KXCPIYOY-26SEP-T3.0") == ("KXCPIYOY", "KXCPIYOY-26SEP")


def test_offline_mode_never_calls_the_api(catalog, exchange):
    assert resolve_ticker_meta("KXCPIYOY-26SEP-T3.0", allow_api=False) == ("KXCPIYOY", None)
    assert resolve_ticker_meta("RECSSNBER-23", allow_api=False) == ("KXRECSSNBER", None)
    assert resolve_ticker_meta("KXCPIYOY-26JUN-T3.5", allow_api=False) == ("KXCPIYOY", "KXCPIYOY-26JUN")
    assert exchange.calls == []


def test_registered_tickers_resolve_without_requests_and_do_not_block_the_catalog(catalog, exchange):
    # Registering before the first lookup must not stop the catalog from loading
    register_ticker_meta({"KXCPIYOY-26SEP-T3.1": ("KXCPIYOY", "KXCPIYOY-26SEP")})
    assert resolve_ticker_meta("KXCPIYOY-26SEP-T3.1") == ("KXCPIYOY", "KXCPIYOY-26SEP")
    assert resolve_ticker_meta("KXCPIYOY-26JUN-T3.5") == ("KXCPIYOY", "KXCPIYOY-26JUN")
    assert exchange.calls == []
    # The catalog wins over a conflicting registration
    register_ticker_meta({"KXCPIYOY-26JUN-T3.5": ("KXOTHER", "KXOTHER-1")})
    assert resolve_ticker_meta("KXCPIYOY-26JUN-T3.5") == ("KXCPIYOY", "KXCPIYOY-26JUN")


def test_series_is_spelled_like_the_catalog_directories():
    assert canonical_series("CPIYOY") == "KXCPIYOY"
    assert canonical_series("KXCPIYOY") == "KXCPIYOY"
    assert canonical_series("KXSOMETHINGNEW") == "KXSOMETHINGNEW"
    assert candles._derive_series("RECSSNBER-22") == "KXRECSSNBER"


# ------------------------------------------------------------------ kalshi_io.resolve

def test_resolve_market_never_sends_status_all(exchange):
    """Regression: tier 2 used status=all, which the API now rejects with 400."""
    event = resolve.resolve_event("KXCPIYOY-26SEP")
    assert event.event.series_ticker == "KXCPIYOY"
    assert resolve.resolve_market(event, "KXCPIYOY-26SEP") == "KXCPIYOY-26SEP-T3.0"

    no_markets = resolve.resolve_event("KXCPIYOY-26SEP")
    no_markets.markets = []
    assert resolve.resolve_market(no_markets, "KXCPIYOY-26SEP") == "KXCPIYOY-26SEP-T3.0"
    assert all("status" not in params for _, params, _ in exchange.calls)


def test_resolve_market_falls_through_to_historical_and_to_the_event_ticker(exchange):
    event = resolve.resolve_event("RECSSNBER-23")
    assert event.markets == []                                  # nothing in the live tier
    assert resolve.resolve_market(event, "RECSSNBER-23") == "RECSSNBER-23"
    assert resolve.resolve_market(event, "NOPE-99") is None


def test_resolve_event_derives_the_series_for_an_unknown_event(exchange):
    event = resolve.resolve_event("CPIYOY-19DEC")
    assert event.event.series_ticker == "KXCPIYOY" and event.markets == []


def test_get_market_metadata_live_then_historical_then_unknown(exchange):
    live = resolve.get_market_metadata("KXCPIYOY-26SEP-T3.0")
    # 2026-06-09T22:25:00Z
    assert live == {"open_ts_ms": 1781043900000, "expiration_time": "2027-01-13T14:00:00Z", "status": "active"}
    hist = resolve.get_market_metadata("RECSSNBER-23")
    assert hist["open_ts_ms"] == 1667919600000 and hist["status"] == "finalized"
    assert resolve.get_market_metadata("NOPE-99") == {
        "open_ts_ms": None, "expiration_time": "unknown", "status": "unknown"}


def test_get_market_metadata_raises_when_the_api_is_down(exchange):
    exchange.inject(r"^/markets/", [FakeResponse(500, {"error": {"message": "boom"}})] * 6)
    with pytest.raises(RetriesExhausted):
        resolve.get_market_metadata("KXCPIYOY-26SEP-T3.0")
