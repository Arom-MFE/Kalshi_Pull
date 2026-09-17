"""Offline tests for kalshi_io.discovery against the fake exchange."""

import pytest

from kalshi_io import discovery
from kalshi_io.client import RetriesExhausted
from fakes import FakeResponse, make_event, make_market


@pytest.fixture
def exchange(fake_api):
    fake_api.tags_by_categories = {"Economics": ["Inflation", "Fed"], "Financials": ["Indices"]}
    fake_api.series = [
        {"ticker": "KXCPIYOY", "title": "Inflation", "category": "Economics",
         "categories": ["Economics"], "tags": ["Inflation"], "frequency": "monthly"},
        # Pre-KX spelling of the same series: still listed, but dead
        {"ticker": "CPIYOY", "title": "Inflation", "category": "Economics",
         "categories": ["Economics"], "tags": ["Inflation"], "frequency": "monthly"},
        {"ticker": "KXJOBLESS", "title": "Initial jobless claims", "category": "Economics",
         "categories": ["Economics"], "tags": None, "frequency": "weekly"},
        {"ticker": "KXSPX", "title": "S&P 500 close", "category": "Financials",
         "categories": ["Financials", "Economics"], "tags": ["Indices"], "frequency": "daily"},
    ]
    fake_api.add_event(make_event("KXCPIYOY-26SEP", "KXCPIYOY", title="Inflation in September 2026"), [
        make_market(f"KXCPIYOY-26SEP-T3.{i}", "KXCPIYOY-26SEP", status="active") for i in range(3)
    ])
    fake_api.add_event(make_event("KXCPIYOY-26AUG", "KXCPIYOY"), [
        make_market("KXCPIYOY-26AUG-T3.0", "KXCPIYOY-26AUG", status="finalized"),
        make_market("KXCPIYOY-26AUG-T3.1", "KXCPIYOY-26AUG", status="determined"),
    ])
    # Settled just before the cutoff: present in BOTH tiers
    fake_api.add_event(make_event("KXCPIYOY-26JUN", "KXCPIYOY"), [
        make_market("KXCPIYOY-26JUN-T3.0", "KXCPIYOY-26JUN", status="finalized", tier="both"),
    ])
    # Pre-KX event: the API files it under the KX series; its market is historical-only
    fake_api.add_event(make_event("CPIYOY-22DEC", "KXCPIYOY"), [
        make_market("CPIYOY-22DEC-T6.5", "CPIYOY-22DEC", status="finalized", tier="historical"),
    ])
    return fake_api


# ------------------------------------------------------------------ status filters

def test_status_all_is_rejected_before_any_request_with_a_hint(exchange):
    with pytest.raises(ValueError, match="Omit status to get every status"):
        discovery.list_markets(series_ticker="KXCPIYOY", status="all")
    with pytest.raises(ValueError, match="unopened, open, closed, settled"):
        discovery.list_events("KXCPIYOY", status="all")
    # Filter words are not response statuses, and /markets takes one filter at a time
    with pytest.raises(ValueError):
        discovery.list_markets(series_ticker="KXCPIYOY", status="active")
    with pytest.raises(ValueError):
        discovery.list_markets(series_ticker="KXCPIYOY", status="open,closed")
    with pytest.raises(ValueError):
        discovery.list_events("KXCPIYOY", status="paused")
    assert exchange.calls == []


def test_no_status_means_every_status_and_the_param_is_never_sent(exchange):
    """Regression: the fake answers 400 to status=all, like the live API since 2026-09."""
    markets = discovery.list_markets(series_ticker="KXCPIYOY")
    assert {m["status"] for m in markets} == {"active", "finalized", "determined"}
    events = discovery.list_events("KXCPIYOY")
    assert len(events) == 4
    assert all("status" not in params for _, params, _ in exchange.calls)


def test_status_bucket_maps_response_statuses_to_filter_words():
    assert discovery.status_bucket("active") == "open"
    assert discovery.status_bucket("finalized") == "settled"
    assert discovery.status_bucket("determined") == "closed"
    assert discovery.status_bucket("inactive") == "paused"
    assert discovery.status_bucket("initialized") == "unopened"
    assert discovery.status_bucket(None) == "unknown"
    assert discovery.status_bucket("something_new") == "unknown"
    assert set(discovery.STATUS_BUCKET.values()) == set(discovery.MARKET_STATUS_FILTERS)


# ------------------------------------------------------------------ series

def test_list_categories_returns_tags_per_category(exchange):
    assert discovery.list_categories() == {"Economics": ["Inflation", "Fed"], "Financials": ["Indices"]}


def test_list_series_by_category_matches_the_categories_list_and_normalizes_tags(exchange):
    rows = discovery.list_series(category="Economics")
    assert [s["ticker"] for s in rows] == ["KXCPIYOY", "CPIYOY", "KXJOBLESS", "KXSPX"]
    by_ticker = {s["ticker"]: s for s in rows}
    # A series can match through its categories list while its primary category differs
    assert by_ticker["KXSPX"]["category"] == "Financials"
    assert by_ticker["KXJOBLESS"]["tags"] == []
    assert by_ticker["CPIYOY"]["legacy_twin"] is True
    assert by_ticker["KXCPIYOY"]["legacy_twin"] is False
    assert [s["ticker"] for s in discovery.list_series(category="Financials")] == ["KXSPX"]


def test_list_series_by_tags(exchange):
    assert [s["ticker"] for s in discovery.list_series(tags=["Indices"])] == ["KXSPX"]
    assert exchange.calls[-1][1] == {"tags": "Indices"}


def test_unknown_category_raises_with_a_suggestion_instead_of_returning_nothing(exchange):
    # The API answers a mis-cased category with 200 and an empty list
    with pytest.raises(ValueError, match="Did you mean 'Economics'"):
        discovery.list_series(category="economics")
    with pytest.raises(ValueError, match="Valid categories: Economics, Financials"):
        discovery.list_series(category="Weather")


def test_search_series_is_case_insensitive_ands_terms_and_hides_legacy_twins(exchange):
    assert [s["ticker"] for s in discovery.search_series("inflation")] == ["KXCPIYOY"]
    assert [s["ticker"] for s in discovery.search_series("INFLATION", include_legacy=True)] == ["KXCPIYOY", "CPIYOY"]
    assert [s["ticker"] for s in discovery.search_series("jobless claims")] == ["KXJOBLESS"]
    assert discovery.search_series("jobless inflation") == []
    # Matches tickers and tags too, and can be narrowed by category
    assert [s["ticker"] for s in discovery.search_series("kxspx")] == ["KXSPX"]
    assert [s["ticker"] for s in discovery.search_series("indices", category="Economics")] == ["KXSPX"]


# ------------------------------------------------------------------ events

def test_list_events_by_status_matches_on_child_markets(exchange):
    assert [e["event_ticker"] for e in discovery.list_events("KXCPIYOY", status="open")] == ["KXCPIYOY-26SEP"]
    settled = {e["event_ticker"] for e in discovery.list_events("KXCPIYOY", status="settled")}
    # The status filter also finds events whose markets are all in the historical tier
    assert settled == {"KXCPIYOY-26AUG", "KXCPIYOY-26JUN", "CPIYOY-22DEC"}
    assert exchange.calls[-1][1] == {"series_ticker": "KXCPIYOY", "status": "settled", "limit": 200}


def test_event_payloads_without_available_on_brokers_parse(exchange):
    """Regression: Kalshi removed the field on 2026-09-10; SDK 3.27 crashed on its absence."""
    events = discovery.list_events("KXCPIYOY")
    assert all("available_on_brokers" not in e for e in events)
    assert {e["series_ticker"] for e in events} == {"KXCPIYOY"}
    assert discovery.get_event("KXCPIYOY-26SEP")["title"] == "Inflation in September 2026"


def test_nested_markets_are_always_a_list(exchange):
    events = {e["event_ticker"]: e for e in discovery.list_events("KXCPIYOY", with_nested_markets=True)}
    assert len(events["KXCPIYOY-26SEP"]["markets"]) == 3
    # The API omits the key for an event whose markets are all historical
    assert events["CPIYOY-22DEC"]["markets"] == []


def test_list_events_paginates(exchange):
    exchange.page_size = 3
    assert len(discovery.list_events("KXCPIYOY")) == 4
    assert len(exchange.requests_to("/events")) == 2


def test_get_event_normalizes_both_response_shapes_and_404_is_none(exchange):
    event = discovery.get_event("KXCPIYOY-26SEP")
    assert len(event["markets"]) == 3 and event["series_ticker"] == "KXCPIYOY"
    # Shape without nesting: markets at the top level, none inside the event
    exchange.inject(r"^/events/KXCPIYOY-26SEP$", [FakeResponse(200, {
        "event": {"event_ticker": "KXCPIYOY-26SEP", "series_ticker": "KXCPIYOY"},
        "markets": [{"ticker": "KXCPIYOY-26SEP-T3.0"}],
    })])
    assert [m["ticker"] for m in discovery.get_event("KXCPIYOY-26SEP")["markets"]] == ["KXCPIYOY-26SEP-T3.0"]
    assert discovery.get_event("NOPE-99") is None


# ------------------------------------------------------------------ markets

def test_list_markets_for_event_series_and_status(exchange):
    by_event = discovery.list_markets(event_ticker="KXCPIYOY-26SEP", status="open")
    assert [m["ticker"] for m in by_event] == ["KXCPIYOY-26SEP-T3.0", "KXCPIYOY-26SEP-T3.1", "KXCPIYOY-26SEP-T3.2"]
    closed = discovery.list_markets(series_ticker="KXCPIYOY", status="closed")
    assert [m["ticker"] for m in closed] == ["KXCPIYOY-26AUG-T3.1"]
    settled = discovery.list_markets(series_ticker="KXCPIYOY", status="settled")
    # Live tier only: the 2022 market settled before the cutoff is not here
    assert {m["ticker"] for m in settled} == {"KXCPIYOY-26AUG-T3.0", "KXCPIYOY-26JUN-T3.0"}


def test_list_markets_requires_exactly_one_selector(exchange):
    with pytest.raises(ValueError, match="exactly one"):
        discovery.list_markets()
    with pytest.raises(ValueError, match="exactly one"):
        discovery.list_markets(event_ticker="E", series_ticker="S")
    with pytest.raises(ValueError, match="exactly one"):
        discovery.list_historical_markets(event_ticker="E", tickers=["T"])


def test_list_markets_paginates(exchange):
    exchange.page_size = 2
    assert len(discovery.list_markets(series_ticker="KXCPIYOY")) == 6
    assert len(exchange.requests_to("/markets")) == 3


def test_ticker_lookups_are_chunked_by_100(exchange):
    tickers = [f"T-{i}" for i in range(250)]
    assert discovery.list_markets(tickers=tickers) == []
    sizes = [len(params["tickers"].split(",")) for _, params, _ in exchange.requests_to("/markets")]
    assert sizes == [100, 100, 50]


def test_list_historical_markets(exchange):
    rows = discovery.list_historical_markets(series_ticker="KXCPIYOY")
    assert {m["ticker"] for m in rows} == {"KXCPIYOY-26JUN-T3.0", "CPIYOY-22DEC-T6.5"}
    assert all("series_ticker" not in m for m in rows)


def test_get_market_tries_live_then_historical_then_none(exchange):
    assert discovery.get_market("KXCPIYOY-26SEP-T3.0")["tier"] == "live"
    assert discovery.get_market("CPIYOY-22DEC-T6.5")["tier"] == "historical"
    assert discovery.get_market("KXCPIYOY-26JUN-T3.0")["tier"] == "live"
    assert discovery.get_market("NOPE-99-T1") is None


def test_only_a_404_means_missing_other_errors_propagate(exchange):
    exchange.inject(r"^/markets/KXCPIYOY-26SEP-T3\.0$", [FakeResponse(503, {"error": {"message": "down"}})] * 6)
    with pytest.raises(RetriesExhausted):
        discovery.get_market("KXCPIYOY-26SEP-T3.0")


def test_lookup_markets_covers_both_tiers_and_omits_unknown_tickers(exchange):
    found = discovery.lookup_markets(["KXCPIYOY-26SEP-T3.0", "CPIYOY-22DEC-T6.5", "NOPE-99-T1"])
    assert {t: m["tier"] for t, m in found.items()} == {
        "KXCPIYOY-26SEP-T3.0": "live", "CPIYOY-22DEC-T6.5": "historical",
    }
    # The historical tier is only asked for what the live tier did not return
    assert exchange.requests_to("/historical/markets")[0][1]["tickers"] == "CPIYOY-22DEC-T6.5,NOPE-99-T1"


def test_resolve_market_meta_goes_market_to_event_to_series_and_caches(exchange):
    # Market payloads have no series_ticker, so the series comes from the event
    assert discovery.resolve_market_meta("KXCPIYOY-26SEP-T3.0") == ("KXCPIYOY", "KXCPIYOY-26SEP")
    n = len(exchange.calls)
    assert discovery.resolve_market_meta("KXCPIYOY-26SEP-T3.0") == ("KXCPIYOY", "KXCPIYOY-26SEP")
    assert len(exchange.calls) == n
    # A sibling market reuses the cached event -> series answer: one market call only
    assert discovery.resolve_market_meta("KXCPIYOY-26SEP-T3.1") == ("KXCPIYOY", "KXCPIYOY-26SEP")
    assert len(exchange.calls) == n + 1
    # A pre-KX market resolves to the KX series through its event
    assert discovery.resolve_market_meta("CPIYOY-22DEC-T6.5") == ("KXCPIYOY", "CPIYOY-22DEC")


def test_resolve_market_meta_remembers_unknown_tickers(exchange):
    assert discovery.resolve_market_meta("NOPE-99-T1") is None
    n = len(exchange.calls)
    assert discovery.resolve_market_meta("NOPE-99-T1") is None
    assert len(exchange.calls) == n


def test_get_historical_cutoff(exchange):
    assert discovery.get_historical_cutoff()["trades_created_ts"] == "2026-07-19T00:00:00Z"
