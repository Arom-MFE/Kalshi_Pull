"""The fake exchange must behave like the live API did on 2026-09-17.

Every other test trusts FakeKalshi, so its contract is pinned here.
"""

from fakes import BASE_URL, FakeKalshi, make_candle, make_event, make_market, make_trade


def _exchange() -> FakeKalshi:
    api = FakeKalshi()
    api.add_event(make_event("TEST-26SEP", "TEST"), [
        make_market("TEST-26SEP-T1", "TEST-26SEP", status="active"),
        make_market("TEST-26SEP-T2", "TEST-26SEP", status="active"),
    ])
    api.add_event(make_event("TEST-24JAN", "TEST"), [
        make_market("TEST-24JAN-T1", "TEST-24JAN", status="finalized", tier="historical"),
    ])
    return api


def test_status_all_gets_the_real_400_bodies():
    api = _exchange()
    resp = api.get(f"{BASE_URL}/markets", params={"series_ticker": "TEST", "status": "all"})
    assert resp.status_code == 400
    assert resp.json()["error"]["details"] == "invalid status filter"
    # /events rejects it too, without a details field
    resp = api.get(f"{BASE_URL}/events", params={"series_ticker": "TEST", "status": "all"})
    assert resp.status_code == 400
    assert "details" not in resp.json()["error"]
    resp = api.get(f"{BASE_URL}/markets", params={"status": "open,closed"})
    assert resp.json()["error"]["details"] == "only one status filter may be supplied"


def test_filter_vocabulary_differs_from_response_statuses():
    api = _exchange()
    body = api.get(f"{BASE_URL}/markets", params={"series_ticker": "TEST", "status": "open"}).json()
    assert {m["status"] for m in body["markets"]} == {"active"}
    # No status filter = every status of the live tier; historical-only markets are absent
    body = api.get(f"{BASE_URL}/markets", params={"series_ticker": "TEST"}).json()
    assert [m["ticker"] for m in body["markets"]] == ["TEST-26SEP-T1", "TEST-26SEP-T2"]
    assert all("series_ticker" not in m for m in body["markets"])


def test_historical_only_market_404s_on_live_endpoints():
    api = _exchange()
    assert api.get(f"{BASE_URL}/markets/TEST-24JAN-T1").status_code == 404
    assert api.get(f"{BASE_URL}/historical/markets/TEST-24JAN-T1").status_code == 200
    assert api.get(f"{BASE_URL}/historical/markets/NOPE").json()["error"]["code"].startswith("failed_to_get")


def test_nested_markets_key_is_absent_for_historical_events_and_event_shape_flips():
    api = _exchange()
    events = api.get(f"{BASE_URL}/events", params={"series_ticker": "TEST", "with_nested_markets": "true"}).json()["events"]
    by_ticker = {e["event_ticker"]: e for e in events}
    assert len(by_ticker["TEST-26SEP"]["markets"]) == 2
    assert "markets" not in by_ticker["TEST-24JAN"]
    assert all("available_on_brokers" not in e for e in events)

    flat = api.get(f"{BASE_URL}/events/TEST-26SEP").json()
    assert len(flat["markets"]) == 2 and "markets" not in flat["event"]
    nested = api.get(f"{BASE_URL}/events/TEST-26SEP", params={"with_nested_markets": "true"}).json()
    assert nested["markets"] == [] and len(nested["event"]["markets"]) == 2


def test_candle_windows_are_inclusive_and_capped_at_5000():
    api = _exchange()
    api.candles["TEST-26SEP-T1"] = {1440: [make_candle(86400 * d) for d in (1, 2, 3, 4)]}
    url = f"{BASE_URL}/series/TEST/markets/TEST-26SEP-T1/candlesticks"
    body = api.get(url, params={"start_ts": 86400 * 2, "end_ts": 86400 * 3, "period_interval": 1440}).json()
    assert [c["end_period_ts"] for c in body["candlesticks"]] == [86400 * 2, 86400 * 3]
    assert "close_dollars" in body["candlesticks"][0]["price"]
    resp = api.get(url, params={"start_ts": 0, "end_ts": 864000, "period_interval": 1})
    assert resp.status_code == 400 and "max candlesticks: 5000" in resp.json()["error"]["details"]


def test_trades_are_newest_first_with_inclusive_min_ts_and_cursor_paging():
    api = _exchange()
    api.trades["TEST-26SEP-T1"] = [
        make_trade("t-1", "TEST-26SEP-T1", "2026-09-17T02:42:58.719482Z"),
        make_trade("t-2", "TEST-26SEP-T1", "2026-09-17T02:43:00.732373Z"),
        make_trade("t-3", "TEST-26SEP-T1", "2026-09-17T02:43:00.732373Z"),
    ]
    api.page_size = 2
    url = f"{BASE_URL}/markets/trades"
    page1 = api.get(url, params={"ticker": "TEST-26SEP-T1", "limit": 1000}).json()
    assert [t["trade_id"] for t in page1["trades"]] == ["t-2", "t-3"]
    page2 = api.get(url, params={"ticker": "TEST-26SEP-T1", "limit": 1000, "cursor": page1["cursor"]}).json()
    assert [t["trade_id"] for t in page2["trades"]] == ["t-1"] and page2["cursor"] == ""
    # 02:43:00 is second 1789612980: inclusive, so both same-second trades come back
    body = api.get(url, params={"ticker": "TEST-26SEP-T1", "min_ts": 1789612980}).json()
    assert {t["trade_id"] for t in body["trades"]} == {"t-2", "t-3"}
    assert api.get(url, params={"ticker": "TEST-26SEP-T1", "min_ts": 1789612981}).json()["trades"] == []
