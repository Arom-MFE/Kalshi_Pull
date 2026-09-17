"""Offline tests for kalshi_io.universe against the fake exchange."""

import json

import pytest

import kalshi_io.candles as candles
from kalshi_io import config, universe
from kalshi_io.client import RetriesExhausted
from kalshi_io.tickers import load_tickers, validate_tickers
from kalshi_io.universe import UniverseError
from fakes import FakeResponse, iso_to_ts, make_event, make_market

SEP_CLOSE = "2026-10-14T12:29:00Z"
NOV_CLOSE = "2026-12-10T13:29:00Z"


def _strikes(event, n=3, **kw):
    return [make_market(f"{event}-T{i}", event, **kw) for i in range(1, n + 1)]


@pytest.fixture
def exchange(fake_api):
    # CPI: a settled cycle, the current one, and the one after it
    fake_api.add_event(make_event("KXCPIYOY-26AUG", "KXCPIYOY"), _strikes("KXCPIYOY-26AUG", status="finalized"))
    fake_api.add_event(make_event("KXCPIYOY-26SEP", "KXCPIYOY", title="Inflation in September 2026"),
                       _strikes("KXCPIYOY-26SEP", close_time=SEP_CLOSE))
    fake_api.add_event(make_event("KXCPIYOY-26NOV", "KXCPIYOY", title="Inflation in November 2026"),
                       _strikes("KXCPIYOY-26NOV", n=2, close_time=NOV_CLOSE))
    # Fed: two meetings open at the same time
    fake_api.add_event(make_event("KXFED-26OCT", "KXFED"), _strikes("KXFED-26OCT", n=2, close_time="2026-10-28T17:55:00Z"))
    fake_api.add_event(make_event("KXFED-26DEC", "KXFED"), _strikes("KXFED-26DEC", n=2, close_time="2026-12-09T18:55:00Z"))
    # A series with nothing open
    fake_api.add_event(make_event("KXU3-26AUG", "KXU3"), _strikes("KXU3-26AUG", status="finalized"))
    return fake_api


SERIES = ["KXCPIYOY", "KXFED"]


# ------------------------------------------------------------------ derive_universe

def test_nearest_open_event_of_every_series_is_taken_whole(exchange):
    u = universe.derive_universe(SERIES)
    assert [(g["series"], g["event_ticker"], g["close_time"]) for g in u["groups"]] == [
        ("KXCPIYOY", "KXCPIYOY-26SEP", SEP_CLOSE),
        ("KXFED", "KXFED-26OCT", "2026-10-28T17:55:00Z"),
    ]
    assert u["groups"][0]["title"] == "Inflation in September 2026"
    assert u["tickers"] == ["KXCPIYOY-26SEP-T1", "KXCPIYOY-26SEP-T2", "KXCPIYOY-26SEP-T3",
                            "KXFED-26OCT-T1", "KXFED-26OCT-T2"]
    assert set(u["statuses"].values()) == {"active"}
    assert u["source"] == "derived" and u["events_per_series"] == 1 and u["series"] == SERIES
    assert u["rule"].startswith("nearest 1 open event(s) per series")
    # Two list calls per series, both with the documented "open" filter; never status=all
    assert [(path, p["series_ticker"], p["status"]) for path, p, _ in exchange.calls] == [
        ("/markets", "KXCPIYOY", "open"), ("/events", "KXCPIYOY", "open"),
        ("/markets", "KXFED", "open"), ("/events", "KXFED", "open"),
    ]


def test_next_n_events_per_series(exchange):
    u = universe.derive_universe(["KXCPIYOY"], events_per_series=2)
    assert [g["event_ticker"] for g in u["groups"]] == ["KXCPIYOY-26SEP", "KXCPIYOY-26NOV"]
    assert len(u["tickers"]) == 5
    # Asking for more events than are open takes what there is
    assert len(universe.derive_universe(["KXCPIYOY"], events_per_series=9)["groups"]) == 2
    with pytest.raises(ValueError, match="at least 1"):
        universe.derive_universe(["KXCPIYOY"], events_per_series=-1)


def test_defaults_come_from_config(exchange, monkeypatch):
    monkeypatch.setattr(config, "FOCUS_SERIES", ["KXFED"])
    monkeypatch.setattr(config, "FOCUS_EVENTS_PER_SERIES", 2)
    u = universe.derive_universe()
    assert [g["event_ticker"] for g in u["groups"]] == ["KXFED-26OCT", "KXFED-26DEC"]


def test_ranking_reads_close_time_never_the_ticker_text(exchange):
    # "27JAN" sorts after "26OCT" as text and as a date, but this event closes first
    exchange.add_event(make_event("KXFED-27JAN", "KXFED"), _strikes("KXFED-27JAN", n=1, close_time="2026-09-30T00:00:00Z"))
    assert universe.derive_universe(["KXFED"])["groups"][0]["event_ticker"] == "KXFED-27JAN"


def test_event_is_ranked_by_the_earliest_close_of_its_open_markets(exchange):
    # One early-closing strike makes 26DEC the nearest event
    exchange.add_markets(make_market("KXFED-26DEC-EARLY", "KXFED-26DEC", close_time="2026-10-01T00:00:00Z"))
    group = universe.derive_universe(["KXFED"])["groups"][0]
    assert group["event_ticker"] == "KXFED-26DEC" and group["close_time"] == "2026-10-01T00:00:00Z"
    assert group["tickers"] == ["KXFED-26DEC-EARLY", "KXFED-26DEC-T1", "KXFED-26DEC-T2"]


def test_markets_that_cannot_trade_now_never_select_an_event(exchange):
    # Nearer events exist, but their markets are paused / not opened yet / closed
    exchange.add_event(make_event("KXFED-26SEP", "KXFED"), [
        make_market("KXFED-26SEP-T1", "KXFED-26SEP", status="inactive", close_time="2026-09-20T00:00:00Z"),
        make_market("KXFED-26SEP-T2", "KXFED-26SEP", status="initialized", close_time="2026-09-20T00:00:00Z"),
        make_market("KXFED-26SEP-T3", "KXFED-26SEP", status="closed", close_time="2026-09-16T00:00:00Z"),
    ])
    assert universe.derive_universe(["KXFED"])["groups"][0]["event_ticker"] == "KXFED-26OCT"

    # Even if the API returned such a market for status=open, it would be ignored
    stray = {k: v for k, v in exchange.markets["KXFED-26SEP-T1"].items() if not k.startswith("_")}
    live = [{k: v for k, v in m.items() if not k.startswith("_")}
            for m in exchange.markets.values() if m["event_ticker"].startswith("KXFED-26") and m["status"] == "active"]
    exchange.inject(r"^/markets$", [FakeResponse(200, {"markets": [stray, *live], "cursor": ""})])
    u = universe.derive_universe(["KXFED"])
    assert u["groups"][0]["event_ticker"] == "KXFED-26OCT" and "KXFED-26SEP-T1" not in u["tickers"]


def test_tie_breaks_on_the_event_ticker_and_missing_close_time_falls_back(exchange):
    same = "2026-11-01T00:00:00Z"
    exchange.add_event(make_event("KXTIE-B", "KXTIE"), [make_market("KXTIE-B-1", "KXTIE-B", close_time=same)])
    exchange.add_event(make_event("KXTIE-A", "KXTIE"), [make_market("KXTIE-A-1", "KXTIE-A", close_time=same)])
    assert [g["event_ticker"] for g in universe.derive_universe(["KXTIE"], 2)["groups"]] == ["KXTIE-A", "KXTIE-B"]

    # No close_time: expected_expiration_time ranks the event instead
    exchange.add_event(make_event("KXTIE-C", "KXTIE"), [
        make_market("KXTIE-C-1", "KXTIE-C", close_time=None, expected_expiration_time="2026-10-01T00:00:00Z")])
    # No expected expiration either: latest_expiration_time
    exchange.add_event(make_event("KXTIE-D", "KXTIE"), [
        make_market("KXTIE-D-1", "KXTIE-D", close_time=None, expected_expiration_time=None,
                    latest_expiration_time="2026-09-25T00:00:00Z")])
    # No time at all: ranked last, close_time stays null (nothing is invented)
    exchange.add_event(make_event("KXTIE-0", "KXTIE"), [
        make_market("KXTIE-0-1", "KXTIE-0", close_time=None, expected_expiration_time=None, latest_expiration_time=None)])
    groups = universe.derive_universe(["KXTIE"], 9)["groups"]
    assert [g["event_ticker"] for g in groups] == ["KXTIE-D", "KXTIE-C", "KXTIE-A", "KXTIE-B", "KXTIE-0"]
    assert groups[0]["close_time"] == "2026-09-25T00:00:00Z" and groups[-1]["close_time"] is None


def test_long_dated_series_takes_the_nearest_year(exchange):
    for year in (36, 28, 27, 30):
        event = f"KXGDPYEAR-{year}"
        exchange.add_event(make_event(event, "KXGDPYEAR"), [make_market(f"{event}-T2", event, close_time=f"20{year + 1}-01-28T13:29:00Z")])
    assert [g["event_ticker"] for g in universe.derive_universe(["KXGDPYEAR"], 2)["groups"]] == ["KXGDPYEAR-27", "KXGDPYEAR-28"]


def test_series_without_open_markets_only_warns(exchange):
    u = universe.derive_universe(["KXU3", "KXCPIYOY"])
    assert [g["series"] for g in u["groups"]] == ["KXCPIYOY"]
    assert u["warnings"] == ["KXU3: no open markets, nothing to poll for this series"]
    assert universe.check_universe(u)["tickers"] == u["tickers"]


def test_request_failure_propagates(exchange):
    exchange.inject(r"^/markets$", [FakeResponse(503, {"error": {"message": "down"}})] * 6)
    with pytest.raises(RetriesExhausted):
        universe.derive_universe(SERIES)


# ------------------------------------------------------------------ explicit universes

def test_explicit_universe_looks_its_tickers_up_in_both_tiers(exchange):
    exchange.add_event(make_event("TEST-22DEC", "KXTEST"), [
        make_market("TEST-22DEC-T1", "TEST-22DEC", status="finalized", tier="historical")])
    u = universe.explicit_universe(["KXFED-26OCT-T2", "TEST-22DEC-T1", "KXNOPE-1", "KXFED-26OCT-T2"], "FOCUS_OVERRIDE")
    assert u["tickers"] == ["KXFED-26OCT-T2", "KXNOPE-1", "TEST-22DEC-T1"]
    assert u["statuses"] == {"KXFED-26OCT-T2": "active", "KXNOPE-1": "", "TEST-22DEC-T1": "finalized"}
    assert [(g["series"], g["event_ticker"]) for g in u["groups"]] == [(None, "KXFED-26OCT"), (None, "TEST-22DEC")]
    assert u["source"] == "FOCUS_OVERRIDE" and "never rolled forward" in u["rule"]
    assert {p.get("status") for _, p, _ in exchange.calls} == {None}


def test_build_universe_precedence_cli_then_override_then_derived(exchange, monkeypatch):
    monkeypatch.setattr(config, "FOCUS_SERIES", ["KXCPIYOY"])
    assert universe.build_universe()["source"] == "derived"
    assert universe.build_universe(series_list=["KXFED"])["groups"][0]["series"] == "KXFED"

    monkeypatch.setattr(config, "FOCUS_OVERRIDE", ["KXFED-26DEC-T1"])
    u = universe.build_universe(series_list=["KXFED"])
    assert u["source"] == "FOCUS_OVERRIDE" and u["tickers"] == ["KXFED-26DEC-T1"]

    # --tickers goes through load_tickers: one quoted string with spaces and commas works
    u = universe.build_universe(cli_tickers=["KXFED-26OCT-T1, KXFED-26OCT-T2"])
    assert u["source"] == "--tickers" and u["tickers"] == ["KXFED-26OCT-T1", "KXFED-26OCT-T2"]


# ------------------------------------------------------------------ check_universe

def test_empty_derived_universe_is_refused_with_the_series_and_a_hint(exchange):
    with pytest.raises(UniverseError, match=r"focus universe is empty: no open market found .*\(KXU3\).*find_events\.py"):
        universe.check_universe(universe.derive_universe(["KXU3"]))
    with pytest.raises(UniverseError, match="FOCUS_OVERRIDE lists no tickers"):
        universe.check_universe(universe.explicit_universe([], "FOCUS_OVERRIDE"))


def test_universe_where_nothing_can_trade_is_refused_with_counts_per_status(exchange):
    exchange.markets["KXCPIYOY-26SEP-T3"]["status"] = "determined"
    stale = universe.explicit_universe(
        ["KXCPIYOY-26AUG-T1", "KXCPIYOY-26AUG-T2", "KXCPIYOY-26SEP-T3", "KXNOPE-1"], "--tickers")
    with pytest.raises(UniverseError) as err:
        universe.check_universe(stale)
    assert err.value.status_counts == {"settled": 2, "closed": 1, "not_found": 1}
    message = str(err.value)
    assert "none of its 4 tickers can trade (closed=1, settled=2, not_found=1)" in message
    assert "come from --tickers" in message and "find_events.py --series <SERIES> --status open --markets" in message
    assert "derive the universe from FOCUS_SERIES" in message


def test_mixed_universe_is_pruned_to_what_can_still_trade(exchange):
    exchange.markets["KXFED-26OCT-T1"]["status"] = "inactive"          # paused: can trade again
    exchange.markets["KXFED-26DEC-T1"]["status"] = "initialized"       # not opened yet
    u = universe.check_universe(universe.explicit_universe(
        ["KXFED-26OCT-T1", "KXFED-26OCT-T2", "KXFED-26DEC-T1", "KXCPIYOY-26AUG-T1", "KXNOPE-1"], "--tickers"))
    assert u["tickers"] == ["KXFED-26DEC-T1", "KXFED-26OCT-T1", "KXFED-26OCT-T2"]
    assert u["dropped"] == {"KXCPIYOY-26AUG-T1": "settled", "KXNOPE-1": "not_found"}
    assert u["status_counts"] == {"paused": 1, "open": 1, "unopened": 1, "settled": 1, "not_found": 1}
    assert [g["event_ticker"] for g in u["groups"]] == ["KXFED-26DEC", "KXFED-26OCT"]   # the settled event is gone
    calls_before = len(exchange.calls)
    universe.check_universe(u)
    assert len(exchange.calls) == calls_before                              # checking makes no request


def test_register_makes_derived_tickers_known_without_any_request(exchange, catalog_dir):
    u = universe.check_universe(universe.derive_universe(SERIES))
    universe.register(u)
    exchange.calls.clear()
    assert candles.resolve_ticker_meta("KXCPIYOY-26SEP-T2") == ("KXCPIYOY", "KXCPIYOY-26SEP")
    assert validate_tickers(u["tickers"]) == (u["tickers"], [])
    assert exchange.calls == []
    # Explicit universes have no series; registering them is a no-op, not an error
    universe.register(universe.explicit_universe(["KXFED-26DEC-T1"], "--tickers"))
    assert not candles.is_cataloged("KXFED-26DEC-T1")


# ------------------------------------------------------------------ refresh_universe

def _close(exchange, event, status="closed"):
    for m in exchange.markets.values():
        if m["event_ticker"] == event:
            m["status"] = status


def test_refresh_rolls_to_the_next_event_and_reports_the_dead(exchange, catalog_dir):
    current = universe.check_universe(universe.derive_universe(SERIES))
    _close(exchange, "KXCPIYOY-26SEP")

    fresh, change = universe.refresh_universe(current)

    assert change["rolls"] == ["universe roll KXCPIYOY: KXCPIYOY-26SEP -> KXCPIYOY-26NOV"]
    assert change["dead"] == {f"KXCPIYOY-26SEP-T{i}": "closed" for i in (1, 2, 3)}
    assert change["added"] == ["KXCPIYOY-26NOV-T1", "KXCPIYOY-26NOV-T2"] and change["kept"] == []
    assert fresh["tickers"] == ["KXCPIYOY-26NOV-T1", "KXCPIYOY-26NOV-T2", "KXFED-26OCT-T1", "KXFED-26OCT-T2"]
    # The new tickers are registered: the pullers need no request to place them
    assert candles.resolve_ticker_meta("KXCPIYOY-26NOV-T1", allow_api=False) == ("KXCPIYOY", "KXCPIYOY-26NOV")


def test_refresh_without_changes_reports_nothing(exchange):
    current = universe.check_universe(universe.derive_universe(SERIES))
    fresh, change = universe.refresh_universe(current)
    assert fresh["tickers"] == current["tickers"]
    assert change == {"added": [], "dead": {}, "kept": [], "rolls": []}


def test_refresh_keeps_a_paused_ticker_that_left_the_open_snapshot(exchange):
    current = universe.check_universe(universe.derive_universe(["KXFED"]))
    exchange.markets["KXFED-26OCT-T1"]["status"] = "inactive"

    fresh, change = universe.refresh_universe(current)

    assert change["kept"] == ["KXFED-26OCT-T1"] and change["dead"] == {} and change["rolls"] == []
    assert fresh["tickers"] == ["KXFED-26OCT-T1", "KXFED-26OCT-T2"]
    assert fresh["statuses"]["KXFED-26OCT-T1"] == "inactive"
    assert [g["tickers"] for g in fresh["groups"]] == [["KXFED-26OCT-T1", "KXFED-26OCT-T2"]]


def test_refresh_keeps_a_whole_event_that_pauses_and_still_adds_the_next(exchange):
    current = universe.check_universe(universe.derive_universe(["KXFED"]))
    _close(exchange, "KXFED-26OCT", status="inactive")

    fresh, change = universe.refresh_universe(current)

    assert sorted(change["kept"]) == ["KXFED-26OCT-T1", "KXFED-26OCT-T2"] and change["dead"] == {}
    assert [g["event_ticker"] for g in fresh["groups"]] == ["KXFED-26DEC", "KXFED-26OCT"]
    assert change["rolls"] == ["universe roll KXFED: KXFED-26OCT -> KXFED-26DEC, KXFED-26OCT"]


def test_refresh_keeps_live_tickers_pushed_out_by_a_nearer_event(exchange):
    current = universe.check_universe(universe.derive_universe(["KXFED"]))
    exchange.add_event(make_event("KXFED-26SEPX", "KXFED"), _strikes("KXFED-26SEPX", n=1, close_time="2026-09-20T00:00:00Z"))

    fresh, change = universe.refresh_universe(current)

    assert change["added"] == ["KXFED-26SEPX-T1"] and sorted(change["kept"]) == ["KXFED-26OCT-T1", "KXFED-26OCT-T2"]
    assert fresh["tickers"] == ["KXFED-26OCT-T1", "KXFED-26OCT-T2", "KXFED-26SEPX-T1"]


def test_refresh_of_an_explicit_universe_drops_the_dead_and_never_rolls(exchange):
    current = universe.check_universe(universe.explicit_universe(["KXCPIYOY-26SEP-T1", "KXFED-26OCT-T1"], "--tickers"))
    _close(exchange, "KXCPIYOY-26SEP", status="finalized")

    fresh, change = universe.refresh_universe(current)
    assert fresh["tickers"] == ["KXFED-26OCT-T1"] and change["dead"] == {"KXCPIYOY-26SEP-T1": "settled"}
    assert change["added"] == [] and change["rolls"] == []

    _close(exchange, "KXFED-26OCT")
    with pytest.raises(UniverseError, match="none of its 1 tickers can trade") as err:
        universe.refresh_universe(fresh)
    assert err.value.dead == {"KXFED-26OCT-T1": "closed"}


def test_refresh_failures_propagate_so_the_caller_keeps_the_last_universe(exchange):
    current = universe.check_universe(universe.derive_universe(SERIES))
    exchange.inject(r"^/markets$", [FakeResponse(503, {"error": {"message": "down"}})] * 6)
    with pytest.raises(RetriesExhausted):
        universe.refresh_universe(current)

    # Everything closed and nothing new is open: an error too, with the dead attached
    for event in ("KXCPIYOY-26SEP", "KXCPIYOY-26NOV", "KXFED-26OCT", "KXFED-26DEC"):
        _close(exchange, event)
    with pytest.raises(UniverseError, match="focus universe is empty") as err:
        universe.refresh_universe(current)
    assert sorted(err.value.dead) == current["tickers"]


# ------------------------------------------------------------------ scheduling, files, display

def test_next_refresh_is_periodic_and_once_right_after_the_nearest_close(exchange):
    u = universe.check_universe(universe.derive_universe(SERIES))
    close = iso_to_ts(SEP_CLOSE)
    # Far from any close: plain interval
    assert universe.next_refresh_at(u, close - 86400, 3600) == close - 86400 + 3600
    # The nearest close falls inside the interval: refresh shortly after it
    assert universe.next_refresh_at(u, close - 600, 3600) == close + universe.CLOSE_REFRESH_DELAY_S
    # That refresh happened after the close, so the close no longer pulls the next one in
    assert universe.next_refresh_at(u, close + 95, 3600) == close + 95 + 3600
    assert universe.next_refresh_at(u, close - 600, 0) is None


def test_written_universe_round_trips_and_its_txt_is_a_tickers_file(exchange, catalog_dir):
    u = universe.check_universe(universe.derive_universe(SERIES))
    universe.write_universe(u, catalog_dir / universe.FOCUS_JSON, catalog_dir / universe.FOCUS_TXT)
    assert universe.load_universe(catalog_dir / universe.FOCUS_JSON) == json.loads(json.dumps(u))
    assert load_tickers("focus") == u["tickers"]
    assert load_tickers(str(catalog_dir / universe.FOCUS_JSON)) == u["tickers"]
    assert not list(catalog_dir.glob("*.tmp"))
    assert universe.load_universe(catalog_dir / "absent.json") is None


def test_format_universe_lists_events_drops_warnings_and_tickers(exchange):
    u = universe.check_universe(universe.derive_universe(["KXU3", "KXCPIYOY"]))
    text = universe.format_universe(u)
    assert "Focus universe: 3 tickers in 1 events (derived," in text
    assert "KXCPIYOY-26SEP" in text and f"closes {SEP_CLOSE}" in text and "Inflation in September 2026" in text
    assert "warning: KXU3: no open markets" in text and "Status: open=3" in text
    assert text.splitlines()[-3:] == ["  KXCPIYOY-26SEP-T1", "  KXCPIYOY-26SEP-T2", "  KXCPIYOY-26SEP-T3"]
