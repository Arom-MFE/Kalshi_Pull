"""Offline tests for kalshi_io.catalog against the fake exchange."""

import json
from datetime import datetime, timezone

import pytest

import kalshi_io.candles as candles
from kalshi_io import catalog, config
from kalshi_io.catalog import CatalogValidationError
from kalshi_io.client import RetriesExhausted
from kalshi_io.tickers import load_tickers
from fakes import FakeResponse, make_event, make_market

NOW = datetime(2026, 9, 17, 18, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def exchange(fake_api):
    fake_api.add_event(make_event("KXTEST-26SEP", "KXTEST", title="Test in September 2026"), [
        make_market(f"KXTEST-26SEP-T{i}", "KXTEST-26SEP", status="active") for i in (1, 2, 3)
    ])
    fake_api.add_event(make_event("KXTEST-26AUG", "KXTEST"), [
        make_market("KXTEST-26AUG-T1", "KXTEST-26AUG", status="finalized"),
        make_market("KXTEST-26AUG-T2", "KXTEST-26AUG", status="finalized"),
    ])
    # Settled just before the cutoff: served by both tiers
    fake_api.add_event(make_event("KXTEST-26JUN", "KXTEST"), [
        make_market("KXTEST-26JUN-T1", "KXTEST-26JUN", status="finalized", tier="both"),
    ])
    # Pre-KX event, filed by the API under the KX series; market only in the historical tier
    fake_api.add_event(make_event("TEST-22DEC", "KXTEST", title="Test in December 2022"), [
        make_market("TEST-22DEC-T1", "TEST-22DEC", status="finalized", tier="historical",
                    open_time="2022-11-01T15:00:00Z", close_time="2022-12-13T13:29:00Z"),
    ])
    # Delisted duplicate: /events does not list it, its markets still show up on /markets
    fake_api.add_event(make_event("KXTEST-27JAN30", "KXTEST", title="Delisted duplicate", listed=False), [
        make_market("KXTEST-27JAN30-T1", "KXTEST-27JAN30", status="inactive"),
    ])
    return fake_api


def _v1_file(path, markets):
    """A per-series file as the pre-2026-09 discovery wrote it (no times, no built_at)."""
    path.write_text(json.dumps({
        "series": "KXTEST",
        "events": [{"event_ticker": e, "title": "old"} for e in sorted({m["event_ticker"] for m in markets})],
        "markets": markets,
        "tickers": sorted(m["market_ticker"] for m in markets),
    }))


def _v1_market(ticker, event, status):
    return {"event_ticker": event, "market_ticker": ticker, "title": "old", "status": status, "source": "live_event"}


# ------------------------------------------------------------------ discover_series

def test_market_records_carry_status_times_and_source(exchange, catalog_dir):
    result = catalog.discover_series("KXTEST", now=NOW)
    by_ticker = {m["market_ticker"]: m for m in result["markets"]}
    assert by_ticker["KXTEST-26SEP-T1"] == {
        "event_ticker": "KXTEST-26SEP",
        "market_ticker": "KXTEST-26SEP-T1",
        "title": "Will KXTEST-26SEP-T1 resolve yes?",
        "status": "active",
        "open_time": "2026-06-09T22:25:00Z",
        "close_time": "2026-10-14T12:29:00Z",
        "expected_expiration_time": "2026-10-14T14:00:00Z",
        "latest_expiration_time": "2027-01-13T14:00:00Z",
        "source": "live",
    }
    assert by_ticker["TEST-22DEC-T1"]["source"] == "historical"
    assert by_ticker["TEST-22DEC-T1"]["close_time"] == "2022-12-13T13:29:00Z"
    assert result["built_at"] == "2026-09-17T18:00:00Z"
    assert result["schema_version"] == 2
    assert result["historical_cutoff"] == "2026-07-19T00:00:00Z"
    assert result["status_counts"] == {"active": 3, "finalized": 4, "inactive": 1}


def test_absent_times_stay_null(exchange, catalog_dir):
    del exchange.markets["KXTEST-26SEP-T1"]["expected_expiration_time"]
    by_ticker = {m["market_ticker"]: m for m in catalog.discover_series("KXTEST")["markets"]}
    assert by_ticker["KXTEST-26SEP-T1"]["expected_expiration_time"] is None


def test_active_markets_are_found_with_a_server_that_rejects_status_all(exchange, catalog_dir):
    """Regression for the 2026-09 breakage: a rebuild then kept settled markets only."""
    result = catalog.discover_series("KXTEST")
    active = [m["market_ticker"] for m in result["markets"] if m["status"] == "active"]
    assert active == ["KXTEST-26SEP-T1", "KXTEST-26SEP-T2", "KXTEST-26SEP-T3"]
    # The only status filter ever sent is the oracle's "open"
    assert {p.get("status") for _, p, _ in exchange.calls} == {None, "open"}


def test_tiers_are_unioned_and_the_live_record_wins(exchange, catalog_dir):
    result = catalog.discover_series("KXTEST")
    assert len(result["tickers"]) == 8 and len(set(result["tickers"])) == 8
    by_ticker = {m["market_ticker"]: m for m in result["markets"]}
    assert by_ticker["KXTEST-26JUN-T1"]["source"] == "live"


def test_legacy_event_is_filed_under_the_kx_series_and_the_probe_is_harmless(exchange, catalog_dir):
    result = catalog.discover_series("KXTEST")
    assert "TEST-22DEC" in [e["event_ticker"] for e in result["events"]]
    assert result["series"] == "KXTEST"
    # The pre-KX spelling returns nothing today; it is still asked, as insurance
    assert any(p.get("series_ticker") == "TEST" for _, p, _ in exchange.calls)
    quiet = catalog.discover_series("KXTEST", legacy_probe=False)
    assert quiet["tickers"] == result["tickers"]


def test_event_seen_only_on_markets_is_added_with_its_title(exchange, catalog_dir):
    events = {e["event_ticker"]: e["title"] for e in catalog.discover_series("KXTEST")["events"]}
    assert events["KXTEST-27JAN30"] == "Delisted duplicate"


def test_event_without_markets_in_the_series_listing_gets_a_per_event_lookup(exchange, catalog_dir):
    exchange.add_event(make_event("KXTEST-25NOV", "KXTEST"), [
        make_market("KXTEST-25NOV-T1", "KXTEST-25NOV", status="finalized", tier="historical"),
    ])
    exchange.hidden_from_series_listing = {"KXTEST-25NOV-T1"}
    result = catalog.discover_series("KXTEST")
    assert "KXTEST-25NOV-T1" in result["tickers"]
    assert exchange.requests_to("/historical/markets")[-1][1].get("event_ticker") == "KXTEST-25NOV"


def test_open_event_without_active_markets_refuses_to_write(exchange, catalog_dir):
    _v1_file(catalog_dir / "KXTEST_tickers.json", [_v1_market("KXTEST-26AUG-T1", "KXTEST-26AUG", "finalized")])
    before = (catalog_dir / "KXTEST_tickers.json").read_bytes()
    # The live tier comes back without the active markets (what the old failure looked like):
    # first the series listing, then the per-event lookup that would otherwise recover them
    settled_only = [{k: v for k, v in m.items() if not k.startswith("_")}
                    for m in exchange.markets.values() if m["status"] == "finalized" and m["_tier"] != "historical"]
    exchange.inject(r"^/markets$", [
        FakeResponse(200, {"markets": settled_only, "cursor": ""}),
        FakeResponse(200, {"markets": [], "cursor": ""}),
    ])
    with pytest.raises(CatalogValidationError, match="KXTEST-26SEP"):
        catalog.discover_series("KXTEST", legacy_probe=False)
    assert (catalog_dir / "KXTEST_tickers.json").read_bytes() == before


def test_failed_request_leaves_the_previous_files_untouched(exchange, catalog_dir):
    _v1_file(catalog_dir / "KXTEST_tickers.json", [_v1_market("KXTEST-26AUG-T1", "KXTEST-26AUG", "finalized")])
    (catalog_dir / "KXTEST_tickers.txt").write_text("KXTEST-26AUG-T1\n")
    before = {p.name: p.read_bytes() for p in catalog_dir.iterdir()}
    exchange.inject(r"^/historical/markets$", [FakeResponse(503, {"error": {"message": "down"}})] * 6)
    with pytest.raises(RetriesExhausted):
        catalog.discover_series("KXTEST")
    assert {p.name: p.read_bytes() for p in catalog_dir.iterdir()} == before


def test_vanished_tickers_are_looked_up_carried_forward_or_reported_removed(exchange, catalog_dir):
    exchange.add_event(make_event("KXTEST-25OCT", "KXTEST"), [
        make_market("KXTEST-25OCT-T1", "KXTEST-25OCT", status="finalized", tier="historical"),
    ])
    exchange.hidden_from_series_listing = {"KXTEST-25OCT-T1"}
    del exchange.events["KXTEST-25OCT"]                      # not reachable through any listing
    _v1_file(catalog_dir / "KXTEST_tickers.json", [
        _v1_market("KXTEST-25OCT-T1", "KXTEST-25OCT", "finalized"),    # still answers a ticker lookup
        _v1_market("OLDTEST-21-T1", "OLDTEST-21", "finalized"),        # gone, but it traded once
        _v1_market("KXTEST-28JAN-T1", "KXTEST-28JAN", "initialized"),  # gone, never opened
    ])
    result = catalog.discover_series("KXTEST")
    by_ticker = {m["market_ticker"]: m for m in result["markets"]}
    assert by_ticker["KXTEST-25OCT-T1"]["source"] == "historical"
    assert by_ticker["KXTEST-25OCT-T1"]["close_time"] is not None
    # Carried forward: stored data for it stays resolvable; the new fields are null, not invented
    assert by_ticker["OLDTEST-21-T1"]["source"] == "carried_forward"
    assert by_ticker["OLDTEST-21-T1"]["close_time"] is None
    assert result["carried_forward"] == ["OLDTEST-21-T1"]
    assert "OLDTEST-21" in [e["event_ticker"] for e in result["events"]]
    assert "KXTEST-28JAN-T1" not in by_ticker and result["removed_upstream"] == ["KXTEST-28JAN-T1"]


def test_files_are_sorted_atomic_and_the_txt_matches(exchange, catalog_dir):
    catalog.discover_series("KXTEST", now=NOW)
    data = json.loads((catalog_dir / "KXTEST_tickers.json").read_text())
    assert data["tickers"] == sorted(data["tickers"])
    assert [m["market_ticker"] for m in data["markets"]] == sorted(
        (m["market_ticker"] for m in data["markets"]),
        key=lambda t: (next(x["event_ticker"] for x in data["markets"] if x["market_ticker"] == t), t))
    assert (catalog_dir / "KXTEST_tickers.txt").read_text().splitlines() == data["tickers"]
    assert "removed_upstream" not in data and "carried_forward" not in data
    assert not list(catalog_dir.glob("*.tmp"))


def test_v2_files_are_read_by_the_existing_readers(exchange, catalog_dir):
    catalog.discover_series("KXTEST")
    catalog.build_combined(catalog_dir)
    candles._reset_state()
    assert candles.resolve_ticker_meta("TEST-22DEC-T1", allow_api=False) == ("KXTEST", "TEST-22DEC")
    assert len(load_tickers("KXTEST")) == 8
    assert len(load_tickers(str(catalog_dir / "all_tickers.json"))) == 8
    assert len(load_tickers(str(catalog_dir / "all_tickers.txt"))) == 8


# ------------------------------------------------------------------ build_combined

def test_build_combined_is_idempotent_and_ignores_non_series_json(exchange, catalog_dir):
    catalog.discover_series("KXTEST", now=NOW)
    (catalog_dir / "focus_universe.json").write_text(json.dumps({"tickers": ["X"], "derived_at": "x"}))
    first = catalog.build_combined(catalog_dir, now=NOW)
    # The old build_combined crashed here with KeyError: 'events' (it re-read all_tickers.json)
    second = catalog.build_combined(catalog_dir, now=NOW)
    assert first == second
    assert second["series"] == ["KXTEST"] and second["total_events"] == 5 and second["total_markets"] == 8
    assert second["status_counts"] == {"active": 3, "finalized": 4, "inactive": 1}
    assert second["built_at"] == "2026-09-17T18:00:00Z"
    assert second["by_series"]["KXTEST"]["built_at"] == "2026-09-17T18:00:00Z"
    assert second["oldest_series_built_at"] == "2026-09-17T18:00:00Z"


def test_combined_tolerates_v1_series_files(catalog_dir):
    _v1_file(catalog_dir / "KXOLD_tickers.json", [_v1_market("KXOLD-1", "KXOLD-1", "finalized")])
    data = json.loads((catalog_dir / "KXOLD_tickers.json").read_text())
    data["series"] = "KXOLD"
    (catalog_dir / "KXOLD_tickers.json").write_text(json.dumps(data))
    combined = catalog.build_combined(catalog_dir, now=NOW)
    assert combined["by_series"]["KXOLD"]["built_at"] is None
    assert combined["oldest_series_built_at"] is None
    assert combined["status_counts"] == {"finalized": 1}


def test_catalog_built_at_prefers_the_recorded_stamp_then_mtime(exchange, catalog_dir):
    assert catalog.catalog_built_at(catalog_dir) == (None, "none")
    (catalog_dir / "all_tickers.json").write_text(json.dumps({"series": [], "tickers": []}))
    stamp, basis = catalog.catalog_built_at(catalog_dir)
    assert basis == "mtime" and stamp.tzinfo is not None
    catalog.discover_series("KXTEST", now=NOW)
    catalog.build_combined(catalog_dir, now=NOW)
    assert catalog.catalog_built_at(catalog_dir) == (NOW, "built_at")


def test_market_index_adds_the_series(exchange, catalog_dir):
    catalog.discover_series("KXTEST")
    index = catalog.market_index(catalog_dir)
    assert index["TEST-22DEC-T1"]["series"] == "KXTEST" and index["TEST-22DEC-T1"]["status"] == "finalized"


# ------------------------------------------------------------------ diff / refresh

def test_diff_against_a_v1_catalog(exchange, catalog_dir):
    _v1_file(catalog_dir / "KXTEST_tickers.json", [
        _v1_market("KXTEST-26AUG-T1", "KXTEST-26AUG", "active"),       # settled since
        _v1_market("KXTEST-26SEP-T1", "KXTEST-26SEP", "active"),       # event known, strikes added since
        _v1_market("KXTEST-28JAN-T1", "KXTEST-28JAN", "initialized"),  # removed upstream
    ])
    old = catalog.load_catalog(catalog_dir)
    new = {"KXTEST": catalog.discover_series("KXTEST", save=False)}
    diff = catalog.diff_catalog(old, new)

    assert diff["totals"]["old"]["markets"] == 3 and diff["totals"]["new"]["markets"] == 8
    assert [e["event_ticker"] for e in diff["new_events"]] == ["KXTEST-26JUN", "KXTEST-27JAN30", "TEST-22DEC"]
    new_in_existing = [m["market_ticker"] for m in diff["new_markets"] if not m["in_new_event"]]
    assert new_in_existing == ["KXTEST-26AUG-T2", "KXTEST-26SEP-T2", "KXTEST-26SEP-T3"]
    assert diff["status_changes"] == [{
        "series": "KXTEST", "event_ticker": "KXTEST-26AUG", "market_ticker": "KXTEST-26AUG-T1",
        "old": "active", "new": "finalized"}]
    assert [m["market_ticker"] for m in diff["removed_markets"]] == ["KXTEST-28JAN-T1"]
    assert [e["event_ticker"] for e in diff["removed_events"]] == ["KXTEST-28JAN"]


def test_refresh_dry_run_reports_and_writes_nothing(exchange, catalog_dir):
    report = catalog.refresh_catalog(["KXTEST"], out_dir=catalog_dir, dry_run=True, now=NOW)
    assert list(catalog_dir.iterdir()) == []
    assert report["series_ok"] == ["KXTEST"] and report["series_failed"] == {}
    assert report["combined"]["total_markets"] == 8
    assert all(c["ok"] for c in report["checks"])
    assert report["api_requests"] == len(exchange.calls) > 0
    assert report["previous_built_at"] == (None, "none")


def test_refresh_keeps_the_old_file_of_a_failed_series_and_fails_the_check(exchange, catalog_dir):
    exchange.add_event(make_event("KXOTHER-26SEP", "KXOTHER"), [make_market("KXOTHER-26SEP-T1", "KXOTHER-26SEP")])
    catalog.refresh_catalog(["KXTEST", "KXOTHER"], out_dir=catalog_dir, now=NOW)
    before = (catalog_dir / "KXTEST_tickers.json").read_bytes()

    exchange.inject(r"^/events$", [FakeResponse(500, {"error": {"message": "boom"}})] * 6)
    report = catalog.refresh_catalog(["KXTEST", "KXOTHER"], out_dir=catalog_dir)

    assert list(report["series_failed"]) == ["KXTEST"] and report["series_ok"] == ["KXOTHER"]
    assert (catalog_dir / "KXTEST_tickers.json").read_bytes() == before
    # The failed series stays in the combined files with its previous content
    assert report["combined"]["by_series"]["KXTEST"]["markets"] == 8
    assert [c["ok"] for c in report["checks"] if c["name"] == "every series refreshed"] == [False]


def test_second_refresh_reports_no_changes(exchange, catalog_dir):
    catalog.refresh_catalog(["KXTEST"], out_dir=catalog_dir, now=NOW)
    diff = catalog.refresh_catalog(["KXTEST"], out_dir=catalog_dir, now=NOW)["diff"]
    assert not any(diff[k] for k in ("new_events", "new_markets", "removed_markets", "status_changes"))


# ------------------------------------------------------------------ wrapper script

def test_get_tickers_wrapper_keeps_its_public_names_and_writes_through_the_catalog(exchange, catalog_dir, capsys):
    import get_ticker_info.get_tickers as get_tickers        # importing must not touch the network
    assert exchange.calls == []
    result = get_tickers.discover_series("KXTEST", verbose=True)
    assert len(result["tickers"]) == 8
    combined = get_tickers.build_combined(verbose=True)
    assert combined["total_markets"] == 8
    assert get_tickers.load_tickers("KXTEST") == result["tickers"]
    assert len(get_tickers.load_tickers("KXTEST", key="markets")) == 8
    assert "Markets found: 8" in capsys.readouterr().out


def test_series_list_is_single_sourced():
    import get_ticker_info.get_tickers as get_tickers
    assert get_tickers.SERIES_LIST is config.SERIES_LIST
    assert "SERIES_LIST = [" not in open(get_tickers.__file__).read()
