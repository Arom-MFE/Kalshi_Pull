"""Offline tests for the get_ticker_info/roll.py command against the fake exchange."""

import json

import pandas as pd
import pytest

import get_ticker_info.get_Econ_Info as get_econ_info
import get_ticker_info.roll as roll
from kalshi_io import catalog, config
from kalshi_io.tickers import load_tickers
from fakes import FakeResponse, make_event, make_market

SEP = [f"KXTEST-26SEP-T{i}" for i in (1, 2, 3)]


@pytest.fixture
def exchange(fake_api, catalog_dir, monkeypatch):
    monkeypatch.setattr(catalog, "SERIES_LIST", ["KXTEST", "KXOTHER"])
    monkeypatch.setattr(config, "FOCUS_SERIES", ["KXTEST"])
    fake_api.add_event(make_event("KXTEST-26SEP", "KXTEST", title="Test in September 2026"), [
        make_market(t, "KXTEST-26SEP", close_time="2026-10-14T12:29:00Z") for t in SEP])
    fake_api.add_event(make_event("KXTEST-26NOV", "KXTEST", title="Test in November 2026"), [
        make_market("KXTEST-26NOV-T1", "KXTEST-26NOV", close_time="2026-12-10T13:29:00Z")])
    fake_api.add_event(make_event("KXTEST-26AUG", "KXTEST"), [
        make_market("KXTEST-26AUG-T1", "KXTEST-26AUG", status="finalized"),
        make_market("KXTEST-26AUG-T2", "KXTEST-26AUG", status="finalized")])
    fake_api.add_event(make_event("KXOTHER-26SEP", "KXOTHER"), [make_market("KXOTHER-26SEP-T1", "KXOTHER-26SEP")])
    return fake_api


def run(capsys, *argv) -> tuple[int, str]:
    code = roll.main(list(argv))
    return code, capsys.readouterr().out


def _v1_file(path, series, markets):
    """A per-series file as the pre-2026-09 discovery wrote it (no times, no built_at)."""
    path.write_text(json.dumps({
        "series": series,
        "events": [{"event_ticker": e, "title": "old"} for e in sorted({m["event_ticker"] for m in markets})],
        "markets": markets,
        "tickers": sorted(m["market_ticker"] for m in markets),
    }))


def _v1_market(ticker, event, status):
    return {"event_ticker": event, "market_ticker": ticker, "title": "old", "status": status, "source": "live_event"}


# ------------------------------------------------------------------ a normal roll

def test_roll_writes_the_catalog_the_focus_files_a_log_and_the_report(exchange, catalog_dir, data_dir, capsys):
    code, out = run(capsys)
    assert code == 0

    assert sorted(p.name for p in catalog_dir.iterdir()) == [
        "KXOTHER_tickers.json", "KXOTHER_tickers.txt", "KXTEST_tickers.json", "KXTEST_tickers.txt",
        "all_tickers.json", "all_tickers.txt", "focus_universe.json", "focus_universe.txt"]
    # The proposal comes from the same rule the poller uses, and the pullers can read it
    assert load_tickers("focus") == SEP
    focus = json.loads((catalog_dir / "focus_universe.json").read_text())
    assert [g["event_ticker"] for g in focus["groups"]] == ["KXTEST-26SEP"] and focus["source"] == "derived"
    # build_combined must keep ignoring the focus file on the next run (it is not a series file)
    assert catalog.build_combined(catalog_dir, save=False)["series"] == ["KXOTHER", "KXTEST"]

    assert "Previous build:  none (first build in this directory)" in out
    assert "  events             0         4" in out and "  markets            0         7" in out
    assert "by status now: active=5, finalized=2" in out
    assert "NEW EVENTS (4)" in out and "KXTEST-26SEP                3 markets  Test in September 2026" in out
    assert "PROPOSED FOCUS UNIVERSE\n  3 tickers, derived: nearest 1 open event(s) per series" in out
    assert "KXTEST-26SEP             closes 2026-10-14T12:29:00Z    3 markets  Test in September 2026" in out
    for check in ("every series refreshed: 2 series", "active markets present: 5 active markets",
                  "no previously cataloged ticker lost", "market metadata stored: 7 rows (0 refreshed, 7 new)",
                  "focus universe can be polled: 3 tickers in 1 events (open=3)",
                  "focus tickers are in the refreshed catalog: 3 of 3"):
        assert f"[ok] {check}" in out
    # The same payloads fill the market metadata store next to the candles
    assert "Market metadata: 7 rows in " in out and "metadata/markets.parquet (0 refreshed, 7 new, 0 kept)" in out
    stored = pd.read_parquet(data_dir / "metadata" / "markets.parquet").set_index("market_ticker")
    assert len(stored) == 7 and stored.loc["KXTEST-26AUG-T1", "result"] == "no"
    assert stored.loc["KXTEST-26SEP-T1", "series_ticker"] == "KXTEST" and pd.isna(stored.loc["KXTEST-26SEP-T1", "result"])
    assert "[FAIL]" not in out
    # One driver command for every layer, from market open: no --since (it was today minus 60 days until 0.2.0)
    assert "python -m pull_historical.backfill --tickers focus   #" in out and "--since" not in out
    assert "pull_minute" not in out and "python -m pull_live.poll_focus" in out

    logs = sorted(p.name for p in (data_dir / "logs").iterdir())
    assert len(logs) == 2 and logs[0].startswith("roll_2") and logs[1].startswith("roll_report_2")
    assert logs[1] == logs[0].replace("roll_", "roll_report_").replace(".log", ".txt")
    assert (data_dir / "logs" / logs[1]).read_text() == out
    assert "roll done: exit code 0" in (data_dir / "logs" / logs[0]).read_text()


def test_dry_run_reports_and_writes_nothing_at_all(exchange, catalog_dir, data_dir, capsys):
    code, out = run(capsys, "--dry-run")
    assert code == 0
    assert list(catalog_dir.iterdir()) == [] and not data_dir.exists()
    assert "(dry run: nothing was written)" in out and "NEW EVENTS (4)" in out
    assert "Market metadata: 7 rows would be refreshed" in out
    assert "focus_universe.txt" not in out                  # nothing was written, so nothing is announced
    assert "python get_ticker_info/roll.py                      # same run, written to disk" in out


def test_second_roll_reports_no_changes(exchange, capsys):
    run(capsys)
    code, out = run(capsys)
    assert code == 0
    assert "  markets            7         7" in out
    assert "0.0 days ago" in out and "file modification time" not in out
    for section in ("NEW EVENTS", "NEW MARKETS", "STATUS CHANGES", "REMOVED UPSTREAM", "CARRIED FORWARD", "FAILED SERIES"):
        assert section not in out


def test_roll_over_a_v1_catalog_reports_additions_transitions_and_removals(exchange, catalog_dir, capsys):
    _v1_file(catalog_dir / "KXTEST_tickers.json", "KXTEST", [
        _v1_market("KXTEST-26AUG-T1", "KXTEST-26AUG", "active"),        # settled since
        _v1_market("KXTEST-26AUG-T2", "KXTEST-26AUG", "active"),
        _v1_market("KXTEST-26SEP-T1", "KXTEST-26SEP", "active"),        # strikes were added since
        _v1_market("KXTEST-28JAN-T1", "KXTEST-28JAN", "initialized"),   # removed upstream, never opened
        _v1_market("OLDTEST-21-T1", "OLDTEST-21", "finalized"),         # gone from the API, but it traded
    ])
    (catalog_dir / "all_tickers.json").write_text(json.dumps({"series": ["KXTEST"], "tickers": []}))

    code, out = run(capsys, "--series", "KXTEST")

    assert code == 0
    assert "days ago (file modification time: a git checkout resets it, treat as a hint)" in out
    assert "  markets            5         7" in out          # +3 new, -1 removed upstream
    assert "NEW EVENTS (1)" in out and "KXTEST-26NOV" in out
    assert "NEW MARKETS IN EXISTING EVENTS (1)\n  KXTEST-26SEP: +2  KXTEST-26SEP-T2, KXTEST-26SEP-T3" in out
    assert "STATUS CHANGES BY EVENT (1)\n  KXTEST-26AUG: 2 markets active -> finalized" in out
    assert "REMOVED UPSTREAM, never opened (1)\n  KXTEST-28JAN-T1" in out
    assert "CARRIED FORWARD, no longer returned by the API (1)\n  OLDTEST-21-T1" in out
    assert "[ok] no previously cataloged ticker lost: 1 never-opened tickers removed upstream" in out


def test_long_sections_are_shortened_unless_full_is_given(exchange, capsys):
    for i in range(50):
        event = f"KXTEST-30E{i:02d}"
        exchange.add_event(make_event(event, "KXTEST"), [make_market(f"{event}-T1", event, status="initialized")])
    _, out = run(capsys, "--dry-run")
    assert "NEW EVENTS (54)" in out and "... and 14 more (--full shows all)" in out and "KXTEST-30E49" not in out
    _, out = run(capsys, "--dry-run", "--full")
    assert "more (--full" not in out and "KXTEST-30E49" in out


# ------------------------------------------------------------------ options

def test_out_dir_leaves_the_committed_catalog_alone(exchange, catalog_dir, tmp_path, capsys):
    copy = tmp_path / "catalog copy"
    code, out = run(capsys, "--out-dir", str(copy), "--series", "KXTEST")
    assert code == 0 and list(catalog_dir.iterdir()) == []
    assert (copy / "KXTEST_tickers.json").exists() and (copy / "focus_universe.txt").read_text().split() == SEP
    assert not (copy / "KXOTHER_tickers.json").exists()
    assert f"Catalog dir:     {copy}" in out


def test_no_focus_refreshes_the_catalog_only(exchange, catalog_dir, capsys):
    code, out = run(capsys, "--no-focus")
    assert code == 0 and not (catalog_dir / "focus_universe.json").exists()
    assert "focus universe can be polled" not in out and "--tickers focus" not in out


def test_events_per_series_proposes_further_cycles(exchange, catalog_dir, capsys):
    code, out = run(capsys, "--events-per-series", "2")
    assert code == 0 and load_tickers("focus") == ["KXTEST-26NOV-T1", *SEP]
    assert "4 tickers, derived: nearest 2 open event(s) per series" in out


# ------------------------------------------------------------------ failures

def test_failed_series_keeps_its_files_the_rest_is_written_and_the_exit_code_is_1(exchange, catalog_dir, capsys):
    run(capsys)
    before = (catalog_dir / "KXTEST_tickers.json").read_bytes()
    exchange.add_markets(make_market("KXOTHER-26SEP-T2", "KXOTHER-26SEP"))
    exchange.inject(r"^/events$", [FakeResponse(500, {"error": {"message": "boom"}})] * 6)   # KXTEST goes first

    code, out = run(capsys)

    assert code == 1
    assert (catalog_dir / "KXTEST_tickers.json").read_bytes() == before
    assert "KXOTHER-26SEP-T2" in (catalog_dir / "KXOTHER_tickers.txt").read_text()
    assert "FAILED SERIES, previous files kept (1)\n  KXTEST: RetriesExhausted" in out
    assert "[FAIL] every series refreshed: KXTEST: RetriesExhausted" in out
    # KXTEST was not refreshed, so its focus tickers are not held against the catalog
    assert "[ok] focus tickers are in the refreshed catalog: 0 of 0" in out


def test_stale_override_fails_the_focus_check_without_blocking_the_catalog(exchange, catalog_dir, capsys, monkeypatch):
    monkeypatch.setattr(config, "FOCUS_OVERRIDE", ["KXTEST-26AUG-T1"])
    code, out = run(capsys)
    assert code == 1
    assert (catalog_dir / "all_tickers.json").exists() and not (catalog_dir / "focus_universe.json").exists()
    assert "[FAIL] focus universe can be polled: focus universe has nothing to poll" in out and "settled=1" in out
    assert "PROPOSED FOCUS UNIVERSE\n  none (see CHECKS)" in out and "--tickers focus" not in out


def test_focus_ticker_missing_from_a_refreshed_series_fails_the_cross_check():
    report = {"combined": {"tickers": ["A-1"]}, "series_ok": ["A"]}
    focus = {"tickers": ["A-1", "A-2", "B-1"], "status_counts": {"open": 3}, "groups": [
        {"series": "A", "event_ticker": "A", "tickers": ["A-1", "A-2"]},
        {"series": "B", "event_ticker": "B", "tickers": ["B-1"]},         # B was not refreshed: not held against it
    ]}
    polled, cross = roll.focus_checks(report, focus, None)
    assert polled["ok"] and not cross["ok"] and cross["detail"] == "missing: ['A-2']"
    (failed,) = roll.focus_checks(report, None, "UniverseError: nothing open")
    assert not failed["ok"] and failed["detail"] == "UniverseError: nothing open"


def test_bad_arguments_exit_2(exchange, capsys):
    assert roll.main(["--events-per-series", "two"]) == 2
    assert exchange.calls == []


# ------------------------------------------------------------------ get_Econ_Info.py

def test_series_lister_goes_through_the_discovery_module(fake_api, capsys):
    fake_api.series = [
        {"ticker": "KXCPIYOY", "title": "Inflation", "category": "Economics", "tags": ["Inflation"], "frequency": "monthly"},
        {"ticker": "CPIYOY", "title": "Inflation", "category": "Economics", "tags": None, "frequency": "monthly"},
        {"ticker": "KXSPX", "title": "S&P 500 close", "category": "Financials", "tags": ["Indices"], "frequency": "daily"},
    ]
    get_econ_info.main()
    out = capsys.readouterr().out
    assert "Total series on Kalshi: 3" in out and "=== Economics series (2 total) ===" in out
    assert "KXSPX" not in out.split("=== Economics series")[1]
    # A failing listing raises instead of printing an empty table
    fake_api.inject(r"^/series$", [FakeResponse(503, {"error": {"message": "down"}})] * 6)
    with pytest.raises(Exception, match="gave up after 6 attempts"):
        get_econ_info.main()
