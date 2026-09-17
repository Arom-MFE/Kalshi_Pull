"""Offline tests for pull_live.poll_focus: preflight, flags, and the runtime roll."""

import json
import signal
import time

import pandas as pd
import pytest

import pull_live.poll_focus as poll_focus
from kalshi_io import config, universe
from fakes import FakeResponse, iso_to_ts, make_candle, make_event, make_market, make_trade

NOW = "2026-09-17T20:00:00Z"
OPEN = "2026-09-16T00:00:00Z"
SEP = ["KXCPIYOY-26SEP-T1", "KXCPIYOY-26SEP-T2"]
NOV = ["KXCPIYOY-26NOV-T1"]
FED = ["KXFED-26OCT-T1"]


class Clock:
    """Stand-in for poll_focus._now/_sleep: every loop sleep jumps `step` seconds."""

    def __init__(self, start: float, step: float = 120.0):
        self.now = start
        self.step = step
        self.on_sleep = None

    def time(self) -> float:
        return self.now

    def sleep(self, _seconds: float) -> None:
        self.now += self.step
        if self.on_sleep:
            self.on_sleep()


@pytest.fixture
def exchange(fake_api, catalog_dir, monkeypatch):
    monkeypatch.setattr(config, "FOCUS_SERIES", ["KXCPIYOY", "KXFED"])
    fake_api.add_event(make_event("KXCPIYOY-26AUG", "KXCPIYOY"), [
        make_market("KXCPIYOY-26AUG-T1", "KXCPIYOY-26AUG", status="finalized")])
    fake_api.add_event(make_event("KXCPIYOY-26SEP", "KXCPIYOY"), [
        make_market(t, "KXCPIYOY-26SEP", open_time=OPEN, close_time="2026-10-14T12:29:00Z") for t in SEP])
    fake_api.add_event(make_event("KXCPIYOY-26NOV", "KXCPIYOY"), [
        make_market(t, "KXCPIYOY-26NOV", open_time=OPEN, close_time="2026-12-10T13:29:00Z") for t in NOV])
    fake_api.add_event(make_event("KXFED-26OCT", "KXFED"), [
        make_market(t, "KXFED-26OCT", open_time=OPEN, close_time="2026-10-28T17:55:00Z") for t in FED])
    return fake_api


@pytest.fixture
def clock(monkeypatch, clock):
    c = Clock(iso_to_ts(NOW))
    monkeypatch.setattr(poll_focus, "_now", c.time)
    monkeypatch.setattr(poll_focus, "_sleep", c.sleep)
    return c


@pytest.fixture
def pulls(monkeypatch):
    """Replace the five pullers with recorders: [(name, tickers), ...] in call order."""
    calls: list[tuple[str, list[str]]] = []

    def recorder(name):
        def run(tickers):
            calls.append((name, list(tickers)))
            return {"processed": len(tickers), "rows_written": 0}
        return run

    for name in ("daily", "hourly", "minute", "trades"):
        monkeypatch.setattr(poll_focus, f"run_{name}", recorder(name))
    monkeypatch.setattr(poll_focus, "_run_orderbook", recorder("orderbook"))
    return calls


def _log_text(data_dir) -> str:
    return "".join(p.read_text() for p in sorted((data_dir / "logs").glob("poll_focus_*.log")))


# ------------------------------------------------------------------ preflight

def test_universe_of_settled_tickers_is_refused_with_exit_2_before_any_pull(exchange, pulls, data_dir, capsys):
    assert poll_focus.main(["--tickers", "KXCPIYOY-26AUG-T1", "--iterations", "1"]) == 2
    assert pulls == []
    text = _log_text(data_dir)
    assert " ERROR " in text and "refusing to start" in text and "settled=1" in text
    assert "settled=1" in capsys.readouterr().err
    assert not (data_dir / "logs" / universe.LIVE_SNAPSHOT).exists()


def test_stale_override_in_config_is_refused_too(exchange, pulls, monkeypatch):
    monkeypatch.setattr(config, "FOCUS_OVERRIDE", ["KXCPIYOY-26AUG-T1", "KXNOPE-1"])
    assert poll_focus.main(["--iterations", "1"]) == 2 and pulls == []


def test_empty_derived_universe_is_refused_with_exit_2(exchange, pulls, data_dir):
    assert poll_focus.main(["--series", "KXU3", "--iterations", "1"]) == 2
    assert pulls == [] and "focus universe is empty" in _log_text(data_dir)


def test_api_failure_while_building_the_universe_exits_1(exchange, pulls, data_dir):
    exchange.inject(r"^/markets$", [FakeResponse(503, {"error": {"message": "down"}})] * 6)
    assert poll_focus.main(["--iterations", "1"]) == 1
    assert pulls == [] and "could not build the focus universe" in _log_text(data_dir)


# ------------------------------------------------------------------ one iteration

def test_one_iteration_polls_the_derived_universe_and_writes_the_snapshot(exchange, pulls, clock, data_dir):
    assert poll_focus.main(["--iterations", "1"]) == 0
    expected = sorted(SEP + FED)
    assert pulls == [(name, expected) for name in ("daily", "hourly", "minute", "trades", "orderbook")]

    snapshot = json.loads((data_dir / "logs" / universe.LIVE_SNAPSHOT).read_text())
    assert snapshot["tickers"] == expected and snapshot["source"] == "derived"
    assert [g["event_ticker"] for g in snapshot["groups"]] == ["KXCPIYOY-26SEP", "KXFED-26OCT"]
    text = _log_text(data_dir)
    assert "poll_focus starting — 3 tickers, universe derived" in text
    assert "KXCPIYOY KXCPIYOY-26SEP: 2 tickers, closes 2026-10-14T12:29:00Z" in text
    assert "poll_focus exiting cleanly" in text


def test_flags_select_the_universe(exchange, pulls, clock):
    assert poll_focus.main(["--iterations", "1", "--no-daily", "--no-hourly", "--no-trades", "--no-orderbook",
                            "--series", "KXCPIYOY", "--events-per-series", "2"]) == 0
    assert pulls == [("minute", sorted(SEP + NOV))]

    pulls.clear()
    # A quoted, space-separated --tickers string is several tickers; settled ones are left out with a warning
    assert poll_focus.main(["--iterations", "1", "--no-daily", "--no-hourly", "--no-trades", "--no-orderbook",
                            "--tickers", f"{FED[0]} {NOV[0]}", "KXCPIYOY-26AUG-T1"]) == 0
    assert pulls == [("minute", sorted(FED + NOV))]


def test_all_pullers_disabled_is_a_clean_no_op(exchange, pulls, clock):
    assert poll_focus.main(["--no-daily", "--no-hourly", "--no-minute", "--no-trades", "--no-orderbook"]) == 0
    assert pulls == []


def test_show_universe_prints_the_universe_and_neither_polls_nor_logs(exchange, pulls, data_dir, capsys):
    assert poll_focus.main(["--show-universe"]) == 0
    out = capsys.readouterr().out
    assert "Focus universe: 3 tickers in 2 events (derived," in out
    assert all(t in out for t in SEP + FED) and NOV[0] not in out
    assert pulls == [] and not (data_dir / "logs").exists()

    assert poll_focus.main(["--show-universe", "--tickers", "KXCPIYOY-26AUG-T1"]) == 2
    assert "settled=1" in capsys.readouterr().err
    exchange.inject(r"^/markets$", [FakeResponse(503, {"error": {"message": "down"}})] * 6)
    assert poll_focus.main(["--show-universe"]) == 1


# ------------------------------------------------------------------ runtime roll

def _set_status(exchange, tickers, status):
    for t in tickers:
        exchange.markets[t]["status"] = status


def test_closed_cycle_gets_a_final_sweep_and_the_next_event_takes_over(exchange, pulls, clock, data_dir):
    clock.on_sleep = lambda: _set_status(exchange, SEP, "closed")        # the cycle closes while we sleep

    assert poll_focus.main(["--iterations", "2", "--universe-refresh", "60", "--no-daily", "--no-hourly"]) == 0

    before, after = sorted(SEP + FED), sorted(NOV + FED)
    assert pulls == [
        ("minute", before), ("trades", before), ("orderbook", before),
        # refresh: the closed tickers are pulled one last time (no orderbook: a closed market has no book)...
        ("minute", SEP), ("trades", SEP),
        # ...and the loop continues on the rolled universe
        ("minute", after), ("trades", after), ("orderbook", after),
    ]
    text = _log_text(data_dir)
    assert "universe roll KXCPIYOY: KXCPIYOY-26SEP -> KXCPIYOY-26NOV" in text
    assert "2 tickers stopped trading, final sweep then dropped" in text and "1 tickers added: KXCPIYOY-26NOV-T1" in text
    assert json.loads((data_dir / "logs" / universe.LIVE_SNAPSHOT).read_text())["tickers"] == after


def test_paused_ticker_stays_in_the_universe(exchange, pulls, clock, data_dir):
    clock.on_sleep = lambda: _set_status(exchange, FED, "inactive")
    assert poll_focus.main(["--iterations", "2", "--universe-refresh", "60",
                            "--no-daily", "--no-hourly", "--no-trades", "--no-orderbook"]) == 0
    assert pulls == [("minute", sorted(SEP + FED))] * 2
    assert "left the selection but can still trade; kept: KXFED-26OCT-T1" in _log_text(data_dir)


def test_failed_refresh_keeps_polling_the_last_good_universe(exchange, pulls, clock, data_dir):
    clock.on_sleep = lambda: exchange.inject(r"^/markets$", [FakeResponse(503, {"error": {"message": "down"}})] * 6)
    assert poll_focus.main(["--iterations", "2", "--universe-refresh", "60",
                            "--no-daily", "--no-hourly", "--no-trades", "--no-orderbook"]) == 0
    assert pulls == [("minute", sorted(SEP + FED))] * 2
    text = _log_text(data_dir)
    assert "universe refresh failed; keeping the last universe — RetriesExhausted" in text


def test_derived_universe_with_nothing_left_keeps_the_last_one_and_logs_an_error(exchange, pulls, clock, data_dir):
    clock.on_sleep = lambda: _set_status(exchange, SEP + NOV + FED, "closed")
    assert poll_focus.main(["--iterations", "2", "--universe-refresh", "60",
                            "--no-daily", "--no-hourly", "--no-trades", "--no-orderbook"]) == 0
    assert pulls == [("minute", sorted(SEP + FED))] * 2
    assert "universe refresh found nothing to poll; keeping the last universe" in _log_text(data_dir)


def test_explicit_universe_that_dies_gets_its_final_sweep_and_exits_3(exchange, pulls, clock, data_dir):
    clock.on_sleep = lambda: _set_status(exchange, FED, "closed")
    assert poll_focus.main(["--tickers", FED[0], "--universe-refresh", "60",
                            "--no-daily", "--no-hourly", "--no-orderbook"]) == 3
    assert pulls == [("minute", FED), ("trades", FED), ("minute", FED), ("trades", FED)]
    text = _log_text(data_dir)
    assert "poll_focus stopping" in text and "closed=1" in text and "exiting with code 3" in text


def test_refresh_can_be_switched_off(exchange, pulls, clock):
    clock.on_sleep = lambda: _set_status(exchange, SEP, "closed")
    assert poll_focus.main(["--iterations", "2", "--universe-refresh", "0",
                            "--no-daily", "--no-hourly", "--no-trades", "--no-orderbook"]) == 0
    assert pulls == [("minute", sorted(SEP + FED))] * 2


def test_signal_handlers_are_restored_after_the_run(exchange, pulls, clock):
    before = (signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM))
    poll_focus.main(["--iterations", "1"])
    assert (signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)) == before


# ------------------------------------------------------------------ end to end with the real pullers

def test_one_real_iteration_stores_data_under_the_true_event_and_series(exchange, clock, data_dir, monkeypatch):
    now = iso_to_ts(NOW)
    monkeypatch.setattr(time, "time", lambda: now)
    ticker = SEP[0]
    exchange.candles[ticker] = {
        1: [make_candle(int(now) - 60 * i, traded=(i % 2 == 0)) for i in range(5, 0, -1)],
        60: [make_candle(int(now) - 3600)],
        1440: [make_candle(int(now) - 86400 + 14400)],
    }
    exchange.trades[ticker] = [make_trade("t-1", ticker, "2026-09-17T19:30:00Z")]
    exchange.orderbooks[ticker] = {"yes_dollars": [["0.4900", "10.00"]], "no_dollars": [["0.5000", "5.00"]]}

    assert poll_focus.main(["--iterations", "1"]) == 0

    minute = pd.read_parquet(data_dir / "candles/minute/KXCPIYOY/2026/09" / f"{ticker}.parquet")
    assert len(minute) == 5 and set(minute["event_ticker"]) == {"KXCPIYOY-26SEP"}
    # Three of the five minutes had no trade: NaN prices, but the quotes are there
    assert minute["yes_bid_close"].eq(0.49).all() and minute["close"].isna().sum() == 3
    assert (data_dir / "candles/daily/KXCPIYOY" / f"{ticker}.parquet").exists()
    assert (data_dir / "candles/hourly/KXCPIYOY/2026" / f"{ticker}.parquet").exists()
    assert len(pd.read_parquet(data_dir / "trades/KXCPIYOY" / ticker / "2026-09.parquet")) == 1
    assert len(pd.read_parquet(data_dir / "orderbook" / ticker / "2026-09-17.parquet")) == 2
    # The derived tickers were registered: no event lookup was needed to place any of them
    assert exchange.requests_to("/events/") == []
    # One log file for the whole process, every line once
    logs = sorted(p.name for p in (data_dir / "logs").glob("*.log"))
    assert len(logs) == 1 and logs[0].startswith("poll_focus_")
    lines = (data_dir / "logs" / logs[0]).read_text().splitlines()
    assert len(lines) == len(set(lines))
