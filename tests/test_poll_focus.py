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
    # These tests are about the loop, not about history: every ticker counts as warm
    monkeypatch.setattr(poll_focus, "_cold_tickers", lambda tickers: [])
    return c


@pytest.fixture
def pulls(monkeypatch):
    """Replace the five pullers with recorders: [(name, tickers), ...] in call order."""
    calls: list[tuple[str, list[str]]] = []

    def recorder(name):
        def run(tickers, **kwargs):
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
    # The book comes first: it is the perishable one, and its timestamps stay evenly spaced
    assert pulls == [(name, expected) for name in ("orderbook", "daily", "hourly", "minute", "trades")]

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
        ("orderbook", before), ("minute", before), ("trades", before),
        # refresh: the closed tickers are pulled one last time (no orderbook: a closed market has no book)...
        ("minute", SEP), ("trades", SEP),
        # ...and the loop continues on the rolled universe
        ("orderbook", after), ("minute", after), ("trades", after),
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


# ------------------------------------------------------------------ release windows

IN_WINDOW = "2026-10-14T12:25:00Z"        # the CPI cycle closes at 12:29Z: window 12:24Z to 12:44Z


@pytest.fixture
def fast_pulls(monkeypatch):
    """Recorders that also note a fast (in-window) orderbook sweep."""
    calls: list[tuple[str, list[str]]] = []

    def recorder(name):
        def run(tickers, **kwargs):
            calls.append((f"{name}(fast)" if kwargs.get("fast") else name, list(tickers)))
            return {"processed": len(tickers), "rows_written": 0}
        return run

    for name in ("daily", "hourly", "minute", "trades"):
        monkeypatch.setattr(poll_focus, f"run_{name}", recorder(name))
    monkeypatch.setattr(poll_focus, "_run_orderbook", recorder("orderbook"))
    return calls


def test_inside_a_release_window_only_the_books_are_polled_fast_and_the_rest_waits(exchange, fast_pulls, clock, data_dir):
    clock.now = iso_to_ts(IN_WINDOW)
    # 11 passes of 2 minutes: ten inside the window, the eleventh at 12:45Z after it closed
    assert poll_focus.main(["--iterations", "11", "--no-daily", "--no-hourly"]) == 0
    both = sorted(SEP + FED)
    assert fast_pulls == [("orderbook(fast)", both)] * 10 + [("orderbook", both), ("minute", both), ("trades", both)]
    text = _log_text(data_dir)
    assert "release window open until 2026-10-14T12:44:00Z (KXCPIYOY-26SEP)" in text
    assert "books every 5 s; candle and trade pulls resume after the window" in text
    assert "release window closed (KXCPIYOY-26SEP); back to the normal cadence" in text
    assert "Release windows: books every 5 s from 300 s before to 900 s after a release" in text


def test_a_candle_sweep_is_not_started_right_before_a_window(exchange, fast_pulls, clock):
    clock.now = iso_to_ts("2026-10-14T12:23:30Z")            # the window opens at 12:24:00Z
    assert poll_focus.main(["--iterations", "2", "--no-daily", "--no-hourly"]) == 0
    both = sorted(SEP + FED)
    assert fast_pulls == [("orderbook", both), ("orderbook(fast)", both)]


def test_release_windows_can_be_switched_off(exchange, fast_pulls, clock):
    clock.now = iso_to_ts(IN_WINDOW)
    assert poll_focus.main(["--iterations", "1", "--no-daily", "--no-hourly", "--no-release-windows"]) == 0
    both = sorted(SEP + FED)
    assert fast_pulls == [("orderbook", both), ("minute", both), ("trades", both)]
    fast_pulls.clear()
    # Without orderbook snapshots there is nothing to poll faster: windows are off
    assert poll_focus.main(["--iterations", "1", "--no-daily", "--no-hourly", "--no-orderbook"]) == 0
    assert fast_pulls == [("minute", both), ("trades", both)]


def test_final_sweep_of_a_cycle_that_closed_inside_the_window_runs_when_it_ends(exchange, fast_pulls, clock, data_dir):
    clock.now = iso_to_ts(IN_WINDOW)
    clock.on_sleep = lambda: _set_status(exchange, SEP, "closed")        # the cycle closes at 12:29Z
    assert poll_focus.main(["--iterations", "11", "--no-daily", "--no-hourly", "--universe-refresh", "3600"]) == 0
    before, after = sorted(SEP + FED), sorted(NOV + FED)
    # The refresh at 12:30:30Z rolled to November inside the window: the closed cycle's last
    # minute and trade sweep waited for the end of the window, then the loop went on
    assert fast_pulls == [("orderbook(fast)", before)] * 3 + [("orderbook(fast)", after)] * 7 + [
        ("minute", SEP), ("trades", SEP), ("orderbook", after), ("minute", after), ("trades", after)]
    text = _log_text(data_dir)
    assert "universe roll KXCPIYOY: KXCPIYOY-26SEP -> KXCPIYOY-26NOV" in text
    assert "final sweep for 2 tickers deferred by the release window" in text


def test_calendar_entries_open_windows_too(exchange, fast_pulls, clock, monkeypatch):
    monkeypatch.setattr(config, "RELEASE_CALENDAR", ["2026-09-17T20:07:00Z"])         # seven minutes from NOW
    assert poll_focus.main(["--iterations", "2", "--no-daily", "--no-hourly"]) == 0
    both = sorted(SEP + FED)
    # Pass 1 at 20:00Z is outside the window (it opens at 20:02Z); pass 2 at 20:02Z is inside it
    assert fast_pulls == [("orderbook", both), ("minute", both), ("trades", both), ("orderbook(fast)", both)]


# ------------------------------------------------------------------ history outside the loop

class FakeChild:
    """Stand-in for the backfill child process: exits with `code` after `passes` polls."""
    instances: list["FakeChild"] = []

    def __init__(self, tickers, layers, code=0, passes=1):
        self.tickers, self.layers, self.code, self.passes = list(tickers), layers, code, passes
        self.pid, self.polls, self.terminated = 4242, 0, False
        FakeChild.instances.append(self)

    def poll(self):
        self.polls += 1
        return self.code if self.polls > self.passes else None

    def terminate(self):
        self.terminated = True

    def wait(self, timeout=None):
        return self.code

    def kill(self):
        pass


@pytest.fixture
def child(monkeypatch):
    FakeChild.instances.clear()
    spec = {"code": 0, "passes": 1}
    monkeypatch.setattr(poll_focus, "_spawn_history", lambda tickers, layers: FakeChild(tickers, layers, **spec))
    return spec


def test_cold_tickers_get_a_background_pull_and_rejoin_when_it_is_done(exchange, fast_pulls, clock, child, monkeypatch, data_dir):
    monkeypatch.setattr(poll_focus, "_cold_tickers", lambda tickers: [t for t in tickers if t in SEP])
    assert poll_focus.main(["--iterations", "3", "--no-daily", "--no-hourly"]) == 0
    both = sorted(SEP + FED)
    # Passes 1 and 2: the books of everything, candles and trades of the warm tickers only; the child
    # is seen finished at pass 3 (it is polled once per pass), and from then on every ticker is pulled
    assert fast_pulls == [("orderbook", both), ("minute", FED), ("trades", FED)] * 2 + [
        ("orderbook", both), ("minute", both), ("trades", both)]
    (spawned,) = FakeChild.instances
    assert spawned.tickers == SEP and spawned.layers == "metadata,minute,trades"
    text = _log_text(data_dir)
    assert "2 tickers have no stored history and wait for a background pull" in text
    assert "background pull started for 2 tickers (pid 4242, 3 requests/s)" in text
    assert "background pull finished (exit 0) for 2 tickers; they rejoin the loop" in text


def test_a_failing_history_pull_is_retried_and_finally_given_up(exchange, fast_pulls, clock, child, monkeypatch, data_dir):
    monkeypatch.setattr(poll_focus, "_cold_tickers", lambda tickers: [t for t in tickers if t in SEP])
    child.update(code=2, passes=1)
    # Passes are 2 min apart; a retry waits 10 min; three failures, then the tickers rejoin anyway
    assert poll_focus.main(["--iterations", "20", "--no-daily", "--no-hourly", "--no-trades"]) == 0
    assert len(FakeChild.instances) == 3
    minute_pulls = [tickers for name, tickers in fast_pulls if name == "minute"]
    assert minute_pulls[0] == FED and minute_pulls[-1] == sorted(SEP + FED)
    text = _log_text(data_dir)
    assert "background pull failed (exit 2); retrying in 10 min (1 of 3 attempts)" in text
    assert "background pull failed 3 times; polling 2 tickers anyway" in text


def test_tickers_added_by_a_roll_get_their_history_outside_the_loop(exchange, fast_pulls, clock, child, monkeypatch):
    monkeypatch.setattr(poll_focus, "_cold_tickers", lambda tickers: [t for t in tickers if t in NOV])
    clock.on_sleep = lambda: _set_status(exchange, SEP, "closed")
    child.update(code=0, passes=1)
    assert poll_focus.main(["--iterations", "4", "--universe-refresh", "60", "--no-daily", "--no-hourly", "--no-trades"]) == 0
    before = sorted(SEP + FED)
    assert fast_pulls == [
        ("orderbook", before), ("minute", before),
        ("minute", SEP),                                       # final sweep of the closed cycle
        ("orderbook", sorted(NOV + FED)), ("minute", FED),     # November is cold: books yes, candles not yet
        ("orderbook", sorted(NOV + FED)), ("minute", FED),
        ("orderbook", sorted(NOV + FED)), ("minute", sorted(NOV + FED)),
    ]
    (spawned,) = FakeChild.instances
    assert spawned.tickers == NOV and spawned.layers == "metadata,minute"


def test_no_background_history_pulls_cold_tickers_in_the_loop(exchange, fast_pulls, clock, child, monkeypatch):
    monkeypatch.setattr(poll_focus, "_cold_tickers", lambda tickers: list(tickers))
    assert poll_focus.main(["--iterations", "1", "--no-daily", "--no-hourly", "--no-background-history"]) == 0
    both = sorted(SEP + FED)
    assert fast_pulls == [("orderbook", both), ("minute", both), ("trades", both)] and FakeChild.instances == []


def test_a_running_child_is_stopped_when_the_poller_exits(exchange, fast_pulls, clock, child, monkeypatch):
    monkeypatch.setattr(poll_focus, "_cold_tickers", lambda tickers: list(tickers))
    child.update(code=0, passes=100)
    assert poll_focus.main(["--iterations", "2", "--no-daily", "--no-hourly"]) == 0
    (spawned,) = FakeChild.instances
    assert spawned.terminated is True
    assert fast_pulls == [("orderbook", sorted(SEP + FED))] * 2         # nothing else while the history is missing


_REAL_COLD_TICKERS = poll_focus._cold_tickers          # the clock fixture stubs the module attribute


def test_cold_tickers_are_those_without_daily_or_minute_files(exchange, data_dir):
    universe.register({"groups": [{"series": "KXCPIYOY", "event_ticker": "KXCPIYOY-26SEP", "tickers": SEP}]})
    warm, half = SEP
    for sub in ("candles/daily/KXCPIYOY", "candles/minute/KXCPIYOY/2026/09"):
        path = data_dir / sub / f"{warm}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"ts_ms": [1]}).to_parquet(path)
    daily_only = data_dir / "candles/daily/KXCPIYOY" / f"{half}.parquet"
    pd.DataFrame({"ts_ms": [1]}).to_parquet(daily_only)
    assert _REAL_COLD_TICKERS(SEP + FED) == [half, *FED]
    assert exchange.calls == []                                      # offline


# ------------------------------------------------------------------ the batch orderbook sweep

def test_orderbook_sweep_is_one_batch_request_and_falls_back_per_ticker_on_a_plain_error(exchange, data_dir):
    tickers = sorted(SEP + FED)
    for t in tickers:
        exchange.orderbooks[t] = {"yes_dollars": [["0.4900", "10.00"]], "no_dollars": [["0.5000", "5.00"]]}
    result = poll_focus._run_orderbook(tickers)
    assert result == {"processed": 3, "skipped": 0, "failed": 0, "rows_written": 6}
    assert [c[0] for c in exchange.calls] == ["/markets/orderbooks"]
    stamps = {pd.read_parquet(p)["ts_ms"].iloc[0] for p in (data_dir / "orderbook").rglob("*.parquet")}
    assert len(stamps) == 1                                          # one timestamp for the whole universe

    exchange.calls.clear()
    exchange.inject(r"^/markets/orderbooks$", [FakeResponse(400, {"error": {"code": "bad_request", "message": "bad"}})])
    result = poll_focus._run_orderbook(tickers)
    assert result["processed"] == 3
    assert [c[0] for c in exchange.calls] == ["/markets/orderbooks"] + [f"/markets/{t}/orderbook" for t in tickers]

    # An outage is not retried ticker by ticker: that would burn 3 x 6 attempts before the breaker
    exchange.calls.clear()
    exchange.inject(r"^/markets/orderbooks$", [FakeResponse(503, {"error": {"message": "down"}})] * 6)
    result = poll_focus._run_orderbook(tickers, fast=True)
    assert result == {"processed": 0, "skipped": 3, "failed": 3, "rows_written": 0}
    assert len(exchange.calls) == 2                                  # fast: two attempts, then give up


_REAL_SPAWN = poll_focus._spawn_history                  # conftest replaces the module attribute with a guard


def test_history_child_is_the_backfill_driver_with_its_own_lock_rate_and_data_root(monkeypatch, data_dir):
    seen = {}

    class Proc:
        pid = 7

    def fake_popen(cmd, **kwargs):
        seen["cmd"], seen["kwargs"] = cmd, kwargs
        return Proc()

    monkeypatch.setattr(poll_focus.subprocess, "Popen", fake_popen)
    assert _REAL_SPAWN(SEP, "metadata,minute,trades").pid == 7
    cmd, kwargs = seen["cmd"], seen["kwargs"]
    assert cmd[1:3] == ["-m", "pull_historical.backfill"] and cmd[3:6] == ["--tickers", *SEP]
    assert cmd[6:] == ["--layers", "metadata,minute,trades", "--no-audit", "--log-name", "backfill_history",
                       "--lock-name", "focus_history"]
    assert kwargs["env"]["KALSHI_DATA_DIR"] == str(data_dir) and kwargs["env"]["KALSHI_MAX_RPS"] == "3"
    assert kwargs["cwd"] == str(config.PROJECT_ROOT)
    assert kwargs["stdout"] is poll_focus.subprocess.DEVNULL and kwargs["stderr"] is poll_focus.subprocess.DEVNULL
    assert "start_new_session" not in kwargs                        # Ctrl+C must reach the child too
