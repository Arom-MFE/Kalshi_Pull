"""Offline tests for run logging. The puller's API seams are stubbed.

Duplicate handlers duplicate *lines*, not log records, so these tests read the
log files and captured stderr instead of using caplog.
"""

import logging

import pytest

import pull_historical.pull_daily as pull_daily
from kalshi_io import runlog


def _stub_puller(monkeypatch):
    """Make pull_daily.run() deterministic and offline: every ticker returns 0 candles."""
    monkeypatch.setattr(pull_daily, "load_tickers", lambda source: list(source))
    monkeypatch.setattr(pull_daily, "validate_tickers", lambda tickers, **kw: (list(tickers), []), raising=False)
    monkeypatch.setattr(pull_daily, "resolve_ticker_meta", lambda ticker: ("TEST", "TEST-26"))
    monkeypatch.setattr(
        pull_daily, "get_market_metadata",
        lambda ticker: {"open_ts_ms": 0, "expiration_time": "unknown", "status": "active"},
    )
    monkeypatch.setattr(pull_daily, "fetch_candles", lambda *args: [])


def _file_handlers():
    parent = logging.getLogger(runlog.PARENT_LOGGER)
    return [h for h in parent.handlers if isinstance(h, logging.FileHandler)]


def test_two_consecutive_runs_log_every_line_exactly_once(monkeypatch, data_dir, capsys):
    _stub_puller(monkeypatch)
    pull_daily.run(["T-A"])
    pull_daily.run(["T-B"])

    # Both runs may share one minute-stamped file; count across all of them.
    # The old per-run addHandler() made the second run print every line twice (3, not 2).
    text = "".join(p.read_text() for p in sorted((data_dir / "logs").glob("pull_daily_*.log")))
    assert text.count("pull_daily starting") == 2
    assert text.count("pull_daily done") == 2
    assert text.count("T-A: 0 candles returned") == 1
    assert text.count("T-B: 0 candles returned") == 1

    err = capsys.readouterr().err
    assert err.count("pull_daily starting") == 2
    assert err.count("T-B: 0 candles returned") == 1


def test_run_leaves_no_file_handler_and_one_console_handler(monkeypatch):
    _stub_puller(monkeypatch)
    for _ in range(3):
        pull_daily.run(["T-A"])
    parent = logging.getLogger(runlog.PARENT_LOGGER)
    assert _file_handlers() == []
    assert len([h for h in parent.handlers if getattr(h, "_kalshi_console", False)]) == 1
    # Handlers live on the parent only; child loggers just propagate
    assert pull_daily.logger.handlers == []


def test_file_handler_is_removed_and_closed_when_the_run_raises():
    with pytest.raises(RuntimeError):
        with runlog.run_logging("boom"):
            (handler,) = _file_handlers()
            raise RuntimeError("run failed")
    assert _file_handlers() == []
    assert handler.stream is None or handler.stream.closed


def test_nested_run_logs_into_the_outer_file_only(monkeypatch, data_dir):
    _stub_puller(monkeypatch)
    with runlog.run_logging("poll_focus", stamp_fmt="%Y%m%d") as outer_path:
        pull_daily.run(["T-A"])
        pull_daily.run(["T-B"])
        assert len(_file_handlers()) == 1
    assert sorted((data_dir / "logs").glob("*.log")) == [outer_path]
    text = outer_path.read_text()
    assert text.count("pull_daily starting") == 2
    assert text.count("T-B: 0 candles returned") == 1


def test_configure_logging_is_idempotent():
    for _ in range(5):
        parent = runlog.configure_logging()
    assert len(parent.handlers) == 1
