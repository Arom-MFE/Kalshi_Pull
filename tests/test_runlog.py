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
    monkeypatch.setattr(
        pull_daily, "load_tickers",
        lambda source: [source] if isinstance(source, str) else list(source),
    )
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


# ------------------------------------------------------------------ skip files

LEGACY_LINE = "KXCPIYOY-26JUL-T3.2 KXCPIYOY-26JUL-T3.3\tcould not resolve open_ts_ms\n"


def test_skip_line_is_timestamped_tab_separated_and_sanitized(data_dir):
    skips = runlog.get_skip_recorder("daily")
    assert skips.record("T-A", "ValidationError: 2 errors\n  field\trequired", code="ValidationError")
    (line,) = skips.path.read_text().splitlines()
    stamp, ticker, reason = line.split("\t")
    assert stamp.endswith("Z") and len(stamp) == len("2026-09-17T12:00:00Z")
    assert ticker == "T-A"
    # Tabs and newlines inside the reason must not break the three-column format
    assert reason == "ValidationError: 2 errors field required"
    assert skips.path.parent == data_dir / "logs"
    assert skips.path.name == f"skip_daily_{runlog.PROCESS_STAMP}.txt"


def test_same_problem_is_recorded_once_per_process_window(monkeypatch):
    skips = runlog.get_skip_recorder("minute")
    assert skips.record("T-A", "HTTP 503 after 6 attempts (1st)", code="RetriesExhausted") is True
    # A poller hits the same failure every cycle; the reason text may differ
    assert skips.record("T-A", "HTTP 503 after 6 attempts (2nd)", code="RetriesExhausted") is False
    assert skips.record("T-A", "unknown ticker", code="unknown") is True
    assert skips.record("T-B", "HTTP 503", code="RetriesExhausted") is True
    assert len(skips.path.read_text().splitlines()) == 3

    # The same recorder is handed out again, so dedupe spans run() calls
    assert runlog.get_skip_recorder("minute") is skips

    real_time = runlog.time.time()
    monkeypatch.setattr(runlog.time, "time", lambda: real_time + runlog.SKIP_DEDUPE_SECONDS + 1)
    assert skips.record("T-A", "HTTP 503 again", code="RetriesExhausted") is True


def test_failed_ticker_is_logged_at_error_counted_and_leaves_legacy_skip_file_alone(monkeypatch, data_dir):
    _stub_puller(monkeypatch)

    def _boom(*args):
        raise RuntimeError("API said no")

    monkeypatch.setattr(pull_daily, "fetch_candles", _boom)
    legacy = data_dir / "logs" / "skip_daily.txt"
    legacy.parent.mkdir(parents=True)
    legacy.write_text(LEGACY_LINE)

    summary = pull_daily.run(["T-A", "T-B"])

    assert summary["failed"] == 2 and summary["skipped"] == 2 and summary["processed"] == 0
    assert legacy.read_text() == LEGACY_LINE
    lines = (data_dir / "logs" / f"skip_daily_{runlog.PROCESS_STAMP}.txt").read_text().splitlines()
    assert [ln.split("\t")[1:] for ln in lines] == [
        ["T-A", "RuntimeError: API said no"],
        ["T-B", "RuntimeError: API said no"],
    ]
    log_text = "".join(p.read_text() for p in (data_dir / "logs").glob("pull_daily_*.log"))
    assert log_text.count(" ERROR ") == 2 and "T-A: FAILED — RuntimeError: API said no" in log_text


def test_cli_exit_code_reports_failures(monkeypatch):
    _stub_puller(monkeypatch)
    assert pull_daily.main(["--tickers", "T-A"]) == 0

    def _boom(*args):
        raise RuntimeError("API said no")

    monkeypatch.setattr(pull_daily, "fetch_candles", _boom)
    assert pull_daily.main(["--tickers", "T-A"]) == 1
