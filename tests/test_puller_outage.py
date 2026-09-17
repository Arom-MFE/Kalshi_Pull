"""Offline tests for the outage breaker in the pullers. API seams are stubbed."""

import pull_historical.pull_daily as pull_daily
from kalshi_io import runlog
from kalshi_io.client import RetriesExhausted


def _stub_puller(monkeypatch, outcomes: dict):
    """outcomes maps ticker -> "ok" | "outage" | "bug"."""
    monkeypatch.setattr(pull_daily, "load_tickers", lambda source: list(source))
    monkeypatch.setattr(pull_daily, "validate_tickers", lambda tickers, **kw: (list(tickers), []), raising=False)
    monkeypatch.setattr(pull_daily, "resolve_ticker_meta", lambda ticker: ("TEST", "TEST-26"))
    monkeypatch.setattr(
        pull_daily, "get_market_metadata",
        lambda ticker: {"open_ts_ms": 0, "expiration_time": "unknown", "status": "active"},
    )

    def fetch(ticker, *args):
        if outcomes[ticker] == "outage":
            raise RetriesExhausted(503, "https://example.invalid/x", "HTTP 503: try later", attempts=6)
        if outcomes[ticker] == "bug":
            raise ValueError("unexpected payload")
        return []

    monkeypatch.setattr(pull_daily, "fetch_candles", fetch)


def _skip_lines(data_dir):
    path = data_dir / "logs" / f"skip_daily_{runlog.PROCESS_STAMP}.txt"
    return [ln.split("\t")[1] for ln in path.read_text().splitlines()]


def test_run_aborts_after_three_consecutive_outages(monkeypatch, data_dir):
    tickers = ["T-1", "T-2", "T-3", "T-4", "T-5"]
    _stub_puller(monkeypatch, dict.fromkeys(tickers, "outage"))
    summary = pull_daily.run(tickers)
    # T-4 and T-5 are never attempted: the API is down, hammering it helps nobody
    assert summary["aborted"] is True
    assert summary["failed"] == 3 and summary["skipped"] == 5 and summary["processed"] == 0
    assert _skip_lines(data_dir) == ["T-1", "T-2", "T-3", "*"]


def test_a_success_resets_the_outage_streak(monkeypatch):
    outcomes = {"T-1": "outage", "T-2": "outage", "T-3": "ok", "T-4": "outage", "T-5": "outage", "T-6": "ok"}
    _stub_puller(monkeypatch, outcomes)
    summary = pull_daily.run(list(outcomes))
    # "ok" tickers leave the loop through `continue`; the streak must still reset
    assert summary["aborted"] is False
    assert summary["failed"] == 4 and summary["processed"] == 2


def test_ordinary_errors_never_abort_the_run(monkeypatch):
    tickers = ["T-1", "T-2", "T-3", "T-4", "T-5"]
    _stub_puller(monkeypatch, dict.fromkeys(tickers, "bug"))
    summary = pull_daily.run(tickers)
    assert summary["aborted"] is False and summary["failed"] == 5
