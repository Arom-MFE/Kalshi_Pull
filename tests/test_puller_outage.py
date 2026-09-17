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
        pull_daily, "market_window",
        lambda ticker, allow_api=True: {"open_ts": 0, "close_ts": None, "status": "active", "source": "stub"},
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


# ------------------------------------------------------------------ per-ticker outcomes for a caller that asks

SUMMARY_KEYS = {"processed", "skipped", "failed", "aborted", "unknown", "rows_written", "elapsed_sec"}


def test_results_collector_gets_one_outcome_per_ticker_and_the_summary_stays_as_it_was(monkeypatch):
    outcomes = {"T-1": "ok", "T-2": "bug", "T-3": "outage", "T-4": "outage", "T-5": "outage", "T-6": "ok"}
    _stub_puller(monkeypatch, outcomes)
    results: dict = {}
    summary = pull_daily.run(list(outcomes), results=results)

    # The summary only counts; the poller logs it every minute, so it must stay small
    assert set(summary) == SUMMARY_KEYS and summary["aborted"] is True
    assert {t: r["status"] for t, r in results.items()} == {
        "T-1": "empty", "T-2": "failed", "T-3": "failed", "T-4": "failed", "T-5": "failed", "T-6": "not_attempted"}
    # An outage is told apart from an ordinary error: the driver backs off on the first, not on the second
    assert [results[t]["outage"] for t in outcomes] == [False, False, True, True, True, False]
    assert results["T-2"]["error"] == "ValueError: unexpected payload" and "gave up after 6 attempts" in results["T-3"]["error"]
    assert pull_daily.run(["T-1"])["processed"] == 1                       # the collector is optional


def test_should_stop_leaves_the_remaining_tickers_unattempted(monkeypatch):
    tickers = ["T-1", "T-2", "T-3"]
    _stub_puller(monkeypatch, dict.fromkeys(tickers, "ok"))
    seen: list[int] = []
    results: dict = {}
    summary = pull_daily.run(tickers, results=results, should_stop=lambda: seen.append(1) or len(seen) > 1)
    assert summary["processed"] == 1 and summary["skipped"] == 2 and summary["aborted"] is False
    assert [results[t]["status"] for t in tickers] == ["empty", "not_attempted", "not_attempted"]
