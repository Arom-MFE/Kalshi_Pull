"""Offline end-to-end tests for the three candle pullers against the fake exchange."""

import time

import pandas as pd
import pytest

import pull_historical.pull_daily as pull_daily
import pull_historical.pull_hourly as pull_hourly
import pull_historical.pull_minute as pull_minute
from kalshi_io import runlog
from fakes import FakeResponse, iso_to_ts, make_candle, make_event, make_market

TICKER = "TEST-26JAN-T1"
OPEN = "2025-12-20T00:00:00Z"
NOW = "2026-01-10T00:00:00Z"      # 21 days after open, across a year and a month boundary

# puller, period_interval, seconds between synthetic candles, expected files
CASES = {
    "daily": (pull_daily, 1440, 86400, ["candles/daily/TEST/TEST-26JAN-T1.parquet"]),
    "hourly": (pull_hourly, 60, 6 * 3600, [
        "candles/hourly/TEST/2025/TEST-26JAN-T1.parquet",
        "candles/hourly/TEST/2026/TEST-26JAN-T1.parquet",
    ]),
    "minute": (pull_minute, 1, 3600, [
        "candles/minute/TEST/2025/12/TEST-26JAN-T1.parquet",
        "candles/minute/TEST/2026/01/TEST-26JAN-T1.parquet",
    ]),
}


def _candles(step: int) -> list[dict]:
    start, end = int(iso_to_ts(OPEN)), int(iso_to_ts(NOW))
    return [make_candle(ts) for ts in range(start + step, end + 1, step)]


@pytest.fixture
def exchange(fake_api, catalog_dir, monkeypatch):
    fake_api.now = iso_to_ts(NOW)
    monkeypatch.setattr(time, "time", lambda: fake_api.now)
    fake_api.add_event(make_event("TEST-26JAN", "TEST"), [
        make_market(TICKER, "TEST-26JAN", open_time=OPEN),
    ])
    fake_api.candles[TICKER] = {interval: _candles(step) for _, interval, step, _ in CASES.values()}
    return fake_api


def _stored(data_dir, kind_files) -> pd.DataFrame:
    return pd.concat([pd.read_parquet(data_dir / f) for f in kind_files], ignore_index=True)


def _candle_requests(api):
    return [c for c in api.calls if c[0].endswith("/candlesticks")]


@pytest.mark.parametrize("kind", CASES)
def test_cold_start_then_rerun_adds_zero_rows(kind, exchange, data_dir):
    puller, interval, step, files = CASES[kind]
    expected = len(_candles(step))

    summary = puller.run([TICKER])
    assert summary["rows_written"] == expected and summary["failed"] == 0
    assert sorted(str(p.relative_to(data_dir)) for p in (data_dir / "candles").rglob("*.parquet")) == files
    # Cold start begins at the market's open_time
    assert _candle_requests(exchange)[0][1]["start_ts"] == int(iso_to_ts(OPEN))

    stored = _stored(data_dir, files)
    assert stored["ts_ms"].is_unique and len(stored) == expected
    # Uncataloged ticker: the event comes from the API, never from the ticker text
    assert set(stored["event_ticker"]) == {"TEST-26JAN"}
    assert set(stored["series_ticker"]) == {"TEST"}

    exchange.calls.clear()
    exchange.now += 600                  # ten minutes later, nothing new happened
    rerun = puller.run([TICKER])
    assert rerun["rows_written"] == 0 and rerun["processed"] == 1
    # Windows are inclusive: resuming at the last stored candle re-fetches exactly that one
    assert _candle_requests(exchange)[0][1]["start_ts"] == int(stored["ts_ms"].max() // 1000)
    assert len(_stored(data_dir, files)) == expected


def test_failure_mid_backfill_saves_the_prefix_records_the_gap_and_the_next_run_completes(exchange, data_dir):
    _, _, step, files = CASES["minute"]
    expected = len(_candles(step))
    # 21 days = seven 3-day windows; the third one fails on every attempt
    exchange.inject(r"/candlesticks$", [FakeResponse(503, {"error": {"message": "down"}})] * 6, after=2)

    summary = pull_minute.run([TICKER])

    assert summary["failed"] == 1 and summary["processed"] == 0
    assert 0 < summary["rows_written"] < expected
    saved = pd.read_parquet(data_dir / files[0])
    # The saved rows are the gap-free prefix: everything up to the failed window's start
    fail_start_ms = (int(iso_to_ts(OPEN)) + 2 * 3 * 86400) * 1000
    assert saved["ts_ms"].max() <= fail_start_ms
    assert len(saved) == summary["rows_written"]

    skip_lines = (data_dir / "logs" / f"skip_minute_{runlog.PROCESS_STAMP}.txt").read_text().splitlines()
    assert len(skip_lines) == 1 and "fetch stopped at 2025-12-26T00:00:00Z" in skip_lines[0]
    log_text = "".join(p.read_text() for p in (data_dir / "logs").glob("pull_minute_*.log"))
    assert " ERROR " in log_text and "FAILED — PartialCandlesError" in log_text

    # Next run resumes from the last saved candle: no hole, no duplicates
    summary = pull_minute.run([TICKER])
    assert summary["failed"] == 0
    stored = _stored(data_dir, files)
    assert stored["ts_ms"].is_unique and len(stored) == expected
    assert sorted(stored["ts_ms"]) == [c["end_period_ts"] * 1000 for c in _candles(step)]


def test_first_window_failure_raises_without_writing(exchange, data_dir):
    exchange.inject(r"/candlesticks$", [FakeResponse(500, {"error": {"message": "boom"}})] * 6)
    summary = pull_minute.run([TICKER])
    assert summary["failed"] == 1 and summary["rows_written"] == 0
    assert not (data_dir / "candles").exists()


def test_market_in_the_historical_tier_switches_endpoint_once(exchange, data_dir):
    exchange.markets[TICKER]["_tier"] = "historical"
    exchange.markets[TICKER]["status"] = "finalized"
    summary = pull_minute.run([TICKER])
    assert summary["failed"] == 0 and summary["rows_written"] == len(_candles(3600))
    live = [c for c in _candle_requests(exchange) if c[0].startswith("/series/")]
    hist = [c for c in _candle_requests(exchange) if c[0].startswith("/historical/")]
    # One live 404, then every window goes straight to /historical/
    assert len(live) == 1 and len(hist) == 7
    # The historical shape (price.close, volume) normalizes into the same columns
    stored = _stored(data_dir, CASES["minute"][3])
    assert stored["close"].eq(0.5).all() and stored["volume"].eq(10.0).all()


def test_ticker_unknown_to_the_api_is_a_recorded_failure(exchange, data_dir):
    summary = pull_daily.run(["TEST-26JAN-NOPE"])
    assert summary["failed"] == 1 and summary["rows_written"] == 0
    line = (data_dir / "logs" / f"skip_daily_{runlog.PROCESS_STAMP}.txt").read_text()
    assert "UnknownTickerError" in line
