"""Offline tests for a market's pull window: cold start from the catalog, stop at close for settled tickers."""

import math
import time

import pandas as pd
import pytest

import pull_historical.pull_daily as pull_daily
import pull_historical.pull_hourly as pull_hourly
import pull_historical.pull_minute as pull_minute
from kalshi_io import candles, catalog, resolve
from kalshi_io.config import CHUNK_SECONDS
from fakes import iso_to_ts, make_candle, make_event, make_market

TICKER = "KXTEST-26JAN-T1"
EVENT = "KXTEST-26JAN"
OPEN = "2025-12-20T00:00:00Z"
CLOSE = "2026-01-05T13:29:00Z"          # one minute before a 08:30 ET release
NOW = "2026-01-10T00:00:00Z"            # five days after the close

PULLERS = {"daily": (pull_daily, 1440), "hourly": (pull_hourly, 60), "minute": (pull_minute, 1)}


def _write_catalog(catalog_dir, status="finalized", close_time=CLOSE, with_times=True, source="live"):
    record = {"event_ticker": EVENT, "market_ticker": TICKER, "title": "t", "status": status, "source": source}
    if with_times:
        record.update(open_time=OPEN, close_time=close_time,
                      expected_expiration_time=None, latest_expiration_time=None)
    catalog.write_series({
        "schema_version": 2, "series": "KXTEST", "built_at": "2026-01-09T00:00:00Z", "historical_cutoff": None,
        "status_counts": {status: 1}, "events": [{"event_ticker": EVENT, "title": "e"}],
        "markets": [record], "tickers": [TICKER],
    }, catalog_dir)
    candles._reset_state()


def _life_candles(interval: int) -> list[dict]:
    """A candle per period from open to close, then the closing candle: the period
    that contains close_time ends after it, and nothing follows."""
    period = interval * 60
    open_ts, close_ts = int(iso_to_ts(OPEN)), int(iso_to_ts(CLOSE))
    step = max(period, 3600)
    body = [make_candle(ts) for ts in range(open_ts + step, close_ts, step)]
    closing = (close_ts // period + 1) * period
    return body + [make_candle(closing, close="0.0300")]


@pytest.fixture
def exchange(fake_api, catalog_dir, monkeypatch):
    fake_api.now = iso_to_ts(NOW)
    monkeypatch.setattr(time, "time", lambda: fake_api.now)
    fake_api.add_event(make_event(EVENT, "KXTEST"), [
        make_market(TICKER, EVENT, status="finalized", open_time=OPEN, close_time=CLOSE)])
    fake_api.candles[TICKER] = {interval: _life_candles(interval) for _, interval in PULLERS.values()}
    return fake_api


def _candle_requests(api):
    return [params for path, params, _ in api.calls if path.endswith("/candlesticks")]


# ------------------------------------------------------------------ the pullers

@pytest.mark.parametrize("kind", PULLERS)
def test_settled_ticker_is_pulled_to_its_close_and_keeps_the_closing_candle(kind, exchange, catalog_dir, data_dir):
    puller, interval = PULLERS[kind]
    _write_catalog(catalog_dir)
    open_ts, close_ts, period = int(iso_to_ts(OPEN)), int(iso_to_ts(CLOSE)), interval * 60

    summary = puller.run([TICKER])

    assert summary["failed"] == 0 and summary["rows_written"] == len(_life_candles(interval))
    requests = _candle_requests(exchange)
    # The window ends CLOSE_PAD_PERIODS periods after close_time: the closing candle lies in between
    end_ts = close_ts + resolve.CLOSE_PAD_PERIODS * period
    assert max(r["end_ts"] for r in requests) == end_ts
    assert len(requests) == math.ceil((end_ts - open_ts) / CHUNK_SECONDS[interval])
    # Up to today it would have been more windows, all of them empty
    assert len(requests) <= math.ceil((int(iso_to_ts(NOW)) - open_ts) / CHUNK_SECONDS[interval])
    stored = pd.concat([pd.read_parquet(p) for p in (data_dir / "candles").rglob("*.parquet")])
    closing_ms = (close_ts // period + 1) * period * 1000
    assert stored["ts_ms"].max() == closing_ms and stored.loc[stored["ts_ms"] == closing_ms, "close"].iloc[0] == 0.03
    # A cataloged ticker starts at its open_time without asking the API for metadata
    assert requests[0]["start_ts"] == open_ts
    assert exchange.requests_to(f"/markets/{TICKER}") == [] and exchange.requests_to("/historical/markets/") == []


def test_window_ending_exactly_at_close_time_would_lose_the_closing_candle(exchange):
    """Why the cap lies after close_time (observed on the live API, 2026-09-17)."""
    close_ts = int(iso_to_ts(CLOSE))
    served = [c for c in exchange.candles[TICKER][60] if c["end_period_ts"] <= close_ts]
    assert len(served) == len(exchange.candles[TICKER][60]) - 1


def test_rerun_on_a_settled_ticker_never_looks_past_its_close(exchange, catalog_dir, data_dir):
    _write_catalog(catalog_dir)
    close_ts = int(iso_to_ts(CLOSE))
    for puller, _ in PULLERS.values():
        puller.run([TICKER])
    exchange.calls.clear()
    exchange.now += 30 * 86400                     # a month later

    for kind, (puller, interval) in PULLERS.items():
        rerun = puller.run([TICKER])
        assert rerun["rows_written"] == 0 and rerun["processed"] == 1 and rerun["failed"] == 0, kind
    requests = _candle_requests(exchange)
    # One short window per layer at most: from the closing candle to the cap, never the month since
    assert len(requests) <= 3
    assert all(r["end_ts"] <= close_ts + resolve.CLOSE_PAD_PERIODS * r["period_interval"] * 60 for r in requests)
    assert all(r["end_ts"] - r["start_ts"] <= resolve.CLOSE_PAD_PERIODS * r["period_interval"] * 60 for r in requests)


def test_market_the_catalog_files_under_the_historical_tier_never_asks_the_live_endpoint(exchange, catalog_dir):
    """3,932 of the 4,840 cataloged markets are historical: one wasted 404 each, per interval, until 0.3.0."""
    exchange.markets[TICKER]["_tier"] = "historical"
    _write_catalog(catalog_dir, source="historical")
    for puller, _ in PULLERS.values():
        assert puller.run([TICKER])["failed"] == 0
    paths = [path for path, _, _ in exchange.calls if path.endswith("/candlesticks")]
    assert paths and all(path.startswith("/historical/") for path in paths)


def test_a_stale_tier_hint_costs_one_request_and_no_data(exchange, catalog_dir, data_dir):
    # The catalog says historical, but the market is (still) served by the live tier only
    _write_catalog(catalog_dir, source="historical")
    summary = pull_daily.run([TICKER])
    assert summary["failed"] == 0 and summary["rows_written"] == len(_life_candles(1440))
    paths = [path for path, _, _ in exchange.calls if path.endswith("/candlesticks")]
    assert [path.startswith("/historical/") for path in paths] == [True, False]


def test_market_the_catalog_calls_active_is_pulled_up_to_now(exchange, catalog_dir):
    """The catalog is a snapshot: a status that may be stale never shortens a pull."""
    _write_catalog(catalog_dir, status="active")
    pull_daily.run([TICKER])
    assert max(r["end_ts"] for r in _candle_requests(exchange)) == int(iso_to_ts(NOW))


def test_uncataloged_settled_ticker_learns_its_close_from_the_api(exchange, catalog_dir):
    pull_hourly.run([TICKER])
    assert max(r["end_ts"] for r in _candle_requests(exchange)) == int(iso_to_ts(CLOSE)) + resolve.CLOSE_PAD_PERIODS * 3600
    assert len(exchange.requests_to(f"/markets/{TICKER}")) >= 1


def test_since_still_overrides_the_start(exchange, catalog_dir):
    _write_catalog(catalog_dir)
    pull_daily.run([TICKER], since="2026-01-01")
    assert _candle_requests(exchange)[0]["start_ts"] == int(iso_to_ts("2026-01-01T00:00:00Z"))


def test_pull_minute_cli_resumes_by_default(exchange, catalog_dir, capsys):
    """The CLI default was --since 2025-01-01, which bypassed resume on every CLI run."""
    _write_catalog(catalog_dir)
    assert pull_minute.main(["--tickers", TICKER]) == 0
    assert _candle_requests(exchange)[0]["start_ts"] == int(iso_to_ts(OPEN))
    first_run = len(_candle_requests(exchange))
    assert pull_minute.main(["--tickers", TICKER]) == 0
    # Resumed from the last stored candle (the closing one), not from 2025-01-01 and not from market open
    resumed = _candle_requests(exchange)[first_run:]
    assert [r["start_ts"] for r in resumed] == [int(iso_to_ts(CLOSE)) // 60 * 60 + 60]
    n = len(_candle_requests(exchange))
    assert pull_minute.main(["--tickers", TICKER, "--since", "2026-01-04"]) == 0
    assert _candle_requests(exchange)[n]["start_ts"] == int(iso_to_ts("2026-01-04T00:00:00Z"))
    assert "no --tickers given" not in capsys.readouterr().err


# ------------------------------------------------------------------ resolve.market_window / candle_end_ts

def test_market_window_prefers_the_catalog_and_costs_no_request(exchange, catalog_dir):
    _write_catalog(catalog_dir)
    window = resolve.market_window(TICKER)
    assert window == {"open_ts": int(iso_to_ts(OPEN)), "close_ts": int(iso_to_ts(CLOSE)),
                      "status": "finalized", "tier": "live", "source": "catalog"}
    assert exchange.calls == []


def test_market_window_asks_the_api_once_for_an_uncataloged_ticker_or_a_v1_record(exchange, catalog_dir):
    assert resolve.market_window(TICKER, allow_api=False) == {
        "open_ts": None, "close_ts": None, "status": "unknown", "tier": "", "source": "unknown"}
    assert exchange.calls == []
    window = resolve.market_window(TICKER)
    assert window["source"] == "api" and window["close_ts"] == int(iso_to_ts(CLOSE)) and window["status"] == "finalized"
    n = len(exchange.calls)
    assert resolve.market_window(TICKER)["open_ts"] == int(iso_to_ts(OPEN)) and len(exchange.calls) == n   # remembered

    candles._reset_state()
    _write_catalog(catalog_dir, with_times=False)                     # a v1 record: status, no times
    assert resolve.market_window(TICKER, allow_api=False)["open_ts"] is None
    assert resolve.market_window(TICKER)["source"] == "api"
    assert resolve.market_window("KXNOPE-1")["source"] == "unknown"


def test_an_api_answer_is_newer_than_the_catalog(exchange, catalog_dir):
    _write_catalog(catalog_dir, status="active")
    candles.register_market_windows({TICKER: candles.MarketWindow(int(iso_to_ts(OPEN)), int(iso_to_ts(CLOSE)), "finalized")})
    assert resolve.market_window(TICKER)["status"] == "finalized"


@pytest.mark.parametrize("status,capped", [
    ("initialized", False), ("active", False), ("inactive", False), ("", False), ("something_new", False),
    # A closed market can be reopened with a later close_time: only the terminal status is capped
    ("closed", False), ("determined", False), ("disputed", False), ("amended", False),
    ("finalized", True),
])
def test_only_a_settled_market_is_capped(status, capped):
    now, close = 2_000_000, 1_000_000
    window = {"open_ts": 0, "close_ts": close, "status": status}
    for interval in (1, 60, 1440):
        expected = close + resolve.CLOSE_PAD_PERIODS * interval * 60 if capped else now
        assert resolve.candle_end_ts(window, interval, now) == expected
    # Never later than now, and no close_time means no cap
    assert resolve.candle_end_ts({"close_ts": now - 30, "status": "finalized"}, 1440, now) == now
    assert resolve.candle_end_ts({"close_ts": None, "status": "finalized"}, 60, now) == now


def test_the_pad_covers_the_25_hour_day_on_which_daylight_saving_ends():
    """Daily candles end at midnight Eastern time. On 2026-11-01 that day is 25 hours long: a market closing in
    its first hour has its closing daily candle more than 24 hours after close_time."""
    close = int(iso_to_ts("2026-11-01T04:30:00Z"))          # 00:30 EDT
    closing_daily_candle = int(iso_to_ts("2026-11-02T05:00:00Z"))   # next midnight, now EST
    assert closing_daily_candle - close > 86400
    assert resolve.candle_end_ts({"close_ts": close, "status": "finalized"}, 1440, close + 10 * 86400) >= closing_daily_candle
