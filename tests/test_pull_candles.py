"""Offline end-to-end tests for the three candle pullers against the fake exchange."""

import time

import pandas as pd
import pyarrow.parquet as pq
import pytest

import pull_historical.pull_daily as pull_daily
import pull_historical.pull_hourly as pull_hourly
import pull_historical.pull_minute as pull_minute
from kalshi_io import runlog
from kalshi_io.candles import CANDLE_COLUMNS, QUOTE_COLUMNS
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


def test_ticker_unknown_to_the_api_is_reported_and_recorded_not_fetched(exchange, data_dir):
    summary = pull_daily.run(["TEST-26JAN-NOPE"])
    assert summary["unknown"] == ["TEST-26JAN-NOPE"] and summary["skipped"] == 1
    assert summary["failed"] == 0 and summary["rows_written"] == 0
    assert _candle_requests(exchange) == []
    line = (data_dir / "logs" / f"skip_daily_{runlog.PROCESS_STAMP}.txt").read_text()
    assert "TEST-26JAN-NOPE\tunknown ticker" in line


# ------------------------------------------------------------------ yes_bid / yes_ask columns

@pytest.mark.parametrize("kind", CASES)
def test_stored_candles_carry_the_quote_columns(kind, exchange, data_dir):
    puller, _, _, files = CASES[kind]
    puller.run([TICKER])
    for f in files:
        assert tuple(pq.read_schema(data_dir / f).names) == CANDLE_COLUMNS
    stored = _stored(data_dir, files)
    assert {str(stored[c].dtype) for c in QUOTE_COLUMNS} == {"float64"}
    # Bid and ask are kept apart from each other and from the traded price
    assert stored[[c for c in QUOTE_COLUMNS if "bid" in c]].eq(0.49).all().all()
    assert stored[[c for c in QUOTE_COLUMNS if "ask" in c]].eq(0.51).all().all()
    assert stored["close"].eq(0.5).all()


def test_appending_to_a_file_from_before_the_quote_columns_keeps_its_rows(exchange, data_dir):
    path = data_dir / CASES["daily"][3][0]
    candles = _candles(86400)
    legacy = pd.DataFrame([{
        "ts_ms": c["end_period_ts"] * 1000, "open": 0.5, "high": 0.5, "low": 0.5, "close": 0.5, "mean": 0.5,
        "volume": 10.0, "open_interest": 100.0,
        "market_ticker": TICKER, "event_ticker": "TEST-26JAN", "series_ticker": "TEST",
    } for c in candles[:10]])
    path.parent.mkdir(parents=True)
    legacy.to_parquet(path, engine="pyarrow", compression="zstd", index=False)

    summary = pull_daily.run([TICKER])

    assert summary["failed"] == 0 and summary["rows_written"] == len(candles) - 10
    assert tuple(pq.read_schema(path).names) == CANDLE_COLUMNS     # new columns land after the old eleven
    stored = pd.read_parquet(path)
    assert list(stored["ts_ms"]) == [c["end_period_ts"] * 1000 for c in candles]
    # Rows stored before the change have no quotes (NaN, never 0). The resume window is inclusive,
    # so the last old row was fetched again and now has them, like every new row.
    assert stored[list(QUOTE_COLUMNS)].iloc[:9].isna().all().all()
    assert stored["yes_bid_close"].iloc[9:].eq(0.49).all() and stored["yes_ask_close"].iloc[9:].eq(0.51).all()
    assert stored[list(CANDLE_COLUMNS[1:8])].iloc[:10].equals(legacy[list(CANDLE_COLUMNS[1:8])])

    exchange.now += 600
    assert pull_daily.run([TICKER])["rows_written"] == 0


@pytest.mark.parametrize("tier", ["live", "historical"])
def test_batch_without_any_trade_is_stored_as_float64_nan_with_quotes(tier, exchange, data_dir):
    exchange.candles[TICKER][1] = [make_candle(c["end_period_ts"], traded=False, volume="0.00")
                                   for c in _candles(3600)]
    if tier == "historical":
        exchange.markets[TICKER]["_tier"] = "historical"
        exchange.markets[TICKER]["status"] = "finalized"
    assert pull_minute.run([TICKER])["failed"] == 0
    files = CASES["minute"][3]
    for f in files:
        schema = pq.read_schema(data_dir / f)
        assert {str(schema.field(c).type) for c in ("open", "close", "mean", *QUOTE_COLUMNS)} == {"double"}
    stored = _stored(data_dir, files)
    assert stored["mean"].isna().all() and stored["volume"].eq(0.0).all()
    assert stored["yes_bid_close"].eq(0.49).all() and stored["yes_ask_close"].eq(0.51).all()
    # No trade, no price, on either tier: the quotes are the only prices of these rows
    assert stored[["open", "high", "low", "close"]].isna().all().all()


def test_every_interval_and_both_tiers_store_one_schema(exchange, data_dir):
    """Column names, order and types are identical across daily, hourly and
    minute files, whether the candles came from the live or the historical tier."""
    schemas = {}
    for tier in ("live", "historical"):
        exchange.markets[TICKER]["_tier"] = tier
        exchange.markets[TICKER]["status"] = "active" if tier == "live" else "finalized"
        for kind, (puller, _, _, files) in CASES.items():
            assert puller.run([TICKER])["failed"] == 0
            for f in files:
                schema = pq.read_schema(data_dir / f)
                schemas[(tier, kind, f)] = [(field.name, str(field.type)) for field in schema]
        for f in (data_dir / "candles").rglob("*.parquet"):
            f.unlink()
    (first, *rest) = schemas.values()
    assert [name for name, _ in first] == list(CANDLE_COLUMNS)
    assert {typ for name, typ in first if name not in ("ts_ms", "market_ticker", "event_ticker", "series_ticker")} == {"double"}
    assert dict(first)["ts_ms"] == "int64"
    assert all(schema == first for schema in rest) and len(schemas) == 10
