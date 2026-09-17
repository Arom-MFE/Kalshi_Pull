"""Offline tests for trade resume, tier routing and failure handling (fake exchange)."""

import pandas as pd
import pytest

import kalshi_io.trades as trades_mod
import pull_historical.pull_trades as pull_trades
from kalshi_io.candles import register_ticker_meta
from kalshi_io.config import TRADES_RESUME_OVERLAP_S
from fakes import FakeResponse, make_event, make_market, make_trade

TICKER = "TEST-26SEP-T1"


@pytest.fixture
def exchange(fake_api, catalog_dir):
    fake_api.add_event(make_event("TEST-26SEP", "TEST"), [make_market(TICKER, "TEST-26SEP")])
    register_ticker_meta({TICKER: ("TEST", "TEST-26SEP")})
    fake_api.trades[TICKER] = [
        # Before the 2026-07-19 cutoff: only on /historical/trades
        make_trade("h-1", TICKER, "2026-07-01T10:00:00.000000Z"),
        make_trade("h-2", TICKER, "2026-07-02T10:00:00.000000Z"),
        # Live tier, two months
        make_trade("a-1", TICKER, "2026-08-31T23:59:30.000000Z"),
        make_trade("s-1", TICKER, "2026-09-01T00:00:10.000000Z"),
        make_trade("s-2", TICKER, "2026-09-17T02:43:00.732373Z"),
    ]
    return fake_api


def _stored(data_dir) -> pd.DataFrame:
    files = sorted((data_dir / "trades" / "TEST" / TICKER).glob("*.parquet"))
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def test_cold_start_pulls_both_tiers_and_partitions_by_month(exchange, data_dir):
    summary = pull_trades.run([TICKER])
    assert summary["rows_written"] == 5 and summary["failed"] == 0
    files = sorted(p.name for p in (data_dir / "trades" / "TEST" / TICKER).glob("*.parquet"))
    assert files == ["2026-07.parquet", "2026-08.parquet", "2026-09.parquet"]
    # No min_ts on a cold start: the complete tape from both tiers
    assert all("min_ts" not in params for _, params, _ in exchange.calls)
    assert len(exchange.requests_to("/historical/trades")) == 1


def test_rerun_asks_only_for_the_tail_skips_historical_and_writes_nothing(exchange, data_dir, monkeypatch):
    pull_trades.run([TICKER])
    before = {p: p.read_bytes() for p in (data_dir / "trades").rglob("*.parquet")}
    exchange.calls.clear()
    appended = []
    real_append = pull_trades.append_parquet
    monkeypatch.setattr(pull_trades, "append_parquet", lambda *a, **k: appended.append(a) or real_append(*a, **k))

    summary = pull_trades.run([TICKER])

    assert summary["rows_written"] == 0 and summary["processed"] == 1
    trade_calls = exchange.requests_to("/markets/trades")
    assert len(trade_calls) == 1                       # was: the full tape, every cycle
    last_stored_s = 1789612980                         # 2026-09-17T02:43:00Z
    assert trade_calls[0][1]["min_ts"] == last_stored_s - TRADES_RESUME_OVERLAP_S
    assert exchange.requests_to("/historical/trades") == []
    # Nothing new: no parquet is rewritten, not even with identical content
    assert appended == []
    assert {p: p.read_bytes() for p in (data_dir / "trades").rglob("*.parquet")} == before


def test_new_trades_sharing_the_last_timestamp_are_kept_across_page_boundaries(exchange, data_dir):
    pull_trades.run([TICKER])
    # A sweep: five more fills in the same microsecond as the last stored trade
    exchange.trades[TICKER] += [make_trade(f"sweep-{i}", TICKER, "2026-09-17T02:43:00.732373Z") for i in range(5)]
    exchange.page_size = 2
    summary = pull_trades.run([TICKER])
    assert summary["rows_written"] == 5
    stored = _stored(data_dir)
    assert stored["trade_id"].is_unique and len(stored) == 10
    assert {f"sweep-{i}" for i in range(5)} <= set(stored["trade_id"])


def test_overlap_reaching_into_the_previous_month_does_not_rewrite_it(exchange, data_dir):
    exchange.trades[TICKER] = [t for t in exchange.trades[TICKER] if t["trade_id"] in ("a-1", "s-1")]
    pull_trades.run([TICKER])
    aug = data_dir / "trades" / "TEST" / TICKER / "2026-08.parquet"
    before = aug.read_bytes()
    # The last stored trade is 10 s into September: the 60 s overlap re-fetches a-1 from August
    exchange.trades[TICKER].append(make_trade("s-9", TICKER, "2026-09-01T00:00:40.000000Z"))
    summary = pull_trades.run([TICKER])
    assert summary["rows_written"] == 1
    assert aug.read_bytes() == before
    assert sorted(_stored(data_dir)["trade_id"]) == ["a-1", "s-1", "s-9"]


def test_a_failed_page_writes_nothing_and_the_next_run_completes(exchange, data_dir):
    exchange.page_size = 2
    page1 = FakeResponse(200, {"trades": exchange.trades[TICKER][3:5], "cursor": "2"})
    exchange.inject(r"^/markets/trades$", [page1, *[FakeResponse(503, {"error": {"message": "down"}})] * 6])

    summary = pull_trades.run([TICKER])
    # Pages arrive newest-first: storing page 1 would move the resume point past
    # the older trades that were never fetched. All or nothing.
    assert summary["failed"] == 1 and summary["rows_written"] == 0
    assert not (data_dir / "trades").exists()

    summary = pull_trades.run([TICKER])
    assert summary["failed"] == 0 and summary["rows_written"] == 5
    assert len(_stored(data_dir)) == 5


def test_min_ts_older_than_the_cutoff_also_queries_the_historical_tier(exchange):
    df = trades_mod.fetch_trades(TICKER, min_ts=1782900000)     # 2026-07-01T10:00:00Z
    assert len(exchange.requests_to("/historical/trades")) == 1
    assert sorted(df["trade_id"]) == ["a-1", "h-1", "h-2", "s-1", "s-2"]
    assert exchange.requests_to("/historical/trades")[0][1]["min_ts"] == 1782900000


def test_unknown_cutoff_queries_both_tiers(exchange):
    exchange.inject(r"^/historical/cutoff$", [FakeResponse(503, {"error": {"message": "down"}})] * 6)
    df = trades_mod.fetch_trades(TICKER, min_ts=1789000000)
    assert len(exchange.requests_to("/historical/trades")) == 1
    assert sorted(df["trade_id"]) == ["s-2"]


def test_cutoff_is_cached_for_an_hour(exchange, monkeypatch):
    now = [5000.0]
    monkeypatch.setattr(trades_mod, "_monotonic", lambda: now[0])
    trades_mod.fetch_trades(TICKER, min_ts=1789000000)
    trades_mod.fetch_trades(TICKER, min_ts=1789000000)
    assert len(exchange.requests_to("/historical/cutoff")) == 1
    now[0] += trades_mod.CUTOFF_TTL_SECONDS + 1            # a poller runs for weeks
    trades_mod.fetch_trades(TICKER, min_ts=1789000000)
    assert len(exchange.requests_to("/historical/cutoff")) == 2


def test_since_is_pushed_into_the_request_and_still_filters(exchange, data_dir):
    summary = pull_trades.run([TICKER], since="2026-09-01")
    assert summary["rows_written"] == 2
    assert exchange.requests_to("/markets/trades")[0][1]["min_ts"] == 1788220800    # 2026-09-01T00:00:00Z
    assert sorted(_stored(data_dir)["trade_id"]) == ["s-1", "s-2"]
