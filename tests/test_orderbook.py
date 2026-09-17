"""Offline tests for orderbook snapshots (fake exchange)."""

from types import SimpleNamespace

import pandas as pd
import pytest

import kalshi_io.client as client
from kalshi_io.orderbook import append_orderbook_snapshot, snapshot_orderbook, snapshot_orderbooks
from fakes import FakeResponse, make_event, make_market

TICKER = "TEST-26SEP-T1"
COLUMNS = ["ts_ms", "market_ticker", "side", "price", "quantity", "cumulative_qty", "distance_from_top"]


@pytest.fixture
def exchange(fake_api):
    fake_api.add_event(make_event("TEST-26SEP", "TEST"), [make_market(TICKER, "TEST-26SEP")])
    # Shape mirrors a real GET /markets/{ticker}/orderbook body: [price, quantity] string pairs
    fake_api.orderbooks[TICKER] = {
        "yes_dollars": [["0.9600", "42000.00"], ["0.9800", "124.00"], ["0.9700", "2964.51"]],
        "no_dollars": [["0.0100", "2391.00"]],
    }
    return fake_api


def test_snapshot_parses_the_rest_payload_into_the_stored_schema(exchange):
    df = snapshot_orderbook(TICKER)
    assert list(df.columns) == COLUMNS
    yes = df[df["side"] == "YES"]
    # Best bid first; fractional quantities pass through unscaled
    assert list(yes["price"]) == [0.98, 0.97, 0.96]
    assert list(yes["quantity"]) == [124.0, 2964.51, 42000.0]
    assert list(yes["cumulative_qty"]) == pytest.approx([124.0, 3088.51, 45088.51])
    assert list(yes["distance_from_top"]) == [0, 1, 2]
    assert list(df[df["side"] == "NO"]["price"]) == [0.01]
    assert df["ts_ms"].nunique() == 1 and df["price"].dtype == "float64"
    # Keyless: no auth header was sent
    assert "KALSHI-ACCESS-KEY" not in exchange.calls[0][2]


def test_snapshot_is_written_to_the_day_of_its_own_timestamp(exchange, data_dir):
    df = snapshot_orderbook(TICKER)
    # 2026-09-16T23:59:59.999Z: the file must be the 16th even if the write happens on the 17th
    df["ts_ms"] = 1789603199999
    assert append_orderbook_snapshot(TICKER, df) == 4
    assert [p.name for p in (data_dir / "orderbook" / TICKER).glob("*.parquet")] == ["2026-09-16.parquet"]
    assert append_orderbook_snapshot(TICKER, df) == 0


def test_settled_market_gives_an_empty_frame_and_writes_nothing(exchange, data_dir):
    exchange.orderbooks[TICKER] = {"yes_dollars": [], "no_dollars": []}
    df = snapshot_orderbook(TICKER)
    assert df.empty
    assert append_orderbook_snapshot(TICKER, df) == 0
    assert not (data_dir / "orderbook").exists()


def test_orderbook_falls_back_to_a_signed_request_when_the_api_wants_auth(exchange, monkeypatch):
    """The OpenAPI spec declares this endpoint authenticated; today it answers keyless."""
    exchange.auth_required = {f"/markets/{TICKER}/orderbook"}
    signer = SimpleNamespace(create_auth_headers=lambda method, url: {"KALSHI-ACCESS-KEY": "test-key"})
    monkeypatch.setattr(client, "get_client", lambda: SimpleNamespace(kalshi_auth=signer))
    assert len(snapshot_orderbook(TICKER)) == 4
    assert [("KALSHI-ACCESS-KEY" in headers) for _, _, headers in exchange.calls] == [False, True]


def test_a_body_without_orderbook_fp_raises(exchange):
    exchange.inject(r"/orderbook$", [FakeResponse(200, {"orderbook": {}})])
    with pytest.raises(KeyError):
        snapshot_orderbook(TICKER)


# ------------------------------------------------------------------ batch snapshots

def test_batch_snapshot_returns_one_frame_per_ticker_with_one_timestamp(exchange):
    exchange.add_event(make_event("TEST-26OCT", "TEST"), [make_market("TEST-26OCT-T1", "TEST-26OCT")])
    exchange.orderbooks["TEST-26OCT-T1"] = {"yes_dollars": [["0.4000", "5.00"]], "no_dollars": []}
    exchange.add_markets(make_market("TEST-26SEP-T9", "TEST-26SEP", status="finalized"))

    books = snapshot_orderbooks([TICKER, "TEST-26OCT-T1", "TEST-26SEP-T9", "TEST-NOPE", TICKER])

    assert set(books) == {TICKER, "TEST-26OCT-T1", "TEST-26SEP-T9"}           # unknown ticker absent, duplicate folded
    assert list(books[TICKER].columns) == COLUMNS and books["TEST-26SEP-T9"].empty
    assert books[TICKER]["ts_ms"].iloc[0] == books["TEST-26OCT-T1"]["ts_ms"].iloc[0]
    # The same rows as the single-ticker call, apart from the timestamp
    single = snapshot_orderbook(TICKER).drop(columns="ts_ms")
    assert single.equals(books[TICKER].drop(columns="ts_ms"))
    # One request for the whole universe, tickers as repeated parameters, no auth header
    (path, params, headers), *_ = [c for c in exchange.calls if c[0] == "/markets/orderbooks"]
    assert params["tickers"] == [TICKER, "TEST-26OCT-T1", "TEST-26SEP-T9", "TEST-NOPE"] and "KALSHI-ACCESS-KEY" not in headers
    assert len([c for c in exchange.calls if c[0] == "/markets/orderbooks"]) == 1


def test_batch_snapshot_chunks_at_one_hundred_tickers(exchange):
    tickers = [f"TEST-26SEP-B{i}" for i in range(150)]
    exchange.add_markets(*[make_market(t, "TEST-26SEP") for t in tickers])
    books = snapshot_orderbooks(tickers)
    assert len(books) == 150
    sizes = [len(params["tickers"]) for path, params, _ in exchange.calls if path == "/markets/orderbooks"]
    assert sizes == [100, 50]


def test_batch_snapshot_uses_the_given_attempts_and_timeout(exchange, clock):
    exchange.inject(r"^/markets/orderbooks$", [FakeResponse(503, {"error": {"message": "down"}})] * 6)
    with pytest.raises(Exception, match="gave up after 2 attempts"):
        snapshot_orderbooks([TICKER], max_attempts=2, timeout=(2, 5))
    assert len([c for c in exchange.calls if c[0] == "/markets/orderbooks"]) == 2
