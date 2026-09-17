"""Offline tests for orderbook snapshots (fake exchange)."""

from types import SimpleNamespace

import pandas as pd
import pytest

import kalshi_io.client as client
from kalshi_io.orderbook import append_orderbook_snapshot, snapshot_orderbook
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
