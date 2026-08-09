"""Offline tests for trade normalization. API pagination is monkeypatched."""

import kalshi_io.trades as trades_mod
from kalshi_io.trades import TRADE_COLUMNS, fetch_trades

# Shape and values mirror a real /markets/trades response
WIRE = [
    {
        "trade_id": "t-1",
        "ticker": "TEST-26",
        "created_time": "2022-07-28T13:31:07.61886Z",
        "yes_price_dollars": "0.9900",
        "no_price_dollars": "0.0100",
        "count_fp": "13.41",
        "taker_side": "yes",
        "is_block_trade": False,
    },
    {
        "trade_id": "t-2",
        "ticker": "TEST-26",
        "created_time": "2022-07-28T13:09:01.664058Z",
        "yes_price_dollars": "0.5000",
        "no_price_dollars": "0.5000",
        "count_fp": "50.00",
        "taker_side": "no",
        "is_block_trade": False,
    },
]


def _patch_wire(monkeypatch):
    monkeypatch.setattr(
        trades_mod,
        "_paginate_trades",
        lambda endpoint, ticker: list(WIRE) if endpoint == "/markets/trades" else [],
    )


def test_rename_and_cast(monkeypatch):
    _patch_wire(monkeypatch)
    df = fetch_trades("TEST-26")
    assert list(df.columns) == TRADE_COLUMNS
    assert df["yes_price"].dtype == "float64"
    assert df["no_price"].dtype == "float64"
    assert df["count"].dtype == "float64"
    assert ((df["yes_price"] >= 0.0) & (df["yes_price"] <= 1.0)).all()
    assert ((df["no_price"] >= 0.0) & (df["no_price"] <= 1.0)).all()


def test_fractional_count_preserved_exactly(monkeypatch):
    _patch_wire(monkeypatch)
    df = fetch_trades("TEST-26")
    assert df.loc[df["trade_id"] == "t-1", "count"].iloc[0] == 13.41
    assert df.loc[df["trade_id"] == "t-2", "count"].iloc[0] == 50.0


def test_created_time_iso_to_int64_epoch_ms(monkeypatch):
    _patch_wire(monkeypatch)
    df = fetch_trades("TEST-26")
    assert df["ts_ms"].dtype == "int64"
    # 2022-07-28T13:31:07.61886Z == 1659015067.61886 s UTC
    assert df.loc[df["trade_id"] == "t-1", "ts_ms"].iloc[0] == 1659015067618
    # Sorted ascending by ts_ms — the earlier trade comes first
    assert df.iloc[0]["trade_id"] == "t-2"
