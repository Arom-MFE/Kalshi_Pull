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
        lambda endpoint, ticker, min_ts=None: list(WIRE) if endpoint == "/markets/trades" else [],
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


def test_taker_side_falls_back_to_taker_outcome_side_when_the_deprecated_field_is_gone(monkeypatch):
    # Kalshi deprecated taker_side in May 2026; taker_outcome_side carries the same yes/no
    wire = [{k: v for k, v in t.items() if k != "taker_side"} for t in WIRE]
    wire[0]["taker_outcome_side"] = "yes"
    wire[1]["taker_outcome_side"] = "no"
    monkeypatch.setattr(
        trades_mod, "_paginate_trades",
        lambda endpoint, ticker, min_ts=None: list(wire) if endpoint == "/markets/trades" else [],
    )
    df = fetch_trades("TEST-26")
    assert list(df.columns) == TRADE_COLUMNS
    assert df.loc[df["trade_id"] == "t-1", "taker_side"].iloc[0] == "yes"
    assert df.loc[df["trade_id"] == "t-2", "taker_side"].iloc[0] == "no"


def test_present_taker_side_is_never_overwritten_and_absent_sides_stay_missing(monkeypatch):
    wire = [dict(WIRE[0], taker_outcome_side="no"), {k: v for k, v in WIRE[1].items() if k != "taker_side"}]
    monkeypatch.setattr(
        trades_mod, "_paginate_trades",
        lambda endpoint, ticker, min_ts=None: list(wire) if endpoint == "/markets/trades" else [],
    )
    df = fetch_trades("TEST-26")
    assert df.loc[df["trade_id"] == "t-1", "taker_side"].iloc[0] == "yes"
    # Neither field present: missing stays missing, nothing is invented
    assert df.loc[df["trade_id"] == "t-2", "taker_side"].isna().all()


def test_taker_side_falls_back_to_taker_book_side_last(monkeypatch):
    # The spec's other required direction field: "bid" is equivalent to yes, "ask" to no
    base = [{k: v for k, v in t.items() if k != "taker_side"} for t in WIRE]
    wire = [dict(base[0], taker_book_side="bid"), dict(base[1], taker_book_side="ask"),
            dict(base[0], trade_id="t-3", taker_outcome_side="no", taker_book_side="bid"),   # outcome side wins
            dict(base[0], trade_id="t-4", taker_book_side="sideways")]                       # unknown value: stays missing
    monkeypatch.setattr(
        trades_mod, "_paginate_trades",
        lambda endpoint, ticker, min_ts=None: list(wire) if endpoint == "/markets/trades" else [],
    )
    df = fetch_trades("TEST-26").set_index("trade_id")
    assert list(df.loc[["t-1", "t-2", "t-3"], "taker_side"]) == ["yes", "no", "no"]
    assert df.loc[["t-4"], "taker_side"].isna().all()


def test_legacy_since_trade_id_keeps_same_timestamp_siblings(monkeypatch):
    # Several fills of one sweep share a timestamp; the old strict ">" dropped the siblings
    wire = [dict(WIRE[0], trade_id=f"t-{i}") for i in (1, 2, 3)]
    monkeypatch.setattr(
        trades_mod, "_paginate_trades",
        lambda endpoint, ticker, min_ts=None: list(wire) if endpoint == "/markets/trades" else [],
    )
    df = fetch_trades("TEST-26", since_trade_id="t-2")
    assert sorted(df["trade_id"]) == ["t-1", "t-3"]
