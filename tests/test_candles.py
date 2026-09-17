"""Offline tests for candle normalization. Synthetic wire-JSON fixtures only."""

from types import SimpleNamespace

import pandas as pd

from kalshi_io.candles import CANDLE_COLUMNS, CANDLE_FLOAT_COLUMNS, QUOTE_COLUMNS, candles_frame, parse_candle

# Shape and values mirror a real /historical/markets/.../candlesticks response
HIST_RAW = {
    "end_period_ts": 1658980800,
    "open_interest": "7119.00",
    "volume": "5247.00",
    "price": {
        "open": "0.7900", "high": "0.7900", "low": "0.5900",
        "close": "0.6900", "mean": "0.6886",
    },
    "yes_bid": {
        "open": "0.7900", "high": "0.7900", "low": "0.2500", "close": "0.6500",
    },
    "yes_ask": {
        "open": "0.8100", "high": "0.8200", "low": "0.6000", "close": "0.7000",
    },
}


def _live_raw():
    # Shape mirrors the SDK candlestick object from the live endpoint
    return SimpleNamespace(
        end_period_ts=1786161600,
        volume_fp="11747.08",
        open_interest_fp="897507.46",
        price=SimpleNamespace(
            open_dollars="0.0700", high_dollars="0.0700", low_dollars="0.0300",
            close_dollars="0.0600", mean_dollars="0.0521",
        ),
    )


def test_historical_candle_casts_decimal_strings_to_float():
    c = parse_candle(HIST_RAW, is_historical=True)
    assert c["ts_ms"] == 1658980800000
    for k in ("open", "high", "low", "close", "mean"):
        assert isinstance(c[k], float)
        assert 0.0 <= c[k] <= 1.0
    assert c["close"] == 0.69
    assert isinstance(c["volume"], float) and c["volume"] == 5247.0
    assert isinstance(c["open_interest"], float) and c["open_interest"] == 7119.0


def test_live_candle_fractional_volume_unscaled_passthrough():
    c = parse_candle(_live_raw(), is_historical=False)
    assert c["ts_ms"] == 1786161600000
    assert c["close"] == 0.06
    assert c["mean"] == 0.0521
    # Fixed-point counts are genuine fractional contracts — never rescaled
    assert c["volume"] == 11747.08
    assert c["open_interest"] == 897507.46


def test_zero_price_is_a_trade_price_and_stays_zero():
    raw = {
        **HIST_RAW,
        "price": {"open": "0.0000", "close": "0.0000"},
        "yes_bid": {"open": "0.5000", "close": "0.5000"},
    }
    c = parse_candle(raw, is_historical=True)
    assert c["open"] == 0.0
    assert c["close"] == 0.0


def test_missing_trade_price_stays_missing_and_never_takes_the_bid():
    """Until 0.3.0 the historical tier filled open/high/low/close from yes_bid."""
    raw = {
        **HIST_RAW,
        "price": {"open": None, "high": None, "low": None, "close": None, "mean": None, "previous": "0.5000"},
        "yes_bid": {"open": "0.4000", "high": "0.6000", "low": "0.3000", "close": "0.5500"},
    }
    c = parse_candle(raw, is_historical=True)
    assert [c[k] for k in ("open", "high", "low", "close", "mean")] == [None] * 5
    # The quote is where it belongs
    assert (c["yes_bid_open"], c["yes_bid_high"], c["yes_bid_low"], c["yes_bid_close"]) == (0.40, 0.60, 0.30, 0.55)
    # Absent keys behave like explicit nulls
    assert parse_candle({**raw, "price": {}}, is_historical=True)["close"] is None


def test_absent_keys_become_nan_never_zero():
    raw = {"end_period_ts": 1658980800, "price": {}, "yes_bid": {}}
    c = parse_candle(raw, is_historical=True)
    assert c["open"] is None
    assert c["mean"] is None
    assert c["volume"] is None
    assert c["open_interest"] is None
    df = pd.DataFrame([c])
    assert df["volume"].isna().all()
    assert df["open"].isna().all()


# Shape and values mirror a real live REST candlestick (same names as the SDK attributes)
LIVE_WIRE = {
    "end_period_ts": 1789617600,
    "open_interest_fp": "4728.12",
    "volume_fp": "5934.86",
    "price": {
        "open_dollars": "0.9900", "high_dollars": "0.9900", "low_dollars": "0.1400",
        "close_dollars": "0.9700", "mean_dollars": "0.6758", "previous_dollars": "0.9900",
    },
    "yes_bid": {"open_dollars": "0.9800", "high_dollars": "0.9800", "low_dollars": "0.0800", "close_dollars": "0.9700"},
    "yes_ask": {"open_dollars": "0.9900", "high_dollars": "0.9900", "low_dollars": "0.1400", "close_dollars": "0.9800"},
}


def test_live_rest_dict_parses_like_the_object_shape():
    c = parse_candle(LIVE_WIRE, is_historical=False)
    assert c["ts_ms"] == 1789617600000
    assert (c["open"], c["high"], c["low"], c["close"], c["mean"]) == (0.99, 0.99, 0.14, 0.97, 0.6758)
    assert c["volume"] == 5934.86 and c["open_interest"] == 4728.12
    assert parse_candle(_live_raw(), is_historical=False)["close"] == 0.06


def test_live_candle_without_trades_has_nan_prices_never_zero():
    # A minute without trades carries only previous_dollars: no OHLC keys at all
    raw = {**LIVE_WIRE, "price": {"previous_dollars": "0.9700"}, "volume_fp": "0.00"}
    c = parse_candle(raw, is_historical=False)
    assert c["open"] is None and c["close"] is None and c["mean"] is None
    assert c["volume"] == 0.0
    assert pd.DataFrame([c])["close"].isna().all()


# ------------------------------------------------------------------ yes_bid / yes_ask OHLC

def test_historical_candle_carries_bid_and_ask_ohlc():
    c = parse_candle(HIST_RAW, is_historical=True)
    assert (c["yes_bid_open"], c["yes_bid_high"], c["yes_bid_low"], c["yes_bid_close"]) == (0.79, 0.79, 0.25, 0.65)
    assert (c["yes_ask_open"], c["yes_ask_high"], c["yes_ask_low"], c["yes_ask_close"]) == (0.81, 0.82, 0.60, 0.70)
    # The traded prices stay what they were: quotes never leak into them when a trade exists
    assert (c["open"], c["high"], c["low"], c["close"]) == (0.79, 0.79, 0.59, 0.69)


def test_live_candle_carries_bid_and_ask_ohlc():
    c = parse_candle(LIVE_WIRE, is_historical=False)
    assert (c["yes_bid_open"], c["yes_bid_high"], c["yes_bid_low"], c["yes_bid_close"]) == (0.98, 0.98, 0.08, 0.97)
    assert (c["yes_ask_open"], c["yes_ask_high"], c["yes_ask_low"], c["yes_ask_close"]) == (0.99, 0.99, 0.14, 0.98)
    assert all(isinstance(c[k], float) for k in QUOTE_COLUMNS)


def test_candle_without_trades_still_has_its_quotes_on_both_tiers():
    live = parse_candle({**LIVE_WIRE, "price": {"previous_dollars": "0.9700"}, "volume_fp": "0.00"},
                        is_historical=False)
    assert live["close"] is None
    assert (live["yes_bid_close"], live["yes_ask_close"]) == (0.97, 0.98)

    # The historical tier sends the trade prices as explicit nulls
    nulls = {k: None for k in ("open", "high", "low", "close", "mean")}
    hist = parse_candle({**HIST_RAW, "price": {**nulls, "previous": "0.6900"}, "volume": "0.00"},
                        is_historical=True)
    assert hist["mean"] is None and hist["volume"] == 0.0
    assert (hist["yes_bid_close"], hist["yes_ask_close"]) == (0.65, 0.70)
    # Same on this tier: no trade, no price. The bid is never copied into the price columns
    assert [hist[k] for k in ("open", "high", "low", "close")] == [None] * 4


def _as_live(hist: dict) -> dict:
    """The live wire shape of a historical candle: *_dollars keys, *_fp counts,
    and no OHLC keys at all when the period had no trade."""
    price = {k + "_dollars": v for k, v in hist["price"].items() if v is not None}
    return {
        "end_period_ts": hist["end_period_ts"],
        "price": price,
        "yes_bid": {k + "_dollars": v for k, v in hist["yes_bid"].items()},
        "yes_ask": {k + "_dollars": v for k, v in hist["yes_ask"].items()},
        "volume_fp": hist["volume"],
        "open_interest_fp": hist["open_interest"],
    }


def test_both_tiers_parse_the_same_candle_to_the_same_row():
    nulls = {k: None for k in ("open", "high", "low", "close", "mean")}
    no_trade = {**HIST_RAW, "price": {**nulls, "previous": "0.6900"}, "volume": "0.00"}
    for hist in (HIST_RAW, no_trade):
        assert parse_candle(_as_live(hist), is_historical=False) == parse_candle(hist, is_historical=True)
    # and the row has exactly the numeric columns of the stored schema
    assert list(parse_candle(HIST_RAW, is_historical=True)) == ["ts_ms", *CANDLE_FLOAT_COLUMNS]


def test_empty_book_sides_are_stored_as_the_api_quotes_them():
    # No resting bid is quoted as 0, no resting ask as 1; both are real values, not gaps
    raw = {**LIVE_WIRE,
           "yes_bid": {k + "_dollars": "0.0000" for k in ("open", "high", "low", "close")},
           "yes_ask": {k + "_dollars": "1.0000" for k in ("open", "high", "low", "close")}}
    c = parse_candle(raw, is_historical=False)
    assert c["yes_bid_close"] == 0.0 and c["yes_ask_close"] == 1.0


def test_absent_quotes_become_nan_never_zero():
    for raw, historical in (
        ({"end_period_ts": 1658980800, "price": {}, "yes_bid": {}}, True),      # empty / missing objects
        ({"end_period_ts": 1658980800, "price": None, "yes_bid": None, "yes_ask": None}, True),
        (_live_raw(), False),                                                  # object shape without quotes
    ):
        c = parse_candle(raw, is_historical=historical)
        assert [c[k] for k in QUOTE_COLUMNS] == [None] * 8
        assert pd.DataFrame([c])[list(QUOTE_COLUMNS)].isna().all().all()


def _row(candle: dict) -> dict:
    return {**candle, "market_ticker": "T-1", "event_ticker": "T", "series_ticker": "S"}


def test_candles_frame_has_the_stored_column_order_and_dtypes():
    df = candles_frame([_row(parse_candle(LIVE_WIRE, is_historical=False))])
    assert tuple(df.columns) == CANDLE_COLUMNS
    # Timestamp, trade prices, counts, identifiers, then the quotes: one fixed order for every file
    assert CANDLE_COLUMNS[:11] == ("ts_ms", "open", "high", "low", "close", "mean", "volume", "open_interest",
                                   "market_ticker", "event_ticker", "series_ticker")
    assert CANDLE_COLUMNS[11:] == QUOTE_COLUMNS
    assert str(df["ts_ms"].dtype) == "int64"
    assert {str(df[c].dtype) for c in ("close", "volume", *QUOTE_COLUMNS)} == {"float64"}


def test_candles_frame_types_a_batch_without_any_trade_as_float64():
    no_trade = {**LIVE_WIRE, "price": {"previous_dollars": "0.9700"}, "volume_fp": "0.00"}
    rows = [_row(parse_candle({**no_trade, "end_period_ts": ts}, is_historical=False)) for ts in (60, 120)]
    assert str(pd.DataFrame(rows)["close"].dtype) == "object"        # what reached parquet as an untyped column
    df = candles_frame(rows)
    assert str(df["close"].dtype) == "float64" and df["close"].isna().all()
    assert df["yes_bid_close"].eq(0.97).all()
