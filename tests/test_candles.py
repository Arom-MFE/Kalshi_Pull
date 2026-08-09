"""Offline tests for candle normalization. Synthetic wire-JSON fixtures only."""

from types import SimpleNamespace

import pandas as pd

from kalshi_io.candles import parse_candle

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


def test_zero_price_does_not_trigger_yes_bid_fallback():
    raw = {
        **HIST_RAW,
        "price": {"open": "0.0000", "close": "0.0000"},
        "yes_bid": {"open": "0.5000", "close": "0.5000"},
    }
    c = parse_candle(raw, is_historical=True)
    assert c["open"] == 0.0
    assert c["close"] == 0.0


def test_null_price_falls_back_to_yes_bid():
    raw = {
        **HIST_RAW,
        "price": {"mean": "0.5000"},
        "yes_bid": {"open": "0.4000", "high": "0.6000", "low": "0.3000", "close": "0.5500"},
    }
    c = parse_candle(raw, is_historical=True)
    assert c["open"] == 0.40
    assert c["close"] == 0.55


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
