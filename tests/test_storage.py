"""Offline tests for parquet append/dedupe round trip."""

import pandas as pd

from kalshi_io.storage import append_parquet


def _frame():
    return pd.DataFrame({
        "ts_ms": pd.Series([1000, 2000, 3000], dtype="int64"),
        "market_ticker": ["T-1", "T-1", "T-1"],
        "close": [0.5, 0.6, 0.7],
        "volume": [10.0, 11.5, 12.0],
    })


def test_append_then_reappend_leaves_rows_unchanged(tmp_path):
    path = tmp_path / "t.parquet"
    assert append_parquet(_frame(), path, ["ts_ms", "market_ticker"]) == 3
    # Re-appending the identical frame must dedupe to zero new rows
    assert append_parquet(_frame(), path, ["ts_ms", "market_ticker"]) == 0
    df = pd.read_parquet(path)
    assert len(df) == 3
    assert df["ts_ms"].dtype == "int64"
    assert df["close"].dtype == "float64"
    assert df["volume"].dtype == "float64"
    assert list(df["ts_ms"]) == [1000, 2000, 3000]
