"""
kalshi_io/storage.py — Parquet I/O: append, dedupe, resume, path routing.

All parquet files use pyarrow engine with zstd compression.
Writes are atomic (temp file + rename).
"""

from pathlib import Path

import pandas as pd

from kalshi_io.config import DATA_DIR


def append_parquet(
    df: pd.DataFrame,
    path: Path,
    dedupe_on: list[str],
    sort_by: str | None = None,
) -> int:
    """
    Append rows to a parquet file with deduplication.

    Reads existing file (if any), concats with df, dedupes, sorts by
    sort_by column (or first dedupe column if not specified), writes
    atomically via temp file + rename.

    Returns:
        Number of new rows after dedupe.
    """
    if df.empty:
        return 0

    if path.exists():
        existing = pd.read_parquet(path, engine="pyarrow")
        n_before = len(existing)
        combined = pd.concat([existing, df], ignore_index=True)
    else:
        n_before = 0
        combined = df.copy()

    sort_col = sort_by or dedupe_on[0]
    combined = (
        combined
        .drop_duplicates(subset=dedupe_on, keep="last")
        .sort_values(sort_col)
        .reset_index(drop=True)
    )

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".tmp.parquet")
    combined.to_parquet(tmp_path, engine="pyarrow", compression="zstd", index=False)
    tmp_path.rename(path)

    return len(combined) - n_before


def read_parquet_safe(path: Path) -> pd.DataFrame | None:
    """Read a parquet file, returning None if it doesn't exist."""
    if not path.exists():
        return None
    return pd.read_parquet(path, engine="pyarrow")


def get_last_timestamp(path: Path, col: str = "ts_ms") -> int | None:
    """
    Read the max value of a timestamp column from a parquet file.

    Reads only the single column to avoid loading the full file.

    Returns:
        Max value as int (UTC ms), or None if file missing/empty.
    """
    if not path.exists():
        return None
    df = pd.read_parquet(path, columns=[col], engine="pyarrow")
    if df.empty:
        return None
    return int(df[col].max())


def get_output_path(
    kind: str,
    interval: int | None,
    series: str,
    ticker: str,
    ts: pd.Timestamp | None = None,
) -> Path:
    """
    Build the output parquet path per CLAUDE.md directory layout.

    Args:
        kind:     "candles", "trades", or "orderbook"
        interval: period_interval (1, 60, 1440) for candles; ignored otherwise
        series:   series ticker (e.g. "KXCPI")
        ticker:   market ticker (e.g. "KXCPI-26APR-T0.4")
        ts:       timestamp for partitioning (required for hourly/minute/trades/orderbook)

    Returns:
        Absolute Path to the parquet file.
    """
    if kind == "candles":
        if interval == 1440:
            # daily/{series}/{ticker}.parquet
            return DATA_DIR / "candles" / "daily" / series / f"{ticker}.parquet"
        if interval == 60:
            # hourly/{series}/{year}/{ticker}.parquet
            if ts is None:
                raise ValueError("ts required for hourly candle path partitioning")
            return (
                DATA_DIR / "candles" / "hourly" / series
                / str(ts.year) / f"{ticker}.parquet"
            )
        if interval == 1:
            # minute/{series}/{year}/{month}/{ticker}.parquet
            if ts is None:
                raise ValueError("ts required for minute candle path partitioning")
            return (
                DATA_DIR / "candles" / "minute" / series
                / str(ts.year) / f"{ts.month:02d}" / f"{ticker}.parquet"
            )
        raise ValueError(f"Invalid candle interval: {interval}. Must be 1, 60, or 1440.")

    if kind == "trades":
        # trades/{series}/{ticker}/{yyyy-mm}.parquet
        if ts is None:
            raise ValueError("ts required for trades path partitioning")
        return (
            DATA_DIR / "trades" / series / ticker
            / f"{ts.year}-{ts.month:02d}.parquet"
        )

    if kind == "orderbook":
        # orderbook/{ticker}/{yyyy-mm-dd}.parquet
        if ts is None:
            raise ValueError("ts required for orderbook path partitioning")
        return (
            DATA_DIR / "orderbook" / ticker
            / f"{ts.year}-{ts.month:02d}-{ts.day:02d}.parquet"
        )

    raise ValueError(f"Unknown kind: {kind!r}. Must be 'candles', 'trades', or 'orderbook'.")
