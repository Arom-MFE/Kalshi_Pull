"""
pull_historical/pull_audit.py — Read-only coverage audit for daily candles.

No API calls. Reads existing daily parquet files and reports coverage stats.
Outputs CSV to kalshi_data/logs/audit_{YYYYMMDD}.csv and prints per-series
grouped summary to stdout.

CLI:
    python pull_historical/pull_audit.py --tickers KXRECSSNBER
    python pull_historical/pull_audit.py                        # all tickers
"""

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from kalshi_io.candles import resolve_ticker_meta
from kalshi_io.config import DATA_DIR
from kalshi_io.storage import get_output_path, read_parquet_safe
from kalshi_io.tickers import load_tickers

# Thresholds for hourly/minute density expectations
DENSE_HOURLY_VOLUME = 1000
DENSE_MINUTE_VOLUME_30D = 500


def _audit_ticker(ticker: str) -> dict | None:
    """Read daily parquet for one ticker and return stats, or None if no file."""
    series_ticker, _ = resolve_ticker_meta(ticker)
    path = get_output_path("candles", 1440, series_ticker, ticker)

    df = read_parquet_safe(path)
    if df is None:
        return None

    if df.empty:
        return {
            "ticker": ticker,
            "series": series_ticker,
            "daily_rows": 0,
            "first_ts_ms": None,
            "last_ts_ms": None,
            "total_volume": 0,
            "recent_volume_30d": 0,
            "hourly_expectation": "sparse",
            "minute_expectation": "sparse",
        }

    first_ts = int(df["ts_ms"].min())
    last_ts = int(df["ts_ms"].max())

    # Volume columns are stored as strings (raw API output)
    vol = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
    total_vol = int(vol.sum())

    # Recent 30d volume
    cutoff_ms = int((time.time() - 30 * 86400) * 1000)
    recent_vol = int(vol[df["ts_ms"] >= cutoff_ms].sum())

    # Density expectations
    hourly_exp = "dense" if total_vol >= DENSE_HOURLY_VOLUME or recent_vol >= 100 else "sparse"
    minute_exp = "dense" if recent_vol >= DENSE_MINUTE_VOLUME_30D else "sparse"

    return {
        "ticker": ticker,
        "series": series_ticker,
        "daily_rows": len(df),
        "first_ts_ms": first_ts,
        "last_ts_ms": last_ts,
        "total_volume": total_vol,
        "recent_volume_30d": recent_vol,
        "hourly_expectation": hourly_exp,
        "minute_expectation": minute_exp,
    }


def main():
    parser = argparse.ArgumentParser(description="Audit daily candle coverage (read-only).")
    parser.add_argument(
        "--tickers",
        default="get_ticker_info/kalshi_tickers/all_tickers.txt",
        help="Ticker source: .txt path, .json path, series name, or single ticker",
    )
    parser.add_argument("--limit", type=int, default=None, help="Max tickers to audit")
    args = parser.parse_args()

    ticker_list = load_tickers(args.tickers)
    if args.limit:
        ticker_list = ticker_list[:args.limit]

    print(f"Auditing {len(ticker_list)} tickers...")

    rows = []
    missing = []
    for ticker in ticker_list:
        result = _audit_ticker(ticker)
        if result is None:
            missing.append(ticker)
        else:
            rows.append(result)

    if not rows:
        print("No daily parquet files found.")
        return

    audit_df = pd.DataFrame(rows)

    # Write CSV
    log_dir = DATA_DIR / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    csv_path = log_dir / f"audit_{stamp}.csv"
    audit_df.to_csv(csv_path, index=False)
    print(f"\nCSV written: {csv_path}")

    # Per-series grouped summary
    print("\n=== Per-series summary ===\n")
    for series, grp in audit_df.groupby("series"):
        total_rows = grp["daily_rows"].sum()
        n_dense_h = (grp["hourly_expectation"] == "dense").sum()
        n_dense_m = (grp["minute_expectation"] == "dense").sum()
        vol = grp["total_volume"].sum()
        recent = grp["recent_volume_30d"].sum()
        print(
            f"{series:20s}  tickers={len(grp):4d}  "
            f"rows={total_rows:6d}  vol={vol:10d}  "
            f"recent_30d={recent:8d}  "
            f"dense_h={n_dense_h:3d}  dense_m={n_dense_m:3d}"
        )

    # Overall
    print(f"\n{'TOTAL':20s}  tickers={len(audit_df):4d}  "
          f"rows={audit_df['daily_rows'].sum():6d}  "
          f"vol={audit_df['total_volume'].sum():10d}  "
          f"recent_30d={audit_df['recent_volume_30d'].sum():8d}")

    if missing:
        print(f"\nMissing daily data: {len(missing)} tickers (no parquet file)")
        if len(missing) <= 20:
            for t in missing:
                print(f"  {t}")


if __name__ == "__main__":
    main()
