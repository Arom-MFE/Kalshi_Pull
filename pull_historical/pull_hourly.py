"""
pull_historical/pull_hourly.py — Hourly candle puller (period_interval=60).

CLI:
    python pull_historical/pull_hourly.py --tickers KXRECSSNBER --limit 5
    python pull_historical/pull_hourly.py --since 2025-01-01
    python pull_historical/pull_hourly.py  # all 4,164 tickers

Importable:
    from pull_historical.pull_hourly import run
    result = run("KXRECSSNBER", limit=5)
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Ensure kalshi_io is importable when running as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from kalshi_io.candles import fetch_candles, resolve_ticker_meta
from kalshi_io.config import DATA_DIR, DEDUPE_COLS_CANDLES
from kalshi_io.resolve import get_market_metadata
from kalshi_io.storage import append_parquet, get_last_timestamp, get_output_path
from kalshi_io.tickers import load_tickers

logger = logging.getLogger("pull_hourly")


def _setup_logging() -> Path:
    """Configure file + stderr logging. Returns log file path."""
    log_dir = DATA_DIR / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    log_path = log_dir / f"pull_hourly_{stamp}.log"

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    fh = logging.FileHandler(log_path)
    fh.setFormatter(fmt)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)

    logger.setLevel(logging.INFO)
    logger.addHandler(fh)
    logger.addHandler(sh)

    return log_path


def _get_last_hourly_ts(series: str, ticker: str) -> int | None:
    """Find the max ts_ms across all year-partitioned hourly files for a ticker."""
    base = DATA_DIR / "candles" / "hourly" / series
    matches = sorted(base.glob(f"*/{ticker}.parquet"))
    if not matches:
        return None
    return get_last_timestamp(matches[-1])


def run(
    tickers: str | list[str],
    since: str | None = None,
    limit: int | None = None,
) -> dict:
    """
    Pull hourly candles for every ticker in the input list.

    Args:
        tickers: source for load_tickers (path, series name, list, or single ticker)
        since:   optional "YYYY-MM-DD" — override start date for all tickers
        limit:   optional max number of tickers to process

    Returns:
        {"processed": int, "skipped": int, "rows_written": int, "elapsed_sec": float}
    """
    log_path = _setup_logging()
    logger.info("pull_hourly starting")

    # Skip file
    skip_path = DATA_DIR / "logs" / "skip_hourly.txt"
    skip_path.parent.mkdir(parents=True, exist_ok=True)

    # Resolve tickers
    ticker_list = load_tickers(tickers)
    if limit:
        ticker_list = ticker_list[:limit]
    logger.info(f"Tickers: {len(ticker_list)} (limit={limit})")

    # Parse since
    since_ts: int | None = None
    if since:
        since_ts = int(datetime.strptime(since, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
        logger.info(f"Since override: {since} ({since_ts}s)")

    processed = 0
    skipped = 0
    rows_written = 0
    t0 = time.time()

    for i, ticker in enumerate(ticker_list):
        try:
            # Resolve series for output path
            series_ticker, _ = resolve_ticker_meta(ticker)

            # Determine start_ts (seconds)
            if since_ts is not None:
                start_ts = since_ts
            else:
                last_ts_ms = _get_last_hourly_ts(series_ticker, ticker)
                if last_ts_ms is not None:
                    start_ts = last_ts_ms // 1000
                else:
                    # Cold start: need metadata for open_ts_ms
                    meta = get_market_metadata(ticker)
                    if meta["open_ts_ms"] is None:
                        reason = "could not resolve open_ts_ms"
                        logger.warning(f"[{i+1}/{len(ticker_list)}] {ticker}: SKIP — {reason}")
                        with open(skip_path, "a") as f:
                            f.write(f"{ticker}\t{reason}\n")
                        skipped += 1
                        continue
                    start_ts = meta["open_ts_ms"] // 1000

            now_ts = int(time.time())
            if start_ts >= now_ts:
                logger.info(f"[{i+1}/{len(ticker_list)}] {ticker}: up-to-date")
                processed += 1
                continue

            # Fetch
            rows = fetch_candles(ticker, 60, start_ts, now_ts)

            if not rows:
                logger.info(f"[{i+1}/{len(ticker_list)}] {ticker}: 0 candles returned")
                processed += 1
                continue

            # Write — group by year for partitioned output
            df = pd.DataFrame(rows)
            df["_year"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True).dt.year

            n_ticker = 0
            for year, grp in df.groupby("_year"):
                ts = pd.Timestamp(year=year, month=1, day=1)
                out_path = get_output_path("candles", 60, series_ticker, ticker, ts=ts)
                n = append_parquet(grp.drop(columns="_year"), out_path, DEDUPE_COLS_CANDLES)
                n_ticker += n

            rows_written += n_ticker
            processed += 1
            logger.info(f"[{i+1}/{len(ticker_list)}] {ticker}: {n_ticker} new rows ({len(df)} fetched)")

        except Exception as e:
            reason = f"{type(e).__name__}: {e}"
            logger.warning(f"[{i+1}/{len(ticker_list)}] {ticker}: SKIP — {reason}")
            with open(skip_path, "a") as f:
                f.write(f"{ticker}\t{reason}\n")
            skipped += 1

    elapsed = round(time.time() - t0, 1)
    summary = {
        "processed": processed,
        "skipped": skipped,
        "rows_written": rows_written,
        "elapsed_sec": elapsed,
    }
    logger.info(f"pull_hourly done: {summary}")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pull hourly candles for Kalshi tickers.")
    parser.add_argument(
        "--tickers",
        default="get_ticker_info/kalshi_tickers/all_tickers.txt",
        help="Ticker source: .txt path, .json path, series name, or single ticker",
    )
    parser.add_argument("--limit", type=int, default=None, help="Max tickers to process")
    parser.add_argument("--since", default=None, help="Start date override (YYYY-MM-DD)")
    args = parser.parse_args()

    result = run(args.tickers, since=args.since, limit=args.limit)
    print(result)
