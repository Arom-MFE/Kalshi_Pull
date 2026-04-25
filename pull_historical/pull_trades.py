"""
pull_historical/pull_trades.py — Historical trade puller.

CLI (--tickers required):
    python pull_historical/pull_trades.py --tickers KXRECSSNBER-26
    python pull_historical/pull_trades.py --tickers KXRECSSNBER --limit 3

Importable:
    from pull_historical.pull_trades import run
    result = run("KXRECSSNBER-26")
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

from kalshi_io.candles import resolve_ticker_meta
from kalshi_io.config import DATA_DIR, DEDUPE_COLS_TRADES
from kalshi_io.storage import append_parquet, get_output_path
from kalshi_io.tickers import load_tickers
from kalshi_io.trades import fetch_trades

logger = logging.getLogger("pull_trades")


def _setup_logging() -> Path:
    """Configure file + stderr logging. Returns log file path."""
    log_dir = DATA_DIR / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    log_path = log_dir / f"pull_trades_{stamp}.log"

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    fh = logging.FileHandler(log_path)
    fh.setFormatter(fmt)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)

    logger.setLevel(logging.INFO)
    logger.addHandler(fh)
    logger.addHandler(sh)

    return log_path


def _get_last_trade_id(series: str, ticker: str) -> str | None:
    """Find the last trade_id from the newest monthly parquet for a ticker."""
    base = DATA_DIR / "trades" / series / ticker
    matches = sorted(base.glob("*.parquet"))
    if not matches:
        return None
    df = pd.read_parquet(matches[-1], columns=["trade_id", "ts_ms"], engine="pyarrow")
    if df.empty:
        return None
    # File is sorted by ts_ms — last row is the latest trade
    return str(df.sort_values("ts_ms").iloc[-1]["trade_id"])


def run(
    tickers: str | list[str],
    since: str | None = None,
    limit: int | None = None,
) -> dict:
    """
    Pull trades for every ticker in the input list.

    Args:
        tickers: source for load_tickers (path, series name, list, or single ticker)
        since:   optional "YYYY-MM-DD" — only keep trades after this date
        limit:   optional max number of tickers to process

    Returns:
        {"processed": int, "skipped": int, "rows_written": int, "elapsed_sec": float}
    """
    log_path = _setup_logging()
    logger.info("pull_trades starting")

    # Skip file
    skip_path = DATA_DIR / "logs" / "skip_trades.txt"
    skip_path.parent.mkdir(parents=True, exist_ok=True)

    # Resolve tickers
    ticker_list = load_tickers(tickers)
    if limit:
        ticker_list = ticker_list[:limit]
    logger.info(f"Tickers: {len(ticker_list)} (limit={limit})")

    # Parse since
    since_ts_ms: int | None = None
    if since:
        since_ts_ms = int(
            datetime.strptime(since, "%Y-%m-%d")
            .replace(tzinfo=timezone.utc)
            .timestamp() * 1000
        )
        logger.info(f"Since override: {since} ({since_ts_ms}ms)")

    processed = 0
    skipped = 0
    rows_written = 0
    t0 = time.time()

    for i, ticker in enumerate(ticker_list):
        try:
            # Resolve series for output path
            series_ticker, _ = resolve_ticker_meta(ticker)

            # Resume: find last trade_id
            last_trade_id = _get_last_trade_id(series_ticker, ticker)
            if last_trade_id:
                logger.info(f"[{i+1}/{len(ticker_list)}] {ticker}: resuming from trade_id={last_trade_id}")

            # Fetch
            df = fetch_trades(ticker, since_trade_id=last_trade_id)

            # Apply since filter if provided
            if since_ts_ms is not None and not df.empty:
                df = df[df["ts_ms"] >= since_ts_ms].reset_index(drop=True)

            if df.empty:
                logger.info(f"[{i+1}/{len(ticker_list)}] {ticker}: 0 trades returned")
                processed += 1
                continue

            # Write — group by yyyy-mm for partitioned output
            dt = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
            df["_year"] = dt.dt.year
            df["_month"] = dt.dt.month

            n_ticker = 0
            for (year, month), grp in df.groupby(["_year", "_month"]):
                ts = pd.Timestamp(year=year, month=month, day=1)
                out_path = get_output_path("trades", None, series_ticker, ticker, ts=ts)
                n = append_parquet(
                    grp.drop(columns=["_year", "_month"]),
                    out_path,
                    DEDUPE_COLS_TRADES,
                    sort_by="ts_ms",
                )
                n_ticker += n

            rows_written += n_ticker
            processed += 1
            logger.info(f"[{i+1}/{len(ticker_list)}] {ticker}: {n_ticker} new trades ({len(df)} fetched)")

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
    logger.info(f"pull_trades done: {summary}")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pull trades for Kalshi tickers.")
    parser.add_argument(
        "--tickers",
        required=True,
        help="Ticker source (required): .txt path, series name, or single ticker",
    )
    parser.add_argument("--limit", type=int, default=None, help="Max tickers to process")
    parser.add_argument("--since", default=None, help="Start date override (YYYY-MM-DD)")
    args = parser.parse_args()

    result = run(args.tickers, since=args.since, limit=args.limit)
    print(result)
