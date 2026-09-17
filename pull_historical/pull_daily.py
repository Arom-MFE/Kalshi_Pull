"""
pull_historical/pull_daily.py — Daily candle puller (period_interval=1440).

CLI:
    python pull_historical/pull_daily.py --tickers KXRECSSNBER --limit 5
    python pull_historical/pull_daily.py --since 2025-01-01
    python pull_historical/pull_daily.py  # all ~4,065 tickers

Importable:
    from pull_historical.pull_daily import run
    result = run("KXRECSSNBER", limit=5)
"""

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Ensure kalshi_io is importable when running as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from kalshi_io.candles import fetch_candles, resolve_ticker_meta
from kalshi_io.client import is_outage
from kalshi_io.config import DATA_DIR, DEDUPE_COLS_CANDLES, MAX_CONSECUTIVE_OUTAGES, TICKERS_DIR
from kalshi_io.resolve import get_market_metadata
from kalshi_io.runlog import get_logger, get_skip_recorder, run_logging
from kalshi_io.storage import append_parquet, get_last_timestamp, get_output_path
from kalshi_io.tickers import load_tickers

logger = get_logger("pull_daily")


def run(
    tickers: str | list[str],
    since: str | None = None,
    limit: int | None = None,
) -> dict:
    """
    Pull daily candles for every ticker in the input list.

    Args:
        tickers: source for load_tickers (path, series name, list, or single ticker)
        since:   optional "YYYY-MM-DD" — override start date for all tickers
        limit:   optional max number of tickers to process

    Returns:
        {"processed": int, "skipped": int, "failed": int, "aborted": bool,
         "rows_written": int, "elapsed_sec": float}. "skipped" counts every
        ticker that was not processed; "failed" is the subset that raised
        (logged at ERROR and recorded in this process's skip file); "aborted"
        is True when the run stopped early because MAX_CONSECUTIVE_OUTAGES
        tickers in a row exhausted their retries.
    """
    with run_logging("pull_daily"):
        return _run(tickers, since=since, limit=limit)


def _run(
    tickers: str | list[str],
    since: str | None = None,
    limit: int | None = None,
) -> dict:
    """Body of run(); logging is already set up by the caller."""
    logger.info(f"pull_daily starting (data root: {DATA_DIR})")

    skips = get_skip_recorder("daily")

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
    failed = 0
    rows_written = 0
    outages_in_a_row = 0
    aborted = False
    t0 = time.time()

    for i, ticker in enumerate(ticker_list):
        if outages_in_a_row >= MAX_CONSECUTIVE_OUTAGES:
            remaining = len(ticker_list) - i
            reason = (f"run aborted: {outages_in_a_row} tickers in a row failed after all retries "
                      f"(API down or throttling); {remaining} tickers not attempted")
            logger.error(reason)
            skips.record("*", reason, code="aborted")
            skipped += remaining
            aborted = True
            break

        hit_outage = False
        try:
            # Resolve series for output path
            series_ticker, _ = resolve_ticker_meta(ticker)
            out_path = get_output_path("candles", 1440, series_ticker, ticker)

            # Determine start_ts (seconds)
            if since_ts is not None:
                start_ts = since_ts
            else:
                last_ts_ms = get_last_timestamp(out_path)
                if last_ts_ms is not None:
                    start_ts = last_ts_ms // 1000
                else:
                    # Cold start: need metadata for open_ts_ms
                    meta = get_market_metadata(ticker)
                    if meta["open_ts_ms"] is None:
                        reason = "could not resolve open_ts_ms"
                        logger.warning(f"[{i+1}/{len(ticker_list)}] {ticker}: SKIP — {reason}")
                        skips.record(ticker, reason, code="no_open_ts")
                        skipped += 1
                        continue
                    start_ts = meta["open_ts_ms"] // 1000

            now_ts = int(time.time())
            if start_ts >= now_ts:
                logger.info(f"[{i+1}/{len(ticker_list)}] {ticker}: up-to-date")
                processed += 1
                continue

            # Fetch
            rows = fetch_candles(ticker, 1440, start_ts, now_ts)

            if not rows:
                logger.info(f"[{i+1}/{len(ticker_list)}] {ticker}: 0 candles returned")
                processed += 1
                continue

            # Write
            df = pd.DataFrame(rows)
            n = append_parquet(df, out_path, DEDUPE_COLS_CANDLES)
            rows_written += n
            processed += 1
            logger.info(f"[{i+1}/{len(ticker_list)}] {ticker}: {n} new rows ({len(df)} fetched)")

        except Exception as e:
            reason = f"{type(e).__name__}: {e}"
            logger.error(f"[{i+1}/{len(ticker_list)}] {ticker}: FAILED — {reason}")
            skips.record(ticker, reason, code=type(e).__name__)
            skipped += 1
            failed += 1
            hit_outage = is_outage(e)
        finally:
            # finally also runs on the `continue` exits above
            outages_in_a_row = outages_in_a_row + 1 if hit_outage else 0

    elapsed = round(time.time() - t0, 1)
    summary = {
        "processed": processed,
        "skipped": skipped,
        "failed": failed,
        "aborted": aborted,
        "rows_written": rows_written,
        "elapsed_sec": elapsed,
    }
    logger.info(f"pull_daily done: {summary}")
    return summary


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Exit code 1 when any ticker failed."""
    parser = argparse.ArgumentParser(description="Pull daily candles for Kalshi tickers.")
    parser.add_argument(
        "--tickers",
        default=str(TICKERS_DIR / "all_tickers.txt"),
        help="Ticker source: .txt path, .json path, series name, or single ticker",
    )
    parser.add_argument("--limit", type=int, default=None, help="Max tickers to process")
    parser.add_argument("--since", default=None, help="Start date override (YYYY-MM-DD)")
    args = parser.parse_args(argv)

    result = run(args.tickers, since=args.since, limit=args.limit)
    print(result)
    return 1 if result["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
