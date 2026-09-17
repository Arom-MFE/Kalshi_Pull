"""
pull_historical/pull_hourly.py — Hourly candle puller (period_interval=60).

CLI:
    python pull_historical/pull_hourly.py --tickers KXRECSSNBER --limit 5
    python pull_historical/pull_hourly.py --since 2025-01-01
    python pull_historical/pull_hourly.py  # every cataloged ticker (several thousand)

Importable:
    from pull_historical.pull_hourly import run
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

from kalshi_io.candles import PartialCandlesError, candles_frame, fetch_candles, resolve_ticker_meta
from kalshi_io.client import is_outage
from kalshi_io.config import DATA_DIR, DEDUPE_COLS_CANDLES, MAX_CONSECUTIVE_OUTAGES, TICKERS_DIR
from kalshi_io.resolve import get_market_metadata
from kalshi_io.runlog import get_logger, get_skip_recorder, run_logging
from kalshi_io.storage import append_parquet, get_last_timestamp, get_output_path
from kalshi_io.tickers import load_tickers, validate_tickers

logger = get_logger("pull_hourly")


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
        tickers: source for load_tickers: path, series name, "focus", ticker(s)
                 separated by whitespace or commas, or a list of those
        since:   optional "YYYY-MM-DD" — override start date for all tickers
        limit:   optional max number of tickers to process

    Returns:
        {"processed": int, "skipped": int, "failed": int, "aborted": bool,
         "unknown": list[str], "rows_written": int, "elapsed_sec": float}.
        "skipped" counts every ticker that was not processed; "unknown" lists
        the tickers found neither in the catalog nor on the API (they are
        recorded in the skip file and not attempted); "failed" is the subset
        that raised
        (logged at ERROR and recorded in this process's skip file); "aborted"
        is True when the run stopped early because MAX_CONSECUTIVE_OUTAGES
        tickers in a row exhausted their retries.
    """
    with run_logging("pull_hourly"):
        return _run(tickers, since=since, limit=limit)


def _run(
    tickers: str | list[str],
    since: str | None = None,
    limit: int | None = None,
) -> dict:
    """Body of run(); logging is already set up by the caller."""
    logger.info(f"pull_hourly starting (data root: {DATA_DIR})")

    skips = get_skip_recorder("hourly")

    # Resolve tickers
    ticker_list = load_tickers(tickers)
    if limit:
        ticker_list = ticker_list[:limit]
    ticker_list, unknown = validate_tickers(ticker_list)
    for ticker in unknown:
        logger.warning(f"{ticker}: SKIP — unknown ticker (not in the catalog, not found on the API)")
        skips.record(ticker, "unknown ticker: not in the catalog and not found on the API", code="unknown")
    logger.info(f"Tickers: {len(ticker_list)} (limit={limit}, unknown={len(unknown)})")

    # Parse since
    since_ts: int | None = None
    if since:
        since_ts = int(datetime.strptime(since, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
        logger.info(f"Since override: {since} ({since_ts}s)")

    processed = 0
    skipped = len(unknown)
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
                        skips.record(ticker, reason, code="no_open_ts")
                        skipped += 1
                        continue
                    start_ts = meta["open_ts_ms"] // 1000

            now_ts = int(time.time())
            if start_ts >= now_ts:
                logger.info(f"[{i+1}/{len(ticker_list)}] {ticker}: up-to-date")
                processed += 1
                continue

            # Fetch. If a chunk fails after earlier ones arrived, the contiguous
            # prefix is still saved below, then the failure is raised.
            partial: PartialCandlesError | None = None
            try:
                rows = fetch_candles(ticker, 60, start_ts, now_ts)
            except PartialCandlesError as e:
                partial, rows = e, e.rows

            if not rows:
                logger.info(f"[{i+1}/{len(ticker_list)}] {ticker}: 0 candles returned")
                processed += 1
                continue

            # Write — group by year for partitioned output
            df = candles_frame(rows)
            df["_year"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True).dt.year

            n_ticker = 0
            for year, grp in df.groupby("_year"):
                ts = pd.Timestamp(year=year, month=1, day=1)
                out_path = get_output_path("candles", 60, series_ticker, ticker, ts=ts)
                n = append_parquet(grp.drop(columns="_year"), out_path, DEDUPE_COLS_CANDLES)
                n_ticker += n

            rows_written += n_ticker
            if partial is not None:
                raise partial
            processed += 1
            logger.info(f"[{i+1}/{len(ticker_list)}] {ticker}: {n_ticker} new rows ({len(df)} fetched)")

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
        "unknown": unknown,
        "rows_written": rows_written,
        "elapsed_sec": elapsed,
    }
    logger.info(f"pull_hourly done: {summary}")
    return summary


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Exit code 1 when any ticker failed."""
    parser = argparse.ArgumentParser(description="Pull hourly candles for Kalshi tickers.")
    parser.add_argument(
        "--tickers",
        nargs="+",
        default=str(TICKERS_DIR / "all_tickers.txt"),
        help="Ticker source(s): .txt/.json path, series name, 'focus', "
             "or tickers separated by spaces or commas",
    )
    parser.add_argument("--limit", type=int, default=None, help="Max tickers to process")
    parser.add_argument("--since", default=None, help="Start date override (YYYY-MM-DD)")
    args = parser.parse_args(argv)

    result = run(args.tickers, since=args.since, limit=args.limit)
    print(result)
    return 1 if result["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
