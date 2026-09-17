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
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Ensure kalshi_io is importable when running as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from kalshi_io.candles import resolve_ticker_meta
from kalshi_io.client import is_outage
from kalshi_io.config import (
    DATA_DIR,
    DEDUPE_COLS_TRADES,
    MAX_CONSECUTIVE_OUTAGES,
    TRADES_RESUME_OVERLAP_S,
)
from kalshi_io.runlog import get_logger, get_skip_recorder, note_result, run_logging
from kalshi_io.storage import append_parquet, get_last_timestamp, get_output_path
from kalshi_io.tickers import load_tickers, validate_tickers
from kalshi_io.trades import fetch_trades

logger = get_logger("pull_trades")


def _get_last_trade_ts(series: str, ticker: str) -> int | None:
    """Max ts_ms in the newest monthly parquet for a ticker, or None if nothing is stored."""
    base = DATA_DIR / "trades" / series / ticker
    # yyyy-mm names sort chronologically; skip a temp file left by a killed write
    matches = sorted(p for p in base.glob("*.parquet") if not p.name.endswith(".tmp.parquet"))
    if not matches:
        return None
    return get_last_timestamp(matches[-1])


def _drop_stored(df: pd.DataFrame, path: Path) -> pd.DataFrame:
    """Remove trades whose trade_id is already in the parquet at path.

    The resume window overlaps the stored tape on purpose; dropping the
    overlap here means a cycle without new trades writes nothing at all.
    """
    if not path.exists():
        return df
    stored = pd.read_parquet(path, columns=["trade_id"], engine="pyarrow")["trade_id"]
    return df[~df["trade_id"].isin(set(stored))]


def run(
    tickers: str | list[str],
    since: str | None = None,
    limit: int | None = None,
    *,
    results: dict | None = None,
    should_stop=None,
) -> dict:
    """
    Pull trades for every ticker in the input list.

    Args:
        tickers: source for load_tickers: path, series name, "focus", ticker(s)
                 separated by whitespace or commas, or a list of those
        since:   optional "YYYY-MM-DD" — only request and keep trades from this date on
        limit:   optional max number of tickers to process
        results: optional dict, filled with the outcome per ticker
                 (see kalshi_io.runlog.note_result)
        should_stop: optional callable checked before every ticker; when it
                 returns True the remaining tickers are left unattempted

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
    with run_logging("pull_trades"):
        return _run(tickers, since=since, limit=limit, results=results, should_stop=should_stop)


def _run(
    tickers: str | list[str],
    since: str | None = None,
    limit: int | None = None,
    results: dict | None = None,
    should_stop=None,
) -> dict:
    """Body of run(); logging is already set up by the caller."""
    logger.info(f"pull_trades starting (data root: {DATA_DIR})")

    skips = get_skip_recorder("trades")

    # Resolve tickers
    ticker_list = load_tickers(tickers)
    if limit:
        ticker_list = ticker_list[:limit]
    ticker_list, unknown = validate_tickers(ticker_list)
    for ticker in unknown:
        logger.warning(f"{ticker}: SKIP — unknown ticker (not in the catalog, not found on the API)")
        skips.record(ticker, "unknown ticker: not in the catalog and not found on the API", code="unknown")
        note_result(results, ticker, "unknown")
    logger.info(f"Tickers: {len(ticker_list)} (limit={limit}, unknown={len(unknown)})")

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
            for left in ticker_list[i:]:
                note_result(results, left, "not_attempted")
            skipped += remaining
            aborted = True
            break

        if should_stop is not None and should_stop():
            remaining = len(ticker_list) - i
            logger.info(f"stop requested: {remaining} tickers not attempted")
            for left in ticker_list[i:]:
                note_result(results, left, "not_attempted")
            skipped += remaining
            break

        hit_outage = False
        n_ticker = 0
        try:
            # Resolve series for output path
            series_ticker, _ = resolve_ticker_meta(ticker)

            # Resume: ask the API only for trades since the last stored one,
            # minus an overlap that _drop_stored removes again
            min_ts: int | None = None
            last_ts_ms = _get_last_trade_ts(series_ticker, ticker)
            if last_ts_ms is not None:
                min_ts = max(0, last_ts_ms // 1000 - TRADES_RESUME_OVERLAP_S)
                logger.info(f"[{i+1}/{len(ticker_list)}] {ticker}: resuming from min_ts={min_ts}")
            if since_ts_ms is not None:
                since_s = since_ts_ms // 1000
                min_ts = since_s if min_ts is None else max(min_ts, since_s)

            # Fetch (raises on any failed page; nothing partial is written)
            df = fetch_trades(ticker, min_ts=min_ts)

            # Apply since filter if provided
            if since_ts_ms is not None and not df.empty:
                df = df[df["ts_ms"] >= since_ts_ms].reset_index(drop=True)

            if df.empty:
                logger.info(f"[{i+1}/{len(ticker_list)}] {ticker}: 0 trades returned")
                note_result(results, ticker, "empty")
                processed += 1
                continue

            # Write — group by yyyy-mm for partitioned output
            dt = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
            df["_year"] = dt.dt.year
            df["_month"] = dt.dt.month

            for (year, month), grp in df.groupby(["_year", "_month"]):
                ts = pd.Timestamp(year=year, month=month, day=1)
                out_path = get_output_path("trades", None, series_ticker, ticker, ts=ts)
                grp = _drop_stored(grp, out_path)
                if grp.empty:
                    continue
                n = append_parquet(
                    grp.drop(columns=["_year", "_month"]),
                    out_path,
                    DEDUPE_COLS_TRADES,
                    sort_by="ts_ms",
                )
                n_ticker += n

            rows_written += n_ticker
            processed += 1
            note_result(results, ticker, "ok", rows=n_ticker)
            logger.info(f"[{i+1}/{len(ticker_list)}] {ticker}: {n_ticker} new trades ({len(df)} fetched)")

        except Exception as e:
            reason = f"{type(e).__name__}: {e}"
            logger.error(f"[{i+1}/{len(ticker_list)}] {ticker}: FAILED — {reason}")
            skips.record(ticker, reason, code=type(e).__name__)
            skipped += 1
            failed += 1
            hit_outage = is_outage(e)
            note_result(results, ticker, "failed", rows=n_ticker, error=reason, outage=hit_outage)
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
    logger.info(f"pull_trades done: {summary}")
    return summary


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Exit code 1 when any ticker failed."""
    parser = argparse.ArgumentParser(description="Pull trades for Kalshi tickers.")
    parser.add_argument(
        "--tickers",
        nargs="+",
        required=True,
        help="Ticker source(s) (required): .txt/.json path, series name, 'focus', "
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
