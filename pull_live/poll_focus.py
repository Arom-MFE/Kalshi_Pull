"""
pull_live/poll_focus.py — Single-process scheduler for focus universe polling.

Runs all pullers on configurable cadences against FOCUS_UNIVERSE.
Ctrl+C for graceful shutdown.

CLI:
    python pull_live/poll_focus.py
    python pull_live/poll_focus.py --minute-interval 30 --no-daily
    python pull_live/poll_focus.py --iterations 1 --no-hourly --no-daily
"""

import argparse
import signal
import sys
import time
from pathlib import Path

# Ensure kalshi_io and pull_historical are importable when running as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from kalshi_io.config import DATA_DIR, FOCUS_UNIVERSE
from kalshi_io.orderbook import append_orderbook_snapshot, snapshot_orderbook
from kalshi_io.client import is_outage
from kalshi_io.config import MAX_CONSECUTIVE_OUTAGES
from kalshi_io.runlog import get_logger, get_skip_recorder, run_logging

from pull_historical.pull_daily import run as run_daily
from pull_historical.pull_hourly import run as run_hourly
from pull_historical.pull_minute import run as run_minute
from pull_historical.pull_trades import run as run_trades

logger = get_logger("poll_focus")


def _run_orderbook(tickers: list[str]) -> dict:
    """
    Snapshot the orderbook of each ticker.

    Returns:
        {"processed": int, "skipped": int, "failed": int, "rows_written": int}.
        A failed snapshot is logged at ERROR and recorded in this process's
        skip_orderbook file; after MAX_CONSECUTIVE_OUTAGES exhausted-retry
        failures in a row the sweep stops (API down).
    """
    skips = get_skip_recorder("orderbook")
    processed = 0
    skipped = 0
    failed = 0
    rows_written = 0
    outages_in_a_row = 0
    for i, ticker in enumerate(tickers):
        if outages_in_a_row >= MAX_CONSECUTIVE_OUTAGES:
            remaining = len(tickers) - i
            logger.error(f"orderbook sweep aborted after {outages_in_a_row} outages in a row; "
                         f"{remaining} tickers not attempted")
            skipped += remaining
            break
        try:
            df_book = snapshot_orderbook(ticker)
            n = append_orderbook_snapshot(ticker, df_book)
            rows_written += n
            processed += 1
            outages_in_a_row = 0
        except Exception as e:
            reason = f"{type(e).__name__}: {e}"
            logger.error(f"orderbook {ticker}: FAILED — {reason}")
            skips.record(ticker, reason, code=type(e).__name__)
            skipped += 1
            failed += 1
            outages_in_a_row = outages_in_a_row + 1 if is_outage(e) else 0
    return {"processed": processed, "skipped": skipped, "failed": failed, "rows_written": rows_written}


def main():
    parser = argparse.ArgumentParser(description="Poll focus universe on configurable cadences.")
    parser.add_argument("--minute-interval", type=int, default=60, help="Seconds between minute pulls (default 60)")
    parser.add_argument("--trades-interval", type=int, default=60, help="Seconds between trade pulls (default 60)")
    parser.add_argument("--orderbook-interval", type=int, default=60, help="Seconds between orderbook snapshots (default 60)")
    parser.add_argument("--hourly-interval", type=int, default=900, help="Seconds between hourly pulls (default 900)")
    parser.add_argument("--daily-interval", type=int, default=21600, help="Seconds between daily pulls (default 21600)")
    parser.add_argument("--no-minute", action="store_true", help="Disable minute pulls")
    parser.add_argument("--no-trades", action="store_true", help="Disable trade pulls")
    parser.add_argument("--no-orderbook", action="store_true", help="Disable orderbook snapshots")
    parser.add_argument("--no-hourly", action="store_true", help="Disable hourly pulls")
    parser.add_argument("--no-daily", action="store_true", help="Disable daily pulls")
    parser.add_argument("--iterations", type=int, default=None, help="Number of iterations (default: run forever)")
    args = parser.parse_args()

    if not FOCUS_UNIVERSE:
        print("FOCUS_UNIVERSE is empty — populate kalshi_io/config.py first.")
        sys.exit(1)

    # One log file per UTC day for the whole process; the pullers called from
    # the loop log into it instead of opening a file per run.
    with run_logging("poll_focus", stamp_fmt="%Y%m%d"):
        _poll(args)


def _poll(args) -> None:
    """Scheduler loop; logging is already set up by main()."""
    logger.info(f"poll_focus starting — {len(FOCUS_UNIVERSE)} tickers in FOCUS_UNIVERSE (data root: {DATA_DIR})")

    # Build schedule
    schedule: list[tuple[str, int, object]] = []
    if not args.no_daily:
        schedule.append(("daily", args.daily_interval, lambda t: run_daily(t)))
    if not args.no_hourly:
        schedule.append(("hourly", args.hourly_interval, lambda t: run_hourly(t)))
    if not args.no_minute:
        schedule.append(("minute", args.minute_interval, lambda t: run_minute(t)))
    if not args.no_trades:
        schedule.append(("trades", args.trades_interval, lambda t: run_trades(t)))
    if not args.no_orderbook:
        schedule.append(("orderbook", args.orderbook_interval, lambda t: _run_orderbook(t)))

    if not schedule:
        logger.info("All pullers disabled — nothing to do.")
        return

    logger.info(f"Schedule: {[(n, f'{s}s') for n, s, _ in schedule]}")

    # Graceful shutdown
    shutdown = False

    def _handle_signal(signum, frame):
        nonlocal shutdown
        shutdown = True
        logger.info(f"Received signal {signum}, shutting down after current task...")

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # Main loop
    last_run: dict[str, float] = {name: 0.0 for name, _, _ in schedule}
    iteration = 0

    while not shutdown:
        if args.iterations is not None and iteration >= args.iterations:
            break

        now = time.time()
        for name, interval, fn in schedule:
            if shutdown:
                break
            if now - last_run[name] >= interval:
                try:
                    result = fn(FOCUS_UNIVERSE)
                    logger.info(f"{name}: {result}")
                except Exception as e:
                    logger.error(f"{name}: FAILED — {type(e).__name__}: {e}")
                last_run[name] = time.time()

        iteration += 1
        if not shutdown and (args.iterations is None or iteration < args.iterations):
            time.sleep(5)

    logger.info("poll_focus exiting cleanly")


if __name__ == "__main__":
    main()
