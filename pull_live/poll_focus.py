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
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Ensure kalshi_io and pull_historical are importable when running as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from kalshi_io.config import DATA_DIR, FOCUS_UNIVERSE
from kalshi_io.orderbook import append_orderbook_snapshot, snapshot_orderbook

from pull_historical.pull_daily import run as run_daily
from pull_historical.pull_hourly import run as run_hourly
from pull_historical.pull_minute import run as run_minute
from pull_historical.pull_trades import run as run_trades

logger = logging.getLogger("poll_focus")


def _setup_logging() -> Path:
    """Configure file + stderr logging. Returns log file path."""
    log_dir = DATA_DIR / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    log_path = log_dir / f"poll_focus_{stamp}.log"

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    fh = logging.FileHandler(log_path)
    fh.setFormatter(fmt)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)

    logger.setLevel(logging.INFO)
    logger.addHandler(fh)
    logger.addHandler(sh)

    return log_path


def _run_orderbook(tickers: list[str]) -> dict:
    """Snapshot orderbook for each ticker in the focus universe."""
    processed = 0
    skipped = 0
    rows_written = 0
    for ticker in tickers:
        try:
            df_book, df_top = snapshot_orderbook(ticker)
            n = append_orderbook_snapshot(ticker, df_book)
            rows_written += n
            processed += 1
        except Exception as e:
            logger.warning(f"orderbook {ticker}: {e}")
            skipped += 1
    return {"processed": processed, "skipped": skipped, "rows_written": rows_written}


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

    log_path = _setup_logging()
    logger.info(f"poll_focus starting — {len(FOCUS_UNIVERSE)} tickers in FOCUS_UNIVERSE")

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
