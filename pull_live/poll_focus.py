"""
pull_live/poll_focus.py — Single-process scheduler for focus universe polling.

Runs all pullers on configurable cadences against the focus universe. The
universe is derived from the API at startup (kalshi_io/universe.py: the
nearest open event of every series in FOCUS_SERIES) and refreshed while
polling, so a long run moves to the next event cycle on its own.
Ctrl+C for graceful shutdown.

A preflight refuses to start on a universe that is empty or in which nothing
can trade: a settled market's orderbook answers 200 with empty books, so
polling a stale universe would run clean and capture nothing.

CLI:
    python pull_live/poll_focus.py
    python pull_live/poll_focus.py --show-universe
    python pull_live/poll_focus.py --minute-interval 30 --no-daily
    python pull_live/poll_focus.py --iterations 1 --no-hourly --no-daily
    python pull_live/poll_focus.py --tickers KXCPIYOY-26SEP-T3.0 KXCPIYOY-26SEP-T3.1

Exit codes:
    0  clean stop (Ctrl+C, SIGTERM, --iterations reached, --show-universe)
    1  the universe could not be built (API failure, unreadable --tickers source)
    2  preflight refused to start: universe empty, or nothing in it can trade
    3  an explicit universe (--tickers, FOCUS_OVERRIDE) ran out of tradable
       tickers during the run; it never rolls, so there is nothing left to poll
"""

import argparse
import signal
import sys
import time
from pathlib import Path

# Ensure kalshi_io and pull_historical are importable when running as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from kalshi_io import universe
from kalshi_io.client import is_outage
from kalshi_io.config import DATA_DIR, FOCUS_REFRESH_SECONDS, FOCUS_SERIES, MAX_CONSECUTIVE_OUTAGES
from kalshi_io.orderbook import append_orderbook_snapshot, snapshot_orderbook
from kalshi_io.runlog import configure_logging, get_logger, get_skip_recorder, run_logging
from kalshi_io.universe import UniverseError

from pull_historical.pull_daily import run as run_daily
from pull_historical.pull_hourly import run as run_hourly
from pull_historical.pull_minute import run as run_minute
from pull_historical.pull_trades import run as run_trades

logger = get_logger("poll_focus")

LOOP_SLEEP_SECONDS = 5


def _now() -> float:
    return time.time()


def _sleep(seconds: float) -> None:
    time.sleep(seconds)


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


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Poll the focus universe on configurable cadences.")
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
    parser.add_argument(
        "--tickers", nargs="+", default=None,
        help="Poll exactly these tickers for this run instead of the derived universe: .txt/.json path, "
             "series name, 'focus', or tickers separated by spaces or commas. Never rolls forward",
    )
    parser.add_argument(
        "--series", nargs="+", default=None,
        help=f"Series to derive the universe from (default FOCUS_SERIES: {' '.join(FOCUS_SERIES)})",
    )
    parser.add_argument("--events-per-series", type=int, default=None,
                        help="Nearest open events to take per series (default FOCUS_EVENTS_PER_SERIES)")
    parser.add_argument("--universe-refresh", type=int, default=FOCUS_REFRESH_SECONDS,
                        help=f"Seconds between universe refreshes (default {FOCUS_REFRESH_SECONDS}; 0 = never)")
    parser.add_argument("--show-universe", action="store_true",
                        help="Derive and check the universe, print it, and exit without polling")
    return parser


def _build_universe(args) -> dict:
    """Build, check and register the universe for this run (the preflight)."""
    checked = universe.check_universe(
        universe.build_universe(args.tickers, args.series, args.events_per_series)
    )
    universe.register(checked)
    return checked


def main(argv: list[str] | None = None) -> int:
    """CLI entry point; see the module docstring for the exit codes."""
    args = _build_parser().parse_args(argv)

    if args.show_universe:
        configure_logging()
        try:
            print(universe.format_universe(_build_universe(args)))
        except UniverseError as e:
            print(f"poll_focus: {e}", file=sys.stderr)
            return 2
        except Exception as e:
            print(f"poll_focus: could not build the focus universe — {type(e).__name__}: {e}", file=sys.stderr)
            return 1
        return 0

    # One log file per UTC day for the whole process; the pullers called from
    # the loop log into it instead of opening a file per run.
    with run_logging("poll_focus", stamp_fmt="%Y%m%d"):
        try:
            current = _build_universe(args)
        except UniverseError as e:
            logger.error(f"poll_focus refusing to start: {e}")
            return 2
        except Exception as e:
            logger.error(f"poll_focus could not build the focus universe — {type(e).__name__}: {e}")
            return 1
        return _poll(args, current)


def _log_universe(current: dict) -> None:
    for g in current["groups"]:
        logger.info(f"  {g.get('series') or '-'} {g['event_ticker']}: {len(g['tickers'])} tickers, "
                    f"closes {g.get('close_time') or 'unknown'}")
    for ticker, bucket in sorted((current.get("dropped") or {}).items()):
        logger.warning(f"  not polled: {ticker} ({bucket})")


def _save_snapshot(current: dict) -> None:
    """Record what is being polled, for pull_audit and for whoever reads the logs."""
    try:
        universe.write_universe(current, DATA_DIR / "logs" / universe.LIVE_SNAPSHOT)
    except OSError as e:
        logger.error(f"could not write the universe snapshot — {type(e).__name__}: {e}")


def _final_sweep(tickers: list[str], schedule: list[tuple[str, int, object]]) -> None:
    """Pull the enabled candle and trade pullers once more for tickers that
    stopped trading, so their last minutes are stored before they leave the
    universe. No orderbook: a closed market has no book."""
    if not tickers:
        return
    for name, _, fn in schedule:
        if name == "orderbook":
            continue
        try:
            logger.info(f"final sweep {name}: {fn(tickers)}")
        except Exception as e:
            logger.error(f"final sweep {name}: FAILED — {type(e).__name__}: {e}")


def _refresh(current: dict, schedule: list[tuple[str, int, object]]) -> tuple[dict, int | None]:
    """
    Refresh the universe inside the loop.

    Returns:
        (universe to poll from now on, exit code or None). A refresh that
        fails, or that finds nothing to poll for a derived universe, keeps the
        last good universe and logs at ERROR. An explicit universe without
        any tradable ticker left ends the run with exit code 3.
    """
    try:
        fresh, change = universe.refresh_universe(current)
    except UniverseError as e:
        if current.get("source") == "derived":
            logger.error(f"universe refresh found nothing to poll; keeping the last universe — {e}")
            return current, None
        _final_sweep(sorted(e.dead), schedule)
        logger.error(f"poll_focus stopping: {e}")
        return current, 3
    except Exception as e:
        logger.error(f"universe refresh failed; keeping the last universe — {type(e).__name__}: {e}")
        return current, None

    for line in change["rolls"]:
        logger.info(line)
    if change["dead"]:
        logger.info(f"universe: {len(change['dead'])} tickers stopped trading, final sweep then dropped: "
                    f"{', '.join(sorted(change['dead']))}")
        _final_sweep(sorted(change["dead"]), schedule)
    if change["added"]:
        logger.info(f"universe: {len(change['added'])} tickers added: {', '.join(change['added'])}")
    if change["kept"]:
        logger.info(f"universe: {len(change['kept'])} tickers left the selection but can still trade; "
                    f"kept: {', '.join(change['kept'])}")
    if change["rolls"] or change["dead"] or change["added"]:
        logger.info(f"universe now: {len(fresh['tickers'])} tickers")
        _log_universe(fresh)
    _save_snapshot(fresh)
    return fresh, None


def _poll(args, current: dict) -> int:
    """Scheduler loop; logging is already set up and the universe is checked."""
    logger.info(f"poll_focus starting — {len(current['tickers'])} tickers, universe {current['source']} "
                f"(data root: {DATA_DIR})")
    logger.info(f"Universe rule: {current['rule']}")
    _log_universe(current)
    _save_snapshot(current)

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
        return 0

    logger.info(f"Schedule: {[(n, f'{s}s') for n, s, _ in schedule]}; "
                f"universe refresh: {f'{args.universe_refresh}s' if args.universe_refresh > 0 else 'off'}")

    # Graceful shutdown
    shutdown = False

    def _handle_signal(signum, frame):
        nonlocal shutdown
        shutdown = True
        logger.info(f"Received signal {signum}, shutting down after current task...")

    previous_handlers = {sig: signal.signal(sig, _handle_signal) for sig in (signal.SIGINT, signal.SIGTERM)}

    # Main loop
    last_run: dict[str, float] = {name: 0.0 for name, _, _ in schedule}
    last_refresh = _now()
    iteration = 0
    exit_code = 0

    try:
        while not shutdown:
            if args.iterations is not None and iteration >= args.iterations:
                break

            due = universe.next_refresh_at(current, last_refresh, args.universe_refresh)
            if due is not None and _now() >= due:
                current, stop_code = _refresh(current, schedule)
                last_refresh = _now()
                if stop_code is not None:
                    exit_code = stop_code
                    break

            now = _now()
            for name, interval, fn in schedule:
                if shutdown:
                    break
                if now - last_run[name] >= interval:
                    try:
                        result = fn(current["tickers"])
                        logger.info(f"{name}: {result}")
                    except Exception as e:
                        logger.error(f"{name}: FAILED — {type(e).__name__}: {e}")
                    last_run[name] = _now()

            iteration += 1
            if not shutdown and (args.iterations is None or iteration < args.iterations):
                _sleep(LOOP_SLEEP_SECONDS)
    finally:
        for sig, handler in previous_handlers.items():
            signal.signal(sig, handler)

    logger.info("poll_focus exiting cleanly" if exit_code == 0 else f"poll_focus exiting with code {exit_code}")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
