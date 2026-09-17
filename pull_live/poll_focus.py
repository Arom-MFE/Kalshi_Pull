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

Books first, and faster around releases. Every cycle starts with one batch
orderbook request for the whole universe (one timestamp for every book).
Inside a release window (kalshi_io/releases.py: close_time of the polled
events and of every cataloged event that can still trade, plus
RELEASE_CALENDAR; from RELEASE_WINDOW_BEFORE_S before to RELEASE_WINDOW_AFTER_S
after) only the books are polled, every RELEASE_POLL_SECONDS. Candle and trade
pulls, and the final sweep of tickers that stopped trading, wait for the end
of the window: they can be pulled from the API afterwards, the book cannot.

History outside the loop. A ticker that joins the universe without stored
daily or minute candles (a fresh store, or an event the poller rolled to) is
backfilled by a child process, pull_historical/backfill.py, at
BACKGROUND_HISTORY_RPS. Until it finishes the loop polls only that ticker's
books; the candle and trade pulls of the loop never start a ticker cold, so
no cycle is delayed by a history pull and the stored range stays contiguous
from market open.

CLI:
    python pull_live/poll_focus.py
    python pull_live/poll_focus.py --show-universe
    python pull_live/poll_focus.py --minute-interval 30 --no-daily
    python pull_live/poll_focus.py --iterations 1 --no-hourly --no-daily
    python pull_live/poll_focus.py --tickers KXCPIYOY-26SEP-T3.0 KXCPIYOY-26SEP-T3.1
    python pull_live/poll_focus.py --release-interval 2 --release-after 1800

Exit codes:
    0  clean stop (Ctrl+C, SIGTERM, --iterations reached, --show-universe)
    1  the universe could not be built (API failure, unreadable --tickers source)
    2  preflight refused to start: universe empty, or nothing in it can trade
    3  an explicit universe (--tickers, FOCUS_OVERRIDE) ran out of tradable
       tickers during the run; it never rolls, so there is nothing left to poll
"""

import argparse
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Ensure kalshi_io and pull_historical are importable when running as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from kalshi_io import catalog, releases, universe
from kalshi_io.candles import resolve_ticker_meta
from kalshi_io.client import is_outage
from kalshi_io.config import (
    BACKGROUND_HISTORY_RPS, DATA_DIR, FOCUS_REFRESH_SECONDS, FOCUS_SERIES, MAX_CONSECUTIVE_OUTAGES, PROJECT_ROOT,
    RELEASE_POLL_SECONDS, RELEASE_WINDOW_AFTER_S, RELEASE_WINDOW_BEFORE_S,
)
from kalshi_io.orderbook import append_orderbook_snapshot, snapshot_orderbook, snapshot_orderbooks
from kalshi_io.runlog import configure_logging, get_logger, get_skip_recorder, run_logging
from kalshi_io.storage import get_output_path
from kalshi_io.universe import UniverseError

from pull_historical.pull_daily import run as run_daily
from pull_historical.pull_hourly import run as run_hourly
from pull_historical.pull_minute import run as run_minute
from pull_historical.pull_trades import run as run_trades

logger = get_logger("poll_focus")

LOOP_SLEEP_SECONDS = 5
MIN_SLEEP_SECONDS = 0.2

# A candle or trade sweep is not started this close to a release window: it
# would still be running when the fast book sweeps are due
WINDOW_GUARD_S = 60

# Inside a window a failing book sweep must not eat the window: fewer attempts, shorter timeout
FAST_SWEEP_ATTEMPTS = 2
FAST_SWEEP_TIMEOUT: tuple[float, float] = (5.0, 10.0)

# A failed background history pull is tried again after this long, this many times
HISTORY_RETRY_S = 600
HISTORY_MAX_FAILURES = 3
HISTORY_LOCK_NAME = "focus_history"
EXIT_LOCKED = 75


def _now() -> float:
    return time.time()


def _sleep(seconds: float) -> None:
    time.sleep(seconds)


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ============================================================
# Orderbook sweep
# ============================================================

def _run_orderbook(tickers: list[str], fast: bool = False) -> dict:
    """
    Snapshot the orderbook of every ticker: one batch request per 100 tickers,
    every book with the same timestamp.

    Returns:
        {"processed": int, "skipped": int, "failed": int, "rows_written": int}.
        If the batch request fails for a reason other than an outage, the
        sweep falls back to one request per ticker. A failed snapshot is
        logged at ERROR and recorded in this process's skip_orderbook file.
        `fast` (inside a release window) uses fewer attempts and a shorter
        timeout so a failing sweep cannot eat the window.
    """
    skips = get_skip_recorder("orderbook")
    try:
        if fast:
            books = snapshot_orderbooks(tickers, max_attempts=FAST_SWEEP_ATTEMPTS, timeout=FAST_SWEEP_TIMEOUT)
        else:
            books = snapshot_orderbooks(tickers)
    except Exception as e:
        reason = f"{type(e).__name__}: {e}"
        if is_outage(e):
            logger.error(f"orderbook sweep FAILED — {reason}")
            skips.record("*", f"orderbook sweep failed: {reason}", code=type(e).__name__)
            return {"processed": 0, "skipped": len(tickers), "failed": len(tickers), "rows_written": 0}
        logger.warning(f"batch orderbook request failed ({reason}); falling back to one request per ticker")
        return _run_orderbook_each(tickers)

    processed = skipped = failed = rows_written = 0
    for ticker in tickers:
        df_book = books.get(ticker)
        if df_book is None:
            logger.error(f"orderbook {ticker}: not in the API's answer")
            skips.record(ticker, "orderbook: the API did not return this ticker", code="not_returned")
            skipped += 1
            failed += 1
            continue
        try:
            rows_written += append_orderbook_snapshot(ticker, df_book)
            processed += 1
        except Exception as e:
            reason = f"{type(e).__name__}: {e}"
            logger.error(f"orderbook {ticker}: FAILED — {reason}")
            skips.record(ticker, reason, code=type(e).__name__)
            skipped += 1
            failed += 1
    return {"processed": processed, "skipped": skipped, "failed": failed, "rows_written": rows_written}


def _run_orderbook_each(tickers: list[str]) -> dict:
    """One request per ticker, the way it was done before the batch endpoint."""
    skips = get_skip_recorder("orderbook")
    processed = skipped = failed = rows_written = 0
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
            rows_written += append_orderbook_snapshot(ticker, df_book)
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


# ============================================================
# History outside the loop
# ============================================================

def _cold_tickers(tickers: list[str]) -> list[str]:
    """Tickers with no stored daily or minute candles: their history has to be
    pulled before the loop may append to them. Offline: catalog lookups only."""
    cold = []
    for ticker in tickers:
        series, _ = resolve_ticker_meta(ticker, allow_api=False)
        daily = get_output_path("candles", 1440, series, ticker).exists()
        minute = any((DATA_DIR / "candles" / "minute" / series).glob(f"*/*/{ticker}.parquet"))
        if not (daily and minute):
            cold.append(ticker)
    return cold


def _spawn_history(tickers: list[str], layers: str) -> subprocess.Popen:
    """Start pull_historical/backfill.py for these tickers as a child process.
    It shares the poller's process group (Ctrl+C reaches both), holds the
    focus_history lock (exit 75 when a previous poller's pull still runs),
    writes its own log, and never reads this process's stdout."""
    env = {**os.environ, "KALSHI_DATA_DIR": str(DATA_DIR), "KALSHI_MAX_RPS": str(BACKGROUND_HISTORY_RPS)}
    cmd = [sys.executable, "-m", "pull_historical.backfill", "--tickers", *tickers, "--layers", layers,
           "--no-audit", "--log-name", "backfill_history", "--lock-name", HISTORY_LOCK_NAME]
    return subprocess.Popen(cmd, cwd=str(PROJECT_ROOT), env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


class _HistoryPull:
    """One child process at a time; tickers wait in a queue and are excluded
    from the loop's candle and trade pulls until their history is in."""

    def __init__(self, layers: str, enabled: bool):
        self.layers = layers
        self.enabled = enabled
        self.proc: subprocess.Popen | None = None
        self.running: list[str] = []
        self.queue: list[str] = []
        self.excluded: set[str] = set()
        self.checked: set[str] = set()
        self.failures = 0
        self.retry_at: float | None = None

    def consider(self, tickers: list[str]) -> None:
        """Queue the cold tickers among these; each ticker is checked once per process."""
        if not self.enabled:
            return
        new = [t for t in tickers if t not in self.checked]
        self.checked.update(new)
        cold = _cold_tickers(new)
        if cold:
            self.queue += [t for t in cold if t not in self.queue and t not in self.running]
            self.excluded.update(cold)
            logger.info(f"history: {len(cold)} tickers have no stored history and wait for a background pull: "
                        f"{', '.join(cold)}")

    def start_if_due(self, now: float) -> None:
        if self.proc is not None or not self.queue or (self.retry_at is not None and now < self.retry_at):
            return
        self.running, self.queue = list(self.queue), []
        self.retry_at = None
        self.proc = _spawn_history(self.running, self.layers)
        logger.info(f"history: background pull started for {len(self.running)} tickers (pid {self.proc.pid}, "
                    f"{BACKGROUND_HISTORY_RPS} requests/s); polling their books meanwhile")

    def poll(self, now: float) -> list[str]:
        """Reap a finished child. Returns the tickers that rejoined the loop."""
        if self.proc is None:
            return []
        code = self.proc.poll()
        if code is None:
            return []
        self.proc = None
        done, self.running = self.running, []
        if code in (0, 1):
            self.failures = 0
            self.excluded.difference_update(done)
            logger.info(f"history: background pull finished (exit {code}) for {len(done)} tickers; they rejoin the loop"
                        + ("" if code == 0 else " (some tickers failed; the loop resumes from what was stored)"))
            return done
        if code == EXIT_LOCKED:
            logger.warning("history: another history pull holds the lock; trying again in "
                           f"{HISTORY_RETRY_S // 60} min")
            self.queue = done + self.queue
            self.retry_at = now + HISTORY_RETRY_S
            return []
        self.failures += 1
        if self.failures < HISTORY_MAX_FAILURES:
            logger.error(f"history: background pull failed (exit {code}); retrying in {HISTORY_RETRY_S // 60} min "
                         f"({self.failures} of {HISTORY_MAX_FAILURES} attempts)")
            self.queue = done + self.queue
            self.retry_at = now + HISTORY_RETRY_S
            return []
        logger.error(f"history: background pull failed {self.failures} times; polling {len(done)} tickers anyway "
                     f"(their candles start cold inside the loop)")
        self.failures = 0
        self.excluded.difference_update(done)
        return done

    def drop(self, tickers: list[str]) -> None:
        """Tickers that left the universe."""
        gone = set(tickers)
        self.queue = [t for t in self.queue if t not in gone]
        self.excluded.difference_update(gone)

    def stop(self) -> None:
        if self.proc is None or self.proc.poll() is not None:
            return
        logger.info(f"history: stopping the background pull (pid {self.proc.pid}); it resumes on the next start")
        self.proc.terminate()
        try:
            self.proc.wait(10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(5)


# ============================================================
# CLI
# ============================================================

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
    parser.add_argument("--iterations", type=int, default=None,
                        help="Number of loop passes (default: run forever); passes are shorter inside a release window")
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
    parser.add_argument("--release-interval", type=float, default=RELEASE_POLL_SECONDS,
                        help=f"Seconds between orderbook snapshots inside a release window (default {RELEASE_POLL_SECONDS})")
    parser.add_argument("--release-before", type=float, default=RELEASE_WINDOW_BEFORE_S,
                        help=f"A release window opens this many seconds before an event's close_time (default {RELEASE_WINDOW_BEFORE_S})")
    parser.add_argument("--release-after", type=float, default=RELEASE_WINDOW_AFTER_S,
                        help=f"... and closes this many seconds after it (default {RELEASE_WINDOW_AFTER_S})")
    parser.add_argument("--no-release-windows", action="store_true", help="One cadence everywhere, no fast book polling")
    parser.add_argument("--no-background-history", action="store_true",
                        help="Pull the history of a ticker without stored candles inside the loop (cold start), "
                             "instead of in a child process")
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


# ============================================================
# The loop
# ============================================================

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


def _refresh(current: dict, schedule: list[tuple[str, int, object]], sweep) -> tuple[dict, int | None, dict | None]:
    """
    Refresh the universe inside the loop.

    Returns:
        (universe to poll from now on, exit code or None, change or None). A
        refresh that fails, or that finds nothing to poll for a derived
        universe, keeps the last good universe and logs at ERROR. An explicit
        universe without any tradable ticker left ends the run with exit
        code 3. `sweep(tickers)` gets the tickers that stopped trading; the
        loop runs the final sweep now or, inside a release window, later.
    """
    try:
        fresh, change = universe.refresh_universe(current)
    except UniverseError as e:
        if current.get("source") == "derived":
            logger.error(f"universe refresh found nothing to poll; keeping the last universe — {e}")
            return current, None, None
        sweep(sorted(e.dead))
        logger.error(f"poll_focus stopping: {e}")
        return current, 3, None
    except Exception as e:
        logger.error(f"universe refresh failed; keeping the last universe — {type(e).__name__}: {e}")
        return current, None, None

    for line in change["rolls"]:
        logger.info(line)
    if change["dead"]:
        logger.info(f"universe: {len(change['dead'])} tickers stopped trading, final sweep then dropped: "
                    f"{', '.join(sorted(change['dead']))}")
        sweep(sorted(change["dead"]))
    if change["added"]:
        logger.info(f"universe: {len(change['added'])} tickers added: {', '.join(change['added'])}")
    if change["kept"]:
        logger.info(f"universe: {len(change['kept'])} tickers left the selection but can still trade; "
                    f"kept: {', '.join(change['kept'])}")
    if change["rolls"] or change["dead"] or change["added"]:
        logger.info(f"universe now: {len(fresh['tickers'])} tickers")
        _log_universe(fresh)
    _save_snapshot(fresh)
    return fresh, None, change


def _release_windows(current: dict, args, now: float) -> list[tuple[float, float, str]]:
    """The release windows to poll fast in, from the polled events, the catalog and the config calendar."""
    if args.no_release_windows or args.no_orderbook:
        return []
    try:
        index = catalog.market_index()
    except Exception as e:                                # a broken catalog file must not stop the poller
        logger.error(f"release windows: catalog unreadable ({type(e).__name__}: {e}); using the polled events only")
        index = {}
    times = releases.release_times(current, index, releases.default_calendar(), now, args.release_after)
    return releases.windows(times, args.release_before, args.release_after)


def _poll(args, current: dict) -> int:
    """Scheduler loop; logging is already set up and the universe is checked."""
    logger.info(f"poll_focus starting — {len(current['tickers'])} tickers, universe {current['source']} "
                f"(data root: {DATA_DIR})")
    logger.info(f"Universe rule: {current['rule']}")
    _log_universe(current)
    _save_snapshot(current)

    # Build schedule: the perishable book first, so its timestamps are evenly spaced
    schedule: list[tuple[str, int, object]] = []
    if not args.no_orderbook:
        schedule.append(("orderbook", args.orderbook_interval, lambda t, fast=False: _run_orderbook(t, fast=fast)))
    if not args.no_daily:
        schedule.append(("daily", args.daily_interval, lambda t: run_daily(t)))
    if not args.no_hourly:
        schedule.append(("hourly", args.hourly_interval, lambda t: run_hourly(t)))
    if not args.no_minute:
        schedule.append(("minute", args.minute_interval, lambda t: run_minute(t)))
    if not args.no_trades:
        schedule.append(("trades", args.trades_interval, lambda t: run_trades(t)))

    if not schedule:
        logger.info("All pullers disabled — nothing to do.")
        return 0

    logger.info(f"Schedule: {[(n, f'{s}s') for n, s, _ in schedule]}; "
                f"universe refresh: {f'{args.universe_refresh}s' if args.universe_refresh > 0 else 'off'}")

    windows = _release_windows(current, args, _now())
    if args.no_orderbook and not args.no_release_windows:
        logger.info("Release windows: off (orderbook snapshots are disabled, and only the books are polled faster)")
    elif windows:
        logger.info(f"Release windows: books every {args.release_interval:g} s from {args.release_before:g} s before "
                    f"to {args.release_after:g} s after a release; candle and trade pulls wait for the end of a window. "
                    f"Next: " + " | ".join(releases.describe(windows, _now())))
    else:
        logger.info("Release windows: none scheduled" if not args.no_release_windows else "Release windows: off")

    history_layers = ",".join(["metadata"] + [n for n, _, _ in schedule if n != "orderbook"])
    history = _HistoryPull(history_layers, enabled=not args.no_background_history and history_layers != "metadata")
    history.consider(current["tickers"])

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
    deferred_sweep: list[str] = []
    in_window_before: tuple | None = None

    def sweep_now(tickers: list[str]) -> None:
        history.drop(tickers)
        _final_sweep(tickers, schedule)

    def sweep_later(tickers: list[str]) -> None:
        history.drop(tickers)
        deferred_sweep.extend(t for t in tickers if t not in deferred_sweep)

    try:
        while not shutdown:
            if args.iterations is not None and iteration >= args.iterations:
                break

            now = _now()
            window = releases.active_window(windows, now)
            in_window = window is not None
            if in_window and in_window_before is None:
                logger.info(f"release window open until {_iso(window[1])} ({window[2]}): books every "
                            f"{args.release_interval:g} s; candle and trade pulls resume after the window")
            elif not in_window and in_window_before is not None:
                logger.info(f"release window closed ({in_window_before[2]}); back to the normal cadence")
            in_window_before = window

            due = universe.next_refresh_at(current, last_refresh, args.universe_refresh)
            if due is not None and now >= due:
                current, stop_code, change = _refresh(current, schedule, sweep_later if in_window else sweep_now)
                last_refresh = _now()
                if stop_code is not None:
                    exit_code = stop_code
                    break
                if change is not None:
                    # Sticky: the window of the event that just closed stays open, the
                    # other markets react to its release
                    windows = releases.merge_windows(windows, _release_windows(current, args, _now()), _now())
                    if change["added"]:
                        history.consider(change["added"])

            for ticker in history.poll(now):
                pass                                      # rejoined: the next cycle resumes from what was stored
            history.start_if_due(now)

            if deferred_sweep and not in_window:
                pending, deferred_sweep = list(deferred_sweep), []
                logger.info(f"final sweep for {len(pending)} tickers deferred by the release window")
                _final_sweep(pending, schedule)

            next_window = releases.next_window(windows, now)
            guard = next_window is not None and next_window[0] - now <= WINDOW_GUARD_S
            for name, interval, fn in schedule:
                if shutdown:
                    break
                if name == "orderbook":
                    effective = args.release_interval if in_window else interval
                    tickers = list(current["tickers"])
                else:
                    if in_window or guard:
                        continue                          # recoverable later; the books are not
                    effective = interval
                    tickers = [t for t in current["tickers"] if t not in history.excluded]
                if now - last_run[name] < effective or not tickers:
                    continue
                last_run[name] = now                      # the interval counts from the start of the task
                try:
                    result = fn(tickers, fast=True) if (name == "orderbook" and in_window) else fn(tickers)
                    if not in_window:
                        logger.info(f"{name}: {result}")
                    elif result.get("failed"):
                        logger.warning(f"{name} (release window): {result}")
                except Exception as e:
                    logger.error(f"{name}: FAILED — {type(e).__name__}: {e}")

            iteration += 1
            if not shutdown and (args.iterations is None or iteration < args.iterations):
                _sleep(_sleep_for(now, schedule, last_run, args, in_window, windows))
    finally:
        for sig, handler in previous_handlers.items():
            signal.signal(sig, handler)
        if deferred_sweep:
            _final_sweep(deferred_sweep, schedule)
        history.stop()

    logger.info("poll_focus exiting cleanly" if exit_code == 0 else f"poll_focus exiting with code {exit_code}")
    return exit_code


def _sleep_for(now: float, schedule, last_run: dict, args, in_window: bool, windows) -> float:
    """Seconds until the next task is due, at most LOOP_SLEEP_SECONDS, at least MIN_SLEEP_SECONDS."""
    dues = []
    for name, interval, _ in schedule:
        if name == "orderbook":
            dues.append(last_run[name] + (args.release_interval if in_window else interval))
        elif not in_window:
            dues.append(last_run[name] + interval)
    active = releases.active_window(windows, now)
    if active is not None:
        dues.append(active[1])
    upcoming = releases.next_window(windows, now)
    if upcoming is not None:
        dues.append(upcoming[0])
    wait = min((d - now for d in dues), default=LOOP_SLEEP_SECONDS)
    return max(MIN_SLEEP_SECONDS, min(LOOP_SLEEP_SECONDS, wait))


if __name__ == "__main__":
    sys.exit(main())
