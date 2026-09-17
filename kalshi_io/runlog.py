"""
kalshi_io/runlog.py — Logging and skip-file recording for pipeline runs.

Every logger in the repo is a child of one parent logger, "kalshi". Logging is
configured once per process (a single stderr handler on the parent); each
run() attaches its own log file for exactly as long as it runs and detaches
it again on the way out, so repeated run() calls in one process (poll_focus
calls the pullers every minute) never pile up handlers.

Tickers a run could not process go to a skip file that is new for every
process (skip_{kind}_{process start}.txt), one timestamped line per problem.
"""

import logging
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from kalshi_io import config

PARENT_LOGGER = "kalshi"
LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"

# One stamp per process: every skip file this process writes shares it
PROCESS_STAMP = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

# A long-running poller would otherwise repeat the same line every cycle
SKIP_DEDUPE_SECONDS = 6 * 3600

# Log file owned by the outermost active run_logging() block, if any
_active_run_file: Path | None = None


class _StderrHandler(logging.StreamHandler):
    """StreamHandler that looks up sys.stderr on every emit, so redirected or
    captured stderr keeps working for the lifetime of the process."""

    def __init__(self):
        super().__init__()

    @property
    def stream(self):
        return sys.stderr

    @stream.setter
    def stream(self, _value):
        pass


def get_logger(name: str) -> logging.Logger:
    """Return the logger "kalshi.<name>" (handlers live on the parent only)."""
    return logging.getLogger(f"{PARENT_LOGGER}.{name}")


def configure_logging(level: int = logging.INFO) -> logging.Logger:
    """Attach the single stderr handler to the parent logger. Idempotent.

    Returns:
        The parent "kalshi" logger.
    """
    parent = logging.getLogger(PARENT_LOGGER)
    parent.setLevel(level)
    if not any(getattr(h, "_kalshi_console", False) for h in parent.handlers):
        console = _StderrHandler()
        console.setFormatter(logging.Formatter(LOG_FORMAT))
        console._kalshi_console = True
        parent.addHandler(console)
    return parent


@contextmanager
def run_logging(name: str, stamp_fmt: str = "%Y%m%d_%H%M"):
    """
    Log one run to DATA_DIR/logs/{name}_{utc stamp}.log.

    The file handler is attached on entry and removed and closed on exit,
    also when the run raises. Re-entrant: a run started inside another run
    (a puller called by poll_focus) attaches nothing and logs into the outer
    run's file.

    Yields:
        Path of the log file that receives this run's lines.
    """
    global _active_run_file

    parent = configure_logging()
    if _active_run_file is not None:
        yield _active_run_file
        return

    log_dir = config.DATA_DIR / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime(stamp_fmt)
    path = log_dir / f"{name}_{stamp}.log"

    handler = logging.FileHandler(path)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    parent.addHandler(handler)
    _active_run_file = path
    try:
        yield path
    finally:
        parent.removeHandler(handler)
        handler.close()
        _active_run_file = None


class SkipRecorder:
    """
    Append-only record of tickers a run could not process.

    File: DATA_DIR/logs/skip_{kind}_{PROCESS_STAMP}.txt, created on the first
    record. Line: "{iso utc}<TAB>{ticker}<TAB>{reason}". Skip files written by
    earlier processes (including the legacy skip_{kind}.txt) are never opened.
    """

    def __init__(self, kind: str):
        self.kind = kind
        self._last_written: dict[tuple[str, str], float] = {}

    @property
    def path(self) -> Path:
        return config.DATA_DIR / "logs" / f"skip_{self.kind}_{PROCESS_STAMP}.txt"

    def record(self, ticker: str, reason: str, code: str | None = None) -> bool:
        """
        Record one skipped ticker.

        Args:
            ticker: market ticker (or the raw token that could not be resolved)
            reason: human-readable cause; tabs and newlines are flattened
            code:   short stable key for deduping; defaults to the reason.
                    Pass it when the reason embeds changing text.

        Returns:
            True if a line was written, False if the same (ticker, code) was
            already recorded within SKIP_DEDUPE_SECONDS by this process.
        """
        key = (ticker, code or reason)
        now = time.time()
        last = self._last_written.get(key)
        if last is not None and now - last < SKIP_DEDUPE_SECONDS:
            return False
        self._last_written[key] = now

        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        clean_ticker = " ".join(str(ticker).split())
        clean_reason = " ".join(str(reason).split())
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a") as f:
            f.write(f"{stamp}\t{clean_ticker}\t{clean_reason}\n")
        return True


def note_result(results: dict | None, ticker: str, status: str, *, rows: int = 0,
                error: str | None = None, outage: bool = False) -> None:
    """
    Record what a puller did with one ticker, for a caller that asked.

    The pullers' summaries only count. A caller that needs the outcome per
    ticker (pull_historical/backfill.py) passes a dict as `results=`; it is
    filled with {ticker: {"status", "rows", "error", "outage"}}:
        ok             fetched and stored; rows = new rows
        up_to_date     nothing to ask: the stored data reaches the end of the window
        empty          the API returned no rows for the window
        failed         raised; error says why, outage is True when retries ran
                       out (API down or throttling), rows counts a saved prefix
        skipped        the market's open time could not be resolved
        unknown        not in the catalog and not found on the API
        not_attempted  the run stopped first (outage breaker or stop request)
    """
    if results is not None:
        results[ticker] = {"status": status, "rows": rows, "error": error, "outage": outage}


_recorders: dict[str, SkipRecorder] = {}


def get_skip_recorder(kind: str) -> SkipRecorder:
    """Process-wide recorder for a kind ("daily", "trades", ...), so dedupe
    survives across run() calls in one process."""
    if kind not in _recorders:
        _recorders[kind] = SkipRecorder(kind)
    return _recorders[kind]


def _reset_state() -> None:
    """Forget the active run and all skip recorders (test isolation)."""
    global _active_run_file
    _active_run_file = None
    _recorders.clear()
