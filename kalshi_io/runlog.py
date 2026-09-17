"""
kalshi_io/runlog.py — Logging for pipeline runs.

Every logger in the repo is a child of one parent logger, "kalshi". Logging is
configured once per process (a single stderr handler on the parent); each
run() attaches its own log file for exactly as long as it runs and detaches
it again on the way out, so repeated run() calls in one process (poll_focus
calls the pullers every minute) never pile up handlers.
"""

import logging
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from kalshi_io import config

PARENT_LOGGER = "kalshi"
LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"

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


def _reset_state() -> None:
    """Forget the active run (test isolation)."""
    global _active_run_file
    _active_run_file = None
