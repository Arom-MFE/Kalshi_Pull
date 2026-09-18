"""
kalshi_io/storage.py — Parquet I/O: append, dedupe, resume, path routing, locking.

All parquet files use pyarrow engine with zstd compression.
Writes are atomic (temp file + rename) and serialized per file across
processes (file_lock), so a poller and a backfill can run side by side.
"""

import os
import threading
import time
import zlib
from contextlib import contextmanager
from pathlib import Path

import pandas as pd

from kalshi_io import config
from kalshi_io.config import DATA_DIR
from kalshi_io.runlog import get_logger

try:
    import fcntl
except ImportError:          # Windows
    fcntl = None

logger = get_logger("storage")

# ============================================================
# Locking
# ============================================================
# An append is read, merge, write, rename. Two processes doing that to one file
# at once lose the rows of whoever renames first. flock() serializes them:
# advisory, released by the kernel when the holder dies (no stale locks), and
# it works between processes and between threads of one process.
#
# Lock files live in DATA_DIR/.locks and are striped: a file maps to one of
# LOCK_STRIPES lock files by a hash of its path. Deleting a lock file after use
# would race (a waiter keeps the unlinked inode while a newcomer locks a fresh
# one), so they are never deleted; striping keeps their number bounded instead
# of one per parquet file. Two files sharing a stripe wait for each other for
# a few milliseconds, which is harmless.
LOCK_STRIPES = 256
_LOCK_POLL_S = 0.05

_registry_guard = threading.Lock()
_thread_locks: dict[Path, threading.RLock] = {}
_held: dict[Path, list] = {}              # lock file → [fd, depth] while this process holds it
_lock_warnings: set[str] = set()


class LockTimeout(TimeoutError):
    """Another writer held the lock for longer than the timeout."""


def lock_path_for(target: Path | str) -> Path:
    """The lock file that guards `target`: DATA_DIR/.locks/{stripe}.lock."""
    stripe = zlib.crc32(str(Path(target).resolve()).encode()) % LOCK_STRIPES
    return DATA_DIR / ".locks" / f"{stripe:02x}.lock"


def _warn_once(key: str, message: str) -> None:
    if key not in _lock_warnings:
        _lock_warnings.add(key)
        logger.warning(message)


@contextmanager
def _locked(lock_file: Path, timeout: float, what: str):
    """Hold an exclusive lock on lock_file. Re-entrant within a thread."""
    with _registry_guard:
        thread_lock = _thread_locks.setdefault(lock_file, threading.RLock())
    if not thread_lock.acquire(timeout=max(timeout, 0)):
        raise LockTimeout(f"{what}: another thread of this process held the lock for more than {timeout:g} s")
    try:
        entry = _held.get(lock_file)
        if entry is not None:                          # nested in this thread: already ours
            entry[1] += 1
        else:
            entry = _held[lock_file] = [_flock(lock_file, timeout, what), 1]
        try:
            yield
        finally:
            entry[1] -= 1
            if entry[1] == 0:
                del _held[lock_file]
                if entry[0] is not None:
                    os.close(entry[0])                 # closing the descriptor releases the flock
    finally:
        thread_lock.release()


def _flock(lock_file: Path, timeout: float, what: str) -> int | None:
    """Open lock_file and flock it, waiting up to timeout. Returns the
    descriptor, or None where locking is not available (the write then
    proceeds unlocked, after one warning)."""
    if fcntl is None:
        _warn_once("no-fcntl", "file locking is not available on this platform: do not run the poller and a "
                               "backfill on the same data directory at the same time")
        return None
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(lock_file, os.O_CREAT | os.O_RDWR, 0o644)
    deadline = time.monotonic() + max(timeout, 0)
    while True:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return fd
        except BlockingIOError:
            if time.monotonic() >= deadline:
                os.close(fd)
                raise LockTimeout(f"{what}: another process held {lock_file.name} for more than {timeout:g} s") from None
            time.sleep(_LOCK_POLL_S)
        except OSError as e:                           # ENOTSUP / ENOLCK: network and synced folders
            os.close(fd)
            _warn_once("flock-unsupported", f"file locking does not work under {lock_file.parent} ({e}); writing "
                                            f"without a lock: do not run two writers on this data directory")
            return None


def file_lock(target: Path | str, timeout: float | None = None):
    """
    Context manager: exclusive access to `target` across processes and threads.

    Args:
        target:  the file about to be read, merged and rewritten
        timeout: seconds to wait for another holder (default config.LOCK_TIMEOUT_S)

    Raises:
        LockTimeout: the lock was not free in time. Nothing was written.
    """
    timeout = config.LOCK_TIMEOUT_S if timeout is None else timeout
    return _locked(lock_path_for(target), timeout, str(target))


def named_lock(name: str, timeout: float = 0.0):
    """
    Context manager: a lock of its own, DATA_DIR/.locks/{name}.lock, for "only
    one of these at a time" (a full-catalog backfill, the poller's history
    pull). With the default timeout 0 it raises LockTimeout at once when
    another process holds it.
    """
    return _locked(DATA_DIR / ".locks" / f"{name}.lock", timeout, f"lock {name!r}")


def _reset_state() -> None:
    """Forget lock bookkeeping and one-time warnings (test isolation)."""
    _lock_warnings.clear()


def atomic_write_text(path: Path, text: str) -> None:
    """Write a text file via temp file + rename, so a reader or a crash never
    sees it half-written. Creates the parent directory."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text)
    os.replace(tmp, path)


def temp_path_for(path: Path) -> Path:
    """Where a file is written before it replaces `path`. The name does not end
    in .parquet, so a temp file left by a killed process never matches a
    *.parquet glob, and it carries the pid, so two processes never share one."""
    return path.with_name(f"{path.name}.{os.getpid()}.tmp")


def append_parquet(
    df: pd.DataFrame,
    path: Path,
    dedupe_on: list[str],
    sort_by: str | None = None,
    lock_timeout: float | None = None,
) -> int:
    """
    Append rows to a parquet file with deduplication.

    Reads existing file (if any), concats with df, dedupes, sorts by
    sort_by column (or first dedupe column if not specified), writes
    atomically via temp file + rename. The whole read, merge, write runs
    under file_lock(path), so concurrent appends from another process (the
    poller next to a backfill) never lose rows.

    Args:
        lock_timeout: seconds to wait for another writer of this file
                      (default config.LOCK_TIMEOUT_S)

    Returns:
        Number of new rows after dedupe.

    Raises:
        LockTimeout: another writer held the file too long; nothing was written.
    """
    if df.empty:
        return 0

    with file_lock(path, lock_timeout):
        if path.exists():
            existing = pd.read_parquet(path, engine="pyarrow")
            n_before = len(existing)
            combined = pd.concat([existing, df], ignore_index=True)
        else:
            n_before = 0
            combined = df.copy()

        sort_col = sort_by or dedupe_on[0]
        combined = (
            combined
            .drop_duplicates(subset=dedupe_on, keep="last")
            .sort_values(sort_col)
            .reset_index(drop=True)
        )

        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = temp_path_for(path)
        combined.to_parquet(tmp_path, engine="pyarrow", compression="zstd", index=False)
        os.replace(tmp_path, path)

    return len(combined) - n_before


def read_parquet_safe(path: Path) -> pd.DataFrame | None:
    """Read a parquet file, returning None if it doesn't exist."""
    if not path.exists():
        return None
    return pd.read_parquet(path, engine="pyarrow")


def get_last_timestamp(path: Path, col: str = "ts_ms") -> int | None:
    """
    Read the max value of a timestamp column from a parquet file.

    Reads only the single column to avoid loading the full file.

    Returns:
        Max value as int (UTC ms), or None if file missing/empty.
    """
    if not path.exists():
        return None
    df = pd.read_parquet(path, columns=[col], engine="pyarrow")
    if df.empty:
        return None
    return int(df[col].max())


def get_output_path(
    kind: str,
    interval: int | None,
    series: str,
    ticker: str,
    ts: pd.Timestamp | None = None,
) -> Path:
    """
    Build the output parquet path per the directory layout in README.md.

    Args:
        kind:     "candles", "trades", or "orderbook"
        interval: period_interval (1, 60, 1440) for candles; ignored otherwise
        series:   series ticker (e.g. "KXCPI")
        ticker:   market ticker (e.g. "KXCPI-26APR-T0.4")
        ts:       timestamp for partitioning (required for hourly/minute/trades/orderbook)

    Returns:
        Absolute Path to the parquet file.
    """
    if kind == "candles":
        if interval == 1440:
            # daily/{series}/{ticker}.parquet
            return DATA_DIR / "candles" / "daily" / series / f"{ticker}.parquet"
        if interval == 60:
            # hourly/{series}/{year}/{ticker}.parquet
            if ts is None:
                raise ValueError("ts required for hourly candle path partitioning")
            return (
                DATA_DIR / "candles" / "hourly" / series
                / str(ts.year) / f"{ticker}.parquet"
            )
        if interval == 1:
            # minute/{series}/{year}/{month}/{ticker}.parquet
            if ts is None:
                raise ValueError("ts required for minute candle path partitioning")
            return (
                DATA_DIR / "candles" / "minute" / series
                / str(ts.year) / f"{ts.month:02d}" / f"{ticker}.parquet"
            )
        raise ValueError(f"Invalid candle interval: {interval}. Must be 1, 60, or 1440.")

    if kind == "trades":
        # trades/{series}/{ticker}/{yyyy-mm}.parquet
        if ts is None:
            raise ValueError("ts required for trades path partitioning")
        return (
            DATA_DIR / "trades" / series / ticker
            / f"{ts.year}-{ts.month:02d}.parquet"
        )

    if kind == "orderbook":
        # orderbook/{ticker}/{yyyy-mm-dd}.parquet
        if ts is None:
            raise ValueError("ts required for orderbook path partitioning")
        return (
            DATA_DIR / "orderbook" / ticker
            / f"{ts.year}-{ts.month:02d}-{ts.day:02d}.parquet"
        )

    raise ValueError(f"Unknown kind: {kind!r}. Must be 'candles', 'trades', or 'orderbook'.")


def duckdb_connect():
    """
    In-memory DuckDB connection for counting and checking over the parquet
    store without loading it. Extension download is switched off (a missing
    extension must fail, not fetch), and spill files go under the data root
    instead of the working directory.
    """
    import duckdb

    con = duckdb.connect()
    con.execute("SET autoinstall_known_extensions = false")
    con.execute("SET autoload_known_extensions = false")
    con.execute(f"SET temp_directory = '{(DATA_DIR / '.duckdb_tmp').as_posix()}'")
    return con
