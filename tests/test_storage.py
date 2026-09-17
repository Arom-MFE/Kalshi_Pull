"""Offline tests for parquet append/dedupe round trip."""

import pandas as pd

from kalshi_io.storage import append_parquet


def _frame():
    return pd.DataFrame({
        "ts_ms": pd.Series([1000, 2000, 3000], dtype="int64"),
        "market_ticker": ["T-1", "T-1", "T-1"],
        "close": [0.5, 0.6, 0.7],
        "volume": [10.0, 11.5, 12.0],
    })


def test_append_then_reappend_leaves_rows_unchanged(tmp_path):
    path = tmp_path / "t.parquet"
    assert append_parquet(_frame(), path, ["ts_ms", "market_ticker"]) == 3
    # Re-appending the identical frame must dedupe to zero new rows
    assert append_parquet(_frame(), path, ["ts_ms", "market_ticker"]) == 0
    df = pd.read_parquet(path)
    assert len(df) == 3
    assert df["ts_ms"].dtype == "int64"
    assert df["close"].dtype == "float64"
    assert df["volume"].dtype == "float64"
    assert list(df["ts_ms"]) == [1000, 2000, 3000]


# ------------------------------------------------------------------ file lock

import errno
import subprocess
import sys
import threading
import time

import pytest

from kalshi_io import storage
from kalshi_io.storage import LockTimeout, file_lock, lock_path_for, named_lock, temp_path_for


def _hold_in_child(lock_file, seconds: float) -> subprocess.Popen:
    """Another process that holds the flock on lock_file for a while (no network, no repo code)."""
    code = (
        "import fcntl, os, sys, time\n"
        "fd = os.open(sys.argv[1], os.O_CREAT | os.O_RDWR)\n"
        "fcntl.flock(fd, fcntl.LOCK_EX)\n"
        "print('held', flush=True)\n"
        "time.sleep(float(sys.argv[2]))\n"
    )
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.Popen([sys.executable, "-c", code, str(lock_file), str(seconds)], stdout=subprocess.PIPE, text=True)
    assert proc.stdout.readline().strip() == "held"
    return proc


def test_another_process_holding_the_file_blocks_the_append_until_it_lets_go(data_dir, monkeypatch):
    path = data_dir / "candles" / "t.parquet"
    child = _hold_in_child(lock_path_for(path), 0.6)
    try:
        t0 = time.monotonic()
        with pytest.raises(LockTimeout, match="another process"):
            append_parquet(_frame(), path, ["ts_ms", "market_ticker"], lock_timeout=0.2)
        assert not path.exists() and time.monotonic() - t0 < 0.6            # nothing written, gave up in time
        # With patience the append goes through as soon as the holder is done
        assert append_parquet(_frame(), path, ["ts_ms", "market_ticker"], lock_timeout=5) == 3
    finally:
        child.wait(timeout=5)


def test_lock_serializes_threads_and_is_reentrant_in_one_thread(data_dir):
    path = data_dir / "t.parquet"
    entered, release = threading.Event(), threading.Event()

    def holder():
        with file_lock(path):
            with file_lock(path):                          # nested in the same thread: no self-deadlock
                entered.set()
                release.wait(5)

    thread = threading.Thread(target=holder)
    thread.start()
    assert entered.wait(5)
    with pytest.raises(LockTimeout, match="another thread"):
        with file_lock(path, timeout=0.1):
            pass
    release.set()
    thread.join(5)
    with file_lock(path, timeout=1):
        pass                                               # free again once the holder released it
    assert not storage._held


def test_concurrent_appends_from_threads_lose_no_rows(data_dir):
    path = data_dir / "candles" / "hammer.parquet"
    errors: list[Exception] = []

    def worker(k: int):
        try:
            for j in range(25):
                df = pd.DataFrame({"ts_ms": pd.Series([k * 1000 + j], dtype="int64"), "market_ticker": ["T"], "x": [1.0]})
                append_parquet(df, path, ["ts_ms", "market_ticker"])
        except Exception as e:                             # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(k,)) for k in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    assert errors == []
    stored = pd.read_parquet(path)
    assert len(stored) == 200 and stored["ts_ms"].is_unique and stored["ts_ms"].is_monotonic_increasing
    assert [p.name for p in path.parent.iterdir()] == ["hammer.parquet"]             # no temp file left behind


def test_lock_files_are_striped_under_the_data_root_and_never_deleted(data_dir):
    paths = {lock_path_for(data_dir / "candles" / "minute" / "S" / f"T-{i}.parquet") for i in range(2000)}
    assert len(paths) <= storage.LOCK_STRIPES and all(p.parent == data_dir / ".locks" for p in paths)
    for i in range(3):
        append_parquet(_frame(), data_dir / f"f{i}.parquet", ["ts_ms", "market_ticker"])
    lock_files = list((data_dir / ".locks").glob("*.lock"))
    assert 1 <= len(lock_files) <= 3
    assert lock_path_for("relative/name.parquet").parent == data_dir / ".locks"


def test_temp_files_never_look_like_parquet_files(data_dir):
    path = data_dir / "x.parquet"
    tmp = temp_path_for(path)
    assert tmp.parent == path.parent and not tmp.name.endswith(".parquet") and tmp.name.startswith("x.parquet.")
    assert tmp.name.endswith(".tmp")
    # An empty frame takes no lock and creates nothing
    assert append_parquet(pd.DataFrame(), path, ["ts_ms"]) == 0 and not data_dir.exists()


def test_named_lock_refuses_a_second_holder_at_once(data_dir):
    with named_lock("backfill_full"):
        child = _hold_in_child(data_dir / ".locks" / "other.lock", 0.5)
        try:
            with pytest.raises(LockTimeout):
                with named_lock("other"):
                    pass
        finally:
            child.wait(5)
    with named_lock("backfill_full"):
        pass


def test_without_fcntl_writes_proceed_with_one_warning(data_dir, monkeypatch, caplog):
    monkeypatch.setattr(storage, "fcntl", None)
    with caplog.at_level("WARNING", logger="kalshi.storage"):
        for i in range(2):
            append_parquet(_frame(), data_dir / f"w{i}.parquet", ["ts_ms", "market_ticker"])
    assert [r.message for r in caplog.records] == [
        "file locking is not available on this platform: do not run the poller and a backfill on the same "
        "data directory at the same time"]


def test_an_unsupported_filesystem_degrades_to_unlocked_writes_with_one_warning(data_dir, monkeypatch, caplog):
    def refuse(fd, op):
        raise OSError(errno.ENOTSUP, "Operation not supported")

    monkeypatch.setattr(storage.fcntl, "flock", refuse)
    with caplog.at_level("WARNING", logger="kalshi.storage"):
        for i in range(2):
            assert append_parquet(_frame(), data_dir / f"n{i}.parquet", ["ts_ms", "market_ticker"]) == 3
    assert len(caplog.records) == 1 and "does not work under" in caplog.records[0].message
    assert not storage._held
