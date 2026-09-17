"""Shared fixtures. Every test runs offline, keyless, and against a temp data dir.

Three guards are always on:
    data     — DATA_DIR points at a per-test temp dir; the real kalshi_data/
               can never be touched, not even by accident
    network  — opening a socket fails the test, even when the code under test
               swallows the exception (the pullers catch Exception per ticker)
    secrets  — credentials are removed from the environment and load_dotenv is
               a no-op, so no test can read .env or the private key
"""

import logging
import os
import socket
import sys
import tempfile
from pathlib import Path

import pytest

# Must run before kalshi_io is imported anywhere: DATA_DIR is resolved at
# import time and bound by value in several modules.
_SESSION_DATA_DIR = Path(tempfile.mkdtemp(prefix="kalshi_pull_tests_")).resolve()
os.environ["KALSHI_DATA_DIR"] = str(_SESSION_DATA_DIR)
os.environ.pop("KALSHI_MAX_RPS", None)

import kalshi_io  # noqa: E402,F401
import kalshi_io.client as client_mod  # noqa: E402
import kalshi_io.config as config  # noqa: E402
import pull_historical.pull_audit  # noqa: E402,F401
import pull_historical.pull_daily  # noqa: E402,F401
import pull_historical.pull_hourly  # noqa: E402,F401
import pull_historical.pull_minute  # noqa: E402,F401
import pull_historical.pull_trades  # noqa: E402,F401
import pull_live.poll_focus  # noqa: E402,F401

from fakes import FakeKalshi  # noqa: E402

_REAL_DATA_DIR = (config.PROJECT_ROOT / "kalshi_data").resolve()
if config.DATA_DIR == _REAL_DATA_DIR or _REAL_DATA_DIR in config.DATA_DIR.parents:
    raise pytest.UsageError(f"tests would write into the real data dir: {config.DATA_DIR}")

_OUR_PACKAGES = ("kalshi_io", "pull_historical", "pull_live", "get_ticker_info")


def _our_modules():
    return [m for name, m in list(sys.modules.items())
            if m is not None and name.split(".")[0] in _OUR_PACKAGES]


def patch_everywhere(monkeypatch, attr: str, value) -> None:
    """Patch a by-value module global (DATA_DIR, TICKERS_DIR, ...) in every loaded module."""
    for mod in _our_modules():
        if hasattr(mod, attr):
            monkeypatch.setattr(mod, attr, value)


def _reset_module_state() -> None:
    for mod in _our_modules():
        reset = getattr(mod, "_reset_state", None)
        if callable(reset):
            reset()


def _strip_log_handlers() -> None:
    names = [n for n in logging.root.manager.loggerDict
             if n == "kalshi" or n.startswith(("kalshi.", "pull_", "poll_"))]
    for name in names:
        lg = logging.getLogger(name)
        for h in list(lg.handlers):
            lg.removeHandler(h)
            h.close()


@pytest.fixture(autouse=True)
def _isolation(tmp_path, monkeypatch):
    # --- data guard
    data_dir = tmp_path / "kalshi_data"
    patch_everywhere(monkeypatch, "DATA_DIR", data_dir)

    # --- secrets guard
    monkeypatch.delenv("KALSHI_KEY_PATH", raising=False)
    monkeypatch.delenv("KALSHI_API_KEY_ID", raising=False)
    monkeypatch.setattr(client_mod, "load_dotenv", lambda *a, **k: False)
    for fn in (client_mod.get_client, client_mod.get_session):
        clear = getattr(fn, "cache_clear", None)
        if clear:
            clear()

    # --- process guard: the poller's history pull is a child process that the
    # socket guard cannot reach; a test that needs one fakes _spawn_history
    def _no_child(*args, **kwargs):
        raise RuntimeError("poll_focus tried to start a real history pull inside a test")

    monkeypatch.setattr(pull_live.poll_focus, "_spawn_history", _no_child)

    # --- network guard
    violations: list[str] = []

    def _blocked(*args, **kwargs):
        violations.append(f"socket call with {args[1:] or args}")
        raise RuntimeError("network access attempted in an offline test")

    monkeypatch.setattr(socket.socket, "connect", _blocked)
    monkeypatch.setattr(socket.socket, "connect_ex", _blocked)
    monkeypatch.setattr(socket, "create_connection", _blocked)

    _reset_module_state()
    yield
    _strip_log_handlers()
    _reset_module_state()
    assert not violations, f"network access attempted: {violations}"


@pytest.fixture
def data_dir(tmp_path) -> Path:
    """The per-test data root every module writes to."""
    return tmp_path / "kalshi_data"


class VirtualClock:
    """Stand-in for time.monotonic/time.sleep: sleeping just advances the clock."""

    def __init__(self):
        self.now = 1000.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def clock(monkeypatch) -> VirtualClock:
    """Virtual clock for the HTTP layer; jitter always takes its upper bound."""
    c = VirtualClock()
    monkeypatch.setattr(client_mod, "_sleep", c.sleep)
    monkeypatch.setattr(client_mod, "_monotonic", c.monotonic)
    monkeypatch.setattr(client_mod, "_uniform", lambda low, high: high)
    return c


@pytest.fixture
def fake_api(monkeypatch, clock) -> FakeKalshi:
    """Route every REST call of the code under test to an in-memory fake exchange."""
    api = FakeKalshi()
    patch_everywhere(monkeypatch, "get_session", lambda: api)
    return api


@pytest.fixture
def catalog_dir(tmp_path, monkeypatch) -> Path:
    """Empty temp ticker catalog, patched in everywhere TICKERS_DIR is bound."""
    path = tmp_path / "kalshi_tickers"
    path.mkdir()
    patch_everywhere(monkeypatch, "TICKERS_DIR", path)
    patch_everywhere(monkeypatch, "OUTPUT_DIR", path)
    _reset_module_state()
    return path
