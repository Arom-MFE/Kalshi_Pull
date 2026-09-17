"""Offline tests for the environment-driven settings in kalshi_io.config."""

from pathlib import Path

import kalshi_io.config as config
from kalshi_io.config import PROJECT_ROOT, _resolve_data_dir, _resolve_max_rps


def test_data_dir_defaults_to_repo_kalshi_data():
    assert _resolve_data_dir({}) == PROJECT_ROOT / "kalshi_data"
    # An empty or blank override must not resolve to the current directory
    assert _resolve_data_dir({"KALSHI_DATA_DIR": ""}) == PROJECT_ROOT / "kalshi_data"
    assert _resolve_data_dir({"KALSHI_DATA_DIR": "   "}) == PROJECT_ROOT / "kalshi_data"


def test_data_dir_override_is_resolved_and_expands_home(tmp_path):
    assert _resolve_data_dir({"KALSHI_DATA_DIR": str(tmp_path / "scratch")}) == (tmp_path / "scratch").resolve()
    assert _resolve_data_dir({"KALSHI_DATA_DIR": "~/kalshi_scratch"}) == (Path.home() / "kalshi_scratch").resolve()


def test_test_session_never_points_at_the_real_data_dir():
    # conftest sets KALSHI_DATA_DIR before kalshi_io is imported
    real = (PROJECT_ROOT / "kalshi_data").resolve()
    assert config.DATA_DIR != real
    assert real not in config.DATA_DIR.parents


def test_max_rps_default_clamp_and_garbage():
    # 5 requests/s is what the keyless candlestick endpoints sustain (measured 2026-09-17)
    assert _resolve_max_rps({}) == 5.0 == config.DEFAULT_MAX_RPS
    assert _resolve_max_rps({"KALSHI_MAX_RPS": "10"}) == 10.0
    # 20 requests/s is the documented Basic-tier read budget; never exceed it
    assert _resolve_max_rps({"KALSHI_MAX_RPS": "500"}) == 20.0
    assert _resolve_max_rps({"KALSHI_MAX_RPS": "0.5"}) == 0.5
    # Non-positive or unparsable values fall back to the default
    assert _resolve_max_rps({"KALSHI_MAX_RPS": "0"}) == 5.0
    assert _resolve_max_rps({"KALSHI_MAX_RPS": "-3"}) == 5.0
    assert _resolve_max_rps({"KALSHI_MAX_RPS": "fast"}) == 5.0


def test_focus_universe_is_a_rule_not_a_ticker_list():
    import kalshi_io

    assert config.FOCUS_SERIES and set(config.FOCUS_SERIES) <= set(config.SERIES_LIST)
    assert config.FOCUS_EVENTS_PER_SERIES >= 1
    # The committed override stays empty: a hand-written list is what went stale in 2026-09
    assert config.FOCUS_OVERRIDE == []
    assert not hasattr(config, "FOCUS_UNIVERSE") and not hasattr(kalshi_io, "FOCUS_UNIVERSE")
    assert kalshi_io.FOCUS_SERIES is config.FOCUS_SERIES
