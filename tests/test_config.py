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
    assert _resolve_max_rps({}) == 10.0
    assert _resolve_max_rps({"KALSHI_MAX_RPS": "5"}) == 5.0
    # 20 requests/s is the documented Basic-tier read budget; never exceed it
    assert _resolve_max_rps({"KALSHI_MAX_RPS": "500"}) == 20.0
    assert _resolve_max_rps({"KALSHI_MAX_RPS": "0.5"}) == 0.5
    # Non-positive or unparsable values fall back to the default
    assert _resolve_max_rps({"KALSHI_MAX_RPS": "0"}) == 10.0
    assert _resolve_max_rps({"KALSHI_MAX_RPS": "-3"}) == 10.0
    assert _resolve_max_rps({"KALSHI_MAX_RPS": "fast"}) == 10.0
