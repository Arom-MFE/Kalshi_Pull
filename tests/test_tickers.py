"""Offline tests for ticker parsing and validation."""

import json

import pytest

import pull_historical.pull_daily as pull_daily
from kalshi_io import runlog
from kalshi_io.tickers import load_tickers, validate_tickers
from fakes import FakeResponse, make_event, make_market

# The exact argument that ended up as ONE ticker in both legacy skip files
NINE_STRIKES = (
    "KXCPIYOY-26JUL-T3.2 KXCPIYOY-26JUL-T3.3 KXCPIYOY-26JUL-T3.4 KXCPIYOY-26JUL-T3.5 "
    "KXCPIYOY-26JUL-T3.6 KXCPIYOY-26JUL-T3.7 KXCPIYOY-26JUL-T3.8 KXCPIYOY-26JUL-T3.9 KXCPIYOY-26JUL-T4.0"
)


@pytest.fixture
def catalog(catalog_dir):
    (catalog_dir / "KXTEST_tickers.txt").write_text("KXTEST-26SEP-T1\nKXTEST-26SEP-T2\n")
    (catalog_dir / "KXTEST_tickers.json").write_text(json.dumps({
        "series": "KXTEST",
        "events": [{"event_ticker": "KXTEST-26SEP", "title": "Test"}],
        "markets": [
            {"event_ticker": "KXTEST-26SEP", "market_ticker": t, "title": "", "status": "active", "source": "live"}
            for t in ("KXTEST-26SEP-T1", "KXTEST-26SEP-T2")
        ],
        "tickers": ["KXTEST-26SEP-T1", "KXTEST-26SEP-T2"],
    }))
    return catalog_dir


# ------------------------------------------------------------------ load_tickers

def test_space_separated_string_is_nine_tickers_not_one():
    tickers = load_tickers(NINE_STRIKES)
    assert len(tickers) == 9
    assert tickers[0] == "KXCPIYOY-26JUL-T3.2" and tickers[-1] == "KXCPIYOY-26JUL-T4.0"


def test_commas_newlines_and_padding_all_separate_tickers():
    assert load_tickers("B-2, A-1,,C-3\n  D-4\t") == ["A-1", "B-2", "C-3", "D-4"]
    assert load_tickers("  A-1  ") == ["A-1"]
    assert load_tickers("") == [] and load_tickers([]) == []


def test_every_list_element_is_split_and_deduped():
    # argparse nargs="+" delivers a list; a quoted element may still hold several tickers
    assert load_tickers(["A-1 B-2", "C-3,A-1"]) == ["A-1", "B-2", "C-3"]


def test_series_name_and_files_still_expand_also_inside_a_list(catalog, tmp_path):
    assert load_tickers("KXTEST") == ["KXTEST-26SEP-T1", "KXTEST-26SEP-T2"]
    assert load_tickers(["KXTEST", "OTHER-1"]) == ["KXTEST-26SEP-T1", "KXTEST-26SEP-T2", "OTHER-1"]
    assert load_tickers(str(catalog / "KXTEST_tickers.json")) == ["KXTEST-26SEP-T1", "KXTEST-26SEP-T2"]

    listing = tmp_path / "mine.txt"
    listing.write_text("# my picks\nA-1\n\n  B-2  \n")
    assert load_tickers([str(listing), "C-3"]) == ["A-1", "B-2", "C-3"]


def test_a_path_containing_a_space_is_not_split(tmp_path):
    folder = tmp_path / "my lists"
    folder.mkdir()
    (folder / "cpi strikes.txt").write_text("A-1\nB-2\n")
    assert load_tickers(str(folder / "cpi strikes.txt")) == ["A-1", "B-2"]


def test_missing_file_is_an_error_not_a_ticker(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_tickers(str(tmp_path / "nope.txt"))


def test_a_very_long_ticker_string_does_not_trip_the_file_check():
    many = " ".join(f"KXTEST-26SEP-T{i}" for i in range(400))      # far beyond a legal file name
    assert len(load_tickers(many)) == 400


def test_focus_alias_reads_the_proposed_universe(catalog):
    with pytest.raises(FileNotFoundError, match="roll.py"):
        load_tickers("focus")
    (catalog / "focus_universe.txt").write_text("KXFED-26OCT-T3.50\nKXU3-26SEP-T4.2\n")
    assert load_tickers("focus") == ["KXFED-26OCT-T3.50", "KXU3-26SEP-T4.2"]


# ------------------------------------------------------------------ validate_tickers

@pytest.fixture
def exchange(fake_api):
    fake_api.add_event(make_event("KXNEW-26SEP", "KXNEW"), [make_market("KXNEW-26SEP-T1", "KXNEW-26SEP")])
    return fake_api


def test_validation_catalog_first_then_api_and_reports_unknown(catalog, exchange):
    known, unknown = validate_tickers(["KXTEST-26SEP-T1", "KXNEW-26SEP-T1", "KXTYPO-26SEP-T1"])
    assert known == ["KXTEST-26SEP-T1", "KXNEW-26SEP-T1"]
    assert unknown == ["KXTYPO-26SEP-T1"]
    # The cataloged ticker cost no request
    assert not any("KXTEST" in path for path, _, _ in exchange.calls)


def test_offline_validation_uses_the_catalog_only(catalog, exchange):
    known, unknown = validate_tickers(["KXTEST-26SEP-T1", "KXNEW-26SEP-T1"], allow_api=False)
    assert (known, unknown) == (["KXTEST-26SEP-T1"], ["KXNEW-26SEP-T1"])
    assert exchange.calls == []


def test_an_api_outage_never_turns_a_ticker_into_unknown(catalog, exchange):
    exchange.inject(r"^/markets/KXNEW-26SEP-T1$", [FakeResponse(503, {"error": {"message": "down"}})] * 6)
    known, unknown = validate_tickers(["KXNEW-26SEP-T1"])
    assert (known, unknown) == (["KXNEW-26SEP-T1"], [])


# ------------------------------------------------------------------ pullers

def _stub_fetch(monkeypatch, seen):
    monkeypatch.setattr(
        pull_daily, "market_window",
        lambda ticker, allow_api=True: {"open_ts": 0, "close_ts": None, "status": "active", "source": "stub"},
    )
    monkeypatch.setattr(pull_daily, "fetch_candles", lambda ticker, *a: seen.append(ticker) or [])


def test_puller_reports_unknown_tickers_and_does_not_attempt_them(catalog, exchange, monkeypatch, data_dir):
    seen: list[str] = []
    _stub_fetch(monkeypatch, seen)
    summary = pull_daily.run("KXTEST-26SEP-T1 KXTYPO-26SEP-T1, KXNEW-26SEP-T1")
    assert seen == ["KXNEW-26SEP-T1", "KXTEST-26SEP-T1"]
    assert summary["unknown"] == ["KXTYPO-26SEP-T1"]
    assert summary["processed"] == 2 and summary["skipped"] == 1 and summary["failed"] == 0
    line = (data_dir / "logs" / f"skip_daily_{runlog.PROCESS_STAMP}.txt").read_text()
    assert line.split("\t")[1:] == ["KXTYPO-26SEP-T1", "unknown ticker: not in the catalog and not found on the API\n"]


def test_cli_accepts_several_tickers_quoted_or_not(catalog, exchange, monkeypatch):
    seen: list[str] = []
    _stub_fetch(monkeypatch, seen)
    assert pull_daily.main(["--tickers", "KXTEST-26SEP-T1", "KXTEST-26SEP-T2"]) == 0
    assert pull_daily.main(["--tickers", "KXTEST-26SEP-T1 KXTEST-26SEP-T2"]) == 0
    assert seen == ["KXTEST-26SEP-T1", "KXTEST-26SEP-T2"] * 2
