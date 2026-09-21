"""Offline tests for ticker parsing and validation."""

import json
from pathlib import Path

import pytest

import pull_historical.pull_daily as pull_daily
from kalshi_io import runlog
from kalshi_io.candles import register_ticker_meta
from kalshi_io.tickers import load_tickers, validate_tickers
from fakes import FakeResponse, iso_to_ts, make_candle, make_event, make_market

# The exact argument that ended up as ONE ticker in both legacy skip files
NINE_STRIKES = (
    "KXCPIYOY-26JUL-T3.2 KXCPIYOY-26JUL-T3.3 KXCPIYOY-26JUL-T3.4 KXCPIYOY-26JUL-T3.5 "
    "KXCPIYOY-26JUL-T3.6 KXCPIYOY-26JUL-T3.7 KXCPIYOY-26JUL-T3.8 KXCPIYOY-26JUL-T3.9 KXCPIYOY-26JUL-T4.0"
)

# Two real tickers of the 2026-09-19 catalog: one holds a space, one a comma
SPACED, COMMA = "GDP-232022 Q4-T0.0", "JOBLESS-22JUL23-C250,000"
# ticker → (event, open, close); finalized, historical tier
SEPARATOR_MARKETS = {
    SPACED: ("GDP-232022 Q4", "2022-10-27T14:00:00Z", "2023-01-26T13:25:00Z"),
    COMMA: ("JOBLESS-22JUL23", "2022-07-21T14:00:00Z", "2022-07-28T12:25:00Z"),
}
CATALOGED = [SPACED, COMMA, "KXTEST-26SEP-T1", "KXTEST-26SEP-T2"]           # sorted


@pytest.fixture
def catalog(catalog_dir):
    (catalog_dir / "KXTEST_tickers.txt").write_text("\n".join(CATALOGED) + "\n")
    (catalog_dir / "KXTEST_tickers.json").write_text(json.dumps({
        "series": "KXTEST",
        "events": [{"event_ticker": "KXTEST-26SEP", "title": "Test"},
                   *({"event_ticker": event, "title": "Test"} for event, _, _ in SEPARATOR_MARKETS.values())],
        "markets": [
            *({"event_ticker": "KXTEST-26SEP", "market_ticker": t, "title": "", "status": "active", "source": "live"}
              for t in ("KXTEST-26SEP-T1", "KXTEST-26SEP-T2")),
            *({"event_ticker": event, "market_ticker": t, "title": "", "status": "finalized", "source": "historical",
               "open_time": open_iso, "close_time": close_iso}
              for t, (event, open_iso, close_iso) in SEPARATOR_MARKETS.items()),
        ],
        "tickers": CATALOGED,
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
    assert load_tickers("KXTEST") == CATALOGED
    assert load_tickers(["KXTEST", "OTHER-1"]) == [*CATALOGED, "OTHER-1"]
    assert load_tickers(str(catalog / "KXTEST_tickers.json")) == CATALOGED

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


# ------------------------------------------------------------------ tickers that hold a separator

def test_a_cataloged_ticker_with_a_space_or_a_comma_is_taken_whole(catalog):
    assert load_tickers(SPACED) == [SPACED] and load_tickers(COMMA) == [COMMA]
    assert load_tickers([SPACED, COMMA, "KXTEST-26SEP-T1"]) == [SPACED, COMMA, "KXTEST-26SEP-T1"]
    assert load_tickers(f"  {SPACED}\n") == [SPACED] and load_tickers([f" {COMMA} "]) == [COMMA]


def test_loading_a_loaded_list_changes_nothing(catalog):
    first = load_tickers("KXTEST")
    assert SPACED in first and COMMA in first
    assert load_tickers(first) == first
    assert load_tickers(load_tickers(first)) == first


def test_a_file_is_one_ticker_per_line_and_is_never_split_further(catalog, tmp_path):
    uncataloged = "NEW-1 X,Y"
    expected = sorted([SPACED, COMMA, uncataloged])
    listing = tmp_path / "mine.txt"
    listing.write_text(f"# my picks\n{SPACED}\n  {COMMA}  \n\n{uncataloged}\n")
    assert load_tickers(str(listing)) == expected
    as_json = tmp_path / "mine.json"
    as_json.write_text(json.dumps({"tickers": [SPACED, f"  {COMMA}  ", uncataloged]}))
    assert load_tickers(str(as_json)) == expected
    # Next to a typed argument, which is split as before
    assert load_tickers([str(listing), "A-1 B-2"]) == sorted([*expected, "A-1", "B-2"])


def test_a_typed_argument_that_is_not_a_known_ticker_is_still_split(catalog):
    # The documented limit: a ticker that holds a separator cannot share a quoted string with others
    assert load_tickers(f"A-1, {COMMA}") == ["000", "A-1", "JOBLESS-22JUL23-C250"]
    # Not quoted, the shell delivers two arguments; tokens are never joined back together
    assert load_tickers(["GDP-232022", "Q4-T0.0"]) == ["GDP-232022", "Q4-T0.0"]


def test_a_ticker_registered_in_the_process_is_taken_whole_too(catalog, fake_api, tmp_path):
    focus = "NEWGDP-27 Q1-T1.0"
    assert load_tickers([focus]) == ["NEWGDP-27", "Q1-T1.0"]             # nothing knows it yet
    register_ticker_meta({focus: ("KXGDP", "NEWGDP-27 Q1")})             # the focus universe does this
    assert load_tickers([focus]) == [focus] and load_tickers(load_tickers(focus)) == [focus]

    # A ticker only the API knows comes in through a file; validation resolves and registers it
    resolved = "NEWJOBLESS-27JAN02-C250,000"
    fake_api.add_event(make_event("NEWJOBLESS-27JAN02", "KXJOBLESS"), [make_market(resolved, "NEWJOBLESS-27JAN02")])
    listing = tmp_path / "mine.txt"
    listing.write_text(resolved + "\n")
    first = load_tickers(str(listing))
    assert first == [resolved] and validate_tickers(first) == ([resolved], [])
    assert load_tickers(first) == first
    assert [path for path, _, _ in fake_api.calls] == ["/markets/NEWJOBLESS-27JAN02-C250%2C000",
                                                       "/events/NEWJOBLESS-27JAN02"]


def test_a_long_list_of_known_tickers_costs_no_file_read_per_element(catalog, monkeypatch):
    # The size of the 2026-09-19 catalog; every tenth ticker holds a separator
    big = sorted(f"KXBIG-26SEP-T{i}" + (" X,Y" if i % 10 == 0 else "") for i in range(5027))
    (catalog / "KXBIG_tickers.json").write_text(json.dumps({
        "series": "KXBIG", "events": [{"event_ticker": "KXBIG-26SEP", "title": "Big"}], "tickers": big,
        "markets": [{"event_ticker": "KXBIG-26SEP", "market_ticker": t, "title": "", "status": "active",
                     "source": "live"} for t in big],
    }))
    reads: list[str] = []
    real_read = Path.read_text
    monkeypatch.setattr(Path, "read_text", lambda self, *a, **k: reads.append(self.name) or real_read(self, *a, **k))

    first = load_tickers(big)

    assert first == big
    assert sorted(reads) == ["KXBIG_tickers.json", "KXTEST_tickers.json"]    # the catalog index, built once
    reads.clear()
    assert load_tickers(first) == first and reads == []


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


def test_puller_pulls_tickers_with_a_space_and_a_comma_from_a_list(catalog, exchange, data_dir):
    for ticker, (event, open_iso, close_iso) in SEPARATOR_MARKETS.items():
        exchange.add_event(make_event(event, "KXTEST"), [make_market(
            ticker, event, status="finalized", tier="historical", open_time=open_iso, close_time=close_iso)])
        exchange.candles[ticker] = {1440: [make_candle(int(iso_to_ts(open_iso)) + 86400 * k) for k in (1, 2, 3)]}

    summary = pull_daily.run([SPACED, COMMA])                            # a list, as the driver and the poller pass it

    assert summary["unknown"] == [] and summary["processed"] == 2 and summary["failed"] == 0
    assert summary["rows_written"] == 6
    files = sorted(p.name for p in (data_dir / "candles" / "daily" / "KXTEST").glob("*.parquet"))
    assert files == [f"{SPACED}.parquet", f"{COMMA}.parquet"]
    assert not list((data_dir / "logs").glob("skip_daily_*"))
    # One request each, on the tier the catalog recorded, the ticker URL-quoted in the path
    assert [path for path, _, _ in exchange.calls] == [
        "/historical/markets/GDP-232022%20Q4-T0.0/candlesticks",
        "/historical/markets/JOBLESS-22JUL23-C250%2C000/candlesticks",
    ]
