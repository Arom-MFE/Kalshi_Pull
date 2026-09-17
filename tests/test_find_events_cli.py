"""Offline tests for the get_ticker_info/find_events.py CLI against the fake exchange."""

import ast
import json

import pytest

import get_ticker_info.find_events as find_events
from kalshi_io.tickers import load_tickers
from fakes import FakeResponse, make_event, make_market

SEP = [f"KXCPIYOY-26SEP-T3.{i}" for i in range(3)]


@pytest.fixture
def exchange(fake_api):
    fake_api.tags_by_categories = {"Economics": ["Inflation", "Fed"], "Financials": ["Indices"]}
    fake_api.series = [
        {"ticker": "KXCPIYOY", "title": "Inflation", "category": "Economics",
         "categories": ["Economics"], "tags": ["Inflation"], "frequency": "monthly"},
        {"ticker": "CPIYOY", "title": "Inflation", "category": "Economics",
         "categories": ["Economics"], "tags": ["Inflation"], "frequency": "monthly"},
        {"ticker": "KXFED", "title": "Fed funds rate", "category": "Economics",
         "categories": ["Economics"], "tags": ["Fed"], "frequency": "custom"},
        {"ticker": "KXSPX", "title": "S&P 500 close", "category": "Financials",
         "categories": ["Financials"], "tags": ["Indices"], "frequency": "daily"},
    ]
    fake_api.add_event(make_event("KXCPIYOY-26SEP", "KXCPIYOY", title="Inflation in September 2026"), [
        make_market(t, "KXCPIYOY-26SEP", close_time="2026-10-14T12:29:00Z", yes_sub_title=f"Above 3.{i}%")
        for i, t in enumerate(SEP)])
    fake_api.add_event(make_event("KXCPIYOY-26AUG", "KXCPIYOY", title="Inflation in August 2026"), [
        make_market("KXCPIYOY-26AUG-T3.0", "KXCPIYOY-26AUG", status="finalized", close_time="2026-09-11T12:29:00Z")])
    fake_api.add_event(make_event("CPIYOY-22DEC", "KXCPIYOY", title="Inflation in December 2022"), [
        make_market("CPIYOY-22DEC-T6.5", "CPIYOY-22DEC", status="finalized", tier="historical")])
    fake_api.add_event(make_event("KXFED-26OCT", "KXFED"), [
        make_market("KXFED-26OCT-T3.50", "KXFED-26OCT", status="inactive")])
    return fake_api


def run(capsys, *argv) -> tuple[int, str, str]:
    code = find_events.main(list(argv))
    captured = capsys.readouterr()
    return code, captured.out, captured.err


# ------------------------------------------------------------------ modes

def test_categories(exchange, capsys):
    code, out, _ = run(capsys, "--categories")
    assert code == 0
    assert out.splitlines()[0].split() == ["CATEGORY", "TAGS"]
    assert "Economics   Inflation, Fed" in out and "2 categories" in out


def test_series_by_category_tag_and_keyword(exchange, capsys):
    code, out, _ = run(capsys, "--category", "Economics", "--format", "tickers")
    assert code == 0 and out.split() == ["KXCPIYOY", "KXFED"]            # the dead pre-KX twin is hidden

    _, out, _ = run(capsys, "--category", "Economics", "--include-legacy", "--format", "tickers")
    assert out.split() == ["CPIYOY", "KXCPIYOY", "KXFED"]

    _, out, _ = run(capsys, "--category", "Economics", "--tag", "Fed", "--format", "tickers")
    assert out.split() == ["KXFED"]

    # Keyword search spans every category; words are AND-ed over ticker, title and tags
    _, out, _ = run(capsys, "--keyword", "s&p close", "--format", "tickers")
    assert out.split() == ["KXSPX"]
    _, out, _ = run(capsys, "--keyword", "INFLATION", "--category", "Economics")
    assert "KXCPIYOY  Economics  monthly    Inflation  Inflation" in out and "1 series" in out


def test_events_of_a_series_by_status(exchange, capsys):
    code, out, _ = run(capsys, "--series", "KXCPIYOY", "--status", "open")
    assert code == 0
    header, row = out.splitlines()[:2]
    assert header.split() == ["SERIES_TICKER", "EVENT_TICKER", "LIVE_MARKETS", "OPEN_MARKETS", "CLOSES", "TITLE"]
    assert row.split()[:5] == ["KXCPIYOY", "KXCPIYOY-26SEP", "3", "3", "2026-10-14T12:29:00Z"]
    assert row.endswith("Inflation in September 2026")

    # No status: every event, however old. The pre-KX event has no live-tier market to count
    _, out, _ = run(capsys, "--series", "KXCPIYOY", "--format", "json")
    rows = {r["event_ticker"]: r for r in json.loads(out)}
    assert sorted(rows) == ["CPIYOY-22DEC", "KXCPIYOY-26AUG", "KXCPIYOY-26SEP"]
    assert rows["CPIYOY-22DEC"]["live_markets"] == 0 and rows["CPIYOY-22DEC"]["closes"] is None
    assert rows["KXCPIYOY-26AUG"]["open_markets"] == 0 and rows["KXCPIYOY-26AUG"]["closes"] == "2026-09-11T12:29:00Z"

    _, out, _ = run(capsys, "--series", "KXCPIYOY", "KXFED", "--status", "settled", "--format", "tickers")
    assert out.split() == ["CPIYOY-22DEC", "KXCPIYOY-26AUG"]


def test_markets_of_series_and_of_one_event(exchange, capsys):
    code, out, _ = run(capsys, "--series", "KXCPIYOY", "--status", "open", "--markets")
    assert code == 0 and "3 markets" in out
    assert "KXCPIYOY-26SEP-T3.1  KXCPIYOY-26SEP  active  live  2026-10-14T12:29:00Z  Above 3.1%" in out

    # Paused is a market status, so it is valid here (and only here)
    _, out, _ = run(capsys, "--series", "KXFED", "KXCPIYOY", "--status", "paused", "--markets", "--format", "tickers")
    assert out.split() == ["KXFED-26OCT-T3.50"]

    # A settled event whose markets left the live tier is answered from /historical/
    code, out, _ = run(capsys, "--event", "CPIYOY-22DEC", "--format", "json")
    assert code == 0 and [(r["ticker"], r["tier"], r["status"]) for r in json.loads(out)] == [
        ("CPIYOY-22DEC-T6.5", "historical", "finalized")]
    code, out, _ = run(capsys, "--event", "KXCPIYOY-26SEP", "--status", "open", "--format", "tickers")
    assert out.split() == SEP


# ------------------------------------------------------------------ formats

def test_tickers_format_is_a_valid_tickers_file(exchange, capsys, tmp_path):
    _, out, _ = run(capsys, "--event", "KXCPIYOY-26SEP", "--format", "tickers")
    path = tmp_path / "cpi.txt"
    path.write_text(out)
    assert load_tickers(str(path)) == SEP


def test_py_format_is_a_python_list_literal_ready_for_the_config(exchange, capsys):
    _, out, _ = run(capsys, "--event", "KXCPIYOY-26SEP", "--format", "py")
    assert ast.literal_eval(out) == SEP
    assert out.splitlines()[1] == '    "KXCPIYOY-26SEP-T3.0",'
    _, out, _ = run(capsys, "--category", "Economics", "--format", "py")
    assert ast.literal_eval(out) == ["KXCPIYOY", "KXFED"]             # ready for FOCUS_SERIES


def test_long_titles_are_cut_in_the_table_only(exchange, capsys):
    exchange.events["KXCPIYOY-26SEP"]["title"] = "T" * 200
    _, out, _ = run(capsys, "--series", "KXCPIYOY", "--status", "open")
    assert "T" * 69 + "…" in out and "T" * 71 not in out
    _, out, _ = run(capsys, "--series", "KXCPIYOY", "--status", "open", "--format", "json")
    assert json.loads(out)[0]["title"] == "T" * 200


# ------------------------------------------------------------------ exit codes

def test_nothing_found_exits_1(exchange, capsys):
    code, out, err = run(capsys, "--series", "KXCPIYOY", "--status", "unopened")
    assert (code, out) == (1, "") and "nothing found" in err
    assert run(capsys, "--keyword", "no such thing")[0] == 1
    assert run(capsys, "--event", "KXNOPE-1")[0] == 1


def test_status_all_is_a_usage_error_that_names_the_valid_values(exchange, capsys):
    code, out, err = run(capsys, "--series", "KXCPIYOY", "--status", "all")
    assert (code, out) == (2, "")
    assert "unopened, open, closed, settled" in err and "Omit --status to get every status" in err
    assert exchange.calls == []                                          # refused before any request

    code, _, err = run(capsys, "--series", "KXCPIYOY", "--status", "paused")       # events have no paused filter
    assert code == 2 and "for events" in err
    code, _, err = run(capsys, "--event", "KXCPIYOY-26SEP", "--status", "active")  # a response status, not a filter
    assert code == 2 and "unopened, open, paused, closed, settled" in err


def test_mode_mistakes_are_usage_errors(exchange, capsys):
    assert run(capsys)[0] == 2
    assert run(capsys, "--categories", "--series", "KXCPIYOY")[0] == 2
    assert run(capsys, "--event", "KXCPIYOY-26SEP", "--markets")[0] == 2
    assert run(capsys, "--keyword", "inflation", "--status", "open")[0] == 2
    assert run(capsys, "--format", "xml", "--categories")[0] == 2        # argparse's own error
    assert run(capsys, "--help")[0] == 0
    assert exchange.calls == []


def test_unknown_category_is_a_usage_error_with_a_suggestion(exchange, capsys):
    code, _, err = run(capsys, "--category", "economics")
    assert code == 2 and "Did you mean 'Economics'?" in err


def test_api_failure_exits_3(exchange, capsys):
    exchange.inject(r"^/events$", [FakeResponse(503, {"error": {"message": "down"}})] * 6)
    code, out, err = run(capsys, "--series", "KXCPIYOY")
    assert (code, out) == (3, "") and "API error" in err and "RetriesExhausted" in err
