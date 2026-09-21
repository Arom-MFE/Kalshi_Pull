"""Offline tests for the bulk backfill driver (pull_historical/backfill.py) against the fake exchange."""

import json
import os
import signal
import time
from urllib.parse import unquote

import pandas as pd
import pytest

import pull_historical.backfill as backfill
from kalshi_io import candles, catalog, metadata
from fakes import FakeResponse, iso_to_ts, make_candle, make_event, make_market, make_trade

NOW = "2026-09-17T20:00:00Z"

# ticker → (event, status, tier, open, close)
MARKETS = {
    "KXA-26OCT-T1": ("KXA-26OCT", "active", "live", "2026-09-01T00:00:00Z", "2026-10-14T12:29:00Z"),
    "KXA-26OCT-T2": ("KXA-26OCT", "active", "live", "2026-09-01T00:00:00Z", "2026-10-14T12:29:00Z"),
    "KXA-26AUG-T1": ("KXA-26AUG", "finalized", "live", "2026-08-01T00:00:00Z", "2026-09-11T12:29:00Z"),
    "KXA-26AUG-T2": ("KXA-26AUG", "finalized", "live", "2026-08-01T00:00:00Z", "2026-09-11T12:29:00Z"),
    "KXA-26JUL-T1": ("KXA-26JUL", "finalized", "live", "2026-07-01T00:00:00Z", "2026-07-10T00:00:00Z"),   # never traded
    "A-24MAR-T1": ("A-24MAR", "finalized", "historical", "2024-03-01T00:00:00Z", "2024-03-10T00:00:00Z"),
}
TRADABLE = ["KXA-26OCT-T1", "KXA-26OCT-T2"]
SETTLED = [t for t in MARKETS if t not in TRADABLE]
NEVER_TRADED = "KXA-26JUL-T1"

# Two real tickers of the 2026-09-19 catalog, one with a space and one with a comma: ticker → (event, open, close).
# Not part of the `exchange` fixture; a test adds them with _add_separator_markets
SPACED, COMMA = "GDP-232022 Q4-T0.0", "JOBLESS-22JUL23-C250,000"
SEPARATOR_MARKETS = {
    SPACED: ("GDP-232022 Q4", "2022-10-27T14:00:00Z", "2023-01-26T13:25:00Z"),
    COMMA: ("JOBLESS-22JUL23", "2022-07-21T14:00:00Z", "2022-07-28T12:25:00Z"),
}
FRAGMENTS = {"GDP-232022", "Q4-T0.0", "JOBLESS-22JUL23-C250", "000"}


def _seed(api, ticker, open_iso, close_iso):
    """A few candles per interval inside the market's life, and three trades."""
    start, end = int(iso_to_ts(open_iso)), min(int(iso_to_ts(close_iso)), int(iso_to_ts(NOW)))
    api.candles[ticker] = {
        1440: [make_candle(start + 86400 * k) for k in range(1, 4)],
        60: [make_candle(start + 3600 * k) for k in range(1, 6)],
        1: [make_candle(start + 60 * k, traded=(k % 2 == 0), volume="10.00" if k % 2 == 0 else "0.00") for k in range(1, 8)],
    }
    api.trades[ticker] = [make_trade(f"{ticker}-{k}", ticker, pd.Timestamp(start + 600 * k, unit="s", tz="UTC").isoformat())
                          for k in range(1, 4)]
    assert all(c["end_period_ts"] <= end for cs in api.candles[ticker].values() for c in cs)


@pytest.fixture
def exchange(fake_api, catalog_dir, monkeypatch):
    fake_api.now = iso_to_ts(NOW)
    monkeypatch.setattr(time, "time", lambda: fake_api.now)
    monkeypatch.setattr(catalog, "SERIES_LIST", ["KXA"])
    events: dict[str, list] = {}
    for ticker, (event, status, tier, open_iso, close_iso) in MARKETS.items():
        events.setdefault(event, []).append(
            make_market(ticker, event, status=status, tier=tier, open_time=open_iso, close_time=close_iso))
        if ticker != NEVER_TRADED:
            _seed(fake_api, ticker, open_iso, close_iso)
    for event, markets in events.items():
        fake_api.add_event(make_event(event, "KXA"), markets)
    fake_api.cutoff = "2026-07-19T00:00:00Z"
    report = catalog.refresh_catalog(write_metadata=False)          # the committed catalog the driver reads
    assert report["combined"]["total_markets"] == 6
    candles._reset_state()
    fake_api.calls.clear()
    return fake_api


def _add_separator_markets(api) -> None:
    """List the two separator tickers under series KXA (finalized, historical tier) and roll the catalog."""
    for ticker, (event, open_iso, close_iso) in SEPARATOR_MARKETS.items():
        api.add_event(make_event(event, "KXA"), [make_market(
            ticker, event, status="finalized", tier="historical", open_time=open_iso, close_time=close_iso)])
        _seed(api, ticker, open_iso, close_iso)
    report = catalog.refresh_catalog(write_metadata=False)
    assert report["combined"]["total_markets"] == 8 and {SPACED, COMMA} <= set(report["combined"]["tickers"])
    candles._reset_state()
    api.calls.clear()


@pytest.fixture
def waits(monkeypatch):
    slept: list[float] = []
    monkeypatch.setattr(backfill, "_sleep", slept.append)
    return slept


def _ticker_of(call) -> str | None:
    path, params, _ = call
    if path.endswith("/candlesticks"):
        return unquote(path.split("/")[-2])               # the segment is URL-quoted on the wire
    if path.endswith("/trades"):
        return params.get("ticker")
    return None


def _layer_of(call) -> str | None:
    path, params, _ = call
    if path.endswith("/candlesticks"):
        return {1440: "daily", 60: "hourly", 1: "minute"}[params["period_interval"]]
    if path.endswith("/trades"):
        return "trades"
    if path in ("/markets", "/historical/markets") or path.startswith("/events"):
        return "metadata"
    return None


def _stored_rows(data_dir, sub) -> int:
    files = list((data_dir / sub).rglob("*.parquet"))
    return sum(len(pd.read_parquet(f)) for f in files)


# ------------------------------------------------------------------ estimate

def test_estimate_only_prints_the_plan_and_makes_no_request(exchange, data_dir, capsys):
    assert backfill.main(["--estimate-only"]) == 0
    out = capsys.readouterr().out
    assert exchange.calls == [] and not data_dir.exists()
    assert "Backfill estimate for 6 cataloged tickers" in out and "Runtime is a lower bound at 5 requests/s" in out
    for layer in backfill.LAYERS:
        assert f"\n{layer}" in out

    items, uncataloged = backfill.build_items(sorted(MARKETS))
    est = backfill.estimate(items, backfill.Journal.load(), backfill.LAYERS, int(iso_to_ts(NOW)))
    # 3-day minute windows: tradable 16.8 days up to now (6 each); settled from open to close plus two minutes
    assert est["minute"]["requests"] == 6 + 6 + 14 + 14 + 4 + 4 and uncataloged == []
    assert est["daily"]["requests"] == 6 and est["trades"]["requests"] == 12
    assert est["hourly"]["tickers"] == 6 and est["hourly"]["skipped_final"] == 0


def test_bad_layer_is_a_usage_error(exchange):
    with pytest.raises(SystemExit):
        backfill.main(["--layers", "daily,weekly", "--estimate-only"])
    assert exchange.calls == []


# ------------------------------------------------------------------ a full run

def test_layers_run_in_order_and_events_by_priority(exchange, data_dir, waits, capsys):
    assert backfill.main(["--no-audit"]) == 0

    layers = [layer for layer in map(_layer_of, exchange.calls) if layer]
    order = [layer for i, layer in enumerate(layers) if i == 0 or layers[i - 1] != layer]
    assert order == ["metadata", "daily", "hourly", "trades", "minute"]

    daily = [_ticker_of(c) for c in exchange.calls if _layer_of(c) == "daily"]
    first_seen = list(dict.fromkeys(daily))
    # Tradable event first, then settled events by close_time, newest first
    assert first_seen == ["KXA-26OCT-T1", "KXA-26OCT-T2", "KXA-26AUG-T1", "KXA-26AUG-T2", "KXA-26JUL-T1", "A-24MAR-T1"]

    assert _stored_rows(data_dir, "candles/daily") == 15 and _stored_rows(data_dir, "candles/hourly") == 25
    assert _stored_rows(data_dir, "candles/minute") == 35 and _stored_rows(data_dir, "trades") == 15
    stored = metadata.load_market_metadata()
    assert len(stored) == 6 and set(stored["series_ticker"]) == {"KXA"}
    out = capsys.readouterr().out
    assert "Backfill summary" in out and "Result: complete" in out and "429 answers: 0 (0.00%)" in out
    # The historical-tier market never cost a live 404 (the catalog knew its tier)
    assert [c for c in exchange.calls if c[0].startswith("/series/") and "A-24MAR-T1" in c[0]] == []
    assert waits == []


def test_run_ends_with_the_data_quality_checks_unless_no_audit(exchange, data_dir, waits, capsys):
    assert backfill.main([]) == 0
    out = capsys.readouterr().out
    assert out.index("Result: complete") < out.index("=== Data-quality checks")
    # Every file the run wrote passes its own schema; the metadata store feeds the ladder checks
    assert "0 of " in out and " files rejected" in out and "ladder:             0 inverted mids" in out
    assert "no metadata store" not in out
    (csv_path,) = (data_dir / "logs").glob("quality_*.csv")
    assert set(pd.read_csv(csv_path)["check"]) >= {"coverage"}


def test_summary_journal_and_store_counts_are_written(exchange, data_dir, waits):
    assert backfill.main(["--no-audit"]) == 0
    (summary_path,) = (data_dir / "logs").glob("backfill_summary_*.json")
    summary = json.loads(summary_path.read_text())
    assert summary["exit_code"] == 0 and summary["requests"] == len(exchange.calls) and summary["http_429"] == 0
    assert [r["layer"] for r in summary["reports"]] == list(backfill.LAYERS)
    assert summary["store"]["daily"] == {"files": 5, "rows": 15} and summary["store"]["metadata"]["rows"] == 6
    by_layer = {r["layer"]: r for r in summary["reports"]}
    assert by_layer["minute"]["done"] == 6 and by_layer["minute"]["failed"] == {}

    lines = [json.loads(line) for line in (data_dir / "state" / "backfill_journal.jsonl").read_text().splitlines()]
    final = [r for r in lines if r["type"] == "final"]
    # Only pairs of finalized markets are journaled: 4 settled tickers x 4 data layers
    assert {(r["ticker"], r["layer"]) for r in final} == {(t, layer) for t in SETTLED for layer in backfill.LAYERS[1:]}
    assert {r["has_files"] for r in final if r["ticker"] == NEVER_TRADED} == {False}
    assert [r["type"] for r in lines][-1] == "run" and all(r["v"] == 1 for r in lines)
    assert len(list((data_dir / "logs").glob("backfill_2*.log"))) == 1


def test_rerun_costs_nothing_for_settled_markets(exchange, data_dir, waits, capsys):
    assert backfill.main(["--no-audit"]) == 0
    files_before = {p: p.read_bytes() for p in data_dir.rglob("*.parquet") if "metadata" not in p.parts}
    exchange.calls.clear()
    exchange.now += 600

    assert backfill.main(["--no-audit"]) == 0

    touched = {_ticker_of(c) for c in exchange.calls} - {None}
    assert touched == set(TRADABLE)                       # a market that never traded is final too, without any file
    # The metadata of a finalized market with a result is final as well: only the tradable ones are read again
    lookups = [params["tickers"].split(",") for path, params, _ in exchange.calls if path == "/markets"]
    assert sorted(t for chunk in lookups for t in chunk) == TRADABLE
    assert {p: p.read_bytes() for p in files_before} == files_before
    out = capsys.readouterr().out
    assert "final, skipped" in out


def test_metadata_layer_still_skips_final_rows_and_they_are_filled_anyway(exchange, data_dir, waits):
    old = "A-24MAR-T1"                                    # the exchange sends no strike fields for it, only "Above 3.0%"
    for key in ("strike_type", "floor_strike"):
        exchange.markets[old].pop(key)
    assert backfill.main(["--no-audit", "--layers", "metadata"]) == 0
    # The store as 0.3.0 left it: 32 columns, the strike of the old market missing
    path = metadata.metadata_path()
    stored = metadata.load_market_metadata()
    stored.loc[stored["market_ticker"] == old, ["strike_type", "floor_strike"]] = [None, None]
    stored.drop(columns="strike_source").to_parquet(path, engine="pyarrow", compression="zstd", index=False)
    exchange.calls.clear()

    assert backfill.main(["--no-audit", "--layers", "metadata"]) == 0

    # A finalized row with a result is still not read again ...
    lookups = [params["tickers"].split(",") for path_, params, _ in exchange.calls if "tickers" in params]
    assert sorted(t for chunk in lookups for t in chunk) == TRADABLE
    assert not any(old in path_ for path_, _, _ in exchange.calls)
    # ... and carries its strike all the same: the upsert of the tradable rows filled it
    row = metadata.load_market_metadata().set_index("market_ticker").loc[old]
    assert (row["strike_type"], row["floor_strike"], row["strike_source"]) == ("greater", 3.0, "subtitle")
    assert len(pd.read_parquet(path).columns) == 33


def test_files_that_went_missing_are_pulled_again_and_ignore_journal_pulls_everything(exchange, data_dir, waits):
    assert backfill.main(["--no-audit"]) == 0
    victim = data_dir / "candles" / "daily" / "KXA" / "KXA-26AUG-T1.parquet"
    victim.unlink()
    exchange.calls.clear()
    assert backfill.main(["--no-audit", "--layers", "daily"]) == 0
    assert "KXA-26AUG-T1" in {_ticker_of(c) for c in exchange.calls} and victim.exists()
    assert "KXA-26AUG-T2" not in {_ticker_of(c) for c in exchange.calls}

    exchange.calls.clear()
    assert backfill.main(["--no-audit", "--layers", "daily", "--ignore-journal"]) == 0
    assert {_ticker_of(c) for c in exchange.calls} - {None} == set(MARKETS)


def test_ticker_list_with_an_uncataloged_and_an_unknown_ticker(exchange, data_dir, waits, capsys):
    exchange.add_event(make_event("KXNEW-26NOV", "KXNEW"), [
        make_market("KXNEW-26NOV-T1", "KXNEW-26NOV", open_time="2026-09-10T00:00:00Z")])
    _seed(exchange, "KXNEW-26NOV-T1", "2026-09-10T00:00:00Z", "2026-12-01T00:00:00Z")
    code = backfill.main(["--no-audit", "--tickers", "KXA-26AUG-T1", "KXNEW-26NOV-T1", "KXNOPE-9"])
    out = capsys.readouterr().out
    assert code == 0 and "1 tickers are not in the catalog" not in out and "2 tickers are not in the catalog" in out
    assert "Unknown tickers, left out: KXNOPE-9" in out
    assert (data_dir / "candles" / "daily" / "KXNEW" / "KXNEW-26NOV-T1.parquet").exists()
    assert set(metadata.load_market_metadata()["market_ticker"]) == {"KXA-26AUG-T1", "KXNEW-26NOV-T1"}


def test_tickers_with_a_space_or_a_comma_go_through_every_layer(exchange, data_dir, waits, capsys):
    _add_separator_markets(exchange)

    assert backfill.main(["--no-audit"]) == 0

    assert "Result: complete" in capsys.readouterr().out
    (summary_path,) = (data_dir / "logs").glob("backfill_summary_*.json")
    reports = {r["layer"]: r for r in json.loads(summary_path.read_text())["reports"]}
    assert {layer: r["not_attempted"] for layer, r in reports.items()} == dict.fromkeys(backfill.LAYERS, 0)
    assert all(r["failed"] == {} and r["other"] == {} and r["done"] == 8 for r in reports.values())
    assert {SPACED, COMMA} <= set(metadata.load_market_metadata()["market_ticker"])     # looked up one by one

    # Files in the four data layers, named after the whole ticker
    items = {i.ticker: i for i in backfill.build_items([SPACED, COMMA])[0]}
    for ticker in (SPACED, COMMA):
        for layer in backfill.LAYERS[1:]:
            files = backfill.stored_files(layer, items[ticker])
            assert files and all(ticker in f.as_posix() for f in files), (ticker, layer)
    assert (data_dir / "candles" / "daily" / "KXA" / f"{SPACED}.parquet").exists()
    assert (data_dir / "candles" / "daily" / "KXA" / f"{COMMA}.parquet").exists()

    journal = [json.loads(line) for line in (data_dir / "state" / "backfill_journal.jsonl").read_text().splitlines()]
    final = {(r["ticker"], r["layer"]) for r in journal if r["type"] == "final"}
    assert {(t, layer) for t in (SPACED, COMMA) for layer in backfill.LAYERS[1:]} <= final
    assert not list((data_dir / "logs").glob("skip_*"))
    # No fragment was ever asked for, by path or by parameter
    asked = {_ticker_of(c) for c in exchange.calls} - {None}
    assert asked == set(MARKETS) | {SPACED, COMMA} and not asked & FRAGMENTS
    assert any("GDP-232022%20Q4-T0.0" in path for path, _, _ in exchange.calls)
    assert any("JOBLESS-22JUL23-C250%2C000" in path for path, _, _ in exchange.calls)

    # A rerun over the two, each typed as one quoted argument: final, nothing to pull
    exchange.calls.clear()
    assert backfill.main(["--no-audit", "--layers", "daily,hourly,trades,minute", "--tickers", SPACED, COMMA]) == 0
    assert exchange.calls == []


# ------------------------------------------------------------------ failures

def test_failed_ticker_is_retried_listed_and_retry_failed_runs_only_it(exchange, data_dir, waits, capsys):
    bad = FakeResponse(400, {"error": {"code": "bad_request", "message": "bad request", "details": "nope"}})
    # KXA-26AUG-T2: the daily request passes, then the hourly one and its end-of-layer retry are refused
    exchange.inject(r"/markets/KXA-26AUG-T2/candlesticks$", [bad, bad], after=1)

    assert backfill.main(["--no-audit"]) == 1

    out = capsys.readouterr().out
    assert "FAILED hourly KXA-26AUG-T2: KalshiAPIError" in out and "retry with --retry-failed" in out
    (failure_list,) = (data_dir / "logs").glob("backfill_failed_*_hourly.txt")
    assert [ln for ln in failure_list.read_text().splitlines() if not ln.startswith("#")] == ["KXA-26AUG-T2"]
    assert not list((data_dir / "candles" / "hourly").rglob("KXA-26AUG-T2.parquet"))
    # The other layers of that ticker and every other ticker went through
    assert list((data_dir / "candles" / "minute").rglob("KXA-26AUG-T2.parquet"))

    exchange.calls.clear()
    assert backfill.main(["--no-audit", "--retry-failed"]) == 0
    assert {(_layer_of(c), _ticker_of(c)) for c in exchange.calls} == {("hourly", "KXA-26AUG-T2")}
    assert list((data_dir / "candles" / "hourly").rglob("KXA-26AUG-T2.parquet"))
    assert backfill.main(["--retry-failed", "--estimate-only"]) == 0


def test_transient_outage_waits_then_finishes(exchange, data_dir, waits):
    down = FakeResponse(503, {"error": {"message": "down"}})
    # Four tickers in a row run out of their six attempts: the first two events fail completely
    exchange.inject(r"/candlesticks$", [down] * 24)

    assert backfill.main(["--no-audit", "--layers", "daily"]) == 0

    # One wait of a minute (in one second steps, so a stop request is noticed), then everything went through
    assert sum(waits) == 60 and set(waits) == {1}
    assert _stored_rows(data_dir, "candles/daily") == 15
    assert not list((data_dir / "logs").glob("backfill_failed_*"))


def test_api_that_stays_down_exits_2_and_the_next_run_resumes(exchange, data_dir, waits, capsys):
    down = FakeResponse(503, {"error": {"message": "down"}})
    exchange.inject(r"/candlesticks$", [down] * 10_000)

    assert backfill.main(["--no-audit", "--layers", "daily"]) == 2

    out = capsys.readouterr().out
    assert sum(waits) == sum(backfill.OUTAGE_WAITS_S) and "stopped because the API was down" in out
    # It did not walk on through the catalog: only the first two events were ever tried
    assert {_ticker_of(c) for c in exchange.calls if _layer_of(c) == "daily"} == {
        "KXA-26OCT-T1", "KXA-26OCT-T2", "KXA-26AUG-T1", "KXA-26AUG-T2"}
    assert _stored_rows(data_dir, "candles/daily") == 0

    exchange._injected.clear()
    assert backfill.main(["--no-audit", "--layers", "daily"]) == 0
    assert _stored_rows(data_dir, "candles/daily") == 15


def test_interrupt_finishes_the_current_ticker_and_the_next_run_resumes(exchange, data_dir, waits, monkeypatch, capsys):
    real = backfill.run_hourly

    def interrupted(tickers, **kw):
        os.kill(os.getpid(), signal.SIGINT)               # Ctrl+C while the hourly layer is running
        return real(tickers, **kw)

    monkeypatch.setattr(backfill, "run_hourly", interrupted)
    assert backfill.main(["--no-audit"]) == 130
    out = capsys.readouterr().out
    assert "Result: interrupted" in out
    assert _stored_rows(data_dir, "candles/daily") == 15 and _stored_rows(data_dir, "candles/hourly") == 0
    assert not (data_dir / "trades").exists()
    assert signal.getsignal(signal.SIGINT) is signal.default_int_handler

    monkeypatch.setattr(backfill, "run_hourly", real)
    assert backfill.main(["--no-audit"]) == 0
    assert _stored_rows(data_dir, "candles/hourly") == 25 and _stored_rows(data_dir, "candles/minute") == 35


def _summary(data_dir) -> dict:
    """The run's summary JSON; removed after reading, because two runs within one second share a stamp."""
    (path,) = (data_dir / "logs").glob("backfill_summary_*.json")
    summary = json.loads(path.read_text())
    path.unlink()
    return summary


def test_a_ticker_the_puller_returns_no_outcome_for_fails_the_run(exchange, data_dir, waits, monkeypatch, capsys):
    victim, stray = "KXA-26AUG-T1", "KXA-26AUG"
    real = backfill.run_daily

    def lossy(tickers, *, results=None, **kw):
        """A puller that loses one ticker of its list and reports a fragment instead (the 0.3.0 defect)."""
        summary = real([t for t in tickers if t != victim], results=results, **kw)
        if victim in tickers:
            results[stray] = {"status": "unknown", "rows": 0, "error": None, "outage": False}
        return summary

    monkeypatch.setattr(backfill, "run_daily", lossy)
    assert backfill.main(["--no-audit", "--layers", "daily"]) == 1

    out = capsys.readouterr().out
    assert "Result: some tickers failed; retry with --retry-failed" in out and "Result: complete" not in out
    assert f"FAILED daily {victim}: no outcome from the puller (it returned results for: {stray})" in out
    (failure_list,) = (data_dir / "logs").glob("backfill_failed_*_daily.txt")
    assert [ln for ln in failure_list.read_text().splitlines() if not ln.startswith("#")] == [victim]
    summary = _summary(data_dir)
    (daily,) = summary["reports"]
    assert summary["exit_code"] == 1 and summary["complete"] is False
    assert list(daily["failed"]) == [victim] and daily["failed"][victim].startswith("no outcome")
    assert daily["unexpected"] == [stray]                 # named once, although the end-of-layer retry saw it again
    assert daily["done"] == 5 and daily["not_attempted"] == 0 and daily["not_attempted_tickers"] == []
    # A lost ticker is not an outage: nothing waited
    assert waits == []

    monkeypatch.setattr(backfill, "run_daily", real)
    exchange.calls.clear()
    assert backfill.main(["--no-audit", "--retry-failed"]) == 0
    assert {(_layer_of(c), _ticker_of(c)) for c in exchange.calls} == {("daily", victim)}
    assert _summary(data_dir)["complete"] is True


def test_a_ticker_left_untried_without_a_stop_fails_the_run(exchange, data_dir, waits, monkeypatch, capsys):
    victim = "KXA-26AUG-T2"
    real = backfill.run_daily

    def gives_up(tickers, *, results=None, **kw):
        summary = real([t for t in tickers if t != victim], results=results, **kw)
        if victim in tickers:
            results[victim] = {"status": "not_attempted", "rows": 0, "error": None, "outage": False}
        return summary

    monkeypatch.setattr(backfill, "run_daily", gives_up)
    assert backfill.main(["--no-audit", "--layers", "daily"]) == 1

    out = capsys.readouterr().out
    assert f"  NOT TRIED daily {victim}" in out and "Result: complete" not in out
    assert "Result: some tickers were not tried; run the same command again" in out
    summary = _summary(data_dir)
    assert summary["complete"] is False and summary["reports"][0]["not_attempted_tickers"] == [victim]
    assert summary["reports"][0]["failed"] == {} and not list((data_dir / "logs").glob("backfill_failed_*"))


def test_summary_json_names_the_tickers_not_attempted_and_the_unknown_ones(exchange, data_dir, waits, monkeypatch, capsys):
    real = backfill.run_hourly

    def interrupted(tickers, **kw):
        os.kill(os.getpid(), signal.SIGINT)               # Ctrl+C while the hourly layer is running
        return real(tickers, **kw)

    monkeypatch.setattr(backfill, "run_hourly", interrupted)
    assert backfill.main(["--no-audit"]) == 130
    out = capsys.readouterr().out
    assert "NOT TRIED" not in out                         # expected after an interrupt, not listed line by line
    summary = _summary(data_dir)
    assert summary["exit_code"] == 130 and summary["complete"] is False and summary["unknown"] == []
    by_layer = {r["layer"]: r for r in summary["reports"]}
    assert all(len(r["not_attempted_tickers"]) == r["not_attempted"] and r["unexpected"] == []
               for r in by_layer.values())
    assert sorted(by_layer["hourly"]["not_attempted_tickers"]) == sorted(MARKETS)
    assert by_layer["daily"]["not_attempted_tickers"] == []

    monkeypatch.setattr(backfill, "run_hourly", real)
    assert backfill.main(["--no-audit", "--layers", "daily", "--tickers", "KXA-26AUG-T1", "KXNOPE-9"]) == 0
    summary = _summary(data_dir)
    assert summary["unknown"] == ["KXNOPE-9"] and summary["complete"] is True and summary["exit_code"] == 0


def test_retry_failed_reads_a_ticker_with_a_space_whole(exchange, data_dir, waits):
    _add_separator_markets(exchange)
    logs = data_dir / "logs"
    logs.mkdir(parents=True)
    (logs / "backfill_failed_20260917_190000_daily.txt").write_text(
        f"# 1 tickers that failed in the daily layer, 2026-09-17T19:00:00Z\n{SPACED}\n")

    assert backfill.main(["--no-audit", "--retry-failed"]) == 0

    assert {(_layer_of(c), _ticker_of(c)) for c in exchange.calls} == {("daily", SPACED)}
    assert (data_dir / "candles" / "daily" / "KXA" / f"{SPACED}.parquet").exists()
    (daily,) = _summary(data_dir)["reports"]
    assert daily["done"] == 1 and daily["not_attempted_tickers"] == [] and daily["unexpected"] == []


def test_carried_forward_ticker_is_reported_gone_and_never_requested(exchange, data_dir, waits, catalog_dir):
    path = catalog_dir / "KXA_tickers.json"
    data = json.loads(path.read_text())
    data["markets"].append({"event_ticker": "GONE-21", "market_ticker": "GONE-21-T1", "title": "gone", "status": "finalized",
                            "open_time": "2021-06-30T14:00:00Z", "close_time": "2021-08-11T12:25:00Z",
                            "expected_expiration_time": None, "latest_expiration_time": None, "source": "carried_forward"})
    data["tickers"].append("GONE-21-T1")
    path.write_text(json.dumps(data))
    code = backfill.main(["--no-audit", "--layers", "daily,trades", "--tickers", "GONE-21-T1", "KXA-26AUG-T1"])
    assert code == 0 and {_ticker_of(c) for c in exchange.calls} - {None} == {"KXA-26AUG-T1"}


def test_pull_all_freq_is_the_driver_over_the_whole_catalog():
    import pull_historical.pull_all_freq as pull_all_freq
    assert pull_all_freq.main is backfill.main


# ------------------------------------------------------------------ locks

def test_a_second_full_catalog_run_is_refused_with_exit_75(exchange, data_dir, waits, capsys):
    from test_storage import _hold_in_child

    child = _hold_in_child(data_dir / ".locks" / f"{backfill.FULL_RUN_LOCK}.lock", 1.5)
    try:
        assert backfill.main(["--no-audit"]) == backfill.EXIT_LOCKED
        assert "another run holds the lock 'backfill_full'" in capsys.readouterr().err
        assert not any(_layer_of(c) for c in exchange.calls)
        # A ticker list is not the full run and takes no lock unless asked to
        assert backfill.main(["--no-audit", "--layers", "daily", "--tickers", "KXA-26JUL-T1"]) == 0
    finally:
        child.wait(5)
    child = _hold_in_child(data_dir / ".locks" / "focus_history.lock", 1.0)
    try:
        assert backfill.main(["--no-audit", "--layers", "daily", "--tickers", "KXA-26JUL-T1",
                              "--lock-name", "focus_history"]) == backfill.EXIT_LOCKED
    finally:
        child.wait(5)
