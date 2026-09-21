"""Offline tests for the market metadata store (kalshi_io.metadata) against the fake exchange."""

import json
from datetime import datetime, timezone

import pandas as pd
import pyarrow.parquet as pq
import pytest

from kalshi_io import catalog, metadata
from kalshi_io.metadata import METADATA_COLUMNS, derive_strikes, market_row, metadata_frame, upsert_market_metadata
from fakes import FakeResponse, make_event, make_market

NOW = datetime(2026, 9, 17, 18, 0, 0, tzinfo=timezone.utc)
BUILT = "2026-09-17T18:00:00Z"

# Shape and values mirror a real GET /markets/{ticker} body of a settled CPI strike
SETTLED = {
    "ticker": "KXCPIYOY-26JUL-T3.5", "event_ticker": "KXCPIYOY-26JUL", "market_type": "binary",
    "title": "Will the rate of CPI inflation be above 3.5% for the year ending in July 2026?",
    "yes_sub_title": "Above 3.5%", "no_sub_title": "Above 3.5%",
    "strike_type": "greater", "floor_strike": 3.5,
    "open_time": "2026-05-12T16:00:00Z", "close_time": "2026-08-12T12:29:00Z",
    "expected_expiration_time": "2026-08-12T14:00:00Z", "expiration_time": "2026-11-11T14:00:00Z",
    "latest_expiration_time": "2026-11-11T14:00:00Z", "settlement_ts": "2026-08-12T13:15:19.078753Z",
    "status": "finalized", "result": "no", "settlement_value_dollars": "0.0000", "expiration_value": "3.4",
    "can_close_early": True, "rules_primary": "If the CPI increases by more than 3.5% ...", "rules_secondary": "",
    "volume_fp": "190017.03", "open_interest_fp": "118937.36", "last_price_dollars": "0.0300",
}


# ------------------------------------------------------------------ rows

def test_market_row_maps_a_settled_threshold_market():
    row = market_row(SETTLED, series="KXCPIYOY", tier="live", built_at=BUILT, mutually_exclusive=False)
    assert list(row) and set(row) == set(METADATA_COLUMNS)
    assert (row["market_ticker"], row["event_ticker"], row["series_ticker"]) == (
        "KXCPIYOY-26JUL-T3.5", "KXCPIYOY-26JUL", "KXCPIYOY")
    assert (row["strike_type"], row["floor_strike"], row["cap_strike"]) == ("greater", 3.5, None)
    # Times are int UTC milliseconds like every ts_ms in the store; microseconds survive to the millisecond
    assert row["open_ts_ms"] == 1778601600000 and row["close_ts_ms"] == 1786537740000
    assert row["settlement_ts_ms"] == 1786540519079
    assert (row["status"], row["result"], row["expiration_value"]) == ("finalized", "no", "3.4")
    # A settlement value of 0.0 is a value, not a gap
    assert row["settlement_value"] == 0.0
    assert (row["volume"], row["open_interest"], row["last_price"]) == (190017.03, 118937.36, 0.03)
    assert row["mutually_exclusive"] is False and row["can_close_early"] is True
    assert row["rules_primary"].startswith("If the CPI") and row["rules_secondary"] is None
    assert (row["tier"], row["built_at"]) == ("live", BUILT)


def test_unsettled_market_has_no_result_and_missing_fields_stay_missing_never_zero():
    live = {k: v for k, v in SETTLED.items() if k not in ("settlement_ts", "settlement_value_dollars")}
    live.update(status="active", result="", expiration_value="")
    row = market_row(live, series="KXCPIYOY", tier="live", built_at=BUILT)
    # The API's "" means "none yet": stored as missing so that `result IS NULL` reads naturally
    assert row["result"] is None and row["expiration_value"] is None
    assert row["settlement_value"] is None and row["settlement_ts_ms"] is None
    assert row["mutually_exclusive"] is None

    # A 2022 market without any strike field, as /historical/markets sends it
    old = {k: v for k, v in SETTLED.items() if k not in ("strike_type", "floor_strike", "expiration_time")}
    row = market_row(old, series="KXFED", tier="historical", built_at=BUILT)
    assert row["strike_type"] is None and row["floor_strike"] is None and row["cap_strike"] is None
    assert row["expiration_ts_ms"] is None and row["latest_expiration_ts_ms"] == 1794405600000


def test_between_and_custom_strikes():
    bucket = market_row({**SETTLED, "strike_type": "between", "floor_strike": 0.6, "cap_strike": 1},
                        series="KXGDPYEAR", tier="live", built_at=BUILT)
    assert (bucket["strike_type"], bucket["floor_strike"], bucket["cap_strike"]) == ("between", 0.6, 1.0)
    custom = {k: v for k, v in SETTLED.items() if k != "floor_strike"}
    row = market_row({**custom, "strike_type": "custom", "custom_strike": {"Hike": "0"}},
                     series="KXFEDDECISION", tier="live", built_at=BUILT, mutually_exclusive=True)
    assert json.loads(row["custom_strike"]) == {"Hike": "0"} and row["floor_strike"] is None
    assert row["mutually_exclusive"] is True


def test_frame_has_fixed_columns_and_types_even_when_a_column_is_all_missing(data_dir):
    rows = [market_row({"ticker": "OLD-1", "status": "finalized"}, series=None, tier="historical", built_at=BUILT)]
    df = metadata_frame(rows)
    assert tuple(df.columns) == METADATA_COLUMNS
    upsert_market_metadata(rows)
    schema = pq.read_schema(metadata.metadata_path())
    assert tuple(schema.names) == METADATA_COLUMNS
    types = {name: str(schema.field(name).type) for name in schema.names}
    assert {types[c] for c in ("floor_strike", "cap_strike", "settlement_value", "volume")} == {"double"}
    assert {types[c] for c in ("open_ts_ms", "close_ts_ms", "settlement_ts_ms")} == {"int64"}
    assert {types[c] for c in ("mutually_exclusive", "can_close_early")} == {"bool"}
    assert {types[c] for c in ("market_ticker", "strike_type", "result", "rules_primary", "custom_strike",
                               "strike_source")} <= {"string", "large_string"}
    assert len(METADATA_COLUMNS) == 33
    assert METADATA_COLUMNS[METADATA_COLUMNS.index("functional_strike") + 1] == "strike_source"


# ------------------------------------------------------------------ strikes the exchange does not send

def _untyped(ticker: str, subtitle: str | None) -> dict:
    """A finalized threshold market as /historical/markets sends the 531: no strike_type, no floor_strike."""
    payload = {k: v for k, v in SETTLED.items() if k not in ("strike_type", "floor_strike")}
    return market_row({**payload, "ticker": ticker, "yes_sub_title": subtitle, "no_sub_title": subtitle},
                      series="KXCPI", tier="historical", built_at=BUILT)


def test_a_threshold_market_without_strike_fields_gets_its_strike_from_the_subtitle():
    subtitles = {"CPI-21AUG-T0.4": ("Above 0.4%", 0.4), "PAYROLLS-22-TN100000": ("Above -100,000", -100000.0),
                 "CPI-22-T0.0": ("Above 0.0%%", 0.0), "U3-22-T2.50": ("Above 2.50%", 2.5),
                 "JOBLESS-22-C370000": ("Above 370,000", 370000.0)}
    rows = [_untyped(ticker, subtitle) for ticker, (subtitle, _) in subtitles.items()]
    assert all(r["strike_type"] is None and r["floor_strike"] is None and r["strike_source"] is None for r in rows)

    df = derive_strikes(metadata_frame(rows)).set_index("market_ticker")

    assert set(df["strike_type"]) == {"greater"} and set(df["strike_source"]) == {"subtitle"}
    assert df["floor_strike"].to_dict() == {ticker: strike for ticker, (_, strike) in subtitles.items()}
    assert str(df["floor_strike"].dtype) == "float64" and df["cap_strike"].isna().all()
    assert df["custom_strike"].isna().all() and df["functional_strike"].isna().all()
    # Idempotent: a second pass changes nothing, and a derived row never turns into an API row
    pd.testing.assert_frame_equal(derive_strikes(df.reset_index()), df.reset_index())


def test_api_strikes_are_never_overwritten_and_other_subtitles_stay_null():
    typed = [
        market_row({**SETTLED, "ticker": "CPICOREYOY-23-T4.1", "yes_sub_title": "Above 4.1%", "floor_strike": 4.099999},
                   series="KXCPICOREYOY", tier="historical", built_at=BUILT),
        market_row({**SETTLED, "ticker": "PCECORE-22NOV-TN0.1", "yes_sub_title": "Above -0.1%", "floor_strike": 0.1},
                   series="KXPCECORE", tier="historical", built_at=BUILT),
        market_row({**SETTLED, "ticker": "GDP-B1", "yes_sub_title": "Above 0.6%", "strike_type": "between",
                    "floor_strike": 0.6, "cap_strike": 1}, series="KXGDPYEAR", tier="live", built_at=BUILT),
        market_row({**{k: v for k, v in SETTLED.items() if k != "floor_strike"}, "ticker": "FEDDECISION-H0",
                    "yes_sub_title": "Above 5%", "strike_type": "custom", "custom_strike": {"Hike": "0"}},
                   series="KXFEDDECISION", tier="live", built_at=BUILT),
    ]
    silent = {"FEDMEET-1": "Before Jan 1, 2025", "FEDDECISION-24JAN-H0": "No change", "RECSSNBER-1": "Starts",
              "CPI-OR": "3.0% or above", "CPI-BARE": "4.5", "CPI-NULL": None, "CPI-BELOW": "Below 3.0%",
              "CPI-RANGE": "Above 3.0% and below 3.5%", "CPI-WORDS": "Above three"}
    df = derive_strikes(metadata_frame(typed + [_untyped(t, sub) for t, sub in silent.items()])).set_index("market_ticker")

    # A value the API sent stays as sent: not the 4.099999 and not the wrong sign
    assert df.loc["CPICOREYOY-23-T4.1", "floor_strike"] == 4.099999 and df.loc["PCECORE-22NOV-TN0.1", "floor_strike"] == 0.1
    assert (df.loc["GDP-B1", "strike_type"], df.loc["GDP-B1", "floor_strike"], df.loc["GDP-B1", "cap_strike"]) == ("between", 0.6, 1.0)
    assert df.loc["FEDDECISION-H0", "strike_type"] == "custom" and pd.isna(df.loc["FEDDECISION-H0", "floor_strike"])
    assert set(df.loc[[r["market_ticker"] for r in typed], "strike_source"]) == {"api"}
    # Nothing but "Above N" is read: no strike, no source
    rest = df.loc[list(silent)]
    assert rest["strike_type"].isna().all() and rest["floor_strike"].isna().all() and rest["strike_source"].isna().all()

    # A floor_strike without a strike_type is an API value too: it is left alone, and nothing is derived around it
    odd = _untyped("CPI-ODD", "Above 0.4%")
    odd["floor_strike"] = 0.5
    out = derive_strikes(metadata_frame([odd])).iloc[0]
    assert out["floor_strike"] == 0.5 and pd.isna(out["strike_type"]) and pd.isna(out["strike_source"])


def test_the_first_upsert_after_the_upgrade_fills_rows_it_did_not_touch(data_dir):
    # The store as 0.3.0 wrote it: 32 columns, a finalized threshold market without strike fields
    old_rows = [_untyped("CPI-21AUG-T0.4", "Above 0.4%"),
                market_row({**SETTLED, "ticker": "KXCPIYOY-26JUL-T3.5"}, series="KXCPIYOY", tier="live", built_at=BUILT)]
    path = metadata.metadata_path()
    path.parent.mkdir(parents=True)
    metadata_frame(old_rows).drop(columns="strike_source").to_parquet(path, engine="pyarrow", compression="zstd", index=False)
    assert len(pq.read_schema(path).names) == 32

    unrelated = market_row({**SETTLED, "ticker": "Z-1"}, series="KXZ", tier="live", built_at=BUILT)
    summary = upsert_market_metadata([unrelated])

    assert (summary["rows"], summary["added"], summary["kept"]) == (3, 1, 2)
    assert tuple(pq.read_schema(path).names) == METADATA_COLUMNS and len(METADATA_COLUMNS) == 33
    stored = metadata.load_market_metadata().set_index("market_ticker")
    old = stored.loc["CPI-21AUG-T0.4"]
    assert (old["strike_type"], old["floor_strike"], old["strike_source"]) == ("greater", 0.4, "subtitle")
    assert stored.loc["KXCPIYOY-26JUL-T3.5", "strike_source"] == "api" and stored.loc["Z-1", "strike_source"] == "api"
    assert old["built_at"] == BUILT and old["result"] == "no"            # nothing else of the old row moved

    upsert_market_metadata([unrelated])
    pd.testing.assert_frame_equal(metadata.load_market_metadata().set_index("market_ticker"), stored)


def test_rederive_rewrites_the_store_offline(fake_api, data_dir, capsys):
    # Without a store: refuse, create nothing
    assert metadata.main(["--rederive"]) == 1
    assert "no metadata store" in capsys.readouterr().err and not data_dir.exists()

    path = metadata.metadata_path()
    path.parent.mkdir(parents=True)
    rows = [_untyped("CPI-21AUG-T0.4", "Above 0.4%"), _untyped("CPI-21AUG-T0.5", "Above 0.5%"),
            _untyped("FEDMEET-1", "Before Jan 1, 2025"),
            market_row({**SETTLED, "ticker": "KXCPIYOY-26JUL-T3.5"}, series="KXCPIYOY", tier="live", built_at=BUILT)]
    metadata_frame(rows).drop(columns="strike_source").to_parquet(path, engine="pyarrow", compression="zstd", index=False)

    assert metadata.main(["--rederive"]) == 0

    out = capsys.readouterr().out
    assert "4 rows, 32 columns before, 33 after" in out
    assert "strike_type null: 3 before, 1 after" in out
    assert "strike_source: api 1, subtitle 2, null 1" in out
    assert "derived from yes_sub_title by series: KXCPI 2" in out
    assert fake_api.calls == []
    stored = metadata.load_market_metadata().set_index("market_ticker")
    assert stored["floor_strike"].to_dict()["CPI-21AUG-T0.5"] == 0.5 and pd.isna(stored.loc["FEDMEET-1", "strike_source"])
    assert [p.name for p in path.parent.iterdir()] == ["markets.parquet"]

    # Again: nothing left to derive, the file keeps its content
    assert metadata.main(["--rederive"]) == 0
    assert "strike_type null: 1 before, 1 after" in capsys.readouterr().out
    pd.testing.assert_frame_equal(metadata.load_market_metadata().set_index("market_ticker"), stored)


def test_the_command_runs_as_a_module_without_a_runpy_warning(tmp_path):
    """The package imports kalshi_io.metadata before `-m` executes it; runpy's warning about that is silenced."""
    import os
    import subprocess
    import sys
    env = {**os.environ, "KALSHI_DATA_DIR": str(tmp_path / "empty_root")}
    done = subprocess.run([sys.executable, "-m", "kalshi_io.metadata", "--rederive"], env=env, text=True,
                          capture_output=True, cwd=str(metadata.config.PROJECT_ROOT), timeout=120)
    assert done.returncode == 1 and "no metadata store" in done.stderr       # offline: no store, nothing to do
    assert "RuntimeWarning" not in done.stderr and not (tmp_path / "empty_root").exists()


# ------------------------------------------------------------------ store

def test_upsert_replaces_by_ticker_keeps_the_rest_and_is_atomic(data_dir):
    a = market_row({**SETTLED, "ticker": "A-1", "status": "active", "result": ""}, series="S", tier="live", built_at=BUILT)
    b = market_row({**SETTLED, "ticker": "B-1"}, series="S", tier="live", built_at=BUILT)
    first = upsert_market_metadata([a, b])
    assert (first["rows"], first["added"], first["updated"], first["kept"]) == (2, 2, 0, 0)
    assert first["path"] == data_dir / "metadata" / "markets.parquet"

    # A-1 settled since; B-1 was not part of this refresh and keeps its row
    later = "2026-09-24T18:00:00Z"
    a2 = market_row({**SETTLED, "ticker": "A-1", "result": "yes", "settlement_value_dollars": "1.0000"},
                    series="S", tier="live", built_at=later)
    second = upsert_market_metadata([a2])
    assert (second["rows"], second["added"], second["updated"], second["kept"]) == (2, 0, 1, 1)
    stored = metadata.load_market_metadata().set_index("market_ticker")
    assert stored.loc["A-1", "result"] == "yes" and stored.loc["A-1", "settlement_value"] == 1.0
    assert stored.loc["A-1", "built_at"] == later and stored.loc["B-1", "built_at"] == BUILT
    assert [p.name for p in (data_dir / "metadata").iterdir()] == ["markets.parquet"]     # no temp file left


def test_carried_forward_fallback_never_overwrites_a_last_known_state(data_dir):
    record = {"event_ticker": "OLD-21", "market_ticker": "OLD-21-T1", "title": "old", "status": "finalized",
              "open_time": "2021-06-30T14:00:00Z", "close_time": "2021-08-11T12:25:00Z"}
    thin = metadata.catalog_row(record, series="KXOLD", built_at=BUILT)
    # No row yet: the thin catalog row is better than nothing
    upsert_market_metadata([], keep_existing=[thin])
    stored = metadata.load_market_metadata().set_index("market_ticker")
    assert stored.loc["OLD-21-T1", "tier"] == "carried_forward" and stored.loc["OLD-21-T1", "close_ts_ms"] == 1628684700000
    assert pd.isna(stored.loc["OLD-21-T1", "result"])

    # A full row exists: the thin one must not replace it
    upsert_market_metadata([market_row({**SETTLED, "ticker": "OLD-21-T1"}, series="KXOLD", tier="historical", built_at=BUILT)])
    upsert_market_metadata([], keep_existing=[thin])
    assert metadata.load_market_metadata().set_index("market_ticker").loc["OLD-21-T1", "result"] == "no"


# ------------------------------------------------------------------ refresh from the API

@pytest.fixture
def exchange(fake_api, catalog_dir):
    fake_api.add_event(make_event("KXTEST-26SEP", "KXTEST"), [
        make_market("KXTEST-26SEP-T1", "KXTEST-26SEP", floor_strike=3),
        make_market("KXTEST-26SEP-T2", "KXTEST-26SEP", floor_strike=3.1)])
    fake_api.add_event(make_event("KXDEC-26OCT", "KXDEC", mutually_exclusive=True), [
        make_market("KXDEC-26OCT-H0", "KXDEC-26OCT", strike_type="custom", custom_strike={"Hike": "0"}, floor_strike=None)])
    fake_api.add_event(make_event("TEST-22DEC", "KXTEST"), [
        make_market("TEST-22DEC-T1", "TEST-22DEC", status="finalized", tier="historical",
                    open_time="2022-11-01T15:00:00Z", close_time="2022-12-13T13:29:00Z")])
    return fake_api


def test_refresh_reads_both_tiers_and_the_event_flag_without_touching_the_catalog(exchange, catalog_dir, data_dir):
    # Two tickers the `tickers=` list form cannot serve: one holds a space, one a comma
    spaced, comma = "GDP-232022 Q4-T0.0", "JOBLESS-22JUL23-C250,000"
    exchange.add_event(make_event("GDP-232022 Q4", "KXGDP"), [
        make_market(spaced, "GDP-232022 Q4", status="finalized", tier="historical", floor_strike=0)])
    exchange.add_event(make_event("JOBLESS-22JUL23", "KXJOBLESS"), [
        make_market(comma, "JOBLESS-22JUL23", status="finalized", tier="historical", floor_strike=250000)])
    tickers = ["KXTEST-26SEP-T1", "KXTEST-26SEP-T2", "KXDEC-26OCT-H0", "TEST-22DEC-T1", "KXNOPE-1", spaced, comma]
    summary = metadata.refresh_market_metadata(tickers, now=NOW)

    assert (summary["requested"], summary["found"], summary["missing"]) == (7, 6, ["KXNOPE-1"])
    assert summary["rows"] == 6 and list(catalog_dir.iterdir()) == []
    stored = metadata.load_market_metadata().set_index("market_ticker")
    assert stored.loc[spaced, "tier"] == "historical" and stored.loc[spaced, "series_ticker"] == "KXGDP"
    assert stored.loc[comma, "floor_strike"] == 250000 and stored.loc[comma, "event_ticker"] == "JOBLESS-22JUL23"
    assert stored.loc["TEST-22DEC-T1", "tier"] == "historical" and stored.loc["KXTEST-26SEP-T1", "tier"] == "live"
    assert stored.loc["TEST-22DEC-T1", "series_ticker"] == "KXTEST"            # the KX series owns the pre-KX event
    assert stored.loc["TEST-22DEC-T1", "result"] == "no" and stored.loc["TEST-22DEC-T1", "settlement_value"] == 0.0
    assert stored.loc["KXTEST-26SEP-T2", "floor_strike"] == 3.1 and pd.isna(stored.loc["KXTEST-26SEP-T2", "result"])
    assert bool(stored.loc["KXDEC-26OCT-H0", "mutually_exclusive"]) is True
    assert bool(stored.loc["KXTEST-26SEP-T1", "mutually_exclusive"]) is False
    assert json.loads(stored.loc["KXDEC-26OCT-H0", "custom_strike"]) == {"Hike": "0"}
    assert set(stored["built_at"]) == {BUILT}
    # One batched lookup per tier, never one request per market that the list form can name
    assert len(exchange.requests_to("/markets")) - len(exchange.requests_to("/markets/")) == 1
    assert len([c for c in exchange.calls if c[0] == "/historical/markets"]) == 1


def test_failed_lookup_writes_nothing(exchange, data_dir):
    exchange.inject(r"^/markets$", [FakeResponse(503, {"error": {"message": "down"}})] * 6)
    with pytest.raises(Exception, match="gave up after 6 attempts"):
        metadata.refresh_market_metadata(["KXTEST-26SEP-T1"])
    assert not (data_dir / "metadata").exists()


# ------------------------------------------------------------------ through the catalog refresh (roll.py)

def test_catalog_refresh_fills_the_store_and_results_arrive_as_markets_settle(exchange, catalog_dir, data_dir, monkeypatch):
    monkeypatch.setattr(catalog, "SERIES_LIST", ["KXTEST"])
    report = catalog.refresh_catalog(now=NOW)
    assert report["metadata"]["written"] and report["metadata"]["rows"] == 3
    assert {"name": "market metadata stored", "ok": True, "detail": "3 rows (0 refreshed, 3 new)"} in report["checks"]
    stored = metadata.load_market_metadata().set_index("market_ticker")
    assert pd.isna(stored.loc["KXTEST-26SEP-T1", "result"]) and stored.loc["KXTEST-26SEP-T1", "status"] == "active"
    # The metadata never leaks into the committed JSON
    series_json = json.loads((catalog_dir / "KXTEST_tickers.json").read_text())
    assert "metadata_rows" not in series_json and "rules_primary" not in series_json["markets"][0]
    requests_first = len(exchange.calls)

    # The cycle settles; the next roll fills in result, settlement value and time
    for ticker, result, value in (("KXTEST-26SEP-T1", "yes", "1.0000"), ("KXTEST-26SEP-T2", "no", "0.0000")):
        exchange.markets[ticker].update(status="finalized", result=result, settlement_value_dollars=value,
                                        expiration_value="3.05", settlement_ts="2026-10-14T13:15:00Z")
    exchange.calls.clear()
    report = catalog.refresh_catalog(now=datetime(2026, 10, 15, tzinfo=timezone.utc))
    assert report["metadata"]["updated"] == 3 and report["metadata"]["added"] == 0
    stored = metadata.load_market_metadata().set_index("market_ticker")
    assert stored.loc["KXTEST-26SEP-T1", "result"] == "yes" and stored.loc["KXTEST-26SEP-T1", "settlement_value"] == 1.0
    assert stored.loc["KXTEST-26SEP-T2", "expiration_value"] == "3.05"
    assert stored.loc["KXTEST-26SEP-T2", "settlement_ts_ms"] == 1791983700000
    assert stored.loc["KXTEST-26SEP-T2", "built_at"] == "2026-10-15T00:00:00Z"
    # The store comes from the payloads the catalog fetched anyway: no request was added for it
    assert len(exchange.calls) <= requests_first


def test_dry_run_and_opt_out_write_no_metadata(exchange, catalog_dir, data_dir, monkeypatch):
    monkeypatch.setattr(catalog, "SERIES_LIST", ["KXTEST"])
    report = catalog.refresh_catalog(dry_run=True)
    assert report["metadata"] == {"rows": 3, "written": False} and not (data_dir / "metadata").exists()
    assert all(c["name"] != "market metadata stored" for c in report["checks"])
    catalog.refresh_catalog(write_metadata=False)
    assert not (data_dir / "metadata").exists() and (catalog_dir / "KXTEST_tickers.json").exists()
