"""Offline tests for the market metadata store (kalshi_io.metadata) against the fake exchange."""

import json
from datetime import datetime, timezone

import pandas as pd
import pyarrow.parquet as pq
import pytest

from kalshi_io import catalog, metadata
from kalshi_io.metadata import METADATA_COLUMNS, market_row, metadata_frame, upsert_market_metadata
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
    assert {types[c] for c in ("market_ticker", "strike_type", "result", "rules_primary", "custom_strike")} <= {
        "string", "large_string"}


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
    tickers = ["KXTEST-26SEP-T1", "KXTEST-26SEP-T2", "KXDEC-26OCT-H0", "TEST-22DEC-T1", "KXNOPE-1"]
    summary = metadata.refresh_market_metadata(tickers, now=NOW)

    assert (summary["requested"], summary["found"], summary["missing"]) == (5, 4, ["KXNOPE-1"])
    assert summary["rows"] == 4 and list(catalog_dir.iterdir()) == []
    stored = metadata.load_market_metadata().set_index("market_ticker")
    assert stored.loc["TEST-22DEC-T1", "tier"] == "historical" and stored.loc["KXTEST-26SEP-T1", "tier"] == "live"
    assert stored.loc["TEST-22DEC-T1", "series_ticker"] == "KXTEST"            # the KX series owns the pre-KX event
    assert stored.loc["TEST-22DEC-T1", "result"] == "no" and stored.loc["TEST-22DEC-T1", "settlement_value"] == 0.0
    assert stored.loc["KXTEST-26SEP-T2", "floor_strike"] == 3.1 and pd.isna(stored.loc["KXTEST-26SEP-T2", "result"])
    assert bool(stored.loc["KXDEC-26OCT-H0", "mutually_exclusive"]) is True
    assert bool(stored.loc["KXTEST-26SEP-T1", "mutually_exclusive"]) is False
    assert json.loads(stored.loc["KXDEC-26OCT-H0", "custom_strike"]) == {"Hike": "0"}
    assert set(stored["built_at"]) == {BUILT}
    # One batched lookup per tier, never one request per market
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
