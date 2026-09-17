"""Offline tests for the catalog and focus-universe report of pull_audit. The audit never calls the API."""

import json
import os
from datetime import datetime, timedelta, timezone

import pandas as pd

import pull_historical.pull_audit as pull_audit
from kalshi_io import catalog, universe

NOW = datetime(2026, 9, 17, 18, 0, 0, tzinfo=timezone.utc)


def _market(ticker, event, status, close_time):
    return {"event_ticker": event, "market_ticker": ticker, "title": "t", "status": status,
            "open_time": "2026-06-01T00:00:00Z", "close_time": close_time,
            "expected_expiration_time": None, "latest_expiration_time": None, "source": "live"}


def _write_catalog(catalog_dir, built: datetime):
    markets = [
        _market("KXA-26SEP-T1", "KXA-26SEP", "active", "2026-10-14T12:29:00Z"),
        _market("KXA-26SEP-T2", "KXA-26SEP", "inactive", "2026-10-14T12:29:00Z"),
        _market("KXA-26AUG-T1", "KXA-26AUG", "active", "2026-09-11T12:29:00Z"),      # closed since the build
        _market("KXA-26JUL-T1", "KXA-26JUL", "finalized", "2026-08-12T12:29:00Z"),
        _market("KXA-26JUL-T2", "KXA-26JUL", "determined", "2026-08-12T12:29:00Z"),
    ]
    series = {
        "schema_version": 2, "series": "KXA", "built_at": built.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "historical_cutoff": None, "status_counts": catalog.status_counts(markets),
        "events": [{"event_ticker": e, "title": e} for e in sorted({m["event_ticker"] for m in markets})],
        "markets": markets, "tickers": sorted(m["market_ticker"] for m in markets),
    }
    catalog.write_series(series, catalog_dir)
    catalog.build_combined(catalog_dir, now=built)
    return [m["market_ticker"] for m in markets]


def _focus(derived: datetime, event="KXA-26SEP", close="2026-10-14T12:29:00Z"):
    tickers = [f"{event}-T1", f"{event}-T2"]
    return {"schema_version": 1, "derived_at": derived.strftime("%Y-%m-%dT%H:%M:%SZ"), "source": "derived",
            "rule": "nearest 1 open event(s) per series", "series": ["KXA"], "events_per_series": 1,
            "groups": [{"series": "KXA", "event_ticker": event, "title": "", "close_time": close, "tickers": tickers}],
            "tickers": tickers, "statuses": {t: "active" for t in tickers}, "warnings": []}


# ------------------------------------------------------------------ catalog header

def test_header_reports_build_time_age_and_status_counts(catalog_dir):
    tickers = _write_catalog(catalog_dir, NOW - timedelta(days=2))
    lines = pull_audit.catalog_header(tickers + ["KXNOPE-1"], catalog.market_index(), now=NOW)
    assert lines[:2] == ["=== Catalog ===", "Built:    2026-09-15T18:00:00Z (2.0 days ago)"]
    assert lines[2] == "Audited:  6 tickers by catalog status: open=2, paused=1, closed=1, settled=1, not in catalog=1"
    # The catalog is a snapshot: KXA-26AUG-T1 is still 'active' in it, but its close_time has passed
    assert lines[3].strip() == "1 of the open ones are presumed closed since the build (their close_time has passed)"
    assert not any("WARNING" in line for line in lines)


def test_catalog_older_than_a_week_warns_with_the_roll_command(catalog_dir):
    tickers = _write_catalog(catalog_dir, NOW - timedelta(days=40, hours=12))
    lines = pull_audit.catalog_header(tickers, catalog.market_index(), now=NOW)
    assert "(40.5 days ago)" in lines[1]
    assert lines[2].startswith("WARNING:  the catalog is older than 7 days") and "python get_ticker_info/roll.py" in lines[2]


def test_legacy_catalog_age_comes_from_the_file_time_and_says_so(catalog_dir):
    path = catalog_dir / "all_tickers.json"
    path.write_text(json.dumps({"series": [], "tickers": []}))           # v1: no built_at
    stamp = (NOW - timedelta(days=3)).timestamp()
    os.utime(path, (stamp, stamp))
    lines = pull_audit.catalog_header([], {}, now=NOW)
    assert lines[1] == ("Built:    2026-09-14T18:00:00Z (3.0 days ago, from the file modification time: "
                        "a git checkout resets it, treat as a hint)")
    assert lines[2] == "Audited:  0 tickers by catalog status: none"


def test_missing_catalog_is_reported_not_raised(catalog_dir):
    lines = pull_audit.catalog_header(["KXA-1"], {}, now=NOW)
    assert "no catalog found" in lines[1] and "roll.py" in lines[1]
    assert lines[2] == "Audited:  1 tickers by catalog status: not in catalog=1"


# ------------------------------------------------------------------ focus header

def test_focus_header_uses_the_newer_of_the_poller_snapshot_and_the_roll_proposal(catalog_dir, data_dir):
    _write_catalog(catalog_dir, NOW - timedelta(days=2))
    index = catalog.market_index()
    assert pull_audit.focus_header(index, now=NOW)[1].startswith("none recorded yet")

    universe.write_universe(_focus(NOW - timedelta(days=2)), catalog_dir / universe.FOCUS_JSON)
    lines = pull_audit.focus_header(index, now=NOW)
    assert lines[1] == "Source:   roll.py proposal, focus_universe.json"
    assert lines[2] == "Derived:  2026-09-15T18:00:00Z (2.0 days ago); derived: nearest 1 open event(s) per series"
    assert lines[3].strip() == "KXA-26SEP: 2 markets, closes 2026-10-14T12:29:00Z"
    assert lines[4] == "Markets:  2 when derived: open=2"
    assert lines[5].strip() == "by catalog status: open=1, paused=1"
    assert not any("WARNING" in line for line in lines)

    # The poller wrote a snapshot later: it wins
    universe.write_universe(_focus(NOW - timedelta(hours=1)), data_dir / "logs" / universe.LIVE_SNAPSHOT)
    lines = pull_audit.focus_header(index, now=NOW)
    assert lines[1] == "Source:   poller snapshot, focus_universe_live.json" and "(0.0 days ago)" in lines[2]


def test_focus_header_warns_when_a_group_closed_since_the_derivation(catalog_dir):
    _write_catalog(catalog_dir, NOW - timedelta(days=2))
    stale = _focus(NOW - timedelta(days=30), event="KXA-26AUG", close="2026-09-11T12:29:00Z")
    universe.write_universe(stale, catalog_dir / universe.FOCUS_JSON)
    lines = pull_audit.focus_header(catalog.market_index(), now=NOW)
    assert lines[-1].startswith("WARNING:  KXA-26AUG closed since this universe was derived")
    assert "by catalog status: open=1, not in catalog=1" in lines[-2]


# ------------------------------------------------------------------ the whole command

def test_audit_prints_both_headers_adds_status_columns_and_makes_no_request(fake_api, catalog_dir, data_dir, capsys):
    _write_catalog(catalog_dir, datetime.now(timezone.utc) - timedelta(hours=12))
    universe.write_universe(_focus(datetime.now(timezone.utc)), catalog_dir / universe.FOCUS_JSON)
    for series, ticker in (("KXA", "KXA-26JUL-T1"), ("KXZ", "KXZ-26JUL-T9")):      # the second is not cataloged
        path = data_dir / "candles" / "daily" / series / f"{ticker}.parquet"
        path.parent.mkdir(parents=True)
        pd.DataFrame({"ts_ms": [1786161600000, 1786248000000], "volume": [5.0, 7.5]}).to_parquet(path, index=False)

    assert pull_audit.main(["--tickers", "KXA-26JUL-T1", "KXZ-26JUL-T9", "KXA-26SEP-T1"]) == 0

    out = capsys.readouterr().out
    assert out.index("=== Catalog ===") < out.index("=== Focus universe ===") < out.index("Auditing 3 tickers")
    assert "(0.5 days ago)" in out and "WARNING" not in out
    assert "Audited:  3 tickers by catalog status: open=1, settled=1, not in catalog=1" in out
    assert "Missing daily data: 1 tickers" in out and "KXA-26SEP-T1" in out

    (csv_path,) = (data_dir / "logs").glob("audit_*.csv")
    df = pd.read_csv(csv_path)
    assert list(df.columns[-2:]) == ["status", "close_time"]             # appended after the existing columns
    assert list(df.columns[:3]) == ["ticker", "series", "daily_rows"]
    rows = df.set_index("ticker")
    assert rows.loc["KXA-26JUL-T1", "status"] == "finalized" and rows.loc["KXA-26JUL-T1", "close_time"] == "2026-08-12T12:29:00Z"
    assert pd.isna(rows.loc["KXZ-26JUL-T9", "status"]) and pd.isna(rows.loc["KXZ-26JUL-T9", "close_time"])
    assert rows.loc["KXZ-26JUL-T9", "total_volume"] == 12.5

    assert fake_api.calls == []                                           # offline by design


def test_volume_sums_print_at_the_contract_granularity():
    assert pull_audit._fmt_vol(77762.0) == "77762"
    assert pull_audit._fmt_vol(208059.78) == "208059.78"
    assert pull_audit._fmt_vol(0.1 + 0.2) == "0.30"
    assert pull_audit._fmt_vol(496409.50000000006) == "496409.50"      # float noise from summing a column
