"""Offline tests for the data-quality checks (kalshi_io.quality) on small parquet fixtures.

The fixtures are written with the same frame builders the pullers use
(candles_frame, metadata_frame, upsert_market_metadata), so the schema pass
sees exactly what a real store holds. Every check is a count; the tests pin
the count and the sentence around it.
"""

import math
from datetime import datetime, timezone

import pandas as pd
import pytest

import pull_historical.pull_audit as pull_audit
from kalshi_io import quality
from kalshi_io.candles import CANDLE_COLUMNS, candles_frame
from kalshi_io.metadata import METADATA_COLUMNS, upsert_market_metadata
from kalshi_io.orderbook import BOOK_COLUMNS
from kalshi_io.storage import get_output_path
from kalshi_io.trades import TRADE_COLUMNS

UTC = timezone.utc
DAY_MS = 86_400_000


def bar_ts(day: int, month: int = 7) -> int:
    """ts_ms of the daily bar covering the ET day 2026-{month}-{day}: it ends at 04:00Z the next day."""
    return int(datetime(2026, month, day, 4, tzinfo=UTC).timestamp() * 1000) + DAY_MS


def ts(iso: str) -> int:
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000)


def bar(ts_ms, ticker, event, series, bid=0.49, ask=0.51, close=None, volume=0.0):
    """One stored candle: a quote always, a trade price only when given."""
    row = dict.fromkeys(CANDLE_COLUMNS)
    row.update(ts_ms=ts_ms, close=close, open=close, high=close, low=close, mean=close, volume=volume,
               open_interest=100.0, market_ticker=ticker, event_ticker=event, series_ticker=series)
    for k in ("open", "high", "low", "close"):
        row[f"yes_bid_{k}"], row[f"yes_ask_{k}"] = bid, ask
    return row


def write_candles(layer, series, ticker, rows):
    """Write rows as one candle file of the layer, in the given order (no sort, no dedupe)."""
    interval = {"daily": 1440, "hourly": 60, "minute": 1}[layer]
    path = get_output_path("candles", interval, series, ticker, ts=pd.Timestamp(rows[0]["ts_ms"], unit="ms", tz="UTC"))
    path.parent.mkdir(parents=True, exist_ok=True)
    candles_frame(rows).to_parquet(path, engine="pyarrow", compression="zstd", index=False)
    return path


def write_trades(series, ticker, rows):
    path = get_output_path("trades", None, series, ticker, ts=pd.Timestamp(rows[0]["ts_ms"], unit="ms", tz="UTC"))
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows, columns=TRADE_COLUMNS)
    for col in ("yes_price", "no_price", "count"):
        df[col] = df[col].astype("float64")
    df["ts_ms"] = df["ts_ms"].astype("int64")
    df.to_parquet(path, engine="pyarrow", compression="zstd", index=False)
    return path


def trade(trade_id, ticker, ts_ms, taker_side="yes"):
    return {"trade_id": trade_id, "market_ticker": ticker, "ts_ms": ts_ms, "yes_price": 0.5, "no_price": 0.5,
            "count": 1.0, "taker_side": taker_side}


def meta(ticker, event, series, status="finalized", strike_type=None, floor=None, cap=None, mutex=None,
         open_iso="2026-07-01T00:00:00Z", close_iso="2026-08-01T12:00:00Z", volume=None):
    row = dict.fromkeys(METADATA_COLUMNS)
    row.update(market_ticker=ticker, event_ticker=event, series_ticker=series, status=status, strike_type=strike_type,
               floor_strike=floor, cap_strike=cap, mutually_exclusive=mutex, open_ts_ms=ts(open_iso),
               close_ts_ms=ts(close_iso), volume=volume, tier="live", built_at="2026-09-17T18:00:00Z",
               result="no" if status == "finalized" else None)
    return row


def run(now="2026-07-21T12:00:00Z", write_csv=False):
    return quality.run_checks(now=datetime.fromisoformat(now.replace("Z", "+00:00")), write_csv=write_csv)


# ------------------------------------------------------------------ empty store, schema pass

def test_empty_store_skips_every_check_and_still_writes_the_csv(data_dir):
    report = run(write_csv=True)
    assert report.files == {k: 0 for k in ("daily", "hourly", "minute", "trades", "orderbook", "metadata")}
    assert all(c.skipped for c in report.checks if c.key != "schema")
    text = quality.format_report(report)
    assert "0 of 0 files rejected" in text and "skipped: no parquet files passed the schema pass" in text
    assert report.csv_path == data_dir / "logs" / "quality_20260721.csv"
    assert list(pd.read_csv(report.csv_path).columns) == list(quality.CSV_COLUMNS)


def test_schema_pass_rejects_wrong_files_and_leaves_them_out_of_the_other_checks(data_dir):
    write_candles("daily", "KXA", "KXA-26JUL-T1", [bar(bar_ts(10), "KXA-26JUL-T1", "KXA-26JUL", "KXA")])
    old = data_dir / "candles" / "daily" / "KXA" / "KXA-26JUL-T0.parquet"          # a pre-0.2 file: two columns
    pd.DataFrame({"ts_ms": [bar_ts(10), bar_ts(10)], "volume": [5.0, 7.5]}).to_parquet(old, index=False)
    wrong_type = write_trades("KXA", "KXA-26JUL-T1", [trade("t1", "KXA-26JUL-T1", ts("2026-07-10T12:00:00Z"))])
    df = pd.read_parquet(wrong_type)
    df["count"] = df["count"].astype("int64")
    df.to_parquet(wrong_type, index=False)
    (data_dir / "candles" / "daily" / "KXA" / "KXA-26JUL-T1.parquet.123.tmp").write_bytes(b"")   # a killed writer
    (data_dir / "logs").mkdir()
    (data_dir / "logs" / "skip_daily.txt").write_text("legacy\n")

    report = run()
    assert report.files == {"daily": 2, "hourly": 0, "minute": 0, "trades": 1, "orderbook": 0, "metadata": 0}
    assert report.rejected == {"daily": 1, "hourly": 0, "minute": 0, "trades": 1, "orderbook": 0, "metadata": 0}
    assert report.temp_files == 1 and report.legacy_skip_files == 1
    schema = report.check("schema")
    assert schema.text.startswith("2 of 3 files rejected")
    details = sorted(r["detail"] for r in schema.rows)
    assert details[0].startswith("candles/daily/KXA/KXA-26JUL-T0.parquet: columns: missing ['open'")
    assert details[1] == "trades/KXA/KXA-26JUL-T1/2026-07.parquet: count is int64, expected float64"
    # The duplicate rows of the rejected file are not seen; the good file is
    assert report.check("duplicates").text.startswith("0 duplicate rows on (ts_ms, market_ticker) in 1 candle files")
    assert report.check("taker_side").skipped == "no trade files"
    text = quality.format_report(report)
    assert "rejected by the schema pass 2; stray temp files 1; legacy skip files 1" in text


def test_schema_problem_names_the_first_difference(data_dir):
    good = write_candles("daily", "KXA", "KXA-26JUL-T1", [bar(bar_ts(10), "KXA-26JUL-T1", "KXA-26JUL", "KXA")])
    assert quality.schema_problem(good, quality.SCHEMAS["daily"]) is None
    df = pd.read_parquet(good)
    df[list(reversed(CANDLE_COLUMNS))].to_parquet(good, index=False)
    assert quality.schema_problem(good, quality.SCHEMAS["daily"]) == "columns out of order"
    good.write_bytes(b"not parquet")
    assert quality.schema_problem(good, quality.SCHEMAS["daily"]).startswith("unreadable (")
    assert quality.SCHEMAS["orderbook"] == {c: ("int64" if c in ("ts_ms", "distance_from_top") else "string"
                                                if c in ("market_ticker", "side") else "float64") for c in BOOK_COLUMNS}


# ------------------------------------------------------------------ file-level checks

def test_duplicate_pairs_and_out_of_order_rows_are_counted_per_file(data_dir):
    t1, t2 = "KXA-26JUL-T1", "KXA-26JUL-T2"
    write_candles("daily", "KXA", t1, [bar(bar_ts(d), t1, "KXA-26JUL", "KXA") for d in (10, 11, 11, 11)])
    write_candles("hourly", "KXA", t2, [bar(ts(f"2026-07-10T{h:02d}:00:00Z"), t2, "KXA-26JUL", "KXA") for h in (12, 10, 11, 9)])
    write_trades("KXA", t1, [trade("a", t1, ts("2026-07-10T12:00:00Z")), trade("a", t1, ts("2026-07-10T12:00:00Z")),
                             trade("b", t1, ts("2026-07-10T11:00:00Z"))])
    report = run()
    dupes = report.check("duplicates")
    assert dupes.text == ("2 duplicate rows on (ts_ms, market_ticker) in 2 candle files; "
                          "1 duplicate trade_ids in 1 trade files")
    assert [(r["market"], r["period"], r["count"]) for r in dupes.rows] == [(t1, "daily", 2), (t1, "trades", 1)]
    order = report.check("order")
    # hourly 12, 10, 11, 9: two rows below the row before; trades 12:00, 12:00, 11:00: one
    assert order.text == "3 rows out of order (ts_ms below the row before) in 3 files"
    assert [(r["market"], r["period"], r["count"]) for r in order.rows] == [(t2, "hourly", 2), (t1, "trades", 1)]


def test_volume_and_close_must_agree_and_every_bar_needs_a_quote(data_dir):
    t = "KXA-26JUL-T1"
    rows = [
        bar(bar_ts(10), t, "KXA-26JUL", "KXA"),                                    # no trade: NaN close, volume 0
        bar(bar_ts(11), t, "KXA-26JUL", "KXA", close=0.5, volume=3.0),             # a trade
        bar(bar_ts(12), t, "KXA-26JUL", "KXA", close=0.5, volume=0.0),             # a price without a trade
        bar(bar_ts(13), t, "KXA-26JUL", "KXA", close=None, volume=2.0),            # a trade without a price
        bar(bar_ts(14), t, "KXA-26JUL", "KXA", bid=None, ask=0.5),                 # no quote
    ]
    write_candles("daily", "KXA", t, rows)
    write_candles("minute", "KXA", t, [bar(ts("2026-07-10T12:01:00Z"), t, "KXA-26JUL", "KXA", close=0.4, volume=0.0)])
    check = run().check("volume_close")
    assert check.text == ("2 bars with a close price but volume 0, 1 with volume but no close, 0 without a volume, "
                          "1 without a quote, of 6 bars")
    assert [(r["period"], r["count"], r["detail"]) for r in check.rows] == [
        ("daily", 1, "bars with a close price but volume 0"),
        ("daily", 1, "bars with volume but no close price"),
        ("daily", 1, "bars without a closing quote"),
        ("minute", 1, "bars with a close price but volume 0"),
    ]


def test_taker_side_is_counted_per_month_so_a_removal_shows_up(data_dir):
    t = "KXA-26JUL-T1"
    write_trades("KXA", t, [trade("a", t, ts("2026-07-10T12:00:00Z"), "yes"), trade("b", t, ts("2026-07-11T12:00:00Z"), "no")])
    write_trades("KXA", t, [trade("c", t, ts("2026-08-10T12:00:00Z"), "yes"), trade("d", t, ts("2026-08-11T12:00:00Z"), None),
                            trade("e", t, ts("2026-08-12T12:00:00Z"), "bid")])
    check = run().check("taker_side")
    assert check.text == "5 trades: yes 2, no 1, null 1, other 1; months with a null or other value: 1 of 2 (2026-08)"
    assert [(r["period"], r["count"], r["detail"]) for r in check.rows] == [
        ("2026-07", 2, "yes 1, no 1, null 0, other 0"), ("2026-08", 3, "yes 1, no 0, null 1, other 1")]


# ------------------------------------------------------------------ checks that need the metadata store

def test_checks_that_need_metadata_say_so_when_the_store_is_missing(data_dir):
    write_candles("daily", "KXA", "KXA-26JUL-T1", [bar(bar_ts(10), "KXA-26JUL-T1", "KXA-26JUL", "KXA")])
    report = run()
    for key in ("volume_exchange", "ladder", "mutex", "stale", "missing"):
        assert report.check(key).skipped.startswith("no metadata store (metadata/markets.parquet)")
    assert not report.check("history_start").skipped


def test_daily_volume_is_compared_with_the_exchange_lifetime_volume_of_finalized_markets(data_dir):
    e, s = "KXA-26JUL", "KXA"
    upsert_market_metadata([
        meta("KXA-26JUL-T1", e, s, volume=30.0),                       # 10 + 20 stored: equal
        meta("KXA-26JUL-T2", e, s, volume=50.0),                       # 10 stored: differs
        meta("KXA-26JUL-T3", e, s, volume=1.0),                        # nothing stored: differs
        meta("KXA-26JUL-T4", e, s, status="active", volume=99.0),      # still trading: not compared
        meta("KXA-26JUL-T5", e, s, volume=None),                       # the exchange sent no volume
    ])
    write_candles("daily", s, "KXA-26JUL-T1", [bar(bar_ts(10), "KXA-26JUL-T1", e, s, close=0.5, volume=10.0),
                                               bar(bar_ts(11), "KXA-26JUL-T1", e, s, close=0.5, volume=20.004)])
    write_candles("daily", s, "KXA-26JUL-T2", [bar(bar_ts(10), "KXA-26JUL-T2", e, s, close=0.5, volume=10.0)])
    write_candles("daily", s, "KXA-26JUL-T4", [bar(bar_ts(10), "KXA-26JUL-T4", e, s, close=0.5, volume=1.0)])
    check = run().check("volume_exchange")
    assert check.text == ("2 of 3 finalized markets whose daily volume sum differs from the exchange's lifetime "
                          "volume by more than 0.005 contracts (1 of them have no daily file at all)")
    assert [(r["market"], r["count"], r["detail"]) for r in check.rows] == [
        ("KXA-26JUL-T2", -40.0, "daily volume sum 10.00, exchange lifetime volume 50.00"),
        ("KXA-26JUL-T3", -1.0, "no daily file; exchange lifetime volume 1.00"),       # a store problem, not the exchange's
    ]


def _ladder_store():
    """Two threshold ladders (greater and less), an event of between strikes,
    an event without metadata and a one-strike event; quotes vary by day."""
    a, s = "KXA-26JUL", "KXA"
    rows = [meta(f"{a}-T{k}", a, s, strike_type="greater", floor=float(k)) for k in (1, 2, 3)]
    b = "KXB-26JUL"
    rows += [meta(f"{b}-L{k}", b, "KXB", strike_type="less", cap=float(k)) for k in (1, 2)]
    rows += [meta(f"KXC-26JUL-B{k}", "KXC-26JUL", "KXC", strike_type="between", floor=float(k), cap=k + 1.0) for k in (1, 2)]
    rows += [meta("KXE-26JUL-T1", "KXE-26JUL", "KXE", strike_type="greater_or_equal", floor=1.0)]
    upsert_market_metadata(rows)

    quotes = {   # day → {ticker: (bid, ask)}
        10: {"T1": (0.70, 0.72), "T2": (0.50, 0.52), "T3": (0.30, 0.32)},           # consistent
        11: {"T1": (0.40, 0.60), "T2": (0.45, 0.58), "T3": (0.60, 0.65)},           # T1/T2 inverted; T2/T3 inverted and crossed
        12: {"T1": (0.70, 0.72), "T3": (0.30, 0.32)},                               # T2 has no bar: T1/T3 compared
        13: {"T1": (0.0, 1.0), "T2": (0.50, 0.52), "T3": (0.30, 0.32)},             # T1 empty book: ignored
        14: {"T1": (0.0, 0.30), "T2": (0.20, 0.50), "T3": (0.10, 0.15)},            # T1/T2 inverted through a one-sided book
    }
    for k in (1, 2, 3):
        t = f"{a}-T{k}"
        write_candles("daily", s, t, [bar(bar_ts(d), t, a, s, *q[f"T{k}"]) for d, q in quotes.items() if f"T{k}" in q])
    less = {10: {"L1": (0.20, 0.22), "L2": (0.60, 0.62)}, 11: {"L1": (0.60, 0.62), "L2": (0.20, 0.22)}}
    for k in (1, 2):
        t = f"{b}-L{k}"
        write_candles("daily", "KXB", t, [bar(bar_ts(d), t, b, "KXB", *q[f"L{k}"]) for d, q in less.items()])
    for k in (1, 2):
        t = f"KXC-26JUL-B{k}"
        write_candles("daily", "KXC", t, [bar(bar_ts(10), t, "KXC-26JUL", "KXC", 0.4, 0.6)])
    write_candles("daily", "KXD", "KXD-26JUL-T1", [bar(bar_ts(10), "KXD-26JUL-T1", "KXD-26JUL", "KXD")])
    write_candles("daily", "KXE", "KXE-26JUL-T1", [bar(bar_ts(10), "KXE-26JUL-T1", "KXE-26JUL", "KXE")])


def test_ladder_compares_adjacent_strikes_quoted_the_same_day_in_the_ladder_direction(data_dir):
    _ladder_store()
    check = run().check("ladder")
    # KXA: 2 + 2 + 1 + 1 + 2 pairs over five days, KXB: 1 + 1 over two
    assert check.text == ("4 inverted mids (3 between two-sided books) and 2 strictly crossed pairs in 10 adjacent pairs "
                          "over 7 ladder-days of 2 events; 3 events skipped (no strike metadata)")
    findings = [(r["event"], r["period"], r["count"], r["detail"]) for r in check.rows if r["period"] != "skipped"]
    assert findings == [
        ("KXA-26JUL", "2026-07-11", 2, "greater ladder: 2 of 2 adjacent pairs inverted (2 between two-sided books), 1 strictly crossed"),
        ("KXA-26JUL", "2026-07-14", 1, "greater ladder: 1 of 2 adjacent pairs inverted (0 between two-sided books), 0 strictly crossed"),
        ("KXB-26JUL", "2026-07-11", 1, "less ladder: 1 of 1 adjacent pairs inverted (1 between two-sided books), 1 strictly crossed"),
    ]


def test_events_without_strike_metadata_are_skipped_never_parsed_from_the_ticker(data_dir):
    _ladder_store()
    check = run().check("ladder")
    skipped = {r["event"]: r["detail"] for r in check.rows if r["period"] == "skipped"}
    assert skipped == {
        "KXC-26JUL": "no threshold strikes in the metadata (between, custom or none)",
        "KXD-26JUL": "no metadata row for any market",                  # KXD-26JUL-T1 looks like a strike; not parsed
        "KXE-26JUL": "one threshold strike, nothing to compare",
    }


def test_ladder_covers_events_whose_strikes_came_from_the_subtitle(data_dir):
    """The exchange sends no strike fields for 531 old threshold markets; the store derives them on write."""
    e, s = "CPI-21AUG", "KXCPI"
    rows = []
    for k in (4, 5, 6):
        row = meta(f"{e}-T0.{k}", e, s)                                  # strike_type, floor_strike: none, as sent
        row["yes_sub_title"] = f"Above 0.{k}%"
        rows.append(row)
    upsert_market_metadata(rows)
    quotes = {10: {4: (0.70, 0.72), 5: (0.50, 0.52), 6: (0.30, 0.32)},               # consistent
              11: {4: (0.40, 0.56), 5: (0.50, 0.52), 6: (0.30, 0.32)}}               # T0.4's mid below T0.5's: inverted
    for k in (4, 5, 6):
        t = f"{e}-T0.{k}"
        write_candles("daily", s, t, [bar(bar_ts(d), t, e, s, *q[k]) for d, q in quotes.items()])
    check = run().check("ladder")
    assert check.text == ("1 inverted mids (1 between two-sided books) and 0 strictly crossed pairs in 4 adjacent pairs "
                          "over 2 ladder-days of 1 events; 0 events skipped (no strike metadata)")
    assert [(r["event"], r["period"], r["count"]) for r in check.rows] == [(e, "2026-07-11", 1)]


def test_mutually_exclusive_events_need_every_listed_market_quoted_before_the_sum_is_judged(data_dir):
    e, s = "KXM-26JUL", "KXM"
    upsert_market_metadata([meta(f"{e}-B{k}", e, s, strike_type="between", floor=float(k), cap=k + 1.0, mutex=True) for k in (1, 2, 3)]
                           + [meta(f"{e}-B4", e, s, strike_type="between", floor=4.0, cap=5.0, mutex=True,
                                   open_iso="2026-07-13T15:00:00Z")]                    # listed from day 13 only
                           + [meta("KXN-26JUL-T1", "KXN-26JUL", "KXN", mutex=False)])
    mids = {10: (0.5, 0.3, 0.2), 11: (0.6, 0.4, 0.2), 12: (0.5, 0.3, None), 13: (0.5, 0.3, "empty"), 14: (0.4, 0.3, 0.2),
            15: (0.3, 0.3, 0.2)}
    for k in (1, 2, 3):
        t = f"{e}-B{k}"
        rows = []
        for d, m in mids.items():
            if m[k - 1] is None:
                continue
            bid, ask = (0.0, 1.0) if m[k - 1] == "empty" else (m[k - 1] - 0.01, m[k - 1] + 0.01)
            rows.append(bar(bar_ts(d), t, e, s, bid, ask))
        write_candles("daily", s, t, rows)
    write_candles("daily", s, f"{e}-B4", [bar(bar_ts(d), f"{e}-B4", e, s, 0.09, 0.11) for d in (14, 15)])
    write_candles("daily", "KXN", "KXN-26JUL-T1", [bar(bar_ts(10), "KXN-26JUL-T1", "KXN-26JUL", "KXN", 0.0, 0.1)])
    check = run().check("mutex")
    # day 10 sums to 1.0, day 11 to 1.2, day 14 to 1.0 and day 15 to 0.9 with the fourth market; days 12 and 13 are
    # incomplete (B4 is listed from day 13, the day it opened, and has no bar until day 14)
    assert check.text == ("1 above and 1 below 0.95 to 1.05 among 4 complete event-days (1 and 1 with every book "
                          "two-sided); only a sum above the band speaks against the flag, which does not promise that "
                          "the outcomes are exhaustive; 2 incomplete event-days (a listed market without a quote); "
                          "1 events carry the flag, 0 of them made of threshold strikes and left out")
    assert [(r["period"], r["count"], r["detail"]) for r in check.rows] == [
        ("2026-07-11", 1.2, "mids of all 3 listed markets sum to 1.200, above the band (every book two-sided)"),
        ("2026-07-15", 0.9, "mids of all 4 listed markets sum to 0.900, below the band (every book two-sided)"),
        ("2026-07-12", 1, "incomplete: 1 of 3 listed markets without a quote"),
        ("2026-07-13", 2, "incomplete: 2 of 4 listed markets without a quote"),
    ]


def test_a_sum_on_the_edge_of_the_band_is_inside_whatever_the_order_of_addition(data_dir, monkeypatch):
    """In 0.3.0 the count moved by about ten between runs over the same files: mids sit on a half-cent grid, many
    days sum to exactly 1.05 or 0.95, and the float sum landed on either side with the order DuckDB added in."""
    e, s = "KXFEDDECISION-26JUN", "KXFEDDECISION"
    # The closing quotes of 2026-01-21: mids 0.30, 0.07, 0.60, 0.055 and 0.025 sum to exactly 1.05
    quotes = {"C25": (0.26, 0.34), "C26": (0.02, 0.12), "H0": (0.55, 0.65), "H25": (0.0, 0.11), "H26": (0.0, 0.05)}
    low = {"C25": (0.26, 0.34), "C26": (0.02, 0.12), "H0": (0.45, 0.55), "H25": (0.0, 0.11), "H26": (0.0, 0.05)}   # 0.95
    high = {**quotes, "H0": (0.56, 0.66)}                                                                          # 1.06
    upsert_market_metadata([meta(f"{e}-{k}", e, s, strike_type="custom", mutex=True) for k in quotes])
    for k in quotes:
        t = f"{e}-{k}"
        write_candles("daily", s, t, [bar(bar_ts(10), t, e, s, *quotes[k]), bar(bar_ts(11), t, e, s, *low[k]),
                                      bar(bar_ts(12), t, e, s, *high[k])])

    def with_noise(noise):
        """The summed mids as another order of addition would have left them: off by a few ulps."""
        real = quality._rows

        def rows(con, sql):
            out = real(con, sql)
            for r in out:
                if r.get("mid_sum") is not None:
                    r["mid_sum"] = float(r["mid_sum"]) + noise
            return out
        monkeypatch.setattr(quality, "_rows", rows)
        check = run().check("mutex")
        monkeypatch.setattr(quality, "_rows", real)
        return check

    for noise in (0.0, 4e-16, -4e-16):
        check = with_noise(noise)
        assert check.text.startswith("1 above and 0 below 0.95 to 1.05 among 3 complete event-days (0 and 0 with"), noise
        assert [(r["period"], r["count"]) for r in check.rows] == [("2026-07-12", 1.06)], noise


def test_a_flagged_event_made_of_threshold_strikes_is_reported_and_left_out_of_the_sum(data_dir):
    """KXCPICORE-25DEC carries mutually_exclusive although it is a ladder of `greater` strikes: its mids sum to 2.4."""
    ladder, s = "KXL-26JUL", "KXL"
    rows = [meta(f"{ladder}-T{k}", ladder, s, strike_type="greater", floor=float(k), mutex=True) for k in (1, 2, 3)]
    # A flagged event with one bucket among its thresholds is not a pure ladder: it is judged like any other
    mixed = "KXX-26JUL"
    rows += [meta(f"{mixed}-T1", mixed, "KXX", strike_type="less", cap=1.0, mutex=True),
             meta(f"{mixed}-B1", mixed, "KXX", strike_type="between", floor=1.0, cap=2.0, mutex=True)]
    # ... and so is a flagged event with a single threshold market
    rows += [meta("KXY-26JUL-T1", "KXY-26JUL", "KXY", strike_type="greater", floor=1.0, mutex=True)]
    upsert_market_metadata(rows)
    for k, mid in ((1, 0.9), (2, 0.8), (3, 0.7)):
        t = f"{ladder}-T{k}"
        write_candles("daily", s, t, [bar(bar_ts(10), t, ladder, s, mid - 0.01, mid + 0.01)])
    for t, mid in ((f"{mixed}-T1", 0.4), (f"{mixed}-B1", 0.6)):
        write_candles("daily", "KXX", t, [bar(bar_ts(10), t, mixed, "KXX", mid - 0.01, mid + 0.01)])
    write_candles("daily", "KXY", "KXY-26JUL-T1", [bar(bar_ts(10), "KXY-26JUL-T1", "KXY-26JUL", "KXY", 0.49, 0.51)])

    check = run().check("mutex")

    # The ladder's 2.4 is not counted as a sum above the band; KXX sums to 1.0, KXY's single market to 0.5
    assert check.text == ("0 above and 1 below 0.95 to 1.05 among 2 complete event-days (0 and 1 with every book "
                          "two-sided); only a sum above the band speaks against the flag, which does not promise that "
                          "the outcomes are exhaustive; 0 incomplete event-days (a listed market without a quote); "
                          "3 events carry the flag, 1 of them made of threshold strikes and left out")
    assert [(r["event"], r["period"], r["count"], r["detail"]) for r in check.rows] == [
        (ladder, "flag", 3, "the event is flagged mutually exclusive but its 3 markets are threshold strikes"),
        ("KXY-26JUL", "2026-07-10", 0.5, "mids of all 1 listed markets sum to 0.500, below the band (every book two-sided)"),
    ]
    # The stored flag is not touched
    from kalshi_io.metadata import load_market_metadata
    assert bool(load_market_metadata().set_index("market_ticker").loc[f"{ladder}-T1", "mutually_exclusive"]) is True


def test_stale_tradable_markets_and_listed_strikes_without_a_bar(data_dir):
    e, s = "KXS-26OCT", "KXS"
    upsert_market_metadata([
        meta(f"{e}-T1", e, s, status="active"),                                          # bar 8 hours old
        meta(f"{e}-T2", e, s, status="active"),                                          # newest bar 6 days old
        meta(f"{e}-T3", e, s, status="active"),                                          # no bar at all
        meta(f"{e}-T4", e, s, status="initialized", open_iso="2026-07-21T00:00:00Z"),    # opened 12 hours ago: too young
        meta(f"{e}-T5", e, s, status="finalized"),                                       # settled: not tradable
        meta(f"{e}-T6", e, s, status="inactive"),                                        # paused, newest bar 11 days old
    ])
    write_candles("daily", s, f"{e}-T1", [bar(bar_ts(d), f"{e}-T1", e, s) for d in (10, 15, 20)])
    write_candles("minute", s, f"{e}-T1", [bar(ts("2026-07-21T04:00:00Z"), f"{e}-T1", e, s)])
    write_candles("daily", s, f"{e}-T2", [bar(bar_ts(d), f"{e}-T2", e, s) for d in (10, 14)])
    write_candles("hourly", s, f"{e}-T2", [bar(ts("2026-07-15T06:00:00Z"), f"{e}-T2", e, s)])
    write_candles("daily", s, f"{e}-T5", [bar(bar_ts(d), f"{e}-T5", e, s) for d in (10, 15)])
    write_candles("daily", s, f"{e}-T6", [bar(bar_ts(10), f"{e}-T6", e, s)])
    report = run(now="2026-07-21T12:00:00Z")
    stale = report.check("stale")
    assert stale.text == ("3 of 4 tradable markets (open for more than a day) without a candle newer than a day, "
                          "1 of them without any candle (active 2, inactive 1)")
    assert [(r["market"], r["period"], r["detail"]) for r in stale.rows] == [
        (f"{e}-T2", "active", "newest candle 2026-07-15"),                # an hourly bar: the UTC date of its instant
        (f"{e}-T3", "active", "no candle in any layer"),
        (f"{e}-T6", "inactive", "newest candle 2026-07-10"),              # a daily bar: the day it covers
    ]
    missing = report.check("missing")
    # Listed on every day: T1, T2, T3, T5, T6; T4 from day 20, the ET day it opened in (07-21T00:00Z is inside its bar).
    # Bars: day 10 all but T3; day 14 T2 only; day 15 T1, T5; day 20 T1
    assert missing.text.startswith("4 of 4 event-days have a listed market without a daily bar (13 market-days)")
    assert [(r["period"], r["count"], r["detail"]) for r in missing.rows] == [
        ("2026-07-10", 1, "1 of 5 listed markets without a daily bar"),
        ("2026-07-14", 4, "4 of 5 listed markets without a daily bar"),
        ("2026-07-15", 3, "3 of 5 listed markets without a daily bar"),
        ("2026-07-20", 5, "5 of 6 listed markets without a daily bar"),
    ]


def test_history_start_flags_finer_layers_that_start_after_the_first_daily_bar(data_dir):
    e, s = "KXH-26JUL", "KXH"
    write_candles("daily", s, f"{e}-T1", [bar(bar_ts(10), f"{e}-T1", e, s), bar(bar_ts(11), f"{e}-T1", e, s)])
    write_candles("minute", s, f"{e}-T1", [bar(ts("2026-07-10T12:00:00Z"), f"{e}-T1", e, s)])    # inside the first daily bar
    write_candles("hourly", s, f"{e}-T1", [bar(ts("2026-07-12T05:00:00Z"), f"{e}-T1", e, s)])    # after it: a gap
    write_candles("daily", s, f"{e}-T2", [bar(bar_ts(10), f"{e}-T2", e, s)])                     # no finer layers at all
    write_candles("minute", s, f"{e}-T3", [bar(ts("2026-07-10T12:00:00Z"), f"{e}-T3", e, s)])    # minute only: not judged
    report = run()
    check = report.check("history_start")
    assert check.text == ("of 2 tickers with daily bars, 0 have minute bars that start after the first daily bar and "
                          "1 hourly; 1 have no minute bars and 1 no hourly bars at all")
    assert [(r["market"], r["period"], r["detail"]) for r in check.rows] == [
        (f"{e}-T1", "hourly", "first hourly bar 2026-07-12, first daily bar 2026-07-10")]
    assert [(c["series"], c["layer"], c["events"], c["markets"], c["files"], c["rows"]) for c in report.coverage] == [
        ("KXH", "daily", 1, 2, 2, 3), ("KXH", "hourly", 1, 1, 1, 1), ("KXH", "minute", 1, 2, 2, 2)]
    text = quality.format_report(report)
    assert "KXH             daily           1       2      2           3  2026-07-10  2026-07-11" in text
    assert "KXH             minute          1       2      2           2  2026-07-10  2026-07-10" in text
    assert "   first day    last day" in text


def test_daily_dates_are_the_day_the_bar_covers_on_both_sides_of_a_clock_change(data_dir):
    e, s, t = "KXH-25DEC", "KXH", "KXH-25DEC-T1"
    summer = ts("2025-07-11T04:00:00Z")            # midnight EDT: the bar covers 2025-07-10
    fall_back = ts("2025-11-03T04:00:00Z")         # the night daylight saving ends the exchange keeps 04:00Z: 2025-11-02
    winter = ts("2025-12-10T05:00:00Z")            # midnight EST: the bar covers 2025-12-09
    assert [quality._date(x, daily=True) for x in (summer, fall_back, winter)] == ["2025-07-10", "2025-11-02", "2025-12-09"]
    # Every other row keeps the UTC date of its instant
    assert [quality._date(x) for x in (summer, fall_back, winter)] == ["2025-07-11", "2025-11-03", "2025-12-10"]
    assert quality._date(None, daily=True) == "-"

    upsert_market_metadata([meta(t, e, s, open_iso="2025-07-01T00:00:00Z", close_iso="2025-12-20T00:00:00Z"),
                            meta(f"{e}-T2", e, s, open_iso="2025-07-01T00:00:00Z", close_iso="2025-12-20T00:00:00Z")])
    write_candles("daily", s, t, [bar(x, t, e, s) for x in (summer, fall_back, winter)])
    write_candles("hourly", s, t, [bar(ts("2025-12-10T02:00:00Z"), t, e, s)])       # 21:00 ET on the 9th
    report = run(now="2025-12-11T12:00:00Z", write_csv=True)
    text = quality.format_report(report)
    assert "KXH             daily           1       1      1           3  2025-07-10  2025-12-09" in text
    assert "KXH             hourly          1       1      1           1  2025-12-10  2025-12-10" in text
    csv = pd.read_csv(report.csv_path)
    details = dict(zip(csv.loc[csv["check"] == "coverage", "period"], csv.loc[csv["check"] == "coverage", "detail"]))
    assert details["daily"].endswith("2025-07-10 to 2025-12-09") and details["hourly"].endswith("2025-12-10 to 2025-12-10")
    # One convention in one report: the per-day checks label the same three bars with the same days
    assert [r["period"] for r in report.check("missing").rows] == ["2025-07-10", "2025-11-02", "2025-12-09"]


def test_coverage_takes_the_trades_series_from_the_path_and_the_books_from_the_metadata(data_dir):
    t = "KXA-26JUL-T1"
    write_trades("KXA", t, [trade("a", t, ts("2026-07-10T12:00:00Z"))])
    book = data_dir / "orderbook" / t / "2026-07-10.parquet"
    book.parent.mkdir(parents=True)
    pd.DataFrame([{"ts_ms": ts("2026-07-10T12:00:00Z"), "market_ticker": t, "side": "yes", "price": 0.5, "quantity": 1.0,
                   "cumulative_qty": 1.0, "distance_from_top": 0}], columns=BOOK_COLUMNS).to_parquet(book, index=False)
    cov = run().coverage
    assert [(c["series"], c["layer"], c["events"], c["markets"], c["rows"]) for c in cov] == [
        ("KXA", "trades", None, 1, 1), ("(all)", "orderbook", None, 1, 1)]
    upsert_market_metadata([meta(t, "KXA-26JUL", "KXA")])
    cov = run().coverage
    assert [(c["series"], c["layer"], c["events"], c["markets"], c["rows"]) for c in cov] == [
        ("KXA", "trades", 1, 1, 1), ("KXA", "orderbook", 1, 1, 1)]


def test_null_and_nan_prices_are_both_missing(data_dir):
    t = "KXA-26JUL-T1"
    path = write_candles("daily", "KXA", t, [bar(bar_ts(10), t, "KXA-26JUL", "KXA", close=None, volume=0.0),
                                             bar(bar_ts(11), t, "KXA-26JUL", "KXA", close=None, volume=1.0)])
    # pyarrow writes NaN as null; force a real NaN into the file through DuckDB-visible parquet
    import pyarrow as pa
    import pyarrow.parquet as pq
    table = pq.read_table(path)
    close = pa.array([math.nan, math.nan], type=pa.float64())
    table = table.set_column(table.schema.get_field_index("close"), "close", close)
    pq.write_table(table, path)
    con = quality.duckdb_connect()
    try:
        assert con.execute(f"SELECT count(*) FILTER (WHERE isnan(close)) FROM read_parquet('{path.as_posix()}')").fetchone()[0] == 2
    finally:
        con.close()
    check = run().check("volume_close")
    assert check.text.startswith("0 bars with a close price but volume 0, 1 with volume but no close")


# ------------------------------------------------------------------ the audit command

def test_audit_runs_the_checks_after_the_coverage_unless_told_not_to(catalog_dir, data_dir, capsys):
    t = "KXA-26JUL-T1"
    write_candles("daily", "KXA", t, [bar(bar_ts(10), t, "KXA-26JUL", "KXA", close=0.5, volume=2.0)])

    assert pull_audit.main(["--tickers", t, "--no-checks"]) == 0
    out = capsys.readouterr().out
    assert "=== Per-series summary ===" in out and "Data-quality checks" not in out
    assert list((data_dir / "logs").glob("quality_*.csv")) == []

    assert pull_audit.main(["--tickers", t]) == 0
    out = capsys.readouterr().out
    assert out.index("=== Per-series summary ===") < out.index("=== Data-quality checks")
    assert "0 of 1 files rejected" in out and "history start:" in out
    (csv_path,) = (data_dir / "logs").glob("quality_*.csv")
    assert set(pd.read_csv(csv_path)["check"]) == {"coverage"}          # nothing to report but the coverage rows
