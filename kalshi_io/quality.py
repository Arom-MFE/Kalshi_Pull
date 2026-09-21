"""
kalshi_io/quality.py — Data-quality checks over the parquet store: counts, never repairs.

Read-only and offline: DuckDB over the candle, trade and orderbook files and
the metadata store (kalshi_io/metadata.py). pull_historical/pull_audit.py
runs the checks after its coverage report (--no-checks skips them) and
pull_historical/backfill.py at the end of a run (--no-audit). Every check is
a count with what it means written next to it, because several are natural
for a thin market rather than a defect: an inverted mid between two wide
books, a strike without a bar on a quiet day.

Checks (the key is the `check` column of the CSV):
    schema        every parquet file has the fixed column set, order and
                  dtypes of its kind (CANDLE_COLUMNS, TRADE_COLUMNS,
                  BOOK_COLUMNS, METADATA_COLUMNS). A file that fails is
                  counted and left out of every other check
    duplicates    rows repeated on (ts_ms, market_ticker) inside a candle
                  file, and on trade_id inside a trade file
    order         rows whose ts_ms is below the row before it, per file
    volume_close  bars with a close price but volume 0, bars with volume
                  but no close (the two must agree), bars without a quote
    taker_side    yes / no / null / other per month, so a removal of the
                  field by the exchange shows up as a month with nulls
    volume_exchange
                  finalized markets whose daily volume sum differs from the
                  lifetime volume the exchange reports (metadata `volume`),
                  and how many of them have no daily file at all: that part
                  is a gap in the store, the rest is the exchange's own
                  daily aggregation
    ladder        per event and ET day: adjacent strikes of a threshold
                  ladder (greater* by floor_strike, less* by cap_strike)
                  whose mids are inverted, and pairs that are strictly
                  crossed (one strike's bid above the other's ask). Only
                  strikes with a bar on that day are compared, so both
                  quotes are that day's closing quotes; empty books (bid 0,
                  ask 1) are ignored. Events without strike metadata are
                  counted as skipped, never parsed from the ticker text
    mutex         events flagged mutually_exclusive: days on which every
                  listed market has a quote and the mids sum to more than
                  1.05 or to less than 0.95, counted separately, plus days on
                  which a listed market has no quote. Only a sum above the
                  band speaks against the flag: it does not promise that the
                  outcomes are exhaustive, so a sum below 1 can be right.
                  The band is inclusive and the sum is rounded to six
                  decimals before it is compared, so a day that sums to
                  exactly 1.05 is inside on every run. A
                  flagged event whose markets are all threshold strikes
                  (greater* or less*, two or more) is a ladder and cannot be
                  mutually exclusive: it is left out of the sums and listed
                  with period "flag". The stored flag is never touched
    stale         markets the metadata calls tradable (active, inactive,
                  initialized) whose newest candle in any layer is older
                  than a day, or that have none
    missing       per event and day, markets listed that day (open per the
                  metadata) without a daily bar
    history_start tickers whose minute or hourly history starts after their
                  first daily bar (a gap at the start of the finer layer)
    coverage      per series and layer: events, markets, files, rows, first
                  and last day

Dates: a daily bar ends at midnight Eastern time (04:00Z or 05:00Z) and is
labelled with the day it covers everywhere in the report, the coverage table
included; hourly, minute, trade and book rows keep the UTC date of their
instant.

Output: the summary text (format_report) and DATA_DIR/logs/quality_{date}.csv
with one row per finding: check, series, event, market, period, count, detail.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from kalshi_io import config, metadata
from kalshi_io.candles import CANDLE_COLUMNS, CANDLE_FLOAT_COLUMNS
from kalshi_io.discovery import POLLABLE_BUCKETS, STATUS_BUCKET
from kalshi_io.metadata import METADATA_COLUMNS
from kalshi_io.orderbook import BOOK_COLUMNS
from kalshi_io.storage import duckdb_connect
from kalshi_io.trades import TRADE_COLUMNS

CSV_COLUMNS = ("check", "series", "event", "market", "period", "count", "detail")

CANDLE_LAYERS = ("daily", "hourly", "minute")
LAYER_DIRS = {"daily": "candles/daily", "hourly": "candles/hourly", "minute": "candles/minute",
              "trades": "trades", "orderbook": "orderbook", "metadata": "metadata"}

# A market the metadata calls tradable: its status maps to a pollable bucket
TRADABLE_STATUSES = tuple(s for s, bucket in STATUS_BUCKET.items() if bucket in POLLABLE_BUCKETS)

# A complete mutually exclusive event should have mids that sum to about one
MUTEX_SUM_LOW, MUTEX_SUM_HIGH = 0.95, 1.05
# Quotes have four decimals, so a sum of mids is a multiple of 0.00005 and rounding it to six decimals gives
# back its exact value. Without that a day that sums to exactly 1.05 came out as 1.0500000000000003 or
# 1.0499999999999998 with the order DuckDB happened to add in, and the count changed from run to run
MUTEX_SUM_DECIMALS = 6
# Contract counts have a granularity of 0.01: a smaller difference is float noise
VOLUME_TOLERANCE = 0.005
DAY_MS = 86_400_000

# Legacy per-run skip files of the phase 1 pullers (never written nor read since 0.2.0)
LEGACY_SKIP_FILES = ("skip_daily.txt", "skip_hourly.txt")


def _expected(columns, ints=(), floats=(), bools=()) -> dict[str, str]:
    return {c: "int64" if c in ints else "float64" if c in floats else "bool" if c in bools else "string"
            for c in columns}


_CANDLE_SCHEMA = _expected(CANDLE_COLUMNS, ints=("ts_ms",), floats=CANDLE_FLOAT_COLUMNS)
# Expected dtype per column, per kind of file, in stored order
SCHEMAS: dict[str, dict[str, str]] = {
    "daily": _CANDLE_SCHEMA, "hourly": _CANDLE_SCHEMA, "minute": _CANDLE_SCHEMA,
    "trades": _expected(TRADE_COLUMNS, ints=("ts_ms",), floats=("yes_price", "no_price", "count")),
    "orderbook": _expected(BOOK_COLUMNS, ints=("ts_ms", "distance_from_top"),
                           floats=("price", "quantity", "cumulative_qty")),
    "metadata": _expected(METADATA_COLUMNS, ints=metadata._INT_COLUMNS, floats=metadata._FLOAT_COLUMNS,
                          bools=metadata._BOOL_COLUMNS),
}


# ============================================================
# Results
# ============================================================

@dataclass
class Check:
    """One check: a count with its denominator and a sentence saying what the count means."""
    key: str
    label: str
    text: str = ""                                 # the summary line after the label
    skipped: str = ""                              # why the check did not run, if it did not
    rows: list[dict] = field(default_factory=list)  # CSV detail rows

    def row(self, **fields) -> None:
        self.rows.append({**dict.fromkeys(CSV_COLUMNS), "check": self.key, **fields})


@dataclass
class Report:
    data_dir: Path
    audited_at: str
    files: dict[str, int]                          # kind → parquet files found
    rejected: dict[str, int]                       # kind → files that failed the schema pass
    temp_files: int                                # *.tmp left behind by a killed writer
    legacy_skip_files: int                         # LEGACY_SKIP_FILES present in logs/
    checks: list[Check]
    coverage: list[dict]                           # one row per series and layer
    csv_path: Path | None = None

    def check(self, key: str) -> Check:
        return next(c for c in self.checks if c.key == key)

    def csv_rows(self) -> list[dict]:
        rows = [row for check in self.checks for row in check.rows]
        for cov in self.coverage:
            events = "-" if cov["events"] is None else cov["events"]
            daily = cov["layer"] == "daily"
            rows.append({**dict.fromkeys(CSV_COLUMNS), "check": "coverage", "series": cov["series"],
                         "period": cov["layer"], "count": cov["rows"],
                         "detail": f"events {events}, markets {cov['markets']}, files {cov['files']}, "
                                   f"{_date(cov['first_ts'], daily)} to {_date(cov['last_ts'], daily)}"})
        return rows


def _date(ts_ms, daily: bool = False) -> str:
    """The UTC date of an instant or, for a daily bar, the day the bar covers: ts_ms is the bar's end, midnight
    Eastern time (04:00Z or 05:00Z), so twelve hours earlier is inside that day, as in _DAY below."""
    if ts_ms is None or pd.isna(ts_ms):
        return "-"
    instant = int(ts_ms) - (DAY_MS // 2 if daily else 0)
    return datetime.fromtimestamp(instant / 1000, timezone.utc).strftime("%Y-%m-%d")


# ============================================================
# Files and the schema pass
# ============================================================

def scan_files(root: Path) -> dict[str, list[Path]]:
    """Parquet files per kind. Temp files never end in .parquet (storage.temp_path_for)."""
    return {kind: sorted((root / sub).rglob("*.parquet")) if (root / sub).exists() else []
            for kind, sub in LAYER_DIRS.items()}


def _type_name(t: pa.DataType) -> str:
    if pa.types.is_int64(t):
        return "int64"
    if pa.types.is_float64(t):
        return "float64"
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "string"
    if pa.types.is_boolean(t):
        return "bool"
    return str(t)


def schema_problem(path: Path, expected: dict[str, str]) -> str | None:
    """Why the file does not have the expected columns, order and dtypes, or None if it does. Reads the footer only."""
    try:
        schema = pq.read_schema(path)
    except Exception as e:                          # not a parquet file, or truncated
        return f"unreadable ({type(e).__name__}: {e})"
    names = list(schema.names)
    if names != list(expected):
        missing = [c for c in expected if c not in names]
        extra = [c for c in names if c not in expected]
        if missing or extra:
            return f"columns: missing {missing}, extra {extra}"
        return "columns out of order"
    for f in schema:
        if _type_name(f.type) != expected[f.name]:
            return f"{f.name} is {_type_name(f.type)}, expected {expected[f.name]}"
    return None


def check_schema(found: dict[str, list[Path]], root: Path) -> tuple[Check, dict[str, list[Path]]]:
    """The schema check, and the files that passed it per kind."""
    check = Check("schema", "schema")
    passed: dict[str, list[Path]] = {}
    n_files = n_bad = 0
    for kind, files in found.items():
        passed[kind] = []
        for path in files:
            n_files += 1
            problem = schema_problem(path, SCHEMAS[kind])
            if problem is None:
                passed[kind].append(path)
            else:
                n_bad += 1
                check.row(period=kind, count=1, detail=f"{_short(path, root)}: {problem}")
    check.text = (f"{n_bad:,} of {n_files:,} files rejected (column set, order and dtypes); "
                  f"a rejected file is left out of every other check")
    return check, passed


def _short(path, root: Path) -> str:
    text = str(path)
    prefix = root.as_posix() + "/"
    return text[len(prefix):] if text.startswith(prefix) else text


# ============================================================
# DuckDB views over the files that passed
# ============================================================

def _create_views(con, passed: dict[str, list[Path]]) -> set[str]:
    """One view per kind with files (daily, hourly, minute, trades, orderbook,
    meta) plus `candles`, the three candle layers with a `layer` column.
    Returns the names created."""
    present: set[str] = set()
    for kind, files in passed.items():
        if not files:
            continue
        name = "meta" if kind == "metadata" else kind
        con.read_parquet([p.as_posix() for p in files], filename=True, file_row_number=True).create_view(name)
        present.add(name)
    layers = [layer for layer in CANDLE_LAYERS if layer in present]
    if layers:
        con.execute("CREATE VIEW candles AS " + " UNION ALL ".join(f"SELECT *, '{l}' AS layer FROM {l}" for l in layers))
        present.add("candles")
    return present


def _rows(con, sql: str) -> list[dict]:
    cur = con.execute(sql)
    columns = [d[0] for d in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def _one(con, sql: str) -> dict:
    return _rows(con, sql)[0]


def _skip(check: Check, why: str) -> Check:
    check.skipped = why
    return check


NO_CANDLES = "no candle files"
NO_DAILY = "no daily candle files"
NO_META = "no metadata store (metadata/markets.parquet); a roll or a backfill writes it"
NO_TRADES = "no trade files"

# A bar has a quote when both closing quotes exist; an empty book (no bid, no ask) is not a quote
_QUOTED = ("(yes_bid_close IS NOT NULL AND yes_ask_close IS NOT NULL AND NOT isnan(yes_bid_close) "
           "AND NOT isnan(yes_ask_close) AND NOT (yes_bid_close = 0 AND yes_ask_close = 1))")
_NO_CLOSE = "(close IS NULL OR isnan(close))"
# The ET calendar day a daily bar covers: bars end at midnight ET (04:00Z or 05:00Z)
_DAY = "strftime(make_timestamp(ts_ms * 1000) - INTERVAL 12 HOUR, '%Y-%m-%d')"
_MONTH = "strftime(make_timestamp(ts_ms * 1000), '%Y-%m')"


# ============================================================
# Checks
# ============================================================

def check_duplicates(con, present: set[str], root: Path) -> Check:
    check = Check("duplicates", "duplicates")
    if "candles" not in present and "trades" not in present:
        return _skip(check, NO_CANDLES)
    parts = []
    if "candles" in present:
        dupes = _rows(con, """
            SELECT layer, filename, market_ticker, sum(n - 1) AS extra FROM (
                SELECT layer, filename, market_ticker, ts_ms, count(*) AS n FROM candles
                GROUP BY ALL HAVING count(*) > 1)
            GROUP BY ALL ORDER BY layer, filename""")
        n_files = _one(con, "SELECT count(DISTINCT filename) AS n FROM candles")["n"]
        for d in dupes:
            check.row(market=d["market_ticker"], period=d["layer"], count=int(d["extra"]),
                      detail=f"{_short(d['filename'], root)}: rows repeated on (ts_ms, market_ticker)")
        parts.append(f"{sum(int(d['extra']) for d in dupes):,} duplicate rows on (ts_ms, market_ticker) in "
                     f"{n_files:,} candle files")
    if "trades" in present:
        dupes = _rows(con, """
            SELECT filename, market_ticker, sum(n - 1) AS extra FROM (
                SELECT filename, market_ticker, trade_id, count(*) AS n FROM trades GROUP BY ALL HAVING count(*) > 1)
            GROUP BY ALL ORDER BY filename""")
        n_files = _one(con, "SELECT count(DISTINCT filename) AS n FROM trades")["n"]
        for d in dupes:
            check.row(market=d["market_ticker"], period="trades", count=int(d["extra"]),
                      detail=f"{_short(d['filename'], root)}: rows repeated on trade_id")
        parts.append(f"{sum(int(d['extra']) for d in dupes):,} duplicate trade_ids in {n_files:,} trade files")
    check.text = "; ".join(parts)
    return check


def check_order(con, present: set[str], root: Path) -> Check:
    check = Check("order", "order")
    sources = [(view, layer) for view, layer in (("candles", "layer"), ("trades", "'trades'")) if view in present]
    if not sources:
        return _skip(check, NO_CANDLES)
    total = files = 0
    for view, layer in sources:
        bad = _rows(con, f"""
            SELECT layer, filename, market_ticker, count(*) FILTER (WHERE ts_ms < prev_ts) AS out_of_order FROM (
                SELECT {layer} AS layer, filename, market_ticker, ts_ms,
                       lag(ts_ms) OVER (PARTITION BY filename ORDER BY file_row_number) AS prev_ts
                FROM {view})
            GROUP BY ALL HAVING out_of_order > 0 ORDER BY filename""")
        files += _one(con, f"SELECT count(DISTINCT filename) AS n FROM {view}")["n"]
        for d in bad:
            total += int(d["out_of_order"])
            check.row(market=d["market_ticker"], period=d["layer"], count=int(d["out_of_order"]),
                      detail=f"{_short(d['filename'], root)}: rows whose ts_ms is below the row before")
    check.text = f"{total:,} rows out of order (ts_ms below the row before) in {files:,} files"
    return check


def check_volume_close(con, present: set[str]) -> Check:
    check = Check("volume_close", "volume vs close")
    if "candles" not in present:
        return _skip(check, NO_CANDLES)
    per_layer = _rows(con, f"""
        SELECT layer, count(*) AS bars,
               count(*) FILTER (WHERE volume = 0 AND NOT {_NO_CLOSE}) AS price_no_volume,
               count(*) FILTER (WHERE volume > 0 AND {_NO_CLOSE}) AS volume_no_price,
               count(*) FILTER (WHERE volume IS NULL OR isnan(volume)) AS no_volume,
               count(*) FILTER (WHERE yes_bid_close IS NULL OR isnan(yes_bid_close)
                                   OR yes_ask_close IS NULL OR isnan(yes_ask_close)) AS no_quote
        FROM candles GROUP BY layer ORDER BY layer""")
    totals = {k: sum(int(d[k]) for d in per_layer) for k in ("bars", "price_no_volume", "volume_no_price", "no_volume", "no_quote")}
    for d in per_layer:
        for key, what in (("price_no_volume", "bars with a close price but volume 0"),
                          ("volume_no_price", "bars with volume but no close price"),
                          ("no_volume", "bars without a volume"),
                          ("no_quote", "bars without a closing quote")):
            if d[key]:
                check.row(period=d["layer"], count=int(d[key]), detail=what)
    check.text = (f"{totals['price_no_volume']:,} bars with a close price but volume 0, {totals['volume_no_price']:,} "
                  f"with volume but no close, {totals['no_volume']:,} without a volume, {totals['no_quote']:,} "
                  f"without a quote, of {totals['bars']:,} bars")
    return check


def check_taker_side(con, present: set[str]) -> Check:
    check = Check("taker_side", "taker_side")
    if "trades" not in present:
        return _skip(check, NO_TRADES)
    months = _rows(con, f"""
        SELECT {_MONTH} AS month, count(*) AS trades,
               count(*) FILTER (WHERE taker_side = 'yes') AS yes, count(*) FILTER (WHERE taker_side = 'no') AS no,
               count(*) FILTER (WHERE taker_side IS NULL) AS nulls,
               count(*) FILTER (WHERE taker_side IS NOT NULL AND taker_side NOT IN ('yes', 'no')) AS other
        FROM trades GROUP BY 1 ORDER BY 1""")
    for m in months:
        check.row(period=m["month"], count=int(m["trades"]),
                  detail=f"yes {m['yes']}, no {m['no']}, null {m['nulls']}, other {m['other']}")
    t = {k: sum(int(m[k]) for m in months) for k in ("trades", "yes", "no", "nulls", "other")}
    bad = [m["month"] for m in months if m["nulls"] or m["other"]]
    check.text = (f"{t['trades']:,} trades: yes {t['yes']:,}, no {t['no']:,}, null {t['nulls']:,}, other {t['other']:,}; "
                  f"months with a null or other value: {len(bad)} of {len(months)}"
                  + (f" ({', '.join(bad[:6])}{', ...' if len(bad) > 6 else ''})" if bad else ""))
    return check


def check_volume_exchange(con, present: set[str]) -> Check:
    check = Check("volume_exchange", "volume vs exchange")
    if "daily" not in present:
        return _skip(check, NO_DAILY)
    if "meta" not in present:
        return _skip(check, NO_META)
    rows = _rows(con, f"""
        WITH stored AS (SELECT market_ticker, sum(volume) AS stored FROM daily GROUP BY 1)
        SELECT m.market_ticker, m.series_ticker, m.event_ticker, m.volume AS exchange, coalesce(s.stored, 0) AS stored,
               s.market_ticker IS NULL AS no_file
        FROM meta m LEFT JOIN stored s USING (market_ticker)
        WHERE m.status = 'finalized' AND m.volume IS NOT NULL AND NOT isnan(m.volume)
        ORDER BY m.market_ticker""")
    differ = [r for r in rows if abs(float(r["stored"]) - float(r["exchange"])) > VOLUME_TOLERANCE]
    # A market without a daily file is a gap in the store, not a difference in the exchange's aggregation
    no_file = [r for r in differ if r["no_file"]]
    for r in differ:
        detail = (f"no daily file; exchange lifetime volume {float(r['exchange']):.2f}" if r["no_file"] else
                  f"daily volume sum {float(r['stored']):.2f}, exchange lifetime volume {float(r['exchange']):.2f}")
        check.row(series=r["series_ticker"], event=r["event_ticker"], market=r["market_ticker"],
                  count=round(float(r["stored"]) - float(r["exchange"]), 2), detail=detail)
    check.text = (f"{len(differ):,} of {len(rows):,} finalized markets whose daily volume sum differs from the "
                  f"exchange's lifetime volume by more than {VOLUME_TOLERANCE:g} contracts "
                  f"({len(no_file):,} of them have no daily file at all)")
    return check


def check_ladder(con, present: set[str]) -> Check:
    check = Check("ladder", "ladder")
    if "daily" not in present:
        return _skip(check, NO_DAILY)
    if "meta" not in present:
        return _skip(check, NO_META)
    # A threshold ladder: greater* markets ordered by floor_strike, less* markets by cap_strike, per event
    con.execute("""
        CREATE OR REPLACE TEMP VIEW ladder_markets AS
        SELECT * FROM (
            SELECT market_ticker, event_ticker, series_ticker,
                   CASE WHEN strike_type LIKE 'greater%' THEN 'greater' WHEN strike_type LIKE 'less%' THEN 'less' END AS dir,
                   CASE WHEN strike_type LIKE 'greater%' THEN floor_strike WHEN strike_type LIKE 'less%' THEN cap_strike END AS strike
            FROM meta)
        WHERE dir IS NOT NULL AND strike IS NOT NULL AND NOT isnan(strike)""")
    # Adjacent strikes of one ladder on one day, both with a quoted bar that day
    pairs = _rows(con, f"""
        WITH q AS (
            SELECT d.ts_ms, m.event_ticker, m.series_ticker, m.dir, m.strike, d.market_ticker,
                   d.yes_bid_close AS bid, d.yes_ask_close AS ask, (d.yes_bid_close + d.yes_ask_close) / 2 AS mid
            FROM daily d JOIN ladder_markets m USING (market_ticker)
            WHERE {_QUOTED}
        ), p AS (
            SELECT *, lag(strike) OVER w AS strike0, lag(mid) OVER w AS mid0, lag(bid) OVER w AS bid0, lag(ask) OVER w AS ask0
            FROM q WINDOW w AS (PARTITION BY event_ticker, dir, ts_ms ORDER BY strike, market_ticker)
        ), flagged AS (
            SELECT *,
                   ((dir = 'greater' AND mid > mid0) OR (dir = 'less' AND mid < mid0)) AS inverted,
                   (bid > 0 AND ask < 1 AND bid0 > 0 AND ask0 < 1) AS two_sided,
                   ((dir = 'greater' AND bid > ask0) OR (dir = 'less' AND bid0 > ask)) AS crossed
            FROM p WHERE strike0 IS NOT NULL AND strike0 < strike
        )
        SELECT event_ticker, any_value(series_ticker) AS series_ticker, {_DAY} AS day, dir, count(*) AS pairs,
               count(*) FILTER (WHERE inverted) AS inverted,
               count(*) FILTER (WHERE inverted AND two_sided) AS inverted_two_sided,
               count(*) FILTER (WHERE crossed) AS crossed
        FROM flagged GROUP BY event_ticker, ts_ms, dir ORDER BY event_ticker, ts_ms, dir""")
    skipped = _rows(con, """
        WITH ev AS (
            SELECT coalesce(m.event_ticker, d.event_ticker) AS event_ticker, any_value(d.series_ticker) AS series_ticker,
                   count(m.market_ticker) AS with_metadata
            FROM daily d LEFT JOIN meta m USING (market_ticker) GROUP BY 1
        ), ladders AS (SELECT event_ticker, count(*) AS strikes FROM ladder_markets GROUP BY 1)
        SELECT ev.event_ticker, ev.series_ticker, ev.with_metadata, coalesce(l.strikes, 0) AS strikes
        FROM ev LEFT JOIN ladders l USING (event_ticker) WHERE coalesce(l.strikes, 0) < 2 ORDER BY 1""")
    for s in skipped:
        reason = ("no metadata row for any market" if not s["with_metadata"]
                  else "one threshold strike, nothing to compare" if s["strikes"] == 1
                  else "no threshold strikes in the metadata (between, custom or none)")
        check.row(series=s["series_ticker"], event=s["event_ticker"], period="skipped", count=0, detail=reason)
    for p in pairs:
        if p["inverted"] or p["crossed"]:
            check.row(series=p["series_ticker"], event=p["event_ticker"], period=p["day"], count=int(p["inverted"]),
                      detail=f"{p['dir']} ladder: {p['inverted']} of {p['pairs']} adjacent pairs inverted "
                             f"({p['inverted_two_sided']} between two-sided books), {p['crossed']} strictly crossed")
    n_events = len({p["event_ticker"] for p in pairs})
    t = {k: sum(int(p[k]) for p in pairs) for k in ("pairs", "inverted", "inverted_two_sided", "crossed")}
    check.text = (f"{t['inverted']:,} inverted mids ({t['inverted_two_sided']:,} between two-sided books) and "
                  f"{t['crossed']:,} strictly crossed pairs in {t['pairs']:,} adjacent pairs over {len(pairs):,} "
                  f"ladder-days of {n_events:,} events; {len(skipped):,} events skipped (no strike metadata)")
    return check


def _listed_sql(events_sql: str) -> str:
    """Markets listed on each event-day: the event's markets whose open and
    close (metadata) overlap the bar's period. Missing times count as always listed."""
    return f"""
        days AS (SELECT DISTINCT coalesce(m.event_ticker, d.event_ticker) AS event_ticker, d.ts_ms
                 FROM daily d LEFT JOIN meta m USING (market_ticker) WHERE {events_sql}),
        listed AS (
            SELECT days.event_ticker, days.ts_ms, m.market_ticker, m.series_ticker
            FROM days JOIN meta m USING (event_ticker)
            WHERE (m.open_ts_ms IS NULL OR m.open_ts_ms < days.ts_ms)
              AND (m.close_ts_ms IS NULL OR m.close_ts_ms > days.ts_ms - {DAY_MS})),
        joined AS (
            SELECT l.event_ticker, l.ts_ms, l.market_ticker, l.series_ticker, d.ts_ms IS NOT NULL AS has_bar,
                   coalesce(d.yes_bid_close IS NOT NULL AND d.yes_ask_close IS NOT NULL AND NOT isnan(d.yes_bid_close)
                            AND NOT isnan(d.yes_ask_close) AND NOT (d.yes_bid_close = 0 AND d.yes_ask_close = 1), false) AS quoted,
                   coalesce(d.yes_bid_close > 0 AND d.yes_ask_close < 1, false) AS two_sided,
                   (d.yes_bid_close + d.yes_ask_close) / 2 AS mid
            FROM listed l LEFT JOIN daily d ON d.market_ticker = l.market_ticker AND d.ts_ms = l.ts_ms)"""


def check_mutex(con, present: set[str]) -> Check:
    check = Check("mutex", "mutex sum")
    if "daily" not in present:
        return _skip(check, NO_DAILY)
    if "meta" not in present:
        return _skip(check, NO_META)
    # A flagged event whose markets are all threshold strikes is a ladder: several of its markets resolve yes
    # together, so it cannot be mutually exclusive whatever the flag says. Left out of the sums, listed on its own
    flagged = _rows(con, """
        SELECT event_ticker, any_value(series_ticker) AS series_ticker, count(*) AS markets,
               count(*) FILTER (WHERE strike_type LIKE 'greater%' OR strike_type LIKE 'less%') AS thresholds
        FROM meta GROUP BY 1 HAVING bool_or(mutually_exclusive) ORDER BY 1""")
    ladders = [f for f in flagged if f["markets"] >= 2 and f["thresholds"] == f["markets"]]
    names = ", ".join("'" + f["event_ticker"].replace("'", "''") + "'" for f in ladders)
    left_out = f"AND event_ticker NOT IN ({names})" if ladders else ""
    days = _rows(con, f"""
        WITH mx AS (SELECT event_ticker FROM meta GROUP BY 1 HAVING bool_or(mutually_exclusive) {left_out}),
        {_listed_sql("coalesce(m.event_ticker, d.event_ticker) IN (SELECT event_ticker FROM mx)")}
        SELECT event_ticker, any_value(series_ticker) AS series_ticker, {_DAY} AS day, count(*) AS listed,
               count(*) FILTER (WHERE quoted) AS quoted, sum(mid) FILTER (WHERE quoted) AS mid_sum,
               count(*) FILTER (WHERE quoted AND two_sided) AS two_sided
        FROM joined GROUP BY event_ticker, ts_ms ORDER BY event_ticker, ts_ms""")
    for d in days:                                          # the band is inclusive: a sum on its edge is inside
        if d["mid_sum"] is not None:
            d["mid_sum"] = round(float(d["mid_sum"]), MUTEX_SUM_DECIMALS)
    complete = [d for d in days if d["quoted"] == d["listed"]]
    incomplete = [d for d in days if d["quoted"] != d["listed"]]
    above = [d for d in complete if float(d["mid_sum"]) > MUTEX_SUM_HIGH]
    below = [d for d in complete if float(d["mid_sum"]) < MUTEX_SUM_LOW]
    # A one-sided book (no bid, or no ask) puts its mid halfway to the empty side, which inflates a sum
    above_two_sided = [d for d in above if d["two_sided"] == d["listed"]]
    below_two_sided = [d for d in below if d["two_sided"] == d["listed"]]
    for f in ladders:
        check.row(series=f["series_ticker"], event=f["event_ticker"], period="flag", count=int(f["markets"]),
                  detail=f"the event is flagged mutually exclusive but its {f['markets']} markets are threshold strikes")
    for d in sorted(above + below, key=lambda d: (d["event_ticker"], d["day"])):
        books = "every book two-sided" if d["two_sided"] == d["listed"] else f"{d['listed'] - d['two_sided']} one-sided books"
        side = "above" if float(d["mid_sum"]) > MUTEX_SUM_HIGH else "below"
        check.row(series=d["series_ticker"], event=d["event_ticker"], period=d["day"], count=round(float(d["mid_sum"]), 4),
                  detail=f"mids of all {d['listed']} listed markets sum to {float(d['mid_sum']):.3f}, {side} the band ({books})")
    for d in incomplete:
        check.row(series=d["series_ticker"], event=d["event_ticker"], period=d["day"], count=int(d["listed"] - d["quoted"]),
                  detail=f"incomplete: {d['listed'] - d['quoted']} of {d['listed']} listed markets without a quote")
    check.text = (f"{len(above):,} above and {len(below):,} below {MUTEX_SUM_LOW} to {MUTEX_SUM_HIGH} among "
                  f"{len(complete):,} complete event-days ({len(above_two_sided):,} and {len(below_two_sided):,} with "
                  f"every book two-sided); only a sum above the band speaks against the flag, which does not promise "
                  f"that the outcomes are exhaustive; {len(incomplete):,} incomplete event-days (a listed market "
                  f"without a quote); {len(flagged):,} events carry the flag, {len(ladders):,} of them made of "
                  f"threshold strikes and left out")
    return check


def check_stale(con, present: set[str], now_ms: int) -> Check:
    check = Check("stale", "stale tradable")
    if "meta" not in present:
        return _skip(check, NO_META)
    if "candles" not in present:
        return _skip(check, NO_CANDLES)
    statuses = ", ".join(f"'{s}'" for s in TRADABLE_STATUSES)
    rows = _rows(con, f"""
        WITH last AS (SELECT market_ticker, max(ts_ms) AS last_ts,
                             max(ts_ms) FILTER (WHERE layer <> 'daily') AS last_finer FROM candles GROUP BY 1)
        SELECT m.market_ticker, m.series_ticker, m.event_ticker, m.status, l.last_ts, l.last_finer
        FROM meta m LEFT JOIN last l USING (market_ticker)
        WHERE m.status IN ({statuses}) AND (m.open_ts_ms IS NULL OR m.open_ts_ms <= {now_ms - DAY_MS})
        ORDER BY m.market_ticker""")
    stale = [r for r in rows if r["last_ts"] is None or int(r["last_ts"]) < now_ms - DAY_MS]
    none = [r for r in stale if r["last_ts"] is None]
    for r in stale:
        # The newest candle is a daily bar when no finer layer reaches its ts_ms: label it with the day it covers
        daily = r["last_finer"] is None or int(r["last_finer"]) < int(r["last_ts"] or 0)
        age = "no candle in any layer" if r["last_ts"] is None else f"newest candle {_date(r['last_ts'], daily)}"
        check.row(series=r["series_ticker"], event=r["event_ticker"], market=r["market_ticker"], period=r["status"],
                  count=1, detail=age)
    by_status = [f"{s} {n:,}" for s in TRADABLE_STATUSES if (n := sum(1 for r in stale if r["status"] == s))]
    check.text = (f"{len(stale):,} of {len(rows):,} tradable markets (open for more than a day) without a candle "
                  f"newer than a day, {len(none):,} of them without any candle"
                  + (f" ({', '.join(by_status)})" if by_status else ""))
    return check


def check_missing(con, present: set[str]) -> Check:
    check = Check("missing", "missing strikes")
    if "daily" not in present:
        return _skip(check, NO_DAILY)
    if "meta" not in present:
        return _skip(check, NO_META)
    days = _rows(con, f"""
        WITH {_listed_sql("m.event_ticker IS NOT NULL")}
        SELECT event_ticker, any_value(series_ticker) AS series_ticker, {_DAY} AS day, count(*) AS listed,
               count(*) FILTER (WHERE NOT has_bar) AS missing
        FROM joined GROUP BY event_ticker, ts_ms ORDER BY event_ticker, ts_ms""")
    with_missing = [d for d in days if d["missing"]]
    for d in with_missing:
        check.row(series=d["series_ticker"], event=d["event_ticker"], period=d["day"], count=int(d["missing"]),
                  detail=f"{d['missing']} of {d['listed']} listed markets without a daily bar")
    check.text = (f"{len(with_missing):,} of {len(days):,} event-days have a listed market without a daily bar "
                  f"({sum(int(d['missing']) for d in with_missing):,} market-days); a bar exists only when "
                  f"something happened, so a quiet strike has none")
    return check


def check_history_start(con, present: set[str]) -> Check:
    check = Check("history_start", "history start")
    if "daily" not in present:
        return _skip(check, NO_DAILY)
    rows = _rows(con, """
        SELECT market_ticker, any_value(series_ticker) AS series_ticker, any_value(event_ticker) AS event_ticker,
               min(ts_ms) FILTER (WHERE layer = 'daily') AS first_daily,
               min(ts_ms) FILTER (WHERE layer = 'hourly') AS first_hourly,
               min(ts_ms) FILTER (WHERE layer = 'minute') AS first_minute
        FROM candles GROUP BY 1 HAVING first_daily IS NOT NULL ORDER BY 1""")
    late = {"hourly": [], "minute": []}
    absent = {"hourly": 0, "minute": 0}
    for r in rows:
        for layer in late:
            first = r[f"first_{layer}"]
            if first is None:
                absent[layer] += 1
            elif int(first) > int(r["first_daily"]):
                late[layer].append(r)
                check.row(series=r["series_ticker"], event=r["event_ticker"], market=r["market_ticker"], period=layer,
                          count=1, detail=f"first {layer} bar {_date(first)}, "
                                          f"first daily bar {_date(r['first_daily'], daily=True)}")
    check.text = (f"of {len(rows):,} tickers with daily bars, {len(late['minute']):,} have minute bars that start "
                  f"after the first daily bar and {len(late['hourly']):,} hourly; {absent['minute']:,} have no minute "
                  f"bars and {absent['hourly']:,} no hourly bars at all")
    return check


def coverage(con, present: set[str]) -> list[dict]:
    """One row per series and layer: events, markets, files, rows, first and last ts_ms."""
    out: list[dict] = []
    for layer in CANDLE_LAYERS:
        if layer in present:
            out += _rows(con, f"""
                SELECT series_ticker AS series, '{layer}' AS layer, count(DISTINCT event_ticker) AS events,
                       count(DISTINCT market_ticker) AS markets, count(DISTINCT filename) AS files, count(*) AS rows,
                       min(ts_ms) AS first_ts, max(ts_ms) AS last_ts
                FROM {layer} GROUP BY 1 ORDER BY 1""")
    if "trades" in present:
        # trades files carry no series column: it is the directory, trades/{series}/{ticker}/{yyyy-mm}.parquet
        events = "count(DISTINCT m.event_ticker)" if "meta" in present else "NULL"
        join = "LEFT JOIN meta m USING (market_ticker)" if "meta" in present else ""
        out += _rows(con, f"""
            SELECT string_split(t.filename, '/')[-3] AS series, 'trades' AS layer, {events} AS events,
                   count(DISTINCT t.market_ticker) AS markets, count(DISTINCT t.filename) AS files, count(*) AS rows,
                   min(t.ts_ms) AS first_ts, max(t.ts_ms) AS last_ts
            FROM trades t {join} GROUP BY 1 ORDER BY 1""")
    if "orderbook" in present:
        series = "coalesce(m.series_ticker, '(no metadata)')" if "meta" in present else "'(all)'"
        events = "count(DISTINCT m.event_ticker)" if "meta" in present else "NULL"
        join = "LEFT JOIN meta m USING (market_ticker)" if "meta" in present else ""
        out += _rows(con, f"""
            SELECT {series} AS series, 'orderbook' AS layer, {events} AS events,
                   count(DISTINCT o.market_ticker) AS markets, count(DISTINCT o.filename) AS files, count(*) AS rows,
                   min(o.ts_ms) AS first_ts, max(o.ts_ms) AS last_ts
            FROM orderbook o {join} GROUP BY 1 ORDER BY 1""")
    return out


# ============================================================
# Running and reporting
# ============================================================

def run_checks(now: datetime | None = None, write_csv: bool = True) -> Report:
    """
    Run every check over DATA_DIR. Offline; nothing is modified except the
    CSV it writes to DATA_DIR/logs/quality_{date}.csv (write_csv=False skips it).

    Args:
        now: the audit time (the stale check counts from it); default now
    """
    now = now or datetime.now(timezone.utc)
    now_ms = int(now.timestamp() * 1000)
    root = config.DATA_DIR
    found = scan_files(root)
    schema, passed = check_schema(found, root)
    temp_files = sum(1 for _ in root.rglob("*.tmp")) if root.exists() else 0
    legacy = sum(1 for name in LEGACY_SKIP_FILES if (root / "logs" / name).exists())

    checks = [schema]
    cov: list[dict] = []
    if any(passed.values()):
        con = duckdb_connect()
        try:
            present = _create_views(con, passed)
            checks += [
                check_duplicates(con, present, root),
                check_order(con, present, root),
                check_volume_close(con, present),
                check_taker_side(con, present),
                check_volume_exchange(con, present),
                check_ladder(con, present),
                check_mutex(con, present),
                check_stale(con, present, now_ms),
                check_missing(con, present),
                check_history_start(con, present),
            ]
            cov = coverage(con, present)
        finally:
            con.close()
    else:
        for key, label in (("duplicates", "duplicates"), ("order", "order"), ("volume_close", "volume vs close"),
                           ("taker_side", "taker_side"), ("volume_exchange", "volume vs exchange"),
                           ("ladder", "ladder"), ("mutex", "mutex sum"), ("stale", "stale tradable"),
                           ("missing", "missing strikes"), ("history_start", "history start")):
            checks.append(_skip(Check(key, label), "no parquet files passed the schema pass"))

    report = Report(
        data_dir=root, audited_at=now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        files={kind: len(files) for kind, files in found.items()},
        rejected={kind: len(found[kind]) - len(passed[kind]) for kind in found},
        temp_files=temp_files, legacy_skip_files=legacy, checks=checks, coverage=cov,
    )
    if write_csv:
        report.csv_path = write_report_csv(report, root / "logs" / f"quality_{now.strftime('%Y%m%d')}.csv")
    return report


def write_report_csv(report: Report, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(report.csv_rows(), columns=list(CSV_COLUMNS)).to_csv(path, index=False)
    return path


def format_report(report: Report) -> str:
    """The summary: one line per check, the coverage table, the CSV path."""
    files = ", ".join(f"{kind} {n:,}" for kind, n in report.files.items())
    rejected = sum(report.rejected.values())
    lines = [
        "=== Data-quality checks (counts only, nothing is modified) ===",
        f"Store:    {report.data_dir} at {report.audited_at}",
        f"Files:    {files}; rejected by the schema pass {rejected:,}; stray temp files {report.temp_files:,}; "
        f"legacy skip files {report.legacy_skip_files:,}",
    ]
    for check in report.checks:
        text = f"skipped: {check.skipped}" if check.skipped else check.text
        lines.append(f"{check.label + ':':<20}{text}")
    if report.coverage:
        lines += ["", "Coverage per series and layer:",
                  f"{'series':<16}{'layer':<10}{'events':>7}{'markets':>8}{'files':>7}{'rows':>12}"
                  f"{'first day':>12}{'last day':>12}"]
        for c in report.coverage:
            events = "-" if c["events"] is None else f"{int(c['events']):,}"
            daily = c["layer"] == "daily"                   # a daily bar is labelled with the day it covers
            lines.append(f"{c['series']:<16}{c['layer']:<10}{events:>7}{int(c['markets']):>8,}{int(c['files']):>7,}"
                         f"{int(c['rows']):>12,}{_date(c['first_ts'], daily):>12}{_date(c['last_ts'], daily):>12}")
    if report.csv_path is not None:
        lines.append(f"CSV:      {report.csv_path} ({sum(len(c.rows) for c in report.checks):,} finding rows)")
    return "\n".join(lines)
