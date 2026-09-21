"""
pull_historical/pull_audit.py — Read-only coverage audit for daily candles.

No API calls. Reads existing daily parquet files and reports coverage stats.
Outputs CSV to kalshi_data/logs/audit_{YYYYMMDD}.csv and prints per-series
grouped summary to stdout.

Before the coverage it reports how fresh its inputs are, from local files
only: when the ticker catalog was built, how many of the audited tickers the
catalog calls open or settled, and what the focus universe holds (the
poller's snapshot, or the file written by get_ticker_info/roll.py).

After the coverage it runs the data-quality checks of kalshi_io/quality.py
over the whole store (schema, duplicates, order, ladders, mutually exclusive
events, stale and missing strikes, coverage per series; counts only) and
writes them to kalshi_data/logs/quality_{YYYYMMDD}.csv. --no-checks skips them.

CLI:
    python pull_historical/pull_audit.py --tickers KXRECSSNBER
    python pull_historical/pull_audit.py --tickers focus
    python pull_historical/pull_audit.py                        # all tickers
    python pull_historical/pull_audit.py --no-checks            # coverage only
"""

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from kalshi_io import catalog, quality, universe
from kalshi_io.candles import resolve_ticker_meta
from kalshi_io.config import DATA_DIR, TICKERS_DIR
from kalshi_io.discovery import status_bucket
from kalshi_io.storage import get_output_path, read_parquet_safe
from kalshi_io.tickers import load_tickers

# Thresholds for hourly/minute density expectations
DENSE_HOURLY_VOLUME = 1000
DENSE_MINUTE_VOLUME_30D = 500

# The catalog header warns once the catalog is older than this
STALE_CATALOG_DAYS = 7

_BUCKETS = ("open", "paused", "unopened", "closed", "settled", "unknown")
NOT_CATALOGED = "not in catalog"


def _age_days(stamp: datetime, now: datetime) -> float:
    return (now - stamp).total_seconds() / 86400


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _count_by_bucket(tickers: list[str], index: dict[str, dict], now: datetime) -> tuple[dict[str, int], int]:
    """
    Count tickers per status bucket as the catalog recorded them.

    Returns:
        ({bucket: n}, n_presumed_closed). The second number counts tickers the
        catalog still calls open, paused or unopened although their close_time
        has passed since: the catalog is a snapshot, not the API.
    """
    counts: dict[str, int] = {}
    presumed_closed = 0
    for ticker in tickers:
        record = index.get(ticker)
        bucket = status_bucket(record.get("status")) if record else NOT_CATALOGED
        counts[bucket] = counts.get(bucket, 0) + 1
        close = _parse_iso(record.get("close_time")) if record else None
        if bucket in ("open", "paused", "unopened") and close is not None and close < now:
            presumed_closed += 1
    return counts, presumed_closed


def _format_counts(counts: dict[str, int]) -> str:
    order = [b for b in (*_BUCKETS, NOT_CATALOGED) if counts.get(b)]
    return ", ".join(f"{b}={counts[b]:,}" for b in order) or "none"


def catalog_header(tickers: list[str], index: dict[str, dict], now: datetime | None = None) -> list[str]:
    """Lines describing the catalog's age and the audited tickers' statuses. Offline."""
    now = now or datetime.now(timezone.utc)
    lines = ["=== Catalog ==="]
    stamp, basis = catalog.catalog_built_at()
    if stamp is None:
        lines.append("Built:    no catalog found; build one with: python get_ticker_info/roll.py")
    else:
        age = _age_days(stamp, now)
        hint = "" if basis == "built_at" else ", from the file modification time: a git checkout resets it, treat as a hint"
        lines.append(f"Built:    {stamp.strftime('%Y-%m-%dT%H:%M:%SZ')} ({age:.1f} days ago{hint})")
        if age > STALE_CATALOG_DAYS:
            lines.append(f"WARNING:  the catalog is older than {STALE_CATALOG_DAYS} days; new events and status changes "
                         f"are missing. Refresh it with: python get_ticker_info/roll.py")
    counts, presumed_closed = _count_by_bucket(tickers, index, now)
    lines.append(f"Audited:  {len(tickers):,} tickers by catalog status: {_format_counts(counts)}")
    if presumed_closed:
        lines.append(f"          {presumed_closed:,} of the open ones are presumed closed since the build "
                     f"(their close_time has passed)")
    return lines


def focus_header(index: dict[str, dict], now: datetime | None = None) -> list[str]:
    """Lines describing the focus universe, from whichever local file is
    newer: the poller's snapshot or the one written by roll.py. Offline."""
    now = now or datetime.now(timezone.utc)
    candidates = [
        ("poller snapshot", DATA_DIR / "logs" / universe.LIVE_SNAPSHOT),
        ("roll.py proposal", TICKERS_DIR / universe.FOCUS_JSON),
    ]
    found = [(label, path, u) for label, path in candidates if (u := universe.load_universe(path)) is not None]
    lines = ["=== Focus universe ==="]
    if not found:
        lines.append("none recorded yet; poll_focus writes a snapshot, get_ticker_info/roll.py a proposal")
        return lines

    label, path, focus = max(found, key=lambda item: item[2].get("derived_at") or "")
    derived = _parse_iso(focus.get("derived_at"))
    age = f" ({_age_days(derived, now):.1f} days ago)" if derived else ""
    lines.append(f"Source:   {label}, {path.name}")
    lines.append(f"Derived:  {focus.get('derived_at')}{age}; {focus.get('source')}: {focus.get('rule')}")
    for g in focus.get("groups", []):
        lines.append(f"          {g['event_ticker']}: {len(g['tickers'])} markets, closes {g.get('close_time') or 'unknown'}")

    tickers = focus.get("tickers", [])
    then: dict[str, int] = {}
    for ticker in tickers:
        bucket = status_bucket((focus.get("statuses") or {}).get(ticker))
        then[bucket] = then.get(bucket, 0) + 1
    lines.append(f"Markets:  {len(tickers)} when derived: {_format_counts(then)}")
    counts, presumed_closed = _count_by_bucket(tickers, index, now)
    lines.append(f"          by catalog status: {_format_counts(counts)}")
    closed_groups = [g["event_ticker"] for g in focus.get("groups", [])
                     if (c := _parse_iso(g.get("close_time"))) is not None and c < now]
    if closed_groups:
        lines.append(f"WARNING:  {', '.join(closed_groups)} closed since this universe was derived; "
                     f"poll_focus rolls on its own, a saved proposal needs: python get_ticker_info/roll.py")
    return lines


def _fmt_vol(v: float) -> str:
    """Format a volume sum: whole numbers bare, fractional ones at two decimals.
    Contract counts have a granularity of 0.01, so anything beyond that in a
    sum is floating-point noise (496409.50000000006)."""
    v = round(float(v), 2)
    return str(int(v)) if v == int(v) else f"{v:.2f}"


def _audit_ticker(ticker: str, index: dict[str, dict] | None = None) -> dict | None:
    """Read daily parquet for one ticker and return stats, or None if no file.
    `index` (catalog.market_index()) supplies the status and close_time columns."""
    # Offline on purpose: an uncataloged ticker gets a prefix-derived series
    series_ticker, _ = resolve_ticker_meta(ticker, allow_api=False)
    path = get_output_path("candles", 1440, series_ticker, ticker)
    if not path.exists():
        # The puller may have filed it under the series the API reported
        matches = sorted((DATA_DIR / "candles" / "daily").glob(f"*/{ticker}.parquet"))
        if matches:
            path = matches[0]
            series_ticker = path.parent.name

    df = read_parquet_safe(path)
    if df is None:
        return None

    record = (index or {}).get(ticker) or {}
    from_catalog = {"status": record.get("status") or None, "close_time": record.get("close_time") or None}

    if df.empty:
        return {
            "ticker": ticker,
            "series": series_ticker,
            "daily_rows": 0,
            "first_ts_ms": None,
            "last_ts_ms": None,
            "total_volume": 0.0,
            "recent_volume_30d": 0.0,
            "hourly_expectation": "sparse",
            "minute_expectation": "sparse",
            **from_catalog,
        }

    first_ts = int(df["ts_ms"].min())
    last_ts = int(df["ts_ms"].max())

    # Volume is float64 in the current schema; coerce defensively for files
    # written before the dtype fix
    vol = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
    total_vol = float(vol.sum())

    # Recent 30d volume
    cutoff_ms = int((time.time() - 30 * 86400) * 1000)
    recent_vol = float(vol[df["ts_ms"] >= cutoff_ms].sum())

    # Density expectations
    hourly_exp = "dense" if total_vol >= DENSE_HOURLY_VOLUME or recent_vol >= 100 else "sparse"
    minute_exp = "dense" if recent_vol >= DENSE_MINUTE_VOLUME_30D else "sparse"

    return {
        "ticker": ticker,
        "series": series_ticker,
        "daily_rows": len(df),
        "first_ts_ms": first_ts,
        "last_ts_ms": last_ts,
        "total_volume": total_vol,
        "recent_volume_30d": recent_vol,
        "hourly_expectation": hourly_exp,
        "minute_expectation": minute_exp,
        **from_catalog,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit daily candle coverage (read-only).")
    parser.add_argument(
        "--tickers",
        nargs="+",
        default=str(TICKERS_DIR / "all_tickers.txt"),
        help="Ticker source(s): .txt/.json path, series name, 'focus', "
             "or tickers separated by spaces or commas. "
             "A ticker that holds a space or a comma: quote it on its own, or use a file",
    )
    parser.add_argument("--limit", type=int, default=None, help="Max tickers to audit")
    parser.add_argument("--no-checks", action="store_true",
                        help="Skip the data-quality checks over the whole store (kalshi_io/quality.py)")
    args = parser.parse_args(argv)

    ticker_list = load_tickers(args.tickers)
    if args.limit:
        ticker_list = ticker_list[:args.limit]

    index = catalog.market_index()
    print("\n".join(catalog_header(ticker_list, index)))
    print()
    print("\n".join(focus_header(index)))

    print(f"\nAuditing {len(ticker_list)} tickers...")

    rows = []
    missing = []
    for ticker in ticker_list:
        result = _audit_ticker(ticker, index)
        if result is None:
            missing.append(ticker)
        else:
            rows.append(result)

    if not rows:
        print("No daily parquet files found.")
        return 0

    audit_df = pd.DataFrame(rows)

    # Write CSV
    log_dir = DATA_DIR / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    csv_path = log_dir / f"audit_{stamp}.csv"
    audit_df.to_csv(csv_path, index=False)
    print(f"\nCSV written: {csv_path}")

    # Per-series grouped summary
    print("\n=== Per-series summary ===\n")
    for series, grp in audit_df.groupby("series"):
        total_rows = grp["daily_rows"].sum()
        n_dense_h = (grp["hourly_expectation"] == "dense").sum()
        n_dense_m = (grp["minute_expectation"] == "dense").sum()
        vol = grp["total_volume"].sum()
        recent = grp["recent_volume_30d"].sum()
        print(
            f"{series:20s}  tickers={len(grp):4d}  "
            f"rows={total_rows:6d}  vol={_fmt_vol(vol):>10}  "
            f"recent_30d={_fmt_vol(recent):>8}  "
            f"dense_h={n_dense_h:3d}  dense_m={n_dense_m:3d}"
        )

    # Overall
    print(f"\n{'TOTAL':20s}  tickers={len(audit_df):4d}  "
          f"rows={audit_df['daily_rows'].sum():6d}  "
          f"vol={_fmt_vol(audit_df['total_volume'].sum()):>10}  "
          f"recent_30d={_fmt_vol(audit_df['recent_volume_30d'].sum()):>8}")

    if missing:
        print(f"\nMissing daily data: {len(missing)} tickers (no parquet file)")
        if len(missing) <= 20:
            for t in missing:
                print(f"  {t}")

    # The checks cover the whole store, not only the audited tickers: a ladder
    # or a mutually exclusive event is only readable with all its strikes
    if not args.no_checks:
        print()
        print(quality.format_report(quality.run_checks()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
