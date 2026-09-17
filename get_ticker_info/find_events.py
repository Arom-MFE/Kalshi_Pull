"""
get_ticker_info/find_events.py — Search Kalshi for series, events and markets.

Keyless: public REST endpoints only, through kalshi_io/discovery.py. Prints
tickers ready for a --tickers argument, a tickers file, or kalshi_io/config.py.

MODES (pick one):
    --categories                               every category with its tags
    --category C [--tag T ...] [--keyword K]   series of a category
    --keyword K                                series whose ticker, title or tags match
    --series S [S ...] [--status X]            events of those series
    --series S [S ...] --markets [--status X]  markets of those series
    --event E [--status X]                     markets of one event (both tiers)

STATUS: events take unopened, open, closed, settled; markets also take
paused. Omit --status to get every status. The API rejects "all".

FORMAT:
    table    aligned columns (default)
    tickers  one ticker per line; for markets this is a valid --tickers file
    json     the rows as JSON
    py       a Python list literal, ready for FOCUS_OVERRIDE or FOCUS_SERIES

EXIT CODES: 0 results, 1 nothing found, 2 usage error, 3 API error

EXAMPLES:
    python get_ticker_info/find_events.py --categories
    python get_ticker_info/find_events.py --category Economics --keyword inflation
    python get_ticker_info/find_events.py --series KXCPIYOY --status open
    python get_ticker_info/find_events.py --series KXCPIYOY --status open --markets --format tickers
    python get_ticker_info/find_events.py --event KXCPIYOY-26SEP --format py
"""

import argparse
import json
import sys
from pathlib import Path

# Ensure kalshi_io is importable when running as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests

from kalshi_io import discovery
from kalshi_io.client import KalshiAPIError
from kalshi_io.runlog import configure_logging

EXIT_OK, EXIT_NONE, EXIT_USAGE, EXIT_API = 0, 1, 2, 3

# kind -> (key printed by --format tickers/py, table columns)
LAYOUT = {
    "categories": ("category", ["category", "tags"]),
    "series": ("ticker", ["ticker", "category", "frequency", "tags", "title"]),
    "events": ("event_ticker", ["series_ticker", "event_ticker", "live_markets", "open_markets", "closes", "title"]),
    "markets": ("ticker", ["ticker", "event_ticker", "status", "tier", "close_time", "title"]),
}

_TITLE_WIDTH = 70


class UsageError(ValueError):
    """The arguments do not make a valid query."""


# ============================================================
# Queries (rows are flat dicts; see LAYOUT for the columns)
# ============================================================

def category_rows() -> list[dict]:
    return [{"category": c, "tags": ", ".join(tags)} for c, tags in sorted(discovery.list_categories().items())]


def series_rows(category=None, tags=None, keyword=None, include_legacy=False) -> list[dict]:
    if keyword:
        found = discovery.search_series(keyword, category=category, tags=tags, include_legacy=include_legacy)
    else:
        found = [s for s in discovery.list_series(category=category, tags=tags)
                 if include_legacy or not s["legacy_twin"]]
    return [{
        "ticker": s.get("ticker"),
        "category": s.get("category"),
        "frequency": s.get("frequency"),
        "tags": ", ".join(s["tags"]),
        "title": s.get("title"),
    } for s in sorted(found, key=lambda s: s.get("ticker") or "")]


def event_rows(series_list: list[str], status=None) -> list[dict]:
    """Events of the given series. Market counts cover the live tier only:
    the API does not nest markets that moved to the historical tier."""
    rows = []
    for series in series_list:
        for e in discovery.list_events(series, status=status, with_nested_markets=True):
            live = e["markets"]
            open_now = [m for m in live if discovery.status_bucket(m.get("status")) == "open"]
            closes = sorted(m["close_time"] for m in (open_now or live) if m.get("close_time"))
            rows.append({
                "series_ticker": e.get("series_ticker"),
                "event_ticker": e["event_ticker"],
                "live_markets": len(live),
                "open_markets": len(open_now),
                "closes": closes[0] if closes else None,
                "title": e.get("title"),
            })
    return sorted(rows, key=lambda r: (r["series_ticker"] or "", r["closes"] or "", r["event_ticker"]))


def market_rows(series_list=None, event_ticker=None, status=None) -> list[dict]:
    markets = []
    if event_ticker:
        markets = discovery.find_markets(event_ticker=event_ticker, status=status)
    for series in series_list or []:
        markets += discovery.find_markets(series_ticker=series, status=status)
    return [{
        "ticker": m["ticker"],
        "event_ticker": m.get("event_ticker"),
        "status": m.get("status"),
        "tier": m["tier"],
        "close_time": m.get("close_time"),
        "title": m.get("yes_sub_title") or m.get("subtitle") or m.get("title"),
    } for m in markets]


# ============================================================
# Output
# ============================================================

def render(kind: str, rows: list[dict], fmt: str) -> str:
    key, columns = LAYOUT[kind]
    if fmt == "json":
        return json.dumps(rows, indent=2)
    if fmt == "tickers":
        return "\n".join(str(r[key]) for r in rows)
    if fmt == "py":
        # A JSON string is also a valid Python string literal
        return "[\n" + "".join(f"    {json.dumps(str(r[key]), ensure_ascii=False)},\n" for r in rows) + "]"

    def cell(row, col):
        text = "" if row[col] is None else str(row[col])
        return text if len(text) <= _TITLE_WIDTH else text[:_TITLE_WIDTH - 1] + "…"

    widths = {c: max(len(c), *(len(cell(r, c)) for r in rows)) for c in columns}
    lines = ["  ".join(c.upper().ljust(widths[c]) for c in columns).rstrip()]
    lines += ["  ".join(cell(r, c).ljust(widths[c]) for c in columns).rstrip() for r in rows]
    lines.append(f"\n{len(rows)} {kind}")
    return "\n".join(lines)


# ============================================================
# CLI
# ============================================================

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Search Kalshi for series, events and markets (keyless).",
        epilog="Exit codes: 0 results, 1 nothing found, 2 usage error, 3 API error.",
    )
    parser.add_argument("--categories", action="store_true", help="List every category with its tags")
    parser.add_argument("--category", help='Series of this category, e.g. Economics or "Financials" (case-sensitive)')
    parser.add_argument("--tag", nargs="+", help="Series with any of these tags")
    parser.add_argument("--keyword", help="Series whose ticker, title or tags contain every word (case-insensitive)")
    parser.add_argument("--include-legacy", action="store_true",
                        help="Also show pre-KX series spellings (dead twins such as CPIYOY)")
    parser.add_argument("--series", nargs="+", help="Events (or, with --markets, markets) of these series")
    parser.add_argument("--markets", action="store_true", help="With --series: list markets instead of events")
    parser.add_argument("--event", help="Markets of this event, from the live and the historical tier")
    parser.add_argument("--status", help="unopened, open, closed or settled; for markets also paused. "
                                         "Omit for every status")
    parser.add_argument("--format", choices=["table", "tickers", "json", "py"], default="table")
    return parser


def _query(args) -> tuple[str, list[dict]]:
    """Validate the mode and run it. Returns (kind, rows)."""
    series_search = bool(args.category or args.keyword or args.tag)
    modes = [args.categories, series_search, bool(args.series), bool(args.event)]
    if sum(modes) != 1:
        raise UsageError("pick one mode: --categories, --category/--keyword/--tag, --series, or --event")
    if args.markets and not args.series:
        raise UsageError("--markets goes with --series (an --event always lists its markets)")
    if args.status and not (args.series or args.event):
        raise UsageError("--status goes with --series or --event")

    if args.status:
        listing_markets = bool(args.event or args.markets)
        allowed = discovery.MARKET_STATUS_FILTERS if listing_markets else discovery.EVENT_STATUS_FILTERS
        if args.status not in allowed:
            hint = " Omit --status to get every status; the API rejects 'all'." if args.status == "all" else ""
            raise UsageError(
                f"invalid --status {args.status!r} for {'markets' if listing_markets else 'events'}: "
                f"use one of {', '.join(allowed)}.{hint}"
            )

    if args.categories:
        return "categories", category_rows()
    if series_search:
        return "series", series_rows(args.category, args.tag, args.keyword, args.include_legacy)
    if args.event:
        return "markets", market_rows(event_ticker=args.event, status=args.status)
    if args.markets:
        return "markets", market_rows(series_list=args.series, status=args.status)
    return "events", event_rows(args.series, status=args.status)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point; see the module docstring for modes and exit codes."""
    try:
        args = _build_parser().parse_args(argv)
    except SystemExit as e:                     # argparse: 2 on bad arguments, 0 on --help
        return int(e.code or 0)

    configure_logging()
    try:
        kind, rows = _query(args)
    except ValueError as e:                     # UsageError, or an unknown category with its suggestion
        print(f"find_events: {e}", file=sys.stderr)
        return EXIT_USAGE
    except (KalshiAPIError, requests.RequestException) as e:
        print(f"find_events: API error — {type(e).__name__}: {e}", file=sys.stderr)
        return EXIT_API

    if not rows:
        print("find_events: nothing found", file=sys.stderr)
        return EXIT_NONE
    print(render(kind, rows, args.format))
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
