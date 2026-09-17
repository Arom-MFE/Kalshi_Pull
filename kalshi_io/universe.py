"""
kalshi_io/universe.py — The focus universe: the tickers poll_focus polls.

The universe is derived from the API instead of being listed by hand. For
every series in config.FOCUS_SERIES, the markets that are open right now are
grouped by event, the events are ranked by the earliest close_time of their
open markets, and the nearest config.FOCUS_EVENTS_PER_SERIES events are taken
whole. When a cycle closes, the same rule selects the next one, so the
pipeline rolls forward without an edit.

An explicit universe (config.FOCUS_OVERRIDE, or `poll_focus --tickers`)
replaces the rule. It is checked the same way and never rolls.

A universe is a plain dict, written as JSON by the poller and by roll.py:
    schema_version, derived_at (UTC), source ("derived" or where the explicit
    tickers came from), rule, series, events_per_series,
    groups:   [{series, event_ticker, title, close_time, tickers}]
    tickers:  sorted market tickers
    statuses: {ticker: API status when the universe was built}
    warnings: [str]
check_universe() adds status_counts and dropped.

Keyless REST through kalshi_io.discovery: two list calls per series.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

from kalshi_io import candles, config, discovery
from kalshi_io.discovery import POLLABLE_BUCKETS, status_bucket
from kalshi_io.runlog import get_logger
from kalshi_io.storage import atomic_write_text
from kalshi_io.tickers import load_tickers

logger = get_logger("universe")

SCHEMA_VERSION = 1

RULE = ("nearest {n} open event(s) per series, ranked by the earliest close_time "
        "of their open markets")

# A refresh is scheduled this long after the nearest close_time, so the roll
# to the next event does not wait for the hourly refresh.
CLOSE_REFRESH_DELAY_S = 90

# Order of the buckets in messages
_BUCKET_ORDER = ("open", "paused", "unopened", "closed", "settled", "not_found", "unknown")

# File names under DATA_DIR/logs (poller snapshot) and TICKERS_DIR (roll.py)
LIVE_SNAPSHOT = "focus_universe_live.json"
FOCUS_JSON = "focus_universe.json"
FOCUS_TXT = "focus_universe.txt"


class UniverseError(RuntimeError):
    """The universe is empty, or nothing in it can trade. Polling it would run
    clean and capture nothing (a settled market's orderbook answers 200 with
    empty books), so poll_focus refuses to start on it."""

    def __init__(self, message: str, status_counts: dict[str, int] | None = None):
        super().__init__(message)
        self.status_counts = status_counts or {}
        # Set by refresh_universe: {ticker: bucket} of the tickers that died
        self.dead: dict[str, str] = {}


def _utc_stamp(when: datetime | None = None) -> str:
    return (when or datetime.now(timezone.utc)).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _market_close(market: dict) -> datetime | None:
    """When a market stops trading: close_time, else the expiration times.
    Read from the market payload only, never guessed from the ticker text."""
    for key in ("close_time", "expected_expiration_time", "latest_expiration_time"):
        when = _parse_time(market.get(key))
        if when is not None:
            return when
    return None


def _bucket(status: str | None) -> str:
    return status_bucket(status) if status else "not_found"


def _format_counts(counts: dict[str, int]) -> str:
    ordered = [b for b in _BUCKET_ORDER if counts.get(b)] + sorted(set(counts) - set(_BUCKET_ORDER))
    return ", ".join(f"{b}={counts[b]}" for b in ordered) or "none"


# ============================================================
# Building
# ============================================================

def derive_universe(
    series_list: list[str] | None = None,
    events_per_series: int | None = None,
    *,
    now: datetime | None = None,
) -> dict:
    """
    Derive the focus universe from the API.

    Per series, one snapshot of GET /markets?series_ticker=S&status=open is
    grouped by event_ticker. Events are ranked by the earliest close_time of
    their open markets (expected_expiration_time, then latest_expiration_time,
    when a market has no close_time; events without any time rank last; ties
    break on the event ticker) and the first `events_per_series` are taken
    with all their open markets. Ranking and selection use the same snapshot,
    so a market cannot change status in between.
    GET /events?series_ticker=S&status=open supplies the event titles.

    Args:
        series_list:       series tickers (default config.FOCUS_SERIES)
        events_per_series: events to take per series (default
                           config.FOCUS_EVENTS_PER_SERIES)
        now:               derived_at override (tests)

    Returns:
        A universe dict (see module docstring). A series without open markets
        adds a warning and contributes nothing; it is not an error here.
        check_universe() decides whether the result is usable.

    Raises:
        kalshi_io.client.KalshiAPIError: a request failed.
    """
    series_list = list(series_list or config.FOCUS_SERIES)
    n = int(events_per_series or config.FOCUS_EVENTS_PER_SERIES)
    if n < 1:
        raise ValueError(f"events_per_series must be at least 1, got {n}")

    groups: list[dict] = []
    statuses: dict[str, str] = {}
    warnings: list[str] = []

    for series in series_list:
        by_event: dict[str, list[dict]] = {}
        for m in discovery.list_markets(series_ticker=series, status="open"):
            if status_bucket(m.get("status")) == "open":
                by_event.setdefault(m["event_ticker"], []).append(m)
        if not by_event:
            warnings.append(f"{series}: no open markets, nothing to poll for this series")
            logger.warning(warnings[-1])
            continue

        titles = {e["event_ticker"]: e.get("title") or "" for e in discovery.list_events(series, status="open")}

        def rank(event_ticker: str):
            closes = [c for c in map(_market_close, by_event[event_ticker]) if c is not None]
            return (0, min(closes), event_ticker) if closes else (1, datetime.max.replace(tzinfo=timezone.utc), event_ticker)

        for event_ticker in sorted(by_event, key=rank)[:n]:
            markets = by_event[event_ticker]
            closes = [c for c in map(_market_close, markets) if c is not None]
            groups.append({
                "series": series,
                "event_ticker": event_ticker,
                "title": titles.get(event_ticker, ""),
                "close_time": _utc_stamp(min(closes)) if closes else None,
                "tickers": sorted(m["ticker"] for m in markets),
            })
            statuses.update({m["ticker"]: m.get("status") or "" for m in markets})

    return {
        "schema_version": SCHEMA_VERSION,
        "derived_at": _utc_stamp(now),
        "source": "derived",
        "rule": RULE.format(n=n),
        "series": series_list,
        "events_per_series": n,
        "groups": groups,
        "tickers": sorted(t for g in groups for t in g["tickers"]),
        "statuses": statuses,
        "warnings": warnings,
    }


def explicit_universe(tickers: list[str], source: str, *, now: datetime | None = None) -> dict:
    """
    A universe made of exactly these market tickers (config.FOCUS_OVERRIDE or
    `--tickers`). Their current status is looked up in both tiers so that
    check_universe() can tell what still trades; a ticker found in neither
    tier gets the status "" (bucket "not_found").

    Groups are built per event; `series` stays None because market payloads
    carry no series (the pullers resolve it when they need a storage path).

    Raises:
        kalshi_io.client.KalshiAPIError: the lookup failed.
    """
    tickers = sorted(dict.fromkeys(tickers))
    found = discovery.lookup_markets(tickers) if tickers else {}
    by_event: dict[str, list[dict]] = {}
    for ticker in tickers:
        if ticker in found:
            by_event.setdefault(found[ticker]["event_ticker"], []).append(found[ticker])

    groups = []
    for event_ticker in sorted(by_event):
        closes = [c for c in map(_market_close, by_event[event_ticker]) if c is not None]
        groups.append({
            "series": None,
            "event_ticker": event_ticker,
            "title": "",
            "close_time": _utc_stamp(min(closes)) if closes else None,
            "tickers": sorted(m["ticker"] for m in by_event[event_ticker]),
        })
    return {
        "schema_version": SCHEMA_VERSION,
        "derived_at": _utc_stamp(now),
        "source": source,
        "rule": f"explicit tickers from {source}; never rolled forward",
        "series": [],
        "events_per_series": None,
        "groups": groups,
        "tickers": tickers,
        "statuses": {t: (found[t].get("status") or "") if t in found else "" for t in tickers},
        "warnings": [],
    }


def build_universe(
    cli_tickers: str | list[str] | None = None,
    series_list: list[str] | None = None,
    events_per_series: int | None = None,
) -> dict:
    """
    The universe for a poll_focus run, by precedence: `--tickers` (any source
    load_tickers accepts), then config.FOCUS_OVERRIDE, then the derived rule.
    """
    if cli_tickers:
        return explicit_universe(load_tickers(cli_tickers), "--tickers")
    if config.FOCUS_OVERRIDE:
        return explicit_universe(list(config.FOCUS_OVERRIDE), "FOCUS_OVERRIDE")
    return derive_universe(series_list, events_per_series)


# ============================================================
# Checking
# ============================================================

def check_universe(universe: dict) -> dict:
    """
    Keep what can still trade and refuse a universe that holds nothing.

    A ticker is pollable while its market is open, paused or unopened. No
    request is made: the statuses were recorded when the universe was built.

    Returns:
        A copy of the universe restricted to pollable tickers, plus
        "status_counts" ({bucket: n} over the input) and "dropped"
        ({ticker: bucket} for what was removed).

    Raises:
        UniverseError: the universe is empty, or no ticker is pollable. The
        message carries the counts per status and says where to look.
    """
    tickers = list(universe["tickers"])
    statuses = universe.get("statuses") or {}
    buckets = {t: _bucket(statuses.get(t)) for t in tickers}
    counts: dict[str, int] = {}
    for bucket in buckets.values():
        counts[bucket] = counts.get(bucket, 0) + 1

    derived = universe.get("source") == "derived"
    finder = "python get_ticker_info/find_events.py --series <SERIES> --status open --markets"
    if not tickers:
        if derived:
            raise UniverseError(
                f"focus universe is empty: no open market found in any focus series "
                f"({', '.join(universe.get('series') or [])}). List what is open with: {finder}"
            )
        raise UniverseError(f"focus universe is empty: {universe.get('source')} lists no tickers")

    pollable = [t for t in tickers if buckets[t] in POLLABLE_BUCKETS]
    if not pollable:
        raise UniverseError(
            f"focus universe has nothing to poll: none of its {len(tickers)} tickers can trade "
            f"({_format_counts(counts)}). The tickers come from {universe.get('source')}. "
            f"Find the current cycle with: {finder}"
            + ("" if derived else "; or drop the explicit tickers to derive the universe from FOCUS_SERIES."),
            counts,
        )

    keep = set(pollable)
    groups = [{**g, "tickers": [t for t in g["tickers"] if t in keep]} for g in universe.get("groups", [])]
    return {
        **universe,
        "groups": [g for g in groups if g["tickers"]],
        "tickers": pollable,
        "status_counts": counts,
        "dropped": {t: buckets[t] for t in tickers if t not in keep},
    }


def register(universe: dict) -> None:
    """Tell the pullers the event and series of every derived ticker, so
    resolving and validating them costs no API call. Groups without a series
    (explicit universes) are left to resolve_ticker_meta."""
    candles.register_ticker_meta({
        ticker: (g["series"], g["event_ticker"])
        for g in universe.get("groups", []) if g.get("series")
        for ticker in g["tickers"]
    })


# ============================================================
# Refreshing while polling
# ============================================================

def refresh_universe(current: dict) -> tuple[dict, dict]:
    """
    Re-evaluate a universe that is being polled.

    A derived universe is derived again with the same series and rule; an
    explicit one only has its statuses looked up again (it never rolls).
    Tickers that left the selection are looked up before anything is dropped:
    those that can still trade (a paused market is not in an open-markets
    snapshot; a nearer event can push a live one out of the first N) stay in
    the universe, the rest are reported as dead.

    Returns:
        (universe, change) with change =
            {"added": [tickers], "dead": {ticker: bucket}, "kept": [tickers],
             "rolls": ["universe roll KXCPIYOY: KXCPIYOY-26SEP -> KXCPIYOY-26NOV"]}
        The universe is already checked and registered.

    Raises:
        UniverseError: nothing pollable is left. For a derived universe the
            caller keeps polling the last good one; for an explicit one there
            is nothing to roll to. `.dead` holds the tickers that died.
        kalshi_io.client.KalshiAPIError: a request failed; keep the last one.
    """
    derived = current.get("source") == "derived"
    if derived:
        fresh = derive_universe(current["series"], current["events_per_series"])
    else:
        fresh = explicit_universe(current["tickers"], current["source"])

    selected = {t for t in fresh["tickers"] if _bucket(fresh["statuses"].get(t)) in POLLABLE_BUCKETS}
    gone = sorted(set(current["tickers"]) - selected)
    kept: list[str] = []
    dead: dict[str, str] = {}
    if gone:
        found = discovery.lookup_markets(gone) if derived else {}
        for ticker in gone:
            status = (found[ticker].get("status") or "") if ticker in found else fresh["statuses"].get(ticker, "")
            if _bucket(status) in POLLABLE_BUCKETS:
                kept.append(ticker)
                fresh["statuses"][ticker] = status
            else:
                dead[ticker] = _bucket(status)

    if kept:
        keep = set(kept)
        for g in current.get("groups", []):
            tickers = [t for t in g["tickers"] if t in keep]
            if not tickers:
                continue
            match = next((x for x in fresh["groups"] if x["event_ticker"] == g["event_ticker"]), None)
            if match is None:
                fresh["groups"].append({**g, "tickers": tickers})
            else:
                match["tickers"] = sorted(set(match["tickers"]) | set(tickers))
        fresh["tickers"] = sorted(set(fresh["tickers"]) | keep)

    try:
        universe = check_universe(fresh)
    except UniverseError as e:
        e.dead = dead
        raise
    register(universe)

    rolls = []
    for series in universe.get("series") or []:
        before = sorted(g["event_ticker"] for g in current.get("groups", []) if g.get("series") == series)
        after = sorted(g["event_ticker"] for g in universe["groups"] if g.get("series") == series)
        if before != after:
            rolls.append(f"universe roll {series}: {', '.join(before) or 'nothing'} -> {', '.join(after) or 'nothing'}")

    change = {
        "added": sorted(set(universe["tickers"]) - set(current["tickers"])),
        "dead": dead,
        "kept": kept,
        "rolls": rolls,
    }
    return universe, change


def next_refresh_at(universe: dict, last_refresh_ts: float, interval_s: float) -> float | None:
    """
    Unix time of the next refresh: every interval_s seconds, and once
    CLOSE_REFRESH_DELAY_S after the nearest close_time that lies after the
    last refresh. None when interval_s is 0 or negative (refresh disabled).
    """
    if interval_s <= 0:
        return None
    due = last_refresh_ts + interval_s
    closes = [c.timestamp() for c in (_parse_time(g.get("close_time")) for g in universe.get("groups", []))
              if c is not None and c.timestamp() > last_refresh_ts]
    if closes:
        due = min(due, min(closes) + CLOSE_REFRESH_DELAY_S)
    return due


# ============================================================
# Files and display
# ============================================================

def write_universe(universe: dict, json_path: Path, txt_path: Path | None = None) -> None:
    """Write the universe as JSON (and optionally its tickers, one per line,
    a valid `--tickers` file). Atomic."""
    atomic_write_text(json_path, json.dumps(universe, indent=2) + "\n")
    if txt_path is not None:
        tickers = universe["tickers"]
        atomic_write_text(txt_path, "\n".join(tickers) + ("\n" if tickers else ""))


def load_universe(json_path: Path) -> dict | None:
    """Read a universe file written by write_universe, or None if it is absent."""
    if not json_path.exists():
        return None
    return json.loads(json_path.read_text())


def format_universe(universe: dict) -> str:
    """Human-readable summary: one line per event group, then the tickers."""
    groups = universe.get("groups", [])
    lines = [
        f"Focus universe: {len(universe['tickers'])} tickers in {len(groups)} events "
        f"({universe.get('source')}, {universe.get('derived_at')})",
        f"Rule: {universe.get('rule')}",
    ]
    if universe.get("status_counts"):
        lines.append(f"Status: {_format_counts(universe['status_counts'])}")
    for g in groups:
        lines.append(
            f"  {g.get('series') or '-':<15} {g['event_ticker']:<24} closes {g.get('close_time') or 'unknown':<21}"
            f"{len(g['tickers']):>4} markets  {g.get('title') or ''}".rstrip()
        )
    for ticker, bucket in sorted((universe.get("dropped") or {}).items()):
        lines.append(f"  dropped: {ticker} ({bucket})")
    for warning in universe.get("warnings") or []:
        lines.append(f"  warning: {warning}")
    lines.append("Tickers:")
    lines.extend(f"  {t}" for t in universe["tickers"])
    return "\n".join(lines)
