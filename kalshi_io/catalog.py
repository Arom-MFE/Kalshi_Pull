"""
kalshi_io/catalog.py — Build, read and compare the committed ticker catalog.

The catalog lives in get_ticker_info/kalshi_tickers/:
    {SERIES}_tickers.json   events + markets of one series (format below)
    {SERIES}_tickers.txt    one market ticker per line
    all_tickers.json/.txt   every series combined, with counts

Per-series JSON, schema_version 2 (additive; v1 readers keep working):
    series, built_at (UTC), historical_cutoff, status_counts,
    events:  [{event_ticker, title}]
    markets: [{event_ticker, market_ticker, title, status, open_time,
               close_time, expected_expiration_time, latest_expiration_time,
               source}]
    tickers: sorted market tickers
Times are the API's ISO strings, null when the API sent none. `status` is the
API's response status at build time (active, finalized, ...). `source` is
"live", "historical" (settled before the cutoff) or "carried_forward" (in the
previous catalog, no longer returned by the API).

Discovery is keyless REST through kalshi_io.discovery. A series is written
only if every request succeeded and the result passes validation; otherwise
its previous file stays untouched.
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from kalshi_io import client, discovery
from kalshi_io.config import SERIES_LIST, TICKERS_DIR
from kalshi_io.runlog import get_logger

logger = get_logger("catalog")

SCHEMA_VERSION = 2

MARKET_FIELDS = (
    "event_ticker", "market_ticker", "title", "status", "open_time", "close_time",
    "expected_expiration_time", "latest_expiration_time", "source",
)

# A market that ever reached one of these may have trades and stored data;
# it is carried forward rather than dropped if the API stops returning it.
_OPENED_STATUSES = frozenset({"active", "inactive", "closed", "determined", "disputed", "amended", "finalized"})


class CatalogValidationError(RuntimeError):
    """The discovered result is implausible (e.g. open events without active
    markets, which is exactly how the 2026-09 breakage looked). Nothing is written."""


def _utc_stamp(now: datetime | None = None) -> str:
    return (now or datetime.now(timezone.utc)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _market_record(m: dict, source: str) -> dict:
    """Catalog record from an API market payload."""
    return {
        "event_ticker": m["event_ticker"],
        "market_ticker": m["ticker"],
        "title": m.get("title") or m.get("yes_sub_title") or "",
        "status": m.get("status") or "",
        "open_time": m.get("open_time") or None,
        "close_time": m.get("close_time") or None,
        "expected_expiration_time": m.get("expected_expiration_time") or None,
        "latest_expiration_time": m.get("latest_expiration_time") or None,
        "source": source,
    }


def status_counts(markets: list[dict]) -> dict[str, int]:
    """Count markets per response status, sorted by status name."""
    counts: dict[str, int] = {}
    for m in markets:
        key = m.get("status") or "unknown"
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text)
    os.replace(tmp, path)


# ============================================================
# Reading
# ============================================================

def load_series(series: str, out_dir: Path | None = None) -> dict | None:
    """Read one per-series catalog file (any schema version), or None if absent."""
    path = (out_dir or TICKERS_DIR) / f"{series}_tickers.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())


def load_catalog(out_dir: Path | None = None) -> dict[str, dict]:
    """Read every per-series file: {series: per-series dict}. Files whose
    "series" is not a string (all_tickers.json) are not series files."""
    catalog: dict[str, dict] = {}
    for path in sorted((out_dir or TICKERS_DIR).glob("*_tickers.json")):
        data = json.loads(path.read_text())
        if isinstance(data.get("series"), str):
            catalog[data["series"]] = data
    return catalog


def market_index(out_dir: Path | None = None) -> dict[str, dict]:
    """{market_ticker: market record + "series"} across the whole catalog."""
    index: dict[str, dict] = {}
    for series, data in load_catalog(out_dir).items():
        for m in data.get("markets", []):
            index[m["market_ticker"]] = {**m, "series": series}
    return index


def catalog_built_at(out_dir: Path | None = None) -> tuple[datetime | None, str]:
    """
    When the catalog was built.

    Returns:
        (timestamp, basis): basis is "built_at" (recorded by the build),
        "mtime" (a v1 catalog: file modification time, which a git checkout
        resets, so treat it as a hint) or "none" (no catalog).
    """
    path = (out_dir or TICKERS_DIR) / "all_tickers.json"
    if not path.exists():
        return None, "none"
    stamp = json.loads(path.read_text()).get("built_at")
    if stamp:
        return datetime.fromisoformat(stamp.replace("Z", "+00:00")), "built_at"
    return datetime.fromtimestamp(path.stat().st_mtime, timezone.utc), "mtime"


# ============================================================
# Building
# ============================================================

def discover_series(
    series: str,
    *,
    save: bool = True,
    out_dir: Path | None = None,
    legacy_probe: bool = True,
    now: datetime | None = None,
) -> dict:
    """
    Discover every event and market of a series and (optionally) write its files.

    Sources, unioned by market ticker (the tiers overlap near the cutoff; the
    live record wins because its status is current):
        /events?series_ticker=            every event, however old
        /historical/markets?series_ticker= markets settled before the cutoff
        /markets?series_ticker=           live tier, every status (no filter)
    Events seen only on markets (delisted duplicates) are added; events left
    without markets get a per-event lookup in both tiers. Tickers of the
    previous file that vanished are looked up one by one and otherwise
    carried forward, so a cataloged ticker is never lost.

    Args:
        series:       series ticker, e.g. "KXCPIYOY"
        save:         write {series}_tickers.json/.txt atomically
        out_dir:      catalog directory (default TICKERS_DIR)
        legacy_probe: also query the pre-KX spelling ("CPIYOY"). The KX series
                      returns the legacy events today; the probe is insurance.
        now:          build timestamp override (tests)

    Returns:
        The per-series dict (see module docstring), plus "removed_upstream":
        tickers of the previous file that are gone and were never opened.

    Raises:
        CatalogValidationError: an event listed as open has no active market
            in the result. Nothing is written.
        kalshi_io.client.KalshiAPIError: a request failed. Nothing is written.
    """
    out_dir = out_dir or TICKERS_DIR
    spellings = [series]
    if legacy_probe and series.startswith("KX"):
        spellings.append(series[2:])

    events: dict[str, str] = {}
    markets: dict[str, dict] = {}
    for spelling in spellings:
        for e in discovery.list_events(spelling):
            events.setdefault(e["event_ticker"], e.get("title") or "")
        for m in discovery.list_historical_markets(series_ticker=spelling):
            markets[m["ticker"]] = _market_record(m, "historical")
        for m in discovery.list_markets(series_ticker=spelling):
            markets[m["ticker"]] = _market_record(m, "live")

    # Events that only show up on markets
    for event_ticker in sorted({m["event_ticker"] for m in markets.values()} - set(events)):
        event = discovery.get_event(event_ticker)
        fallback = next(m["title"] for m in markets.values() if m["event_ticker"] == event_ticker)
        events[event_ticker] = (event or {}).get("title") or fallback

    # Events the series listings returned no market for
    covered = {m["event_ticker"] for m in markets.values()}
    for event_ticker in sorted(set(events) - covered):
        for m in discovery.list_historical_markets(event_ticker=event_ticker):
            markets.setdefault(m["ticker"], _market_record(m, "historical"))
        for m in discovery.list_markets(event_ticker=event_ticker):
            markets[m["ticker"]] = _market_record(m, "live")

    # Never lose a ticker the previous catalog had
    previous = load_series(series, out_dir) or {}
    carried: list[str] = []
    removed_upstream: list[str] = []
    old_markets = {m["market_ticker"]: m for m in previous.get("markets", [])}
    missing = sorted(set(old_markets) - set(markets))
    if missing:
        found = discovery.lookup_markets(missing)
        for ticker in missing:
            if ticker in found:
                source = "live" if found[ticker]["tier"] == "live" else "historical"
                markets[ticker] = _market_record(found[ticker], source)
            elif old_markets[ticker].get("status") in _OPENED_STATUSES:
                markets[ticker] = {**dict.fromkeys(MARKET_FIELDS), **old_markets[ticker], "source": "carried_forward"}
                carried.append(ticker)
            else:
                removed_upstream.append(ticker)
        for ticker in carried:
            events.setdefault(markets[ticker]["event_ticker"], markets[ticker].get("title") or "")
        if carried:
            logger.warning(f"{series}: {len(carried)} cataloged tickers are no longer returned by the API; carried forward")

    # Oracle: what the API calls open must be open in the result
    active_events = {m["event_ticker"] for m in markets.values() if m["status"] == "active"}
    open_events = [e["event_ticker"] for e in discovery.list_events(series, status="open")]
    without_active = sorted(set(open_events) - active_events)
    if without_active:
        raise CatalogValidationError(
            f"{series}: the API lists {len(open_events)} open events but the discovered catalog has no "
            f"active market for {without_active}; refusing to write"
        )

    market_list = sorted(markets.values(), key=lambda m: (m["event_ticker"], m["market_ticker"]))
    try:
        cutoff = discovery.get_historical_cutoff().get("market_settled_ts")
    except Exception:
        cutoff = None
    result = {
        "schema_version": SCHEMA_VERSION,
        "series": series,
        "built_at": _utc_stamp(now),
        "historical_cutoff": cutoff,
        "status_counts": status_counts(market_list),
        "events": [{"event_ticker": t, "title": events[t]} for t in sorted(events)],
        "markets": market_list,
        "tickers": sorted(markets),
    }
    if save:
        write_series(result, out_dir)
    return {**result, "removed_upstream": removed_upstream, "carried_forward": carried}


def write_series(result: dict, out_dir: Path | None = None) -> None:
    """Write {series}_tickers.json and .txt atomically (temp file + rename)."""
    out_dir = out_dir or TICKERS_DIR
    series = result["series"]
    stored = {k: v for k, v in result.items() if k not in ("removed_upstream", "carried_forward")}
    _atomic_write(out_dir / f"{series}_tickers.json", json.dumps(stored, indent=2))
    tickers = stored["tickers"]
    _atomic_write(out_dir / f"{series}_tickers.txt", "\n".join(tickers) + ("\n" if tickers else ""))


def build_combined(
    out_dir: Path | None = None,
    *,
    save: bool = True,
    now: datetime | None = None,
    catalog: dict[str, dict] | None = None,
) -> dict:
    """
    Combine every per-series file into all_tickers.json / all_tickers.txt.

    Idempotent: only files whose "series" is a string are series files, so
    all_tickers.json itself is never read back as one.

    Args:
        out_dir: catalog directory (default TICKERS_DIR)
        save:    write the two combined files atomically
        now:     build timestamp override (tests)
        catalog: use this {series: dict} instead of reading the directory
                 (dry runs)

    Returns:
        {"schema_version", "built_at", "oldest_series_built_at", "series",
         "total_events", "total_markets", "status_counts", "tickers",
         "by_series": {S: {"events", "markets", "built_at", "status_counts", "tickers"}}}
    """
    out_dir = out_dir or TICKERS_DIR
    catalog = load_catalog(out_dir) if catalog is None else catalog

    combined = {
        "schema_version": SCHEMA_VERSION,
        "built_at": _utc_stamp(now),
        "oldest_series_built_at": None,
        "series": [],
        "total_events": 0,
        "total_markets": 0,
        "status_counts": {},
        "tickers": [],
        "by_series": {},
    }
    all_tickers: set[str] = set()
    all_markets: list[dict] = []
    stamps: list[str] = []

    for series in sorted(catalog):
        data = catalog[series]
        combined["series"].append(series)
        combined["total_events"] += len(data["events"])
        combined["total_markets"] += len(data["markets"])
        combined["by_series"][series] = {
            "events": len(data["events"]),
            "markets": len(data["markets"]),
            "built_at": data.get("built_at"),
            "status_counts": status_counts(data["markets"]),
            "tickers": data["tickers"],
        }
        all_tickers.update(data["tickers"])
        all_markets.extend(data["markets"])
        if data.get("built_at"):
            stamps.append(data["built_at"])

    combined["tickers"] = sorted(all_tickers)
    combined["status_counts"] = status_counts(all_markets)
    combined["oldest_series_built_at"] = min(stamps) if stamps else None

    if save:
        _atomic_write(out_dir / "all_tickers.json", json.dumps(combined, indent=2))
        _atomic_write(out_dir / "all_tickers.txt", "\n".join(combined["tickers"]) + "\n")
    return combined


# ============================================================
# Comparing and refreshing
# ============================================================

def _totals(catalog: dict[str, dict]) -> dict:
    markets = [m for data in catalog.values() for m in data.get("markets", [])]
    return {
        "series": len(catalog),
        "events": sum(len(d.get("events", [])) for d in catalog.values()),
        "markets": len(markets),
        "status_counts": status_counts(markets),
    }


def diff_catalog(old: dict[str, dict], new: dict[str, dict]) -> dict:
    """
    Compare two catalogs ({series: per-series dict}; v1 and v2 both work).

    Returns:
        {"totals": {"old", "new"},
         "new_events":      [{series, event_ticker, title, n_markets}],
         "removed_events":  [{series, event_ticker}],
         "new_markets":     [{series, event_ticker, market_ticker, status, in_new_event}],
         "removed_markets": [{series, event_ticker, market_ticker}],
         "status_changes":  [{series, event_ticker, market_ticker, old, new}],
         "carried_forward": [market_ticker]}
    """
    def events_of(cat):
        return {e["event_ticker"]: (s, e) for s, d in cat.items() for e in d.get("events", [])}

    def markets_of(cat):
        return {m["market_ticker"]: (s, m) for s, d in cat.items() for m in d.get("markets", [])}

    old_events, new_events = events_of(old), events_of(new)
    old_markets, new_markets = markets_of(old), markets_of(new)
    added_events = set(new_events) - set(old_events)

    per_event: dict[str, int] = {}
    for _, m in new_markets.values():
        per_event[m["event_ticker"]] = per_event.get(m["event_ticker"], 0) + 1

    return {
        "totals": {"old": _totals(old), "new": _totals(new)},
        "new_events": [
            {"series": new_events[t][0], "event_ticker": t, "title": new_events[t][1].get("title", ""),
             "n_markets": per_event.get(t, 0)}
            for t in sorted(added_events)
        ],
        "removed_events": [
            {"series": old_events[t][0], "event_ticker": t} for t in sorted(set(old_events) - set(new_events))
        ],
        "new_markets": [
            {"series": new_markets[t][0], "event_ticker": new_markets[t][1]["event_ticker"], "market_ticker": t,
             "status": new_markets[t][1].get("status"), "in_new_event": new_markets[t][1]["event_ticker"] in added_events}
            for t in sorted(set(new_markets) - set(old_markets))
        ],
        "removed_markets": [
            {"series": old_markets[t][0], "event_ticker": old_markets[t][1]["event_ticker"], "market_ticker": t}
            for t in sorted(set(old_markets) - set(new_markets))
        ],
        "status_changes": [
            {"series": new_markets[t][0], "event_ticker": new_markets[t][1]["event_ticker"], "market_ticker": t,
             "old": old_markets[t][1].get("status"), "new": new_markets[t][1].get("status")}
            for t in sorted(set(old_markets) & set(new_markets))
            if old_markets[t][1].get("status") != new_markets[t][1].get("status")
        ],
        "carried_forward": sorted(
            t for t, (_, m) in new_markets.items() if m.get("source") == "carried_forward"
        ),
    }


def refresh_catalog(
    series_list: list[str] | None = None,
    *,
    out_dir: Path | None = None,
    dry_run: bool = False,
    now: datetime | None = None,
) -> dict:
    """
    Rediscover the given series (default SERIES_LIST), rebuild the combined
    files and report what changed since the previous build.

    A series that fails keeps its previous file and is reported; the others
    are still written. With dry_run nothing is written at all.

    Returns:
        {"built_at", "previous_built_at": (iso | None, basis), "series_ok",
         "series_failed": {series: error}, "diff" (see diff_catalog),
         "combined" (see build_combined), "removed_upstream": [tickers],
         "checks": [{"name", "ok", "detail"}], "api_requests", "elapsed_sec"}
    """
    out_dir = out_dir or TICKERS_DIR
    series_list = list(series_list or SERIES_LIST)
    started = datetime.now(timezone.utc)
    requests_before = client.stats["requests"]

    previous_stamp, basis = catalog_built_at(out_dir)
    old = load_catalog(out_dir)
    new = dict(old)
    ok: list[str] = []
    failed: dict[str, str] = {}
    removed_upstream: list[str] = []

    for series in series_list:
        try:
            result = discover_series(series, save=not dry_run, out_dir=out_dir, now=now)
        except Exception as e:
            failed[series] = f"{type(e).__name__}: {e}"
            logger.error(f"{series}: discovery failed, previous file kept ({failed[series]})")
            continue
        removed_upstream += result.pop("removed_upstream")
        result.pop("carried_forward")
        new[series] = result
        ok.append(series)
        logger.info(f"{series}: {len(result['events'])} events, {len(result['markets'])} markets "
                    f"{result['status_counts']}")

    combined = build_combined(out_dir, save=not dry_run, now=now, catalog=new)
    diff = diff_catalog(old, new)

    lost = sorted({m["market_ticker"] for m in diff["removed_markets"]} - set(removed_upstream))
    checks = [
        {"name": "every series refreshed", "ok": not failed,
         "detail": "; ".join(f"{s}: {err}" for s, err in failed.items()) or f"{len(ok)} series"},
        {"name": "active markets present", "ok": combined["status_counts"].get("active", 0) > 0,
         "detail": f"{combined['status_counts'].get('active', 0)} active markets"},
        {"name": "no previously cataloged ticker lost", "ok": not lost,
         "detail": f"lost: {lost[:10]}" if lost else
                   f"{len(removed_upstream)} never-opened tickers removed upstream"},
    ]
    return {
        "built_at": combined["built_at"],
        "previous_built_at": (previous_stamp.strftime("%Y-%m-%dT%H:%M:%SZ") if previous_stamp else None, basis),
        "series_ok": ok,
        "series_failed": failed,
        "diff": diff,
        "combined": combined,
        "removed_upstream": sorted(removed_upstream),
        "checks": checks,
        "api_requests": client.stats["requests"] - requests_before,
        "elapsed_sec": round((datetime.now(timezone.utc) - started).total_seconds(), 1),
    }
