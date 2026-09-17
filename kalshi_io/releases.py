"""
kalshi_io/releases.py — When to poll fast: windows around economic releases.

A release moves every market that depends on it: the CPI print settles the
CPI strikes and repricing runs through the Fed strikes within seconds. Books
cannot be backfilled, so poll_focus polls them every few seconds inside a
window around each release and every minute outside.

The anchor is close_time. Kalshi closes a market shortly before the number it
settles on comes out: the BLS series close at 12:29Z for a 12:30Z print, the
Fed series a few minutes before the 18:00Z statement (observed 2026-09-17).
Release times come from three sources and are merged:
    - the close_time of every event the poller is polling
    - the close_time of every cataloged market that can still trade (grouped
      by event), so a PCE, GDP or CPI release counts even when its own markets
      are not polled
    - config.RELEASE_CALENDAR, extra ISO-8601 UTC times
A window is [t - before, t + after]; windows that overlap are merged.
"""

from datetime import datetime, timezone

from kalshi_io import config
from kalshi_io.discovery import POLLABLE_BUCKETS, status_bucket

# Releases further ahead than this are not scheduled (the catalog holds year-end closes into the 2030s)
HORIZON_S = 90 * 86400


def _parse(value) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def release_times(
    universe: dict | None,
    catalog_index: dict[str, dict] | None,
    calendar: list[str] | None,
    now_ts: float,
    after_s: float,
) -> list[tuple[float, str]]:
    """
    Distinct release times (Unix seconds) with a label, from now - after_s to
    now + HORIZON_S, sorted. Times that coincide share one entry whose label
    lists every event.
    """
    found: dict[float, set[str]] = {}

    def add(ts: float | None, label: str) -> None:
        if ts is not None and now_ts - after_s <= ts <= now_ts + HORIZON_S:
            found.setdefault(ts, set()).add(label)

    for group in (universe or {}).get("groups", []):
        add(_parse(group.get("close_time")), group.get("event_ticker") or "?")

    per_event: dict[str, float] = {}
    for record in (catalog_index or {}).values():
        if status_bucket(record.get("status")) not in POLLABLE_BUCKETS:
            continue
        close = _parse(record.get("close_time"))
        event = record.get("event_ticker") or record.get("market_ticker")
        if close is not None and (event not in per_event or close < per_event[event]):
            per_event[event] = close
    for event, close in per_event.items():
        add(close, event)

    for i, value in enumerate(calendar or []):
        ts = _parse(value)
        if ts is None:
            raise ValueError(f"RELEASE_CALENDAR[{i}] is not an ISO-8601 time: {value!r}")
        add(ts, f"calendar:{value}")

    return [(ts, ", ".join(sorted(labels))) for ts, labels in sorted(found.items())]


def windows(times: list[tuple[float, str]], before_s: float, after_s: float) -> list[tuple[float, float, str]]:
    """[(start, end, label), ...] sorted, overlapping windows merged."""
    out: list[list] = []
    for ts, label in sorted(times):
        start, end = ts - before_s, ts + after_s
        if out and start <= out[-1][1]:
            out[-1][1] = max(out[-1][1], end)
            out[-1][2] = f"{out[-1][2]}; {label}"
        else:
            out.append([start, end, label])
    return [tuple(w) for w in out]


def merge_windows(
    existing: list[tuple[float, float, str]],
    fresh: list[tuple[float, float, str]],
    now_ts: float,
) -> list[tuple[float, float, str]]:
    """
    The windows to keep after a recomputation: every fresh window plus every
    existing one that has not ended yet, overlaps merged. A window must never
    disappear while it is open: once an event closes and the poller rolls to
    the next one, its close_time is gone from the universe, but the release it
    marks is about to happen and the other markets react to it.
    """
    kept = [w for w in existing if w[1] >= now_ts]
    out: list[list] = []
    for start, end, label in sorted(kept + list(fresh)):
        if out and start <= out[-1][1]:
            out[-1][1] = max(out[-1][1], end)
            if label not in out[-1][2]:
                out[-1][2] = f"{out[-1][2]}; {label}"
        else:
            out.append([start, end, label])
    return [tuple(w) for w in out]


def active_window(wins: list[tuple[float, float, str]], now_ts: float) -> tuple[float, float, str] | None:
    """The window that contains now, if any."""
    for start, end, label in wins:
        if start <= now_ts <= end:
            return start, end, label
    return None


def next_window(wins: list[tuple[float, float, str]], now_ts: float) -> tuple[float, float, str] | None:
    """The first window that starts after now, if any."""
    for start, end, label in wins:
        if start > now_ts:
            return start, end, label
    return None


def describe(wins: list[tuple[float, float, str]], now_ts: float, limit: int = 5) -> list[str]:
    """Human-readable lines for the next windows."""
    lines = []
    for start, end, label in [w for w in wins if w[1] >= now_ts][:limit]:
        a, b = (datetime.fromtimestamp(t, timezone.utc) for t in (start, end))
        lines.append(f"{a:%Y-%m-%d %H:%M}Z to {b:%H:%M}Z  {label}")
    return lines


def default_calendar() -> list[str]:
    return list(config.RELEASE_CALENDAR)
