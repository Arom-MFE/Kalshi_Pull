"""
kalshi_io/metadata.py — The market metadata store: one row per market.

DATA_DIR/metadata/markets.parquet holds what the API says about every market
the pipeline knows: the strike, the times, the status, and once a market has
settled its result, its settlement value and the value it settled on. The
candle and trade files say what a market traded at; this file says what the
market was.

The store is refreshed from API market payloads, never from ticker text:
    get_ticker_info/roll.py        every catalog refresh (same payloads, no
                                   extra request), so results fill in as
                                   markets settle
    pull_historical/backfill.py    refresh_market_metadata() for the tickers
                                   of a run, without touching the catalog
A refresh replaces the rows of the markets it saw and keeps every other row,
so a market the API stopped returning keeps its last known state.

Columns (METADATA_COLUMNS, fixed order):
    market_ticker, event_ticker, series_ticker     identifiers
    title, yes_sub_title, no_sub_title, market_type
    strike_type          greater, greater_or_equal, less, less_or_equal,
                         between, functional, custom, structured; null when
                         the API sends none (single-outcome and some 2022
                         markets)
    floor_strike, cap_strike      float64; null when not applicable
    custom_strike        JSON text of the API object, e.g. {"Cut": "25"}
    functional_strike    text
    mutually_exclusive   the event's flag: at most one market resolves yes
    open_ts_ms, close_ts_ms, expected_expiration_ts_ms, expiration_ts_ms,
    latest_expiration_ts_ms, settlement_ts_ms
                         int64 UTC milliseconds like every ts_ms in the
                         store, from open_time, close_time, ... settlement_ts;
                         null when the API sent none. expiration_time is
                         deprecated by Kalshi and may disappear
    status               API status when the row was built (active, ...,
                         finalized)
    result               yes, no, scalar; null until determined (the API's "")
    settlement_value     dollars paid per YES contract, float64; null until
                         determined (API: settlement_value_dollars)
    expiration_value     the value the market settled on, as text ("3.4",
                         "No recession"); null until known
    can_close_early, early_close_condition
    rules_primary, rules_secondary                 the rules text
    volume, open_interest, last_price              float64, as of built_at
    tier                 live, historical (settled before the cutoff) or
                         carried_forward (no longer returned by the API)
    built_at             UTC time this row was read from the API; for a
                         catalog refresh it is the catalog's built_at
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from kalshi_io import config, discovery
from kalshi_io.candles import resolve_ticker_meta
from kalshi_io.runlog import get_logger

logger = get_logger("metadata")

METADATA_COLUMNS: tuple[str, ...] = (
    "market_ticker", "event_ticker", "series_ticker",
    "title", "yes_sub_title", "no_sub_title", "market_type",
    "strike_type", "floor_strike", "cap_strike", "custom_strike", "functional_strike",
    "mutually_exclusive",
    "open_ts_ms", "close_ts_ms", "expected_expiration_ts_ms", "expiration_ts_ms",
    "latest_expiration_ts_ms", "settlement_ts_ms",
    "status", "result", "settlement_value", "expiration_value",
    "can_close_early", "early_close_condition",
    "rules_primary", "rules_secondary",
    "volume", "open_interest", "last_price",
    "tier", "built_at",
)

# API time field → stored column
_TIME_FIELDS = {
    "open_time": "open_ts_ms",
    "close_time": "close_ts_ms",
    "expected_expiration_time": "expected_expiration_ts_ms",
    "expiration_time": "expiration_ts_ms",
    "latest_expiration_time": "latest_expiration_ts_ms",
    "settlement_ts": "settlement_ts_ms",
}
_INT_COLUMNS = tuple(_TIME_FIELDS.values())
_FLOAT_COLUMNS = ("floor_strike", "cap_strike", "settlement_value", "volume", "open_interest", "last_price")
_BOOL_COLUMNS = ("mutually_exclusive", "can_close_early")
_STRING_COLUMNS = tuple(c for c in METADATA_COLUMNS if c not in _INT_COLUMNS + _FLOAT_COLUMNS + _BOOL_COLUMNS)

_SORT_COLUMNS = ["series_ticker", "event_ticker", "market_ticker"]


def metadata_path() -> Path:
    """DATA_DIR/metadata/markets.parquet (resolved at call time)."""
    return config.DATA_DIR / "metadata" / "markets.parquet"


def utc_stamp(now: datetime | None = None) -> str:
    return (now or datetime.now(timezone.utc)).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def iso_to_ms(value) -> int | None:
    """API ISO time ("2026-08-12T13:15:19.078753Z") → int UTC milliseconds. Missing or unparsable → None."""
    if not value:
        return None
    try:
        return int(round(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1000))
    except ValueError:
        return None


def _float(value) -> float | None:
    """API decimal string or number → float. None or "" stays missing, never 0."""
    if value is None or value == "":
        return None
    return float(value)


def _text(value) -> str | None:
    """API string → str; the API's empty string means "none yet" and is stored as null."""
    if value is None or value == "":
        return None
    return str(value)


# ============================================================
# Rows
# ============================================================

def market_row(
    payload: dict,
    *,
    series: str | None,
    tier: str,
    built_at: str,
    mutually_exclusive: bool | None = None,
) -> dict:
    """
    One metadata row from an API market payload (GET /markets, /markets/{t},
    /historical/markets: the same schema on both tiers).

    Args:
        payload:            the market dict as sent by the API
        series:             series ticker (market payloads carry none)
        tier:               "live" or "historical"
        built_at:           UTC stamp of the request, "YYYY-MM-DDTHH:MM:SSZ"
        mutually_exclusive: the flag of the market's event, if known
    """
    custom = payload.get("custom_strike")
    row = {
        "market_ticker": payload["ticker"],
        "event_ticker": _text(payload.get("event_ticker")),
        "series_ticker": _text(series),
        "title": _text(payload.get("title")),
        "yes_sub_title": _text(payload.get("yes_sub_title")),
        "no_sub_title": _text(payload.get("no_sub_title")),
        "market_type": _text(payload.get("market_type")),
        "strike_type": _text(payload.get("strike_type")),
        "floor_strike": _float(payload.get("floor_strike")),
        "cap_strike": _float(payload.get("cap_strike")),
        "custom_strike": json.dumps(custom, sort_keys=True) if custom else None,
        "functional_strike": _text(payload.get("functional_strike")),
        "mutually_exclusive": None if mutually_exclusive is None else bool(mutually_exclusive),
        "status": _text(payload.get("status")),
        "result": _text(payload.get("result")),
        "settlement_value": _float(payload.get("settlement_value_dollars")),
        "expiration_value": _text(payload.get("expiration_value")),
        "can_close_early": payload.get("can_close_early"),
        "early_close_condition": _text(payload.get("early_close_condition")),
        "rules_primary": _text(payload.get("rules_primary")),
        "rules_secondary": _text(payload.get("rules_secondary")),
        "volume": _float(payload.get("volume_fp")),
        "open_interest": _float(payload.get("open_interest_fp")),
        "last_price": _float(payload.get("last_price_dollars")),
        "tier": tier,
        "built_at": built_at,
    }
    for api_field, column in _TIME_FIELDS.items():
        row[column] = iso_to_ms(payload.get(api_field))
    return row


def catalog_row(record: dict, *, series: str, built_at: str) -> dict:
    """
    Minimal row for a cataloged market the API no longer returns
    (source "carried_forward"): identifiers, status and times from the catalog
    record, everything else missing.
    """
    row = dict.fromkeys(METADATA_COLUMNS)
    row.update({
        "market_ticker": record["market_ticker"],
        "event_ticker": _text(record.get("event_ticker")),
        "series_ticker": series,
        "title": _text(record.get("title")),
        "status": _text(record.get("status")),
        "open_ts_ms": iso_to_ms(record.get("open_time")),
        "close_ts_ms": iso_to_ms(record.get("close_time")),
        "expected_expiration_ts_ms": iso_to_ms(record.get("expected_expiration_time")),
        "latest_expiration_ts_ms": iso_to_ms(record.get("latest_expiration_time")),
        "tier": "carried_forward",
        "built_at": built_at,
    })
    return row


def metadata_frame(rows: list[dict] | pd.DataFrame) -> pd.DataFrame:
    """
    The stored DataFrame: METADATA_COLUMNS in order, with fixed dtypes (text as
    nullable string, times as nullable Int64, numbers as float64, flags as
    nullable boolean), so a column that is entirely missing keeps its type.
    """
    df = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(list(rows), columns=list(METADATA_COLUMNS))
    df = df.reindex(columns=list(METADATA_COLUMNS))
    for col in _STRING_COLUMNS:
        df[col] = df[col].astype("string")
    for col in _INT_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in _FLOAT_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    for col in _BOOL_COLUMNS:
        df[col] = df[col].astype("boolean")
    return df


# ============================================================
# Store
# ============================================================

def load_market_metadata() -> pd.DataFrame | None:
    """The metadata store, or None if it does not exist yet."""
    path = metadata_path()
    if not path.exists():
        return None
    return metadata_frame(pd.read_parquet(path, engine="pyarrow"))


def upsert_market_metadata(rows: list[dict], *, keep_existing: list[dict] | None = None) -> dict:
    """
    Merge rows into the metadata store and write it atomically.

    Rows replace the stored rows of the same market_ticker; every other stored
    row is kept as it is.

    Args:
        rows:          rows from market_row() for the markets just read from the API
        keep_existing: rows from catalog_row() for markets the API no longer
                       returns; each is written only if the store has no row
                       for that market yet, so a last known state is never
                       overwritten by a thinner one

    Returns:
        {"path", "rows" (stored after the merge), "added", "updated", "kept"}
    """
    path = metadata_path()
    existing = load_market_metadata()
    fresh = metadata_frame(rows)
    fresh = fresh.drop_duplicates(subset="market_ticker", keep="last")

    known = set(existing["market_ticker"]) if existing is not None else set()
    fallback = [r for r in (keep_existing or [])
                if r["market_ticker"] not in known and r["market_ticker"] not in set(fresh["market_ticker"])]
    parts = [fresh]
    if fallback:
        parts.append(metadata_frame(fallback))
    if existing is not None:
        parts.insert(0, existing[~existing["market_ticker"].isin(set(fresh["market_ticker"]))])
    parts = [part for part in parts if not part.empty]

    combined = metadata_frame(pd.concat(parts, ignore_index=True) if parts else [])
    combined = combined.sort_values(_SORT_COLUMNS, na_position="last").reset_index(drop=True)

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp.parquet")
    combined.to_parquet(tmp, engine="pyarrow", compression="zstd", index=False)
    os.replace(tmp, path)

    updated = len(set(fresh["market_ticker"]) & known)
    return {
        "path": path,
        "rows": len(combined),
        "added": len(fresh) - updated + len(fallback),
        "updated": updated,
        "kept": len(combined) - len(fresh) - len(fallback),
    }


# Up to this many events of one series are looked up one by one
# (GET /events/{event}); more than that and one listing of the series is cheaper
_EVENT_LOOKUPS_PER_SERIES = 2


def refresh_market_metadata(
    tickers: list[str],
    *,
    now: datetime | None = None,
    event_flags: dict[str, bool | None] | None = None,
) -> dict:
    """
    Read the given markets from the API and merge them into the store.

    Requests: GET /markets?tickers= in batches of 100, then
    GET /historical/markets?tickers= for what the live tier did not return,
    plus the events' mutually_exclusive flag: GET /events/{event} for a
    series with one or two events in the batch, one GET /events?series_ticker=
    listing otherwise. The committed catalog is not touched.

    Args:
        tickers:     market tickers
        now:         built_at override (tests)
        event_flags: {event_ticker: mutually_exclusive} cache shared between
                     calls, so a caller working in chunks lists a series once

    Returns:
        {"requested", "found", "missing": [tickers in neither tier], "markets":
         {ticker: payload with "tier"}, "series": {ticker: series},
         **upsert_market_metadata() result}

    Raises:
        kalshi_io.client.KalshiAPIError: a request failed; nothing is written.
    """
    wanted = list(dict.fromkeys(tickers))
    built_at = utc_stamp(now)
    found = discovery.lookup_markets(wanted)

    series_of: dict[str, str | None] = {}
    for ticker in found:
        try:
            series_of[ticker] = resolve_ticker_meta(ticker)[0]
        except Exception:
            series_of[ticker] = resolve_ticker_meta(ticker, allow_api=False)[0]

    flags = event_flags if event_flags is not None else {}
    events_by_series: dict[str, set[str]] = {}
    for ticker, payload in found.items():
        event = payload.get("event_ticker")
        if event and event not in flags and series_of[ticker]:
            events_by_series.setdefault(series_of[ticker], set()).add(event)
    for series, events in sorted(events_by_series.items()):
        if len(events) <= _EVENT_LOOKUPS_PER_SERIES:
            for event_ticker in sorted(events):
                flags[event_ticker] = (discovery.get_event(event_ticker) or {}).get("mutually_exclusive")
        else:
            for event in discovery.list_events(series):
                flags[event["event_ticker"]] = event.get("mutually_exclusive")
            for event_ticker in events:
                flags.setdefault(event_ticker, None)          # not listed (delisted duplicate): unknown

    rows = [
        market_row(payload, series=series_of[ticker], tier=payload["tier"], built_at=built_at,
                   mutually_exclusive=flags.get(payload.get("event_ticker")))
        for ticker, payload in found.items()
    ]
    missing = [t for t in wanted if t not in found]
    if missing:
        logger.warning(f"metadata: {len(missing)} of {len(wanted)} tickers are in neither API tier: {missing[:10]}")
    summary = upsert_market_metadata(rows) if rows else {"path": metadata_path(), "rows": 0, "added": 0, "updated": 0, "kept": 0}
    return {"requested": len(wanted), "found": len(found), "missing": missing, "markets": found,
            "series": series_of, **summary}
