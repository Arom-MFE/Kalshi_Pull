"""
pull_historical/backfill.py — Bulk backfill over the whole catalog, or over a ticker list.

Runs the layers one after the other over all tickers: market metadata, daily
candles, hourly candles, trades, minute candles (the expensive one last).
Within a layer the tickers are walked event by event in priority order: events
that can still trade first (nearest close first), then settled events, newest
close first. Every layer goes through the existing pullers, so storage,
dedupe, resume and failure handling are theirs.

Before it starts it prints an estimate: requests per layer, runtime at the
configured request rate, expected rows and disk. `--estimate-only` stops
there and makes no request.

Resumable and idempotent:
    - every puller resumes from the last stored row, so a stopped run
      continues where it was with the same command
    - DATA_DIR/state/backfill_journal.jsonl records every (ticker, layer)
      that was pulled completely while the market was finalized. Such a pair
      can never change again, so a rerun skips it without a request. Pairs of
      markets that can still trade are not journaled; they cost one cheap
      resume request per rerun
    - the committed catalog is read, never written; refresh it with
      get_ticker_info/roll.py

Failures: a ticker that fails is retried once at the end of its layer. What
still fails is written to DATA_DIR/logs/backfill_failed_{stamp}_{layer}.txt,
one ticker per line, and `--retry-failed` runs exactly those again. When the
API is down (three tickers in a row ran out of retries) the run waits 1, 2, 4,
8 and 16 minutes, retrying in between, and then exits with code 2.

CLI:
    python -m pull_historical.backfill                      # the whole catalog
    python -m pull_historical.backfill --estimate-only
    python -m pull_historical.backfill --tickers KXCPIYOY-26SEP-T3.0 RECSSNBER-23
    python -m pull_historical.backfill --layers daily,hourly
    python -m pull_historical.backfill --retry-failed

Exit codes: 0 complete · 1 some tickers failed (see the failure lists) ·
2 stopped because the API was down · 75 another run holds the lock (a second
full-catalog run, or --lock-name) · 130 interrupted (Ctrl+C finishes the
current ticker, a second Ctrl+C stops at once)
"""

import argparse
import json
import math
import signal
import sys
import time
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

# Ensure kalshi_io is importable when running as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from kalshi_io import catalog, client, config, metadata
from kalshi_io.candles import MarketWindow, register_market_windows, resolve_ticker_meta
from kalshi_io.config import CHUNK_SECONDS, MAX_CONSECUTIVE_OUTAGES, MAX_REQUESTS_PER_SECOND, TICKERS_DIR
from kalshi_io.discovery import POLLABLE_BUCKETS, status_bucket
from kalshi_io.resolve import candle_end_ts
from kalshi_io.runlog import get_logger, run_logging
from kalshi_io.storage import (
    LockTimeout, atomic_write_text, duckdb_connect, file_lock, get_last_timestamp, get_output_path, named_lock,
)
from kalshi_io.tickers import load_tickers, validate_tickers

from pull_historical import pull_hourly, pull_minute, pull_trades
from pull_historical.pull_daily import run as run_daily
from pull_historical.pull_hourly import run as run_hourly
from pull_historical.pull_minute import run as run_minute
from pull_historical.pull_trades import run as run_trades

logger = get_logger("backfill")

LAYERS: tuple[str, ...] = ("metadata", "daily", "hourly", "trades", "minute")
CANDLE_INTERVALS = {"daily": 1440, "hourly": 60, "minute": 1}

EXIT_OK, EXIT_FAILED, EXIT_API_DOWN, EXIT_LOCKED, EXIT_INTERRUPTED = 0, 1, 2, 75, 130

# A run over the whole catalog holds this lock, so it cannot be started twice by accident
FULL_RUN_LOCK = "backfill_full"

# Waits between attempts while the API is down (three tickers in a row ran out of retries)
OUTAGE_WAITS_S: tuple[int, ...] = (60, 120, 240, 480, 960)

METADATA_CHUNK = 500
JOURNAL_VERSION = 1

# Puller outcomes that mean "this pair is complete up to the end of its window"
_DONE = ("ok", "up_to_date", "empty")

# Rows per market-day and bytes on disk, measured 2026-09-17 on sampled markets
# of every year and on the 978 files of the previous store. Candles are sparse
# (a candle exists only when something happened), so the range is wide.
ROWS_PER_MARKET_DAY = {"daily": (0.3, 1.0), "hourly": (1.0, 13.0), "minute": (3.0, 85.0), "trades": (0.5, 6.0)}
BYTES_PER_FILE = {"daily": 13_000, "hourly": 14_500, "minute": 14_000, "trades": 5_000}
BYTES_PER_ROW = {"daily": 15.0, "hourly": 6.6, "minute": 5.6, "trades": 27.0}


def _sleep(seconds: float) -> None:
    time.sleep(seconds)


def _stamp(fmt: str = "%Y%m%d_%H%M%S") -> str:
    return datetime.now(timezone.utc).strftime(fmt)


class ApiDown(RuntimeError):
    """The API stayed down through every wait in OUTAGE_WAITS_S."""


# ============================================================
# What to pull
# ============================================================

@dataclass
class Item:
    """One market of the run, with what is known about its life."""
    ticker: str
    series: str = ""
    event: str = ""
    status: str = ""                 # API status: catalog snapshot first, refreshed by the metadata layer
    open_ts: int | None = None       # Unix seconds
    close_ts: int | None = None
    tier: str = ""                   # "live", "historical" or "" (unknown)
    source: str = ""                 # catalog source; "carried_forward" = the API no longer returns it
    fresh: bool = False              # status read from the API in this run

    @property
    def tradable(self) -> bool:
        return status_bucket(self.status) in POLLABLE_BUCKETS

    @property
    def window(self) -> dict:
        return {"open_ts": self.open_ts, "close_ts": self.close_ts, "status": self.status}


def _iso_ts(value) -> int | None:
    from kalshi_io.candles import iso_to_ts
    return iso_to_ts(value)


def build_items(tickers: list[str]) -> tuple[list[Item], list[str]]:
    """
    Items for the cataloged tickers (no request), plus the tickers the catalog
    does not hold (they need the API and are resolved when the run starts).
    """
    index = catalog.market_index()
    items: list[Item] = []
    uncataloged: list[str] = []
    for ticker in tickers:
        record = index.get(ticker)
        if record is None:
            uncataloged.append(ticker)
            continue
        source = record.get("source") or ""
        items.append(Item(
            ticker=ticker, series=record["series"], event=record.get("event_ticker") or ticker,
            status=record.get("status") or "", open_ts=_iso_ts(record.get("open_time")),
            close_ts=_iso_ts(record.get("close_time")),
            tier=source if source in ("live", "historical") else "", source=source,
        ))
    return items, uncataloged


def order_batches(items: list[Item]) -> list[list[Item]]:
    """
    Event batches in priority order: events with a market that can still
    trade first, nearest close first; then settled events, newest close first.
    """
    by_event: dict[str, list[Item]] = {}
    for item in items:
        by_event.setdefault(item.event or item.ticker, []).append(item)

    def priority(batch: list[Item]):
        closes = [i.close_ts for i in batch if i.close_ts is not None]
        if any(i.tradable for i in batch):
            return (0, min(closes) if closes else math.inf, batch[0].event)
        return (1, -(max(closes) if closes else 0), batch[0].event)

    return [sorted(batch, key=lambda i: i.ticker) for batch in sorted(by_event.values(), key=priority)]


# ============================================================
# What is already there
# ============================================================

def stored_files(layer: str, item: Item) -> list[Path]:
    """The parquet files a (ticker, layer) pair has on disk."""
    root = config.DATA_DIR
    if layer == "daily":
        path = get_output_path("candles", 1440, item.series, item.ticker)
        return [path] if path.exists() else []
    if layer == "hourly":
        return sorted((root / "candles" / "hourly" / item.series).glob(f"*/{item.ticker}.parquet"))
    if layer == "minute":
        return sorted((root / "candles" / "minute" / item.series).glob(f"*/*/{item.ticker}.parquet"))
    if layer == "trades":
        return sorted(p for p in (root / "trades" / item.series / item.ticker).glob("*.parquet")
                      if not p.name.endswith(".tmp.parquet"))
    return []


def last_stored_ts(layer: str, item: Item) -> int | None:
    """Unix seconds of the newest stored row of a (ticker, layer) pair, or None."""
    if layer == "daily":
        last_ms = get_last_timestamp(get_output_path("candles", 1440, item.series, item.ticker))
    elif layer == "hourly":
        last_ms = pull_hourly._get_last_hourly_ts(item.series, item.ticker)
    elif layer == "minute":
        last_ms = pull_minute._get_last_minute_ts(item.series, item.ticker)
    else:
        last_ms = pull_trades._get_last_trade_ts(item.series, item.ticker)
    return None if last_ms is None else last_ms // 1000


class Journal:
    """
    Append-only record of finished (ticker, layer) pairs that can never change
    again: DATA_DIR/state/backfill_journal.jsonl, one JSON object per line.

        {"v": 1, "type": "final", "ticker", "layer", "at", "rows", "has_files"}
        {"v": 1, "type": "run", ...}      one per driver process, for reports

    A pair is written only when it was pulled completely while the market was
    finalized (a terminal status). `has_files` records whether the pair has
    files on disk: a market that never traded has none, and that is its
    complete state. A rerun skips a pair while the disk still agrees with the
    journal; files that went missing are pulled again. Lines that cannot be
    parsed (a torn write) are ignored.
    """

    def __init__(self, path: Path, ignore: bool = False):
        self.path = path
        self.final: dict[tuple[str, str], bool] = {}
        if ignore or not path.exists():
            return
        for line in path.read_text().splitlines():
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if isinstance(record, dict) and record.get("v") == JOURNAL_VERSION and record.get("type") == "final":
                self.final[(record["ticker"], record["layer"])] = bool(record.get("has_files"))

    @classmethod
    def load(cls, ignore: bool = False) -> "Journal":
        return cls(config.DATA_DIR / "state" / "backfill_journal.jsonl", ignore=ignore)

    def is_done(self, layer: str, item: Item) -> bool:
        key = (item.ticker, layer)
        return key in self.final and self.final[key] == bool(stored_files(layer, item))

    def add_final(self, layer: str, item: Item, rows: int) -> None:
        has_files = bool(stored_files(layer, item))
        self.final[(item.ticker, layer)] = has_files
        self._append({"v": JOURNAL_VERSION, "type": "final", "ticker": item.ticker, "layer": layer,
                      "at": metadata.utc_stamp(), "rows": rows, "has_files": has_files})

    def add_run(self, record: dict) -> None:
        self._append({"v": JOURNAL_VERSION, "type": "run", **record})

    def _append(self, record: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # A torn last line (kill -9 mid-write) must not swallow this record
        with file_lock(self.path):                        # the poller's history pull may journal at the same time
            torn = self.path.exists() and self.path.stat().st_size > 0 and not self.path.read_bytes().endswith(b"\n")
            with open(self.path, "a") as f:
                f.write(("\n" if torn else "") + json.dumps(record, sort_keys=True) + "\n")


# ============================================================
# Estimate
# ============================================================

def _months_spanned(start_ts: int, end_ts: int) -> int:
    a, b = (datetime.fromtimestamp(t, timezone.utc) for t in (start_ts, end_ts))
    return (b.year - a.year) * 12 + b.month - a.month + 1


def estimate(items: list[Item], journal: Journal, layers: tuple[str, ...], now_ts: int) -> dict:
    """
    Requests, market-days and files per layer for what is left to do. No
    request is made: windows come from the catalog, progress from the journal
    and from the files on disk.

    Returns:
        {layer: {"tickers", "skipped_final", "requests", "market_days", "files"}}
    """
    out = {layer: {"tickers": 0, "skipped_final": 0, "requests": 0, "market_days": 0.0, "files": 0}
           for layer in layers}

    if "metadata" in out:
        stored = settled_in_store()
        need = [i for i in items if i.ticker not in stored]
        # live lookup per 100 tickers, a historical one for what the live tier lacks, event flags per series
        n_hist = sum(1 for i in need if i.tier != "live")
        out["metadata"].update(
            tickers=len(need), skipped_final=len(items) - len(need),
            requests=math.ceil(len(need) / 100) + math.ceil(n_hist / 100) + 2 * len({i.series for i in need}),
        )

    for item in items:
        if item.source == "carried_forward":
            continue
        for layer in layers:
            if layer == "metadata":
                continue
            row = out[layer]
            if journal.is_done(layer, item):
                row["skipped_final"] += 1
                continue
            row["tickers"] += 1
            if item.open_ts is None:
                continue
            interval = CANDLE_INTERVALS.get(layer)
            end = candle_end_ts(item.window, interval or 1, now_ts)
            last = last_stored_ts(layer, item)
            start = item.open_ts if last is None else last
            if end <= start:
                continue
            days = (end - start) / 86400
            row["market_days"] += days
            if interval is None:                         # trades: both tiers on a cold start, one on a resume
                row["requests"] += 2 if last is None else 1
                row["files"] += _months_spanned(start, end)
            else:
                row["requests"] += math.ceil((end - start) / CHUNK_SECONDS[interval]) + (0 if item.tier else 1)
                row["files"] += {1440: 1, 60: (_months_spanned(start, end) + 11) // 12, 1: _months_spanned(start, end)}[interval]
    return out


def settled_in_store() -> set[str]:
    """Tickers whose stored metadata row is final: finalized with a result. Nothing about them changes anymore."""
    stored = metadata.load_market_metadata()
    if stored is None:
        return set()
    done = stored[(stored["status"] == "finalized") & stored["result"].notna()]
    return set(done["market_ticker"])


def _hours(seconds: float) -> str:
    return f"{seconds / 3600:.1f} h" if seconds >= 3600 else f"{seconds / 60:.0f} min"


def format_estimate(est: dict, n_items: int, n_uncataloged: int, rps: float) -> str:
    lines = [
        f"Backfill estimate for {n_items:,} cataloged tickers (data root: {config.DATA_DIR})",
        f"{'layer':<10}{'tickers':>9}{'final, skipped':>16}{'requests':>11}{'runtime':>10}"
        f"{'rows, low to high':>26}{'disk':>16}",
    ]
    total_requests = 0
    total_disk = [0.0, 0.0]
    for layer, row in est.items():
        total_requests += row["requests"]
        rows_text = disk_text = ""
        if layer in ROWS_PER_MARKET_DAY:
            low, high = (row["market_days"] * d for d in ROWS_PER_MARKET_DAY[layer])
            disk = [row["files"] * BYTES_PER_FILE[layer] + n * BYTES_PER_ROW[layer] for n in (low, high)]
            total_disk = [a + b for a, b in zip(total_disk, disk)]
            rows_text = f"{low:,.0f} to {high:,.0f}"
            disk_text = f"{disk[0] / 1e6:,.0f} to {disk[1] / 1e6:,.0f} MB"
        lines.append(f"{layer:<10}{row['tickers']:>9,}{row['skipped_final']:>16,}{row['requests']:>11,}"
                     f"{_hours(row['requests'] / rps):>10}{rows_text:>26}{disk_text:>16}")
    lines += [
        f"{'TOTAL':<10}{'':>9}{'':>16}{total_requests:>11,}{_hours(total_requests / rps):>10}"
        f"{'':>26}{f'{total_disk[0] / 1e6:,.0f} to {total_disk[1] / 1e6:,.0f} MB':>16}",
        f"Runtime is a lower bound at {rps:g} requests/s (KALSHI_MAX_RPS): request starts are spaced, so a "
        f"response slower than {1 / rps:.2f} s slows the run; at 80 percent of the rate it takes "
        f"{_hours(total_requests / (0.8 * rps))}. Trades add one request per 1,000 trades of a market.",
        "Rows are a range: candles are sparse, 0.3 to 1 daily, 1 to 13 hourly and 3 to 85 minute candles per "
        "market-day were measured.",
    ]
    if n_uncataloged:
        lines.append(f"{n_uncataloged:,} tickers are not in the catalog; they are resolved through the API when "
                     f"the run starts and are not part of this estimate.")
    return "\n".join(lines)


def _poller_note(max_age_s: int = 900) -> str:
    """A hint when poll_focus seems to be running: both processes share one keyless rate limit."""
    snapshot = config.DATA_DIR / "logs" / "focus_universe_live.json"
    try:
        age = time.time() - snapshot.stat().st_mtime
    except OSError:
        return ""
    if age > max_age_s:
        return ""
    return (f"Note: poll_focus wrote its universe {age / 60:.0f} min ago and seems to be running. Both processes share "
            f"the keyless limit of about 5 requests/s: a full poller takes about 2 of them, a books-only poller "
            f"(--no-daily --no-hourly --no-minute --no-trades) next to nothing. Expect 429 retries, or run this "
            f"with KALSHI_MAX_RPS=3.")


# ============================================================
# Running
# ============================================================

@dataclass
class LayerReport:
    layer: str
    done: dict[str, str] = field(default_factory=dict)          # ticker → ok / up_to_date / empty
    skipped_final: int = 0
    failed: dict[str, str] = field(default_factory=dict)        # ticker → error
    other: dict[str, str] = field(default_factory=dict)         # ticker → unknown / skipped / gone_upstream
    not_attempted: list[str] = field(default_factory=list)
    rows: int = 0
    requests: int = 0
    elapsed_sec: float = 0.0


@dataclass
class State:
    journal: Journal
    items: dict[str, Item]
    stop_requested: bool = False
    outage_streak: int = 0

    def should_stop(self) -> bool:
        return self.stop_requested


PULLERS = {"daily": lambda *a, **k: run_daily(*a, **k), "hourly": lambda *a, **k: run_hourly(*a, **k),
           "trades": lambda *a, **k: run_trades(*a, **k), "minute": lambda *a, **k: run_minute(*a, **k)}


def _wait_out_the_outage(state: State, waits, what: str) -> None:
    wait = next(waits, None)
    if wait is None:
        raise ApiDown(f"the API stayed down through {len(OUTAGE_WAITS_S)} waits ({sum(OUTAGE_WAITS_S) // 60} minutes); "
                      f"stopped at {what}. Run the same command again to resume.")
    logger.error(f"API outage at {what}: {state.outage_streak} tickers in a row ran out of retries; "
                 f"waiting {wait // 60} min before the next attempt")
    for _ in range(wait):
        if state.stop_requested:
            return
        _sleep(1)
    # The streak is NOT reset here: only a ticker that succeeds ends an outage. If the
    # retry fails again the next, longer wait follows at once instead of walking on
    # through the catalog at six attempts per ticker.


def pull_batch(layer: str, tickers: list[str], state: State, report: LayerReport, what: str) -> None:
    """
    Pull one batch of tickers through the layer's puller and sort the outcomes
    into the report. An outage (the streak of tickers that ran out of retries
    reaches MAX_CONSECUTIVE_OUTAGES, counted across batches) waits and retries
    what was not finished; ApiDown ends the run when the waits are used up.
    """
    pending = list(tickers)
    waits = iter(OUTAGE_WAITS_S)
    while pending and not state.stop_requested:
        results: dict = {}
        PULLERS[layer](pending, results=results, should_stop=state.should_stop)

        unfinished: list[str] = []
        for ticker in pending:
            outcome = results.get(ticker) or {"status": "not_attempted", "rows": 0, "error": None, "outage": False}
            status = outcome["status"]
            report.rows += outcome.get("rows") or 0
            if status in _DONE:
                state.outage_streak = 0
                report.done[ticker] = status
                report.failed.pop(ticker, None)
                item = state.items[ticker]
                if item.status == "finalized":
                    state.journal.add_final(layer, item, outcome.get("rows") or 0)
            elif status == "failed":
                report.failed[ticker] = outcome.get("error") or "failed"
                if outcome.get("outage"):
                    state.outage_streak += 1
                    unfinished.append(ticker)
                else:
                    state.outage_streak = 0
            elif status == "not_attempted":
                unfinished.append(ticker)
            else:                                           # unknown to the API, or no open time
                report.other[ticker] = status

        if state.stop_requested:
            report.not_attempted += [t for t in unfinished if t not in report.failed]
            return
        if unfinished and state.outage_streak >= MAX_CONSECUTIVE_OUTAGES:
            _wait_out_the_outage(state, waits, what)        # raises ApiDown when the waits are used up
            pending = unfinished
            continue
        # An isolated exhausted retry stays in report.failed for the retry at the end of the layer
        report.not_attempted += [t for t in unfinished if t not in report.failed]
        return


def run_metadata_layer(state: State, report: LayerReport, tickers: list[str]) -> None:
    """Refresh the metadata store for these tickers and take fresh status,
    times and tier from the answers, so nothing below relies on a stale catalog."""
    stored = metadata.load_market_metadata()
    settled: dict[str, dict] = {}
    if stored is not None:
        final = stored[(stored["status"] == "finalized") & stored["result"].notna()]
        settled = {row["market_ticker"]: row for row in final.to_dict("records")}

    # A row that is finalized with a result never changes: take it from the store, not from the API
    for ticker in tickers:
        row = settled.get(ticker)
        if row is None:
            continue
        item = state.items[ticker]
        item.status, item.fresh = "finalized", True
        for column, attr in (("open_ts_ms", "open_ts"), ("close_ts_ms", "close_ts")):
            if row.get(column) is not None and not _is_missing(row[column]):
                setattr(item, attr, int(row[column]) // 1000)
        if row.get("tier") in ("live", "historical"):
            item.tier = row["tier"]
    need = [t for t in tickers if t not in settled]
    report.skipped_final += len(tickers) - len(need)

    flags: dict = {}
    for start in range(0, len(need), METADATA_CHUNK):
        if state.stop_requested:
            report.not_attempted += need[start:]
            break
        chunk = need[start:start + METADATA_CHUNK]
        try:
            summary = metadata.refresh_market_metadata(chunk, event_flags=flags)
        except Exception as e:
            reason = f"{type(e).__name__}: {e}"
            logger.error(f"metadata: {len(chunk)} tickers failed ({reason})")
            report.failed.update(dict.fromkeys(chunk, reason))
            continue
        for ticker in chunk:
            payload = summary["markets"].get(ticker)
            if payload is None:
                report.other[ticker] = "gone_upstream"
                continue
            report.done[ticker] = "ok"
            report.failed.pop(ticker, None)
            item = state.items[ticker]
            item.status, item.fresh = payload.get("status") or item.status, True
            item.open_ts = _iso_ts(payload.get("open_time")) or item.open_ts
            item.close_ts = _iso_ts(payload.get("close_time")) or item.close_ts
            item.tier = payload.get("tier") or item.tier
            item.event = payload.get("event_ticker") or item.event
            item.series = summary["series"].get(ticker) or item.series
        report.rows += summary["found"]
        logger.info(f"metadata: {min(start + METADATA_CHUNK, len(need)):,}/{len(need):,} tickers read")

    register_market_windows({
        i.ticker: MarketWindow(i.open_ts, i.close_ts, i.status, i.tier) for i in state.items.values() if i.fresh
    })


def _is_missing(value) -> bool:
    try:
        return value != value or value is None             # NaN / pd.NA-safe enough for ints from the store
    except TypeError:
        return True


def run_layer(layer: str, batches: list[list[Item]], state: State, est_requests: int) -> LayerReport:
    report = LayerReport(layer)
    t0, r0 = time.time(), client.stats["requests"]
    if layer == "metadata":
        run_metadata_layer(state, report, [i.ticker for batch in batches for i in batch])
        if report.failed and not state.stop_requested:
            retry = sorted(report.failed)
            logger.info(f"metadata: retrying {len(retry)} failed tickers once")
            run_metadata_layer(state, report, retry)
    else:
        n_tickers = sum(len(b) for b in batches)
        seen = 0
        for k, batch in enumerate(batches, 1):
            if state.stop_requested:
                report.not_attempted += [i.ticker for b in batches[k - 1:] for i in b]
                break
            todo = []
            for item in batch:
                if item.source == "carried_forward":
                    report.other[item.ticker] = "gone_upstream"
                elif state.journal.is_done(layer, item):
                    report.skipped_final += 1
                else:
                    todo.append(item.ticker)
            seen += len(batch)
            if not todo:
                continue
            rows_before, failed_before = report.rows, len(report.failed)
            pull_batch(layer, todo, state, report, what=f"{layer} {batch[0].event}")
            used = client.stats["requests"] - r0
            rate = used / max(time.time() - t0, 1e-9)
            left = max(est_requests - used, 0)
            eta = f"ETA {_hours(left / rate)}" if rate > 0 and est_requests else "ETA n/a"
            logger.info(
                f"[{layer}] event {k}/{len(batches)} {batch[0].event}: {len(todo)} tickers, "
                f"{report.rows - rows_before:,} new rows, {len(report.failed) - failed_before} failed | "
                f"tickers {seen:,}/{n_tickers:,} | requests {used:,} of about {est_requests:,} | "
                f"{rate:.1f} req/s | {eta}")
        if report.failed and not state.stop_requested:
            retry = sorted(report.failed)
            logger.info(f"[{layer}] retrying {len(retry)} failed tickers once")
            pull_batch(layer, retry, state, report, what=f"{layer} retry")

    report.requests = client.stats["requests"] - r0
    report.elapsed_sec = round(time.time() - t0, 1)
    return report


# ============================================================
# Reporting
# ============================================================

def store_counts() -> dict[str, dict]:
    """Files and rows per kind in the data root, counted by DuckDB (nothing is loaded into memory)."""
    kinds = {"daily": "candles/daily", "hourly": "candles/hourly", "minute": "candles/minute",
             "trades": "trades", "orderbook": "orderbook", "metadata": "metadata"}
    counts: dict[str, dict] = {}
    con = duckdb_connect()
    try:
        for kind, sub in kinds.items():
            base = config.DATA_DIR / sub
            files = [p for p in base.rglob("*.parquet")] if base.exists() else []
            rows = 0
            if files:
                rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{base.as_posix()}/**/*.parquet')").fetchone()[0]
            counts[kind] = {"files": len(files), "rows": int(rows)}
    finally:
        con.close()
    return counts


def write_failure_lists(reports: list[LayerReport], stamp: str) -> list[Path]:
    """logs/backfill_failed_{stamp}_{layer}.txt per layer with failures, a valid --tickers file."""
    paths = []
    for report in reports:
        if not report.failed:
            continue
        path = config.DATA_DIR / "logs" / f"backfill_failed_{stamp}_{report.layer}.txt"
        header = (f"# {len(report.failed)} tickers that failed in the {report.layer} layer, {metadata.utc_stamp()}\n"
                  f"# retry with: python -m pull_historical.backfill --retry-failed\n")
        atomic_write_text(path, header + "\n".join(sorted(report.failed)) + "\n")
        paths.append(path)
    return paths


def newest_failure_lists() -> dict[str, Path]:
    """{layer: path} of the most recent run that wrote failure lists."""
    found: dict[str, dict[str, Path]] = {}
    for path in (config.DATA_DIR / "logs").glob("backfill_failed_*_*.txt"):
        stamp, _, layer = path.stem.removeprefix("backfill_failed_").rpartition("_")
        if layer in LAYERS:
            found.setdefault(stamp, {})[layer] = path
    return found[max(found)] if found else {}


def format_summary(reports: list[LayerReport], counts: dict, stats: dict, elapsed: float, exit_code: int) -> str:
    lines = ["", "Backfill summary",
             f"{'layer':<10}{'done':>8}{'final, skipped':>16}{'failed':>8}{'other':>7}{'not tried':>11}"
             f"{'new rows':>12}{'requests':>10}{'elapsed':>10}"]
    for r in reports:
        lines.append(f"{r.layer:<10}{len(r.done):>8,}{r.skipped_final:>16,}{len(r.failed):>8,}{len(r.other):>7,}"
                     f"{len(r.not_attempted):>11,}{r.rows:>12,}{r.requests:>10,}{_hours(r.elapsed_sec):>10}")
    share = stats["http_429"] / stats["requests"] if stats["requests"] else 0.0
    lines += [
        f"Requests: {stats['requests']:,} in {_hours(elapsed)} ({stats['requests'] / max(elapsed, 1e-9):.2f}/s), "
        f"429 answers: {stats['http_429']:,} ({share:.2%}), retries: {stats['retries']:,}",
        "Store now holds: " + ", ".join(f"{k} {v['rows']:,} rows in {v['files']:,} files" for k, v in counts.items()),
    ]
    for r in reports:
        for ticker, reason in sorted(r.failed.items())[:20]:
            lines.append(f"  FAILED {r.layer} {ticker}: {reason}")
        if len(r.failed) > 20:
            lines.append(f"  ... and {len(r.failed) - 20} more in the failure list")
    lines.append({EXIT_OK: "Result: complete", EXIT_FAILED: "Result: some tickers failed; retry with --retry-failed",
                  EXIT_API_DOWN: "Result: stopped because the API was down; run the same command again to resume",
                  EXIT_INTERRUPTED: "Result: interrupted; run the same command again to resume"}[exit_code])
    return "\n".join(lines)


# ============================================================
# CLI
# ============================================================

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bulk backfill over the catalog: metadata, daily, hourly, trades, minute.")
    parser.add_argument("--tickers", nargs="+", default=None,
                        help="Ticker source(s): .txt/.json path, series name, 'focus', or tickers separated by spaces "
                             "or commas. Default: every cataloged ticker")
    parser.add_argument("--layers", default=",".join(LAYERS),
                        help=f"Comma-separated subset of {','.join(LAYERS)}; they always run in that order")
    parser.add_argument("--estimate-only", action="store_true", help="Print the estimate and stop; no request is made")
    parser.add_argument("--retry-failed", action="store_true",
                        help="Run only the tickers of the newest failure lists, each in the layer it failed in")
    parser.add_argument("--ignore-journal", action="store_true",
                        help="Do not skip pairs the journal calls final (they are pulled again and resume from disk)")
    parser.add_argument("--no-audit", action="store_true", help="Skip the data-quality checks after the run")
    parser.add_argument("--log-name", default="backfill", help="Prefix of the log file in DATA_DIR/logs")
    parser.add_argument("--lock-name", default=None,
                        help=f"Hold DATA_DIR/.locks/NAME.lock for the run and exit {EXIT_LOCKED} if another process "
                             f"holds it. A run over the whole catalog always holds {FULL_RUN_LOCK!r}")
    return parser


def _parse_layers(text: str) -> tuple[str, ...]:
    wanted = [part.strip() for part in text.split(",") if part.strip()]
    bad = [name for name in wanted if name not in LAYERS]
    if bad or not wanted:
        raise ValueError(f"unknown layer(s) {bad}: choose from {', '.join(LAYERS)}")
    return tuple(layer for layer in LAYERS if layer in wanted)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point; see the module docstring for the exit codes."""
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        layers = _parse_layers(args.layers)
    except ValueError as e:
        parser.error(str(e))

    per_layer_tickers: dict[str, list[str]] | None = None
    if args.retry_failed:
        lists = newest_failure_lists()
        if not lists:
            print("backfill: no failure list found in the logs directory; nothing to retry")
            return EXIT_OK
        per_layer_tickers = {layer: load_tickers(str(path)) for layer, path in lists.items()}
        layers = tuple(layer for layer in LAYERS if layer in per_layer_tickers)
        tickers = sorted({t for ts in per_layer_tickers.values() for t in ts})
    else:
        tickers = load_tickers(args.tickers or str(TICKERS_DIR / "all_tickers.txt"))

    items, uncataloged = build_items(tickers)
    journal = Journal.load(ignore=args.ignore_journal)
    est = estimate(items, journal, layers, int(time.time()))
    print(format_estimate(est, len(items), len(uncataloged), MAX_REQUESTS_PER_SECOND))
    note = _poller_note()
    if note:
        print(note)
    if args.estimate_only:
        return EXIT_OK

    lock_name = args.lock_name or (FULL_RUN_LOCK if args.tickers is None and not args.retry_failed else None)
    try:
        with named_lock(lock_name) if lock_name else nullcontext():
            with run_logging(args.log_name, stamp_fmt="%Y%m%d_%H%M%S") as log_path:
                return _run(args, layers, items, uncataloged, journal, est, per_layer_tickers, log_path)
    except LockTimeout:
        print(f"backfill: another run holds the lock {lock_name!r} on {config.DATA_DIR}; not starting a second one",
              file=sys.stderr)
        return EXIT_LOCKED


def _run(args, layers, items, uncataloged, journal, est, per_layer_tickers, log_path: Path) -> int:
    stamp = _stamp()
    logger.info(f"backfill starting: {len(items):,} cataloged tickers, layers {', '.join(layers)} "
                f"(data root: {config.DATA_DIR}, {MAX_REQUESTS_PER_SECOND:g} requests/s)")

    unknown: list[str] = []
    if uncataloged:
        known, unknown = validate_tickers(uncataloged)
        for ticker in unknown:
            logger.warning(f"{ticker}: unknown ticker (not in the catalog, not found on the API); left out")
        for ticker in known:
            try:
                series, event = resolve_ticker_meta(ticker)
            except Exception as e:                       # validation kept it because the API was down
                logger.warning(f"{ticker}: could not be resolved ({type(e).__name__}: {e}); the pullers will try again")
                series, event = "", ""
            items.append(Item(ticker=ticker, series=series or "", event=event or ticker))

    state = State(journal=journal, items={i.ticker: i for i in items})
    previous = {}

    def on_signal(signum, _frame):
        if state.stop_requested:                          # second signal: stop at once
            signal.signal(signum, previous.get(signum, signal.SIG_DFL))
            raise KeyboardInterrupt
        state.stop_requested = True
        logger.info(f"signal {signum}: stopping after the current ticker (again to stop at once)")

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            previous[sig] = signal.signal(sig, on_signal)
        except ValueError:                                # not the main thread
            pass

    t0 = time.time()
    stats0 = dict(client.stats)
    reports: list[LayerReport] = []
    exit_code = EXIT_OK
    try:
        for layer in layers:
            if state.stop_requested:
                break
            layer_items = list(state.items.values())
            if per_layer_tickers is not None:
                layer_items = [state.items[t] for t in per_layer_tickers[layer] if t in state.items]
            batches = order_batches(layer_items)
            logger.info(f"=== layer {layer}: {len(layer_items):,} tickers in {len(batches):,} events, "
                        f"about {est[layer]['requests']:,} requests ===")
            reports.append(run_layer(layer, batches, state, est[layer]["requests"]))
            last = reports[-1]
            logger.info(f"=== layer {layer} done: {len(last.done):,} pulled, {last.skipped_final:,} final and skipped, "
                        f"{len(last.failed):,} failed, {last.rows:,} new rows, {last.requests:,} requests, "
                        f"{_hours(last.elapsed_sec)} ===")
    except ApiDown as e:
        logger.error(str(e))
        exit_code = EXIT_API_DOWN
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)

    if exit_code == EXIT_OK:
        if state.stop_requested:
            exit_code = EXIT_INTERRUPTED
        elif any(r.failed for r in reports):
            exit_code = EXIT_FAILED

    failure_paths = write_failure_lists(reports, stamp)
    elapsed = time.time() - t0
    stats = {k: client.stats[k] - stats0.get(k, 0) for k in client.stats}
    counts = store_counts()
    text = format_summary(reports, counts, stats, elapsed, exit_code)
    if unknown:
        text += f"\nUnknown tickers, left out: {', '.join(unknown)}"
    for path in failure_paths:
        text += f"\nFailure list: {path}"

    if not args.no_audit and exit_code in (EXIT_OK, EXIT_FAILED):
        text += "\n" + _audit_text()

    print(text)
    for line in text.splitlines():
        logger.info(line)
    summary = {
        "stamp": stamp, "exit_code": exit_code, "elapsed_sec": round(elapsed, 1), "layers": list(layers),
        "requests": stats["requests"], "http_429": stats["http_429"], "retries": stats["retries"],
        "reports": [{"layer": r.layer, "done": len(r.done), "skipped_final": r.skipped_final, "failed": r.failed,
                     "other": r.other, "not_attempted": len(r.not_attempted), "rows": r.rows,
                     "requests": r.requests, "elapsed_sec": r.elapsed_sec} for r in reports],
        "store": counts,
    }
    atomic_write_text(config.DATA_DIR / "logs" / f"backfill_summary_{stamp}.json", json.dumps(summary, indent=2) + "\n")
    journal.add_run({k: v for k, v in summary.items() if k not in ("reports", "store")})
    return exit_code


def _audit_text() -> str:
    """The data-quality checks of pull_audit over the store, as text (counts only)."""
    try:
        from kalshi_io import quality
    except ImportError:
        return ""
    try:
        return quality.format_report(quality.run_checks())
    except Exception as e:                                # a report must never turn a finished download into a failure
        logger.error(f"data-quality checks failed: {type(e).__name__}: {e}")
        return f"Data-quality checks could not run: {type(e).__name__}: {e}"


if __name__ == "__main__":
    sys.exit(main())
