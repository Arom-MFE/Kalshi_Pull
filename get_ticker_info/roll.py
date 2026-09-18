"""
get_ticker_info/roll.py — Roll the pipeline forward to the current event cycle.

Refreshes the ticker catalog for the configured series, reports what changed
since the last build, and proposes the next focus universe with the same rule
poll_focus applies at startup (kalshi_io/universe.py). Keyless: public REST
endpoints only. About 120 requests and a minute or two for all 15 series.

USAGE:
    python get_ticker_info/roll.py                        # refresh, write, report
    python get_ticker_info/roll.py --dry-run              # report only, write nothing
    python get_ticker_info/roll.py --series KXCPIYOY KXFED
    python get_ticker_info/roll.py --events-per-series 2  # propose two cycles per series
    python get_ticker_info/roll.py --out-dir path/to/a/catalog/copy

WRITES (nothing with --dry-run):
    {out-dir}/{SERIES}_tickers.json/.txt, all_tickers.json/.txt   the catalog
    DATA_DIR/metadata/markets.parquet     one row per market: strikes, times,
                                          status, result, settlement value,
                                          rules (kalshi_io/metadata.py); every
                                          roll fills in what has settled since
    {out-dir}/focus_universe.json/.txt    the proposed universe; the pullers
                                          read the .txt as `--tickers focus`
    DATA_DIR/logs/roll_{stamp}.log and roll_report_{stamp}.txt

A series that fails keeps its previous files; the others are still written.

EXIT CODES: 0 every series refreshed and every check passed; 1 otherwise
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure kalshi_io is importable when running as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from kalshi_io import catalog, universe
from kalshi_io.config import DATA_DIR, PROJECT_ROOT, SERIES_LIST, TICKERS_DIR
from kalshi_io.runlog import configure_logging, get_logger, run_logging
from kalshi_io.storage import atomic_write_text
from kalshi_io.universe import UniverseError

logger = get_logger("roll")

# Lines shown per report section before "... and N more" (--full shows all)
SECTION_LIMIT = 40


# ============================================================
# Report
# ============================================================

def _section(title: str, lines: list[str], limit: int | None) -> list[str]:
    if not lines:
        return []
    shown = lines if limit is None else lines[:limit]
    out = ["", f"{title} ({len(lines)})"] + [f"  {line}" for line in shown]
    if len(shown) < len(lines):
        out.append(f"  ... and {len(lines) - len(shown)} more (--full shows all)")
    return out


def _display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


def _previous_build_line(previous: tuple[str | None, str], now: datetime) -> str:
    stamp, basis = previous
    if stamp is None:
        return "none (first build in this directory)"
    age_days = (now - datetime.fromisoformat(stamp.replace("Z", "+00:00"))).total_seconds() / 86400
    note = "" if basis == "built_at" else " (file modification time: a git checkout resets it, treat as a hint)"
    return f"{stamp}, {age_days:.1f} days ago{note}"


def _metadata_line(stored: dict, dry_run: bool) -> str:
    """What the refresh did to DATA_DIR/metadata/markets.parquet."""
    if stored.get("written"):
        return (f"{stored['rows']:,} rows in {_display_path(stored['path'])} "
                f"({stored['updated']:,} refreshed, {stored['added']:,} new, {stored['kept']:,} kept)")
    if dry_run:
        return f"{stored.get('rows', 0):,} rows would be refreshed"
    return "not written"


def focus_checks(report: dict, focus: dict | None, focus_error: str | None) -> list[dict]:
    """Checks on the proposed universe, in the same shape as refresh_catalog's."""
    if focus is None:
        return [{"name": "focus universe can be polled", "ok": False, "detail": focus_error or "not derived"}]

    counts = ", ".join(f"{k}={v}" for k, v in focus["status_counts"].items())
    checks = [{"name": "focus universe can be polled", "ok": True,
               "detail": f"{len(focus['tickers'])} tickers in {len(focus['groups'])} events ({counts})"}]

    # The universe (open-markets snapshot) and the catalog (unfiltered listing)
    # come from different requests; for a refreshed series they must agree.
    cataloged = set(report["combined"]["tickers"])
    refreshed = set(report["series_ok"])
    expected = [t for g in focus["groups"] if g.get("series") in refreshed for t in g["tickers"]]
    missing = [t for t in expected if t not in cataloged]
    checks.append({
        "name": "focus tickers are in the refreshed catalog", "ok": not missing,
        "detail": f"missing: {missing[:10]}" if missing else f"{len(expected)} of {len(expected)}",
    })
    return checks


def format_report(
    report: dict,
    focus: dict | None,
    checks: list[dict],
    *,
    out_dir: Path,
    dry_run: bool,
    limit: int | None = SECTION_LIMIT,
    now: datetime | None = None,
) -> str:
    """Render the roll report: header, totals, what changed, the proposed
    focus universe, the checks, and the commands to run next."""
    now = now or datetime.now(timezone.utc)
    diff = report["diff"]
    old, new = diff["totals"]["old"], diff["totals"]["new"]

    lines = [
        f"Kalshi_Pull roll, {report['built_at']}" + ("  (dry run: nothing was written)" if dry_run else ""),
        f"Catalog dir:     {_display_path(out_dir)}",
        f"Previous build:  {_previous_build_line(report['previous_built_at'], now)}",
        f"API requests:    {report['api_requests']} in {report['elapsed_sec']} s",
        f"Market metadata: {_metadata_line(report.get('metadata') or {}, dry_run)}",
        "",
        f"{'TOTALS':<12}{'before':>10}{'after':>10}",
    ]
    for key in ("series", "events", "markets"):
        lines.append(f"  {key:<10}{old[key]:>10,}{new[key]:>10,}")
    lines.append("  by status now: " + (", ".join(f"{k}={v:,}" for k, v in new["status_counts"].items()) or "none"))

    lines += _section("FAILED SERIES, previous files kept",
                      [f"{s}: {err}" for s, err in report["series_failed"].items()], None)

    lines += _section("NEW EVENTS", [
        f"{e['series']:<15} {e['event_ticker']:<24} {e['n_markets']:>4} markets  {e['title']}".rstrip()
        for e in diff["new_events"]], limit)

    by_event: dict[str, list[str]] = {}
    for m in diff["new_markets"]:
        if not m["in_new_event"]:
            by_event.setdefault(m["event_ticker"], []).append(m["market_ticker"])
    lines += _section("NEW MARKETS IN EXISTING EVENTS", [
        f"{event}: +{len(tickers)}  " + ", ".join(tickers[:6]) + (" ..." if len(tickers) > 6 else "")
        for event, tickers in sorted(by_event.items())], limit)

    transitions: dict[tuple[str, str, str], int] = {}
    for c in diff["status_changes"]:
        key = (c["event_ticker"], c["old"] or "unknown", c["new"] or "unknown")
        transitions[key] = transitions.get(key, 0) + 1
    lines += _section("STATUS CHANGES BY EVENT", [
        f"{event}: {n} markets {before} -> {after}" for (event, before, after), n in sorted(transitions.items())], limit)

    lines += _section("REMOVED UPSTREAM, never opened", report["removed_upstream"], limit)
    lines += _section("CARRIED FORWARD, no longer returned by the API", diff["carried_forward"], limit)

    lines += ["", "PROPOSED FOCUS UNIVERSE"]
    if focus is None:
        lines.append("  none (see CHECKS)")
    else:
        lines.append(f"  {len(focus['tickers'])} tickers, {focus['source']}: {focus['rule']}")
        for g in focus["groups"]:
            lines.append(f"  {g.get('series') or '-':<15} {g['event_ticker']:<24} closes {g.get('close_time') or 'unknown':<21}"
                         f"{len(g['tickers']):>4} markets  {g.get('title') or ''}".rstrip())
        for ticker, bucket in sorted((focus.get("dropped") or {}).items()):
            lines.append(f"  dropped: {ticker} ({bucket})")
        for warning in focus.get("warnings") or []:
            lines.append(f"  warning: {warning}")
        if not dry_run:
            lines.append(f"  written to {_display_path(out_dir / universe.FOCUS_TXT)}; "
                         f"every puller reads it as `--tickers focus`")

    lines += ["", "CHECKS"]
    lines += [f"  [{'ok' if c['ok'] else 'FAIL'}] {c['name']}: {c['detail']}" for c in checks]

    lines += ["", "NEXT STEPS"]
    if dry_run:
        lines.append("  python get_ticker_info/roll.py                      # same run, written to disk")
    else:
        lines.append(f"  git diff --stat {_display_path(out_dir)}   # review, then commit the catalog")
    if focus is not None:
        lines += [
            "  python -m pull_historical.backfill --tickers focus   # metadata, daily, hourly, trades, minute from "
            "market open; resumes; the API has no depth limit",
            "  python -m pull_live.poll_focus                       # derives the same universe itself and pulls "
            "missing history in the background",
        ]
    return "\n".join(lines)


# ============================================================
# CLI
# ============================================================

def _roll(args, out_dir: Path) -> tuple[int, str]:
    report = catalog.refresh_catalog(args.series, out_dir=out_dir, dry_run=args.dry_run)

    checks = list(report["checks"])
    focus = None
    if not args.no_focus:
        focus_error = None
        try:
            focus = universe.check_universe(universe.build_universe(None, None, args.events_per_series))
        except UniverseError as e:
            focus_error = str(e)
        except Exception as e:
            focus_error = f"{type(e).__name__}: {e}"
            logger.error(f"focus universe could not be derived: {focus_error}")
        checks += focus_checks(report, focus, focus_error)
        if focus is not None and not args.dry_run:
            universe.write_universe(focus, out_dir / universe.FOCUS_JSON, out_dir / universe.FOCUS_TXT)

    text = format_report(report, focus, checks, out_dir=out_dir, dry_run=args.dry_run,
                         limit=None if args.full else SECTION_LIMIT)
    return (0 if all(c["ok"] for c in checks) else 1), text


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Exit code 1 if a series or a check failed."""
    parser = argparse.ArgumentParser(
        description="Refresh the ticker catalog, report what changed, propose the next focus universe.")
    parser.add_argument("--series", nargs="+", default=None,
                        help=f"Series to refresh (default: all {len(SERIES_LIST)} in kalshi_io.config.SERIES_LIST)")
    parser.add_argument("--events-per-series", type=int, default=None,
                        help="Nearest open events per focus series to propose (default FOCUS_EVENTS_PER_SERIES)")
    parser.add_argument("--dry-run", action="store_true", help="Report only; write nothing")
    parser.add_argument("--out-dir", default=None, help="Catalog directory to read and write (default: the committed one)")
    parser.add_argument("--no-focus", action="store_true", help="Refresh the catalog only; propose no focus universe")
    parser.add_argument("--full", action="store_true", help="Do not shorten long report sections")
    try:
        args = parser.parse_args(argv)
    except SystemExit as e:
        return int(e.code or 0)

    out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else TICKERS_DIR

    if args.dry_run:
        configure_logging()
        code, text = _roll(args, out_dir)
        print(text)
        return code

    with run_logging("roll", stamp_fmt="%Y%m%d_%H%M%S") as log_path:
        logger.info(f"roll starting (catalog dir: {out_dir}, data root: {DATA_DIR})")
        code, text = _roll(args, out_dir)
        print(text)
        report_path = log_path.with_name(f"roll_report_{log_path.stem.removeprefix('roll_')}.txt")
        atomic_write_text(report_path, text + "\n")
        logger.info(f"roll done: exit code {code}, report saved to {report_path}")
    return code


if __name__ == "__main__":
    sys.exit(main())
