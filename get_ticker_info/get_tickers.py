"""
Kalshi Series Discovery — rebuilds the committed ticker catalog
================================================================
Discovers all events, markets, and tickers for any Kalshi series and saves
them as JSON + TXT under get_ticker_info/kalshi_tickers/. Keyless: public
REST endpoints only. The logic lives in kalshi_io/catalog.py; this file keeps
the script entry point and the original function names.

USAGE AS A SCRIPT (rebuilds every series in kalshi_io.config.SERIES_LIST):
    python get_ticker_info/get_tickers.py

    For a report of what changed plus the next focus universe, use
    get_ticker_info/roll.py instead.

USAGE AS A MODULE:
    from get_tickers import discover_series, load_tickers

    discover_series("KXCPIYOY")
    tickers = load_tickers("KXCPIYOY")

OUTPUT FILES (per series, in get_ticker_info/kalshi_tickers/):
    {SERIES}_tickers.txt      — one market_ticker per line
    {SERIES}_tickers.json     — structured: events + markets + tickers,
                                with status, open/close/expiration times
                                and a built_at timestamp
"""

import json
import sys
from pathlib import Path

# Ensure kalshi_io is importable when running as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from kalshi_io import catalog
from kalshi_io.config import SERIES_LIST, TICKERS_DIR
from kalshi_io.runlog import configure_logging

OUTPUT_DIR = TICKERS_DIR


def discover_series(series, verbose=True, save=True):
    """
    Full discovery for a Kalshi series (see kalshi_io.catalog.discover_series).

    Args:
        series:  series ticker (e.g. "KXCPIYOY"). Pre-KX events are included.
        verbose: print progress
        save:    write JSON + TXT to OUTPUT_DIR

    Returns:
        dict with keys:
            'events'  — list of {event_ticker, title}
            'markets' — list of {event_ticker, market_ticker, title, status,
                        open_time, close_time, expected_expiration_time,
                        latest_expiration_time, source}
            'tickers' — sorted list of unique market_tickers
            plus 'series', 'built_at', 'status_counts', 'historical_cutoff'

    Raises:
        On any failed request or implausible result; the previous files are
        left untouched in that case.
    """
    if verbose:
        print(f"\n=== Discovering {series} ===")

    result = catalog.discover_series(series, save=save, out_dir=OUTPUT_DIR)

    if verbose:
        print(f"Events found: {len(result['events'])}")
        print(f"Markets found: {len(result['markets'])}  {result['status_counts']}")
        if save:
            print(f"Saved: {OUTPUT_DIR / f'{series}_tickers.json'}")
            print(f"Saved: {OUTPUT_DIR / f'{series}_tickers.txt'}")

    return result


def build_combined(verbose=True):
    """
    Scan OUTPUT_DIR for all {SERIES}_tickers.json files and combine
    into one master file with all tickers across all series you've discovered.
    Safe to run repeatedly.

    Creates:
        all_tickers.txt   — every unique market_ticker, one per line
        all_tickers.json  — structured, with series attribution, status
                            counts and the build timestamp
    """
    combined = catalog.build_combined(OUTPUT_DIR)

    if verbose:
        print("\n=== Combined ===")
        print(f"Series included:  {len(combined['series'])}")
        print(f"Total events:     {combined['total_events']}")
        print(f"Total markets:    {combined['total_markets']}")
        print(f"Unique tickers:   {len(combined['tickers'])}")
        print(f"By status:        {combined['status_counts']}")
        print(f"Built at:         {combined['built_at']}")
        print(f"Saved: {OUTPUT_DIR / 'all_tickers.json'}")
        print(f"Saved: {OUTPUT_DIR / 'all_tickers.txt'}")

    return combined


def load_tickers(series, key="tickers"):
    """
    Load previously discovered data for a series.

    Args:
        series: series ticker
        key:    what to return — 'tickers' (list), 'events', 'markets', or 'all' (full dict)

    Returns:
        requested data
    """
    path = OUTPUT_DIR / f"{series}_tickers.json"
    if not path.exists():
        raise FileNotFoundError(f"Run discover_series('{series}') first — no {path}")

    data = json.loads(path.read_text())

    if key == "all":
        return data
    if key in data:
        return data[key]
    raise ValueError(f"key must be 'tickers', 'events', 'markets', or 'all', got '{key}'")


def main() -> int:
    """Rebuild the catalog for SERIES_LIST. Exit code 1 if any series failed."""
    configure_logging()
    all_results = {}
    failed = {}
    for series in SERIES_LIST:
        try:
            all_results[series] = discover_series(series)
        except Exception as ex:
            failed[series] = ex
            print(f"  {series} failed, previous files kept: {type(ex).__name__}: {ex}")

    # Summary table
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"{'Series':<20} {'Events':>8} {'Markets':>8} {'Active':>8}")
    for series, result in all_results.items():
        print(f"{series:<20} {len(result['events']):>8} {len(result['markets']):>8} "
              f"{result['status_counts'].get('active', 0):>8}")
    for series in failed:
        print(f"{series:<20} {'FAILED':>8}")

    build_combined()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
