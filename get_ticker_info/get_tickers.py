"""
Kalshi Series Discovery — Reusable Module
=========================================
Discovers all events, markets, and tickers for any Kalshi series.
Saves tickers as JSON + TXT (easy to consume in downstream scripts).

USAGE AS A SCRIPT:
    Change SERIES at bottom and run.

USAGE AS A MODULE:
    from series_discovery import discover_series, load_tickers

    discover_series("KXCPIYOY")
    tickers = load_tickers("KXCPIYOY")

OUTPUT FILES (per series, in ./kalshi_tickers/):
    {SERIES}_tickers.txt      — one market_ticker per line
    {SERIES}_tickers.json     — structured: events + markets + tickers
"""

import os
import json
import time
from pathlib import Path

import requests
from dotenv import load_dotenv
from kalshi_python_sync import Configuration, KalshiClient

# ============================================================
# SETUP
# ============================================================
load_dotenv()

with open(os.getenv("KALSHI_KEY_PATH"), "r") as f:
    _private_key = f.read()

_config = Configuration(host="https://api.elections.kalshi.com/trade-api/v2")
_config.api_key_id      = os.getenv("KALSHI_API_KEY_ID")
_config.private_key_pem = _private_key

_client    = KalshiClient(_config)
BASE_URL   = "https://api.elections.kalshi.com/trade-api/v2"
OUTPUT_DIR = Path("kalshi_tickers")
OUTPUT_DIR.mkdir(exist_ok=True)

# ============================================================
# INTERNAL HELPERS
# ============================================================

def _paginate(url, params, result_key="markets"):
    """Fetch all pages with cursor pagination."""
    results = []
    cursor  = ""
    while True:
        p = {**params, "limit": 1000}
        if cursor:
            p["cursor"] = cursor
        resp = requests.get(url, params=p)
        if resp.status_code != 200:
            break
        data = resp.json()
        results.extend(data.get(result_key, []))
        cursor = data.get("cursor", "")
        if not cursor:
            break
        time.sleep(0.2)
    return results


def _series_variants(series):
    """Return [KX version, non-KX version] to catch both old and new tickers."""
    if series.startswith("KX"):
        return [series, series[2:]]
    return ["KX" + series, series]


def _find_events(series):
    """Collect all events for a series via 3-method union across both variants."""
    events = {}  # event_ticker -> title

    for variant in _series_variants(series):
        try:
            resp = _client.get_events(series_ticker=variant)
            for e in resp.events:
                events.setdefault(e.event_ticker, e.title)
        except Exception:
            pass

        hist = _paginate(f"{BASE_URL}/historical/markets", {"series_ticker": variant})
        for m in hist:
            et = m.get("event_ticker")
            if et:
                events.setdefault(et, m.get("title", ""))

        live = _paginate(f"{BASE_URL}/markets", {"series_ticker": variant})
        for m in live:
            et = m.get("event_ticker")
            if et:
                events.setdefault(et, m.get("title", ""))

    return events


def _find_markets_for_event(event_ticker):
    """Get all markets for a single event via 3-method fallback."""
    markets = []

    try:
        event = _client.get_event(event_ticker=event_ticker)
        for m in event.markets:
            markets.append({
                "event_ticker":  event_ticker,
                "market_ticker": m.ticker,
                "title":         m.title,
                "status":        m.status,
                "source":        "live_event",
            })
    except Exception:
        pass

    if not markets:
        live = _paginate(f"{BASE_URL}/markets", {"event_ticker": event_ticker, "status": "all"})
        for m in live:
            markets.append({
                "event_ticker":  event_ticker,
                "market_ticker": m["ticker"],
                "title":         m.get("title", ""),
                "status":        m.get("status", ""),
                "source":        "live_markets",
            })

    if not markets:
        hist = _paginate(f"{BASE_URL}/historical/markets", {"event_ticker": event_ticker})
        for m in hist:
            markets.append({
                "event_ticker":  event_ticker,
                "market_ticker": m["ticker"],
                "title":         m.get("title", ""),
                "status":        m.get("status", ""),
                "source":        "historical",
            })

    return markets


# ============================================================
# PUBLIC API
# ============================================================

def discover_series(series, verbose=True, save=True):
    """
    Full discovery for a Kalshi series.

    Args:
        series:  series ticker (e.g. "KXCPIYOY"). KX prefix auto-handled.
        verbose: print progress
        save:    write JSON + TXT to OUTPUT_DIR

    Returns:
        dict with keys:
            'events'  — list of {event_ticker, title}
            'markets' — list of {event_ticker, market_ticker, title, status, source}
            'tickers' — sorted list of unique market_tickers
    """
    if verbose:
        print(f"\n=== Discovering {series} ===")

    events_dict = _find_events(series)
    events_list = sorted(
        [{"event_ticker": k, "title": v} for k, v in events_dict.items()],
        key=lambda x: x["event_ticker"],
    )

    if verbose:
        print(f"Events found: {len(events_list)}")

    markets_list = []
    for e in events_list:
        markets_list.extend(_find_markets_for_event(e["event_ticker"]))

    if verbose:
        print(f"Markets found: {len(markets_list)}")

    tickers = sorted({m["market_ticker"] for m in markets_list})

    result = {
        "series":  series,
        "events":  events_list,
        "markets": markets_list,
        "tickers": tickers,
    }

    if save:
        json_path = OUTPUT_DIR / f"{series}_tickers.json"
        txt_path  = OUTPUT_DIR / f"{series}_tickers.txt"

        json_path.write_text(json.dumps(result, indent=2))
        txt_path.write_text("\n".join(tickers) + ("\n" if tickers else ""))

        if verbose:
            print(f"Saved: {json_path}")
            print(f"Saved: {txt_path}")

    return result


def build_combined(verbose=True):
    """
    Scan OUTPUT_DIR for all {SERIES}_tickers.json files and combine
    into one master file with all tickers across all series you've discovered.

    Creates:
        all_tickers.txt   — every unique market_ticker, one per line
        all_tickers.json  — structured, with series attribution
    """
    combined = {
        "series":       [],
        "total_events":  0,
        "total_markets": 0,
        "tickers":       [],
        "by_series":     {},
    }

    all_tickers = set()

    # Scan all per-series json files
    for json_path in sorted(OUTPUT_DIR.glob("*_tickers.json")):
        series = json_path.stem.replace("_tickers", "")
        data = json.loads(json_path.read_text())

        combined["series"].append(series)
        combined["total_events"]  += len(data["events"])
        combined["total_markets"] += len(data["markets"])
        combined["by_series"][series] = {
            "events":  len(data["events"]),
            "markets": len(data["markets"]),
            "tickers": data["tickers"],
        }
        all_tickers.update(data["tickers"])

    combined["tickers"] = sorted(all_tickers)

    # Save
    json_path = OUTPUT_DIR / "all_tickers.json"
    txt_path  = OUTPUT_DIR / "all_tickers.txt"

    json_path.write_text(json.dumps(combined, indent=2))
    txt_path.write_text("\n".join(combined["tickers"]) + "\n")

    if verbose:
        print(f"\n=== Combined ===")
        print(f"Series included:  {len(combined['series'])}")
        print(f"Total events:     {combined['total_events']}")
        print(f"Total markets:    {combined['total_markets']}")
        print(f"Unique tickers:   {len(combined['tickers'])}")
        print(f"Saved: {json_path}")
        print(f"Saved: {txt_path}")

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


if __name__ == "__main__":
    SERIES_LIST = [
        # Inflation
        "KXCPI",
        "KXCPIYOY",
        "KXACPI",
        "KXCPICORE",
        "KXPCECORE",
        "KXCPICOREYOY",
        # Labor
        "KXU3",
        "KXJOBLESS",
        "KXPAYROLLS",
        # Growth
        "KXGDP",
        "KXGDPYEAR",
        "KXRECSSNBER",
        # Fed
        "KXFEDDECISION",
        "KXFED",
        "KXFEDMEET",
    ]

    all_results = {}
    for series in SERIES_LIST:
        try:
            all_results[series] = discover_series(series)
        except Exception as ex:
            print(f"  {series} failed: {ex}")

    # Summary table
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"{'Series':<20} {'Events':>8} {'Markets':>8} {'Tickers':>8}")
    for series, result in all_results.items():
        print(f"{series:<20} {len(result['events']):>8} {len(result['markets']):>8} {len(result['tickers']):>8}")

    build_combined()