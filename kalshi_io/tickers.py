"""
kalshi_io/tickers.py — Ticker list loader.

Reads from local files only. No API calls, no network.
"""

import json
from pathlib import Path

from kalshi_io.config import TICKERS_DIR


def load_tickers(source: str | list[str]) -> list[str]:
    """
    Load a ticker list from any supported source.

    Accepts:
        list[str]              → pass-through
        str ending in .txt     → one ticker per line
        str ending in .json    → reads the "tickers" key
        str matching a series  → resolves to TICKERS_DIR/{source}_tickers.txt
        any other str          → single ticker, wrapped in list

    Returns:
        Sorted, deduped list[str].
    """
    if isinstance(source, list):
        tickers = source
    elif source.endswith(".txt"):
        tickers = _read_txt(Path(source))
    elif source.endswith(".json"):
        tickers = _read_json(Path(source))
    else:
        series_path = TICKERS_DIR / f"{source}_tickers.txt"
        if series_path.exists():
            tickers = _read_txt(series_path)
        else:
            tickers = [source]

    return sorted(set(tickers))


def _read_txt(path: Path) -> list[str]:
    """Read a .txt file: one ticker per line, strip whitespace, skip blanks."""
    return [
        line.strip()
        for line in path.read_text().splitlines()
        if line.strip()
    ]


def _read_json(path: Path) -> list[str]:
    """Read a .json file and return the 'tickers' key."""
    return json.loads(path.read_text())["tickers"]