"""
kalshi_io/tickers.py — Ticker list loading and validation.

load_tickers() parses what the user typed and reads local files only.
validate_tickers() then checks each ticker against the catalog and, for
anything not cataloged, against the API.
"""

import json
import re
from pathlib import Path

from kalshi_io.candles import UnknownTickerError, is_cataloged, resolve_ticker_meta
from kalshi_io.config import TICKERS_DIR
from kalshi_io.runlog import get_logger

logger = get_logger("tickers")

_SEPARATORS = re.compile(r"[\s,]+")

# `--tickers focus` = the focus universe proposed by the last roll
FOCUS_ALIAS = "focus"
FOCUS_FILE = "focus_universe.txt"


def load_tickers(source: str | list[str]) -> list[str]:
    """
    Load a ticker list from any supported source. No network.

    Accepts a string or a list of strings; every element may be:
        path to a .txt file      → one ticker per line ("#" lines ignored)
        path to a .json file     → reads the "tickers" key
        a series name            → TICKERS_DIR/{series}_tickers.txt
        "focus"                  → TICKERS_DIR/focus_universe.txt
        a market ticker
        several of the above separated by whitespace and/or commas
        ("A B", "A,B", "A, B\\nC"), so a quoted shell argument works

    An existing file path is always taken whole, so paths may contain spaces.

    Returns:
        Sorted, deduped list[str].
    """
    items = [source] if isinstance(source, str) else list(source)
    tickers: list[str] = []
    for item in items:
        tickers.extend(_expand(str(item)))
    return sorted(set(tickers))


def _is_file(text: str) -> bool:
    try:
        return Path(text).is_file()
    except OSError:          # e.g. "File name too long" for a long ticker string
        return False


def _expand(item: str) -> list[str]:
    """Turn one source element into tickers (see load_tickers)."""
    item = item.strip()
    if not item:
        return []
    if _is_file(item):
        return _read_file(Path(item))

    tokens = [t for t in _SEPARATORS.split(item) if t]
    if len(tokens) > 1:
        out: list[str] = []
        for token in tokens:
            out.extend(_expand(token))
        return out

    token = tokens[0]
    if token.endswith((".txt", ".json")):
        return _read_file(Path(token))          # missing file: FileNotFoundError
    if token == FOCUS_ALIAS:
        path = TICKERS_DIR / FOCUS_FILE
        if not path.exists():
            raise FileNotFoundError(
                f"{path} does not exist yet; run get_ticker_info/roll.py to derive the focus universe"
            )
        return _read_txt(path)
    series_path = TICKERS_DIR / f"{token}_tickers.txt"
    if series_path.exists():
        return _read_txt(series_path)
    return [token]


def _read_file(path: Path) -> list[str]:
    return _read_json(path) if path.suffix == ".json" else _read_txt(path)


def _read_txt(path: Path) -> list[str]:
    """Read a .txt file: one ticker per line, strip whitespace, skip blanks and # comments."""
    return [
        line.strip()
        for line in path.read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


def _read_json(path: Path) -> list[str]:
    """Read a .json file and return the 'tickers' key."""
    return json.loads(path.read_text())["tickers"]


def validate_tickers(tickers: list[str], allow_api: bool = True) -> tuple[list[str], list[str]]:
    """
    Split tickers into (known, unknown).

    A ticker is known if the committed catalog has it or, with allow_api, if
    the API knows it in the live or the historical tier (that lookup also
    primes resolve_ticker_meta, so it costs nothing extra later).

    A lookup that fails for any reason other than "not found" (API down,
    throttled) keeps the ticker in `known`: an outage is not a typo, and the
    puller reports the failure per ticker when it tries to fetch.

    Args:
        tickers:   market tickers, e.g. from load_tickers
        allow_api: False checks the catalog only (offline)

    Returns:
        (known, unknown), both in input order.
    """
    known: list[str] = []
    unknown: list[str] = []
    for ticker in tickers:
        if is_cataloged(ticker):
            known.append(ticker)
        elif not allow_api:
            unknown.append(ticker)
        else:
            try:
                resolve_ticker_meta(ticker)
                known.append(ticker)
            except UnknownTickerError:
                unknown.append(ticker)
            except Exception as e:
                logger.warning(f"{ticker}: could not be validated ({type(e).__name__}: {e}); keeping it")
                known.append(ticker)
    return known, unknown
