"""
kalshi_io/tickers.py — Ticker list loading and validation.

load_tickers() parses what the user typed and reads local files only.
validate_tickers() then checks each ticker against the catalog and, for
anything not cataloged, against the API.

A ticker may hold a space or a comma ("GDP-232022 Q4-T0.0",
"JOBLESS-22JUL23-C250,000"). load_tickers() therefore never splits a line of
a file or a ticker it knows; only a typed argument is split.
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
        path to a .json file     → one ticker per element of the "tickers" key
        a series name            → TICKERS_DIR/{series}_tickers.txt
        "focus"                  → TICKERS_DIR/focus_universe.txt
        a market ticker
        several of the above separated by whitespace and/or commas
        ("A B", "A,B", "A, B\\nC"), so a quoted shell argument works

    Every element is stripped, and then, in this order:
        1. An existing file path is read whole, so paths may contain spaces.
           A line of a .txt file and an element of a .json file is stripped
           and is one ticker; it is never split further. The files behind a
           series name and behind "focus" are read the same way.
        2. An element that is exactly a known ticker is taken whole. Known
           means candles.is_cataloged(): the committed catalog, tickers
           registered by the focus universe, and tickers the API resolved
           earlier in this process (validate_tickers does that). The lookup
           is a dict built once per process, not a file read per element.
        3. Only what is left is a typed argument, and only a typed argument
           is split on whitespace and commas. Each token is then a file,
           "focus", a series name or a ticker. Tokens are never joined back
           together.

    So load_tickers(load_tickers(x)) == load_tickers(x) whenever the tickers
    of x are known, which holds for every list the driver hands to a puller
    (it validates first) and for the poller's derived universe (it registers
    its tickers). Two limits follow from rule 3. A ticker that holds a space or
    a comma cannot share a quoted string with other tickers ("A-1,
    JOBLESS-22JUL23-C250,000" is split into three): give it an argument of its
    own, quoted, or put it in a file. And such a ticker that nothing knows yet
    must come from a file; validate_tickers then resolves it through the API,
    and rule 2 keeps it whole on every later pass.

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
    if is_cataloged(item):                      # a known ticker is never split, whatever it holds
        return [item]

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
    """Read a .txt file: one ticker per line, strip whitespace, skip blanks and # comments.
    A line is one ticker and is never split, so a ticker may hold a space or a comma."""
    return [
        line.strip()
        for line in path.read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


def _read_json(path: Path) -> list[str]:
    """Read a .json file: one ticker per element of the 'tickers' key, stripped, never split."""
    return [str(t).strip() for t in json.loads(path.read_text())["tickers"] if str(t).strip()]


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
