"""
kalshi_io/client.py — REST access to the Kalshi API.

Every data call in this repo goes through request_json(): plain, keyless REST
with client-side rate limiting and retries. Kalshi's market-data endpoints are
public, and its docs name the OpenAPI spec (not the SDK) as the source of
truth, so no SDK response model sits in the data path.

The SDK is only a request signer. If an endpoint answers 401/403, the same
REST call is re-sent with signed headers from the SDK's KalshiAuth; that is
the only moment credentials are read (lazily, from .env).

Retry policy (request_json):
    retried      429, 5xx, connection errors, timeouts, a 200 that is not JSON
    backoff      exponential with jitter: uniform(b/2, b), b = min(cap, base * 2^n);
                 at least 1 s after a 429; a Retry-After header is honored when
                 present (Kalshi documents that 429s do not send one today)
    not retried  every other 4xx; raised at once as KalshiAPIError (404 as
                 KalshiNotFound, which callers use to switch to /historical/)
    exhausted    RetriesExhausted, after one ERROR log line

Exports:
    request_json()  — GET one JSON document
    paginate()      — iterate a cursor-paginated list endpoint
    KalshiAPIError, KalshiNotFound, RetriesExhausted, is_outage()
    stats           — {"requests": n} HTTP attempts made by this process
    get_client()    — authenticated SDK client (signing only), cached
    get_session()   — shared requests.Session, cached
    BASE_URL        — API base URL
"""

import os
import random
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from functools import lru_cache
from typing import Iterator
from urllib.parse import quote

import requests
from dotenv import load_dotenv

from kalshi_io.config import (
    HTTP_BACKOFF_BASE_S,
    HTTP_BACKOFF_CAP_S,
    HTTP_MAX_ATTEMPTS,
    HTTP_RETRY_AFTER_CAP_S,
    HTTP_TIMEOUT,
    RATE_LIMIT_SECONDS,
)
from kalshi_io.runlog import get_logger

# Kalshi now recommends external-api.kalshi.com; this host remains supported.
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

logger = get_logger("client")

# Indirections so tests can run on a virtual clock with deterministic jitter
_sleep = time.sleep
_monotonic = time.monotonic
_uniform = random.uniform

_next_request_at = 0.0    # rate limiter state (single-threaded use)
_use_signing = False      # sticky once a signed request was needed and worked

# HTTP attempts made by this process (retries included); reports read it
stats = {"requests": 0}


class KalshiAPIError(Exception):
    """The API answered with an error that retrying will not fix."""

    def __init__(self, status: int | None, url: str, details: str = ""):
        self.status = status
        self.url = url
        self.details = details
        super().__init__(f"HTTP {status} for {url}: {details}" if status else f"{url}: {details}")


class KalshiNotFound(KalshiAPIError):
    """HTTP 404. On live endpoints this usually means: try /historical/."""


class RetriesExhausted(KalshiAPIError):
    """A retryable failure persisted through every attempt."""

    def __init__(self, status: int | None, url: str, details: str, attempts: int):
        super().__init__(status, url, f"{details} (gave up after {attempts} attempts)")
        self.attempts = attempts


def is_outage(exc: BaseException) -> bool:
    """True if exc is, or was caused by, exhausted retries (API down or throttling)."""
    return isinstance(exc, RetriesExhausted) or isinstance(exc.__cause__, RetriesExhausted)


@lru_cache(maxsize=1)
def get_client():
    """Build the authenticated SDK client on first use (reads .env).

    Only needed to sign requests; the SDK import is deferred so keyless code
    paths do not depend on it.
    """
    from kalshi_python_sync import Configuration, KalshiClient

    load_dotenv()

    key_path = os.getenv("KALSHI_KEY_PATH")
    if not key_path:
        raise RuntimeError("KALSHI_KEY_PATH not set in environment / .env")
    with open(key_path, "r") as f:
        private_key = f.read()

    api_key_id = os.getenv("KALSHI_API_KEY_ID")
    if not api_key_id:
        raise RuntimeError("KALSHI_API_KEY_ID not set in environment / .env")

    config = Configuration(host=BASE_URL)
    config.api_key_id = api_key_id
    config.private_key_pem = private_key
    return KalshiClient(config)


@lru_cache(maxsize=1)
def get_session() -> requests.Session:
    """Shared keyless REST session. Prefer request_json(), which adds rate
    limiting, retries and the signed fallback on top of it."""
    return requests.Session()


def path_part(value: str) -> str:
    """URL-quote a ticker for use as one path segment."""
    return quote(str(value), safe="")


# ============================================================
# Internals
# ============================================================

def _throttle() -> None:
    """Space request starts at least RATE_LIMIT_SECONDS apart."""
    global _next_request_at
    now = _monotonic()
    if now < _next_request_at:
        _sleep(_next_request_at - now)
        now = _next_request_at
    _next_request_at = now + RATE_LIMIT_SECONDS


def _error_details(resp) -> str:
    """Pull the most specific message out of an error body.

    Handles both shapes the API sends: {"error": {"code", "message", "details"}}
    and the 429 body {"error": "too many requests"}.
    """
    try:
        body = resp.json()
    except ValueError:
        return (getattr(resp, "text", "") or "")[:200]
    err = body.get("error") if isinstance(body, dict) else None
    if isinstance(err, dict):
        return str(err.get("details") or err.get("message") or err.get("code") or err)
    if isinstance(err, str):
        return err
    return str(body)[:200]


def _parse_retry_after(value) -> float | None:
    """Seconds to wait from a Retry-After header (delta-seconds or HTTP date)."""
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        pass
    try:
        when = parsedate_to_datetime(str(value))
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())


def _backoff_delay(retry_index: int, status: int | None, retry_after: float | None) -> float:
    """Seconds to sleep before retry number retry_index (0-based)."""
    ceiling = min(HTTP_BACKOFF_CAP_S, HTTP_BACKOFF_BASE_S * (2 ** retry_index))
    delay = _uniform(ceiling / 2, ceiling)
    if status == 429:
        delay = max(delay, 1.0)
    if retry_after is not None:
        delay = max(delay, min(retry_after, HTTP_RETRY_AFTER_CAP_S))
    return delay


def _auth_headers(url: str) -> dict:
    """Signed headers for url, or a clear error when credentials are unusable."""
    try:
        auth = get_client().kalshi_auth
    except Exception as e:
        raise KalshiAPIError(
            401, url,
            f"endpoint requires authentication and credentials are not usable ({type(e).__name__}: {e})",
        ) from e
    if auth is None:
        raise KalshiAPIError(401, url, "endpoint requires authentication and the SDK client has no signer")
    return auth.create_auth_headers("GET", url)


# ============================================================
# Public API
# ============================================================

def request_json(
    path: str,
    params: dict | None = None,
    *,
    timeout: tuple[float, float] = HTTP_TIMEOUT,
    max_attempts: int = HTTP_MAX_ATTEMPTS,
) -> dict:
    """
    GET BASE_URL + path and return the decoded JSON body.

    Args:
        path:         path below the API root, e.g. "/markets/trades"
        params:       query parameters; None values are dropped
        timeout:      (connect, read) seconds per attempt
        max_attempts: total attempts including the first

    Raises:
        KalshiNotFound:   HTTP 404
        KalshiAPIError:   any other non-retryable 4xx (carries .status and .details)
        RetriesExhausted: 429 / 5xx / network failure on every attempt
    """
    global _use_signing

    url = f"{BASE_URL}{path}"
    params = {k: v for k, v in (params or {}).items() if v is not None}
    signed = _use_signing
    status: int | None = None
    problem = "no attempt made"

    for attempt in range(1, max_attempts + 1):
        _throttle()
        retry_after = None
        headers = _auth_headers(url) if signed else None
        stats["requests"] += 1
        try:
            resp = get_session().get(url, params=params, headers=headers, timeout=timeout)
        except (requests.ConnectionError, requests.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            status = None
            problem = f"{type(e).__name__}: {e}"
        else:
            status = resp.status_code
            if status == 200:
                try:
                    data = resp.json()
                except ValueError:
                    problem = "HTTP 200 with a body that is not JSON"
                else:
                    if signed:
                        _use_signing = True
                    return data
            elif status in (401, 403) and not signed:
                # Keyless was refused: repeat the same call signed, right away
                logger.info(f"HTTP {status} on GET {path}: endpoint wants authentication, retrying signed")
                signed = True
                problem = f"HTTP {status}: {_error_details(resp)}"
                continue
            elif status == 429 or status >= 500:
                problem = f"HTTP {status}: {_error_details(resp)}"
                retry_after = _parse_retry_after(resp.headers.get("Retry-After"))
            else:
                if status in (401, 403):
                    _use_signing = False
                error_cls = KalshiNotFound if status == 404 else KalshiAPIError
                raise error_cls(status, url, _error_details(resp))

        if attempt < max_attempts:
            delay = _backoff_delay(attempt - 1, status, retry_after)
            logger.warning(
                f"{problem} on GET {path} (attempt {attempt}/{max_attempts}); retrying in {delay:.1f}s"
            )
            _sleep(delay)

    logger.error(f"giving up on GET {path} after {max_attempts} attempts: {problem}")
    raise RetriesExhausted(status, url, problem, attempts=max_attempts)


def paginate(
    path: str,
    params: dict | None = None,
    *,
    key: str,
    limit: int = 1000,
    max_pages: int = 10_000,
) -> Iterator[dict]:
    """
    Iterate every item of a cursor-paginated list endpoint.

    Follows the response's "cursor" until it comes back empty. Cursors are
    opaque and only valid within one listing; they are never stored.

    Args:
        path:   list endpoint, e.g. "/markets"
        params: filters (without limit/cursor)
        key:    response key holding the items ("markets", "events", "trades")
        limit:  page size; the API caps it at 200 for /events and 1000 elsewhere

    Raises:
        Whatever request_json raises; nothing is swallowed, so a caller never
        mistakes a truncated listing for a complete one.
    """
    query = dict(params or {})
    query["limit"] = limit
    seen: set[str] = set()
    for _ in range(max_pages):
        data = request_json(path, query)
        yield from data.get(key) or []
        cursor = data.get("cursor") or None
        if not cursor:
            return
        if cursor in seen:
            raise KalshiAPIError(200, f"{BASE_URL}{path}", f"pagination cursor repeated: {cursor!r}")
        seen.add(cursor)
        query["cursor"] = cursor
    raise KalshiAPIError(200, f"{BASE_URL}{path}", f"more than {max_pages} pages")


def _reset_state() -> None:
    """Reset limiter and signing state (test isolation)."""
    global _next_request_at, _use_signing
    _next_request_at = 0.0
    _use_signing = False
    stats["requests"] = 0
