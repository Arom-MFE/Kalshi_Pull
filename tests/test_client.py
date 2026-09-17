"""Offline tests for the REST layer: retry, backoff, rate limit, signing, pagination.

The session is a FakeKalshi, the clock is virtual, and jitter always takes its
upper bound, so every sleep below is exact.
"""

import logging
from types import SimpleNamespace

import pytest
import requests

import kalshi_io.client as client
from kalshi_io.client import (
    KalshiAPIError,
    KalshiNotFound,
    RetriesExhausted,
    is_outage,
    paginate,
    request_json,
)
from fakes import TOO_MANY_REQUESTS, FakeResponse, make_event, make_market


def _exchange(api):
    api.add_event(make_event("TEST-26SEP", "TEST"), [
        make_market(f"TEST-26SEP-T{i}", "TEST-26SEP") for i in range(1, 6)
    ])
    return api


# ------------------------------------------------------------------ retry

def test_429_is_retried_with_backoff_and_a_one_second_floor(fake_api, clock):
    _exchange(fake_api)
    fake_api.inject(r"^/markets$", [FakeResponse(429, TOO_MANY_REQUESTS)] * 2)
    data = request_json("/markets", {"event_ticker": "TEST-26SEP"})
    assert len(data["markets"]) == 5
    assert len(fake_api.calls) == 3
    # Exponential ceilings would be 0.5 s and 1 s; a 429 never waits less than 1 s
    assert clock.sleeps == [1.0, 1.0]


def test_retry_after_header_is_honored_capped_and_garbage_ignored(fake_api, clock):
    _exchange(fake_api)
    fake_api.inject(r"^/markets$", [
        FakeResponse(429, TOO_MANY_REQUESTS, headers={"Retry-After": "7"}),
        FakeResponse(429, TOO_MANY_REQUESTS, headers={"Retry-After": "Wed, 21 Oct 2099 07:28:00 GMT"}),
        FakeResponse(429, TOO_MANY_REQUESTS, headers={"Retry-After": "soon"}),
    ])
    request_json("/markets", {"event_ticker": "TEST-26SEP"})
    # 7 s as sent; a far-future date is capped at 120 s; garbage falls back to backoff (2 s)
    assert clock.sleeps == [7.0, 120.0, 2.0]


def test_persistent_5xx_raises_retries_exhausted_after_one_error_line(fake_api, clock, caplog):
    fake_api.inject(r"^/markets$", [FakeResponse(503, {"error": {"code": "unavailable", "message": "try later"}})] * 6)
    with caplog.at_level(logging.WARNING, logger="kalshi"):
        with pytest.raises(RetriesExhausted) as excinfo:
            request_json("/markets", {"event_ticker": "TEST-26SEP"})
    assert excinfo.value.attempts == 6 and excinfo.value.status == 503
    assert "try later" in excinfo.value.details
    assert len(fake_api.calls) == 6
    assert clock.sleeps == [0.5, 1.0, 2.0, 4.0, 8.0]
    assert [r.levelname for r in caplog.records].count("ERROR") == 1
    assert [r.levelname for r in caplog.records].count("WARNING") == 5
    assert is_outage(excinfo.value)


def test_connection_errors_and_timeouts_are_retried(fake_api, clock):
    _exchange(fake_api)
    fake_api.inject(r"^/markets$", [requests.ConnectionError("reset by peer"), requests.Timeout("read timed out")])
    assert len(request_json("/markets", {"event_ticker": "TEST-26SEP"})["markets"]) == 5
    assert clock.sleeps == [0.5, 1.0]


def test_a_200_that_is_not_json_is_retried(fake_api, clock):
    _exchange(fake_api)
    fake_api.inject(r"^/markets$", [FakeResponse(200, payload=None, text="<html>gateway</html>")])
    assert len(request_json("/markets", {"event_ticker": "TEST-26SEP"})["markets"]) == 5
    assert len(fake_api.calls) == 2


def test_status_all_raises_400_with_details_and_is_never_retried(fake_api, clock):
    """Regression: the API rejects status=all; that must surface, not retry or vanish."""
    _exchange(fake_api)
    with pytest.raises(KalshiAPIError) as excinfo:
        request_json("/markets", {"event_ticker": "TEST-26SEP", "status": "all"})
    assert excinfo.value.status == 400
    assert excinfo.value.details == "invalid status filter"
    assert len(fake_api.calls) == 1 and clock.sleeps == []
    assert not is_outage(excinfo.value)


def test_404_raises_kalshi_not_found_immediately(fake_api, clock):
    with pytest.raises(KalshiNotFound) as excinfo:
        request_json("/markets/NOPE")
    assert excinfo.value.status == 404 and clock.sleeps == []


def test_both_error_body_shapes_and_non_json_bodies_parse():
    assert client._error_details(FakeResponse(400, {"error": {"code": "bad_request", "message": "bad request", "details": "invalid status filter"}})) == "invalid status filter"
    assert client._error_details(FakeResponse(400, {"error": {"code": "bad_request", "message": "bad request"}})) == "bad request"
    # The documented 429 body carries a plain string
    assert client._error_details(FakeResponse(429, TOO_MANY_REQUESTS)) == "too many requests"
    assert client._error_details(FakeResponse(502, payload=None, text="<html>Bad Gateway</html>")) == "<html>Bad Gateway</html>"


def test_none_params_are_dropped(fake_api):
    _exchange(fake_api)
    request_json("/markets", {"event_ticker": "TEST-26SEP", "status": None, "cursor": None})
    assert fake_api.calls[0][1] == {"event_ticker": "TEST-26SEP"}


# ------------------------------------------------------------------ rate limit

def test_request_starts_are_spaced_by_the_rate_limit(fake_api, clock):
    _exchange(fake_api)
    for _ in range(5):
        request_json("/markets", {"event_ticker": "TEST-26SEP"})
    # Default 5 requests/s: the first goes out at once, the next four wait 0.2 s each
    assert clock.sleeps == pytest.approx([0.2] * 4)


# ------------------------------------------------------------------ signed fallback

def _signer(monkeypatch, key_id="test-key"):
    signed_urls = []

    def create_auth_headers(method, url):
        signed_urls.append((method, url))
        return {"KALSHI-ACCESS-KEY": key_id, "KALSHI-ACCESS-SIGNATURE": "sig", "KALSHI-ACCESS-TIMESTAMP": "1"}

    fake_client = SimpleNamespace(kalshi_auth=SimpleNamespace(create_auth_headers=create_auth_headers))
    monkeypatch.setattr(client, "get_client", lambda: fake_client)
    return signed_urls


def test_401_is_retried_once_signed_and_signing_sticks(fake_api, monkeypatch):
    _exchange(fake_api)
    fake_api.auth_required = {"/markets/TEST-26SEP-T1/orderbook"}
    signed_urls = _signer(monkeypatch)

    assert "orderbook_fp" in request_json("/markets/TEST-26SEP-T1/orderbook")
    keyless, signed = fake_api.calls
    assert "KALSHI-ACCESS-KEY" not in keyless[2] and signed[2]["KALSHI-ACCESS-KEY"] == "test-key"
    # The signature covers the full URL the request went to (the SDK strips host and query)
    assert signed_urls == [("GET", f"{client.BASE_URL}/markets/TEST-26SEP-T1/orderbook")]

    # Once a signed call was needed and worked, later calls are signed from the start
    request_json("/markets/TEST-26SEP-T1/orderbook")
    assert len(fake_api.calls) == 3 and "KALSHI-ACCESS-KEY" in fake_api.calls[2][2]


def test_401_without_usable_credentials_is_a_clear_error(fake_api, monkeypatch):
    _exchange(fake_api)
    fake_api.auth_required = {"/markets/TEST-26SEP-T1/orderbook"}

    def _no_credentials():
        raise RuntimeError("KALSHI_KEY_PATH not set in environment / .env")

    monkeypatch.setattr(client, "get_client", _no_credentials)
    with pytest.raises(KalshiAPIError) as excinfo:
        request_json("/markets/TEST-26SEP-T1/orderbook")
    assert excinfo.value.status == 401
    assert "requires authentication" in excinfo.value.details
    assert "KALSHI_KEY_PATH not set" in excinfo.value.details


def test_a_rejected_signature_raises_and_clears_the_sticky_flag(fake_api, monkeypatch):
    _exchange(fake_api)
    fake_api.auth_required = {"/markets/TEST-26SEP-T1/orderbook"}
    _signer(monkeypatch, key_id="revoked-key")
    with pytest.raises(KalshiAPIError) as excinfo:
        request_json("/markets/TEST-26SEP-T1/orderbook")
    assert excinfo.value.status == 401
    assert len(fake_api.calls) == 2
    assert client._use_signing is False


# ------------------------------------------------------------------ pagination

def test_paginate_follows_the_cursor_until_it_is_empty(fake_api):
    _exchange(fake_api)
    fake_api.page_size = 2
    tickers = [m["ticker"] for m in paginate("/markets", {"event_ticker": "TEST-26SEP"}, key="markets")]
    assert tickers == [f"TEST-26SEP-T{i}" for i in range(1, 6)]
    cursors = [c[1].get("cursor") for c in fake_api.calls]
    assert cursors == [None, "2", "4"]
    assert all(c[1]["limit"] == 1000 for c in fake_api.calls)


def test_paginate_accepts_an_absent_cursor_key(fake_api):
    fake_api.inject(r"^/markets$", [FakeResponse(200, {"markets": [{"ticker": "A"}]})])
    assert [m["ticker"] for m in paginate("/markets", key="markets")] == ["A"]


def test_paginate_raises_on_a_repeated_cursor(fake_api):
    page = FakeResponse(200, {"markets": [{"ticker": "A"}], "cursor": "same"})
    fake_api.inject(r"^/markets$", [page, page, page])
    with pytest.raises(KalshiAPIError, match="cursor repeated"):
        list(paginate("/markets", key="markets"))


def test_a_failure_mid_listing_propagates_instead_of_truncating(fake_api, clock):
    _exchange(fake_api)
    fake_api.page_size = 2
    fake_api.inject(r"^/markets$", [
        FakeResponse(200, {"markets": [{"ticker": "A"}, {"ticker": "B"}], "cursor": "2"}),
        *[FakeResponse(500, {"error": {"code": "internal", "message": "boom"}})] * 6,
    ])
    seen = []
    with pytest.raises(RetriesExhausted):
        for m in paginate("/markets", {"event_ticker": "TEST-26SEP"}, key="markets"):
            seen.append(m["ticker"])
    # The caller saw page 1 and then an exception: it can never mistake this for a full listing
    assert seen == ["A", "B"]
