"""Offline guard for the pinned SDK. Synthetic wire-JSON fixtures only.

The data path is plain REST; the SDK only signs requests when an endpoint
demands auth. This guard keeps the pin honest: a downgrade below 3.30.0
brings back the crash that broke discovery in September 2026.
"""

import kalshi_python_sync
from kalshi_python_sync.models.event_data import EventData

# Shape and keys mirror a real GET /events item after 2026-09-10, when Kalshi
# removed available_on_brokers (SDK 3.27.0 declared it a required StrictBool)
EVENT_WIRE = {
    "event_ticker": "TEST-26SEP",
    "series_ticker": "TEST",
    "title": "Test event in September 2026",
    "sub_title": "Sep 2026",
    "category": "Economics",
    "collateral_return_type": "binary",
    "mutually_exclusive": False,
    "strike_period": "",
    "settlement_sources": [{"name": "BLS", "url": "https://www.bls.gov/"}],
    "last_updated_ts": "2026-09-16T14:02:11Z",
    "exchange_index": 0,
}


def _version_tuple(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split(".")[:3])


def test_sdk_version_meets_the_pinned_lower_bound():
    assert _version_tuple(kalshi_python_sync.__version__) >= (3, 30, 0)


def test_event_model_accepts_payload_without_available_on_brokers():
    assert "available_on_brokers" not in EVENT_WIRE
    event = EventData.from_dict(EVENT_WIRE)
    assert event.event_ticker == "TEST-26SEP"
    assert event.series_ticker == "TEST"
