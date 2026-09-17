"""Offline tests for kalshi_io.releases: release times, windows, and what is active when."""

import pytest

from kalshi_io import releases
from fakes import iso_to_ts

NOW = iso_to_ts("2026-09-17T20:00:00Z")


def _record(event, status, close):
    return {"event_ticker": event, "market_ticker": f"{event}-T1", "status": status, "close_time": close}


def test_release_times_come_from_the_universe_the_catalog_and_the_calendar():
    universe = {"groups": [
        {"event_ticker": "KXCPIYOY-26SEP", "close_time": "2026-10-14T12:29:00Z"},
        {"event_ticker": "KXFED-26OCT", "close_time": "2026-10-28T17:55:00Z"},
    ]}
    index = {
        "a": _record("KXCPI-26SEP", "active", "2026-10-14T12:25:00Z"),
        "b": _record("KXCPICORE-26SEP", "inactive", "2026-10-14T12:25:00Z"),       # paused still counts: it trades again
        "c": _record("KXCPIYOY-26AUG", "finalized", "2026-09-11T12:29:00Z"),       # settled: no release ahead
        "d": _record("KXGDPYEAR-36", "active", "2037-12-31T13:00:00Z"),            # beyond the horizon
        "e": {**_record("KXPAYROLLS-26SEP", "active", "2026-10-02T12:29:00Z"), "market_ticker": "KXPAYROLLS-26SEP-T2"},
        "f": _record("KXPAYROLLS-26SEP", "active", "2026-10-02T12:30:00Z"),        # one time per event: the earliest
        "g": _record("KXU3-26AUG", "active", "2026-09-17T19:50:00Z"),              # just closed: still inside a window
        "h": _record("KXU3-26JUL", "active", "2026-09-17T19:40:00Z"),              # too long ago
    }
    times = releases.release_times(universe, index, ["2026-10-30T12:30:00Z"], NOW, after_s=900)
    assert times == [
        (iso_to_ts("2026-09-17T19:50:00Z"), "KXU3-26AUG"),
        (iso_to_ts("2026-10-02T12:29:00Z"), "KXPAYROLLS-26SEP"),
        (iso_to_ts("2026-10-14T12:25:00Z"), "KXCPI-26SEP, KXCPICORE-26SEP"),
        (iso_to_ts("2026-10-14T12:29:00Z"), "KXCPIYOY-26SEP"),
        (iso_to_ts("2026-10-28T17:55:00Z"), "KXFED-26OCT"),
        (iso_to_ts("2026-10-30T12:30:00Z"), "calendar:2026-10-30T12:30:00Z"),
    ]
    assert releases.release_times(None, None, [], NOW, 900) == []
    with pytest.raises(ValueError, match="RELEASE_CALENDAR\\[0\\]"):
        releases.release_times(None, None, ["next thursday"], NOW, 900)


def test_windows_merge_when_they_overlap_and_report_what_is_active():
    t1, t2, t3 = iso_to_ts("2026-10-14T12:25:00Z"), iso_to_ts("2026-10-14T12:29:00Z"), iso_to_ts("2026-10-28T17:55:00Z")
    wins = releases.windows([(t1, "KXCPI-26SEP"), (t2, "KXCPIYOY-26SEP"), (t3, "KXFED-26OCT")], before_s=300, after_s=900)
    # The 12:25 and 12:29 closes are one window from 12:20 to 12:44, the Fed one stands alone
    assert wins == [(t1 - 300, t2 + 900, "KXCPI-26SEP; KXCPIYOY-26SEP"), (t3 - 300, t3 + 900, "KXFED-26OCT")]
    assert releases.active_window(wins, t1 - 301) is None
    assert releases.active_window(wins, t1 - 300) == wins[0] and releases.active_window(wins, t2 + 900) == wins[0]
    assert releases.active_window(wins, t2 + 901) is None
    assert releases.next_window(wins, t1 - 1000) == wins[0] and releases.next_window(wins, t1) == wins[1]
    assert releases.next_window(wins, t3 + 1) is None
    assert releases.describe(wins, t1 - 1000) == [
        "2026-10-14 12:20Z to 12:44Z  KXCPI-26SEP; KXCPIYOY-26SEP",
        "2026-10-28 17:50Z to 18:10Z  KXFED-26OCT",
    ]
    assert releases.describe(wins, t3 + 901) == [] and releases.windows([], 1, 1) == []


def test_recomputed_windows_never_drop_one_that_is_still_open():
    t1, t2 = iso_to_ts("2026-10-14T12:29:00Z"), iso_to_ts("2026-12-10T13:29:00Z")
    old = releases.windows([(t1, "KXCPIYOY-26SEP")], 300, 900)
    fresh = releases.windows([(t2, "KXCPIYOY-26NOV")], 300, 900)          # the roll replaced September by November
    now = t1 + 90                                                          # ninety seconds after the close
    merged = releases.merge_windows(old, fresh, now)
    assert merged == old + fresh
    # Once the September window has ended it is gone
    assert releases.merge_windows(old, fresh, t1 + 901) == fresh
    # Overlaps between old and fresh windows merge, labels once
    assert releases.merge_windows(old, old, now) == old
