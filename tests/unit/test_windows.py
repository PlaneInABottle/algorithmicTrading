from __future__ import annotations

from datetime import UTC, datetime, timedelta
from itertools import pairwise

import pytest

from bist_minute_collector.models import RequestWindow
from bist_minute_collector.windows import split_request_window


def test_request_window_splitter_caps_every_window_at_seven_days() -> None:
    start = datetime(2026, 7, 1, 12, tzinfo=UTC)
    end = datetime(2026, 7, 23, 18, tzinfo=UTC)

    windows = split_request_window(start, end)

    assert windows[0].start_utc == start
    assert windows[-1].end_utc == end
    assert all(window.duration <= timedelta(days=7) for window in windows)
    assert all(left.end_utc == right.start_utc for left, right in pairwise(windows))


def test_exactly_seven_days_is_one_window() -> None:
    start = datetime(2026, 7, 1, tzinfo=UTC)
    end = start + timedelta(days=7)

    assert split_request_window(start, end) == (RequestWindow(start, end),)


def test_empty_window_produces_no_request() -> None:
    instant = datetime(2026, 7, 1, tzinfo=UTC)

    assert split_request_window(instant, instant) == ()


def test_naive_or_reverse_windows_are_rejected() -> None:
    aware = datetime(2026, 7, 1, tzinfo=UTC)
    naive = datetime(2026, 7, 1)

    with pytest.raises(ValueError, match="timezone-aware"):
        split_request_window(naive, aware)
    with pytest.raises(ValueError, match="before"):
        split_request_window(aware, aware - timedelta(seconds=1))
