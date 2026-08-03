from __future__ import annotations

from datetime import datetime, timedelta

from .models import RequestWindow

MAX_REQUEST_WINDOW = timedelta(days=7)


def split_request_window(start_utc: datetime, end_utc: datetime) -> tuple[RequestWindow, ...]:
    """Split a half-open UTC interval into windows of no more than seven days."""

    requested = RequestWindow(start_utc=start_utc, end_utc=end_utc)
    if requested.duration == timedelta(0):
        return ()

    windows: list[RequestWindow] = []
    cursor = requested.start_utc
    while cursor < requested.end_utc:
        boundary = min(cursor + MAX_REQUEST_WINDOW, requested.end_utc)
        windows.append(RequestWindow(cursor, boundary))
        cursor = boundary
    return tuple(windows)
