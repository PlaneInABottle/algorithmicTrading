from __future__ import annotations

from datetime import timedelta

import pandas as pd

from bist_backtest.features import Features, compute_features


def synthetic_frame(
    closes: list[float],
    *,
    start_minute: int = 600,
    open_multiplier: float = 1.0,
    volume: int = 1000,
) -> pd.DataFrame:
    """Build a minute frame identical in shape to loader.day_frame output."""
    rows = []
    for offset, close in enumerate(closes):
        rows.append(
            {
                "open": close * open_multiplier,
                "high": close * 1.001,
                "low": close * 0.999,
                "close": close,
                "volume": volume,
                "bar_open_utc": pd.Timestamp("2026-08-03 00:00:00+00:00")
                + timedelta(minutes=start_minute + offset),
                "minute_of_day": start_minute + offset,
            }
        )
    return pd.DataFrame(rows)


def features_from_closes(closes: list[float], **kwargs: object) -> Features:
    return compute_features(synthetic_frame(closes), **kwargs)  # type: ignore[arg-type]


def decisions_for(features: Features, fn: object) -> list[str]:
    from bist_backtest.strategies import decisions_for as _decisions_for

    return _decisions_for(fn, features)  # type: ignore[arg-type]
