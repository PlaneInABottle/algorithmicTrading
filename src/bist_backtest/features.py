from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True, slots=True)
class PreviousSession:
    """Summary of the prior eligible session for the same symbol."""

    close: float
    high: float
    low: float
    session_date: str


@dataclass(frozen=True, slots=True)
class Features:
    frame: pd.DataFrame
    previous: PreviousSession | None
    opening_range_high: float | None
    opening_range_low: float | None
    first_window_return: float | None
    vwap: pd.Series
    sma_fast: pd.Series
    sma_slow: pd.Series
    rsi: pd.Series
    volume_avg: pd.Series


def compute_features(
    frame: pd.DataFrame,
    *,
    previous_session: PreviousSession | None = None,
    opening_minutes: int = 30,
    first_window_minutes: int = 30,
    sma_fast: int = 5,
    sma_slow: int = 20,
    rsi_period: int = 14,
    volume_avg_window: int = 20,
) -> Features:
    """Compute the intraday primitives every registered strategy consumes."""
    if opening_minutes <= 0 or first_window_minutes <= 0:
        raise ValueError("windows must be positive")
    if sma_fast <= 0 or sma_slow <= 0:
        raise ValueError("sma windows must be positive")
    if rsi_period <= 0 or volume_avg_window <= 0:
        raise ValueError("rsi and volume windows must be positive")
    if frame.empty:
        raise ValueError("cannot compute features on an empty frame")
    minute_of_day = frame["minute_of_day"].to_numpy(dtype="int64")
    first = int(minute_of_day[0])
    frame_begin = frame[minute_of_day < first + opening_minutes]
    opening_range_high = float(frame_begin["high"].max()) if not frame_begin.empty else None
    opening_range_low = float(frame_begin["low"].min()) if not frame_begin.empty else None
    window_end = first + first_window_minutes
    window = frame[minute_of_day < window_end]
    first_window_return = None
    if not window.empty:
        close0 = float(window["close"].iloc[0])
        close1 = float(window["close"].iloc[-1])
        if close0 > 0:
            first_window_return = close1 / close0 - 1.0
    typical = (frame["high"] + frame["low"] + frame["close"]) / 3.0
    volume = frame["volume"].astype("float64")
    cum_vol = volume.cumsum()
    vwap = (typical * volume).cumsum() / cum_vol.replace(0.0, np.nan)
    return Features(
        frame=frame,
        previous=previous_session,
        opening_range_high=opening_range_high,
        opening_range_low=opening_range_low,
        first_window_return=first_window_return,
        vwap=vwap,
        sma_fast=frame["close"].rolling(sma_fast).mean(),
        sma_slow=frame["close"].rolling(sma_slow).mean(),
        rsi=_wilders_rsi(frame["close"], rsi_period),
        volume_avg=volume.rolling(volume_avg_window, min_periods=1).mean(),
    )


def _wilders_rsi(close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0).ewm(alpha=1.0 / period, adjust=False).mean()
    loss = (-delta.clip(upper=0.0)).ewm(alpha=1.0 / period, adjust=False).mean()
    rs = gain / loss.replace(0.0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi.fillna(100.0)
