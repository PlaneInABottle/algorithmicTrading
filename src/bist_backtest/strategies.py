from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import numpy as np

from .features import Features

DecisionFn = Callable[[Features, int], str]
ParamValue = float | int | str


def _finite(value: object) -> bool:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return False
    return bool(np.isfinite(number))


def _finite_positive(*values: object) -> bool:
    return all(_finite(value) and float(value) > 0 for value in values)  # type: ignore[arg-type]


def _hold_until(features: Features, i: int, minute: int) -> bool:
    return i > 0 and int(features.frame["minute_of_day"].iloc[i]) >= minute


# --- factories: each returns a closed-over decision function ------------------


def make_opening_breakout(opening_minutes: int = 30, exit_minutes: int = 9999) -> DecisionFn:
    if opening_minutes <= 0 or exit_minutes <= 0:
        raise ValueError("opening_minutes and exit_minutes must be positive")

    def fn(features: Features, i: int) -> str:
        if i == 0:
            return "hold"
        if _hold_at_least(features, i, exit_minutes):
            return "exit"
        close = float(features.frame["close"].iloc[i])
        high = features.opening_range_high
        if high is None:
            return "hold"
        if close <= high:
            return "hold"
        return "long"

    return fn


def make_vwap_reversion(deviation_threshold: float = 0.005) -> DecisionFn:
    if deviation_threshold <= 0:
        raise ValueError("deviation_threshold must be positive")

    def fn(features: Features, i: int) -> str:
        if i == 0:
            return "hold"
        vwap = features.vwap.iloc[i]
        close = float(features.frame["close"].iloc[i])
        if not _finite_positive(vwap, close):
            return "hold"
        deviation = close / vwap - 1.0
        return "long" if deviation <= -deviation_threshold else "hold"

    return fn


def make_intraday_momentum(window_minutes: int = 30, direction: int = 1) -> DecisionFn:
    if window_minutes <= 0:
        raise ValueError("window_minutes must be positive")
    if direction not in (-1, 1):
        raise ValueError("direction must be -1 or 1")

    def fn(features: Features, i: int) -> str:
        if i == 0:
            return "hold"
        first_minute = int(features.frame["minute_of_day"].iloc[0])
        current_minute = int(features.frame["minute_of_day"].iloc[i])
        if current_minute < first_minute + window_minutes - 1:
            return "hold"
        ret = features.first_window_return
        if ret is None:
            return "hold"
        if direction == 1 and ret > 0:
            return "long"
        if direction == -1 and ret < 0:
            return "long"
        return "hold"

    return fn


def make_sma_crossover(fast: int = 5, slow: int = 20) -> DecisionFn:
    if fast <= 0 or slow <= 0 or fast >= slow:
        raise ValueError("sma windows must be positive with fast < slow")

    def fn(features: Features, i: int) -> str:
        fast_series = features.sma_fast
        slow_series = features.sma_slow
        fv = fast_series.iloc[i]
        sv = slow_series.iloc[i]
        if not _finite_positive(fv, sv):
            return "hold"
        if float(fv) > float(sv):
            return "long"
        if float(fv) < float(sv):
            return "exit"
        return "hold"

    return fn


def make_rsi_pullback(
    rsi_period: int = 14, rsi_below: float = 30.0, exit_above: float = 55.0
) -> DecisionFn:
    if rsi_period <= 0 or not (0 < rsi_below < 100) or not (0 < exit_above < 100):
        raise ValueError("invalid rsi parameters")

    def fn(features: Features, i: int) -> str:
        if i == 0:
            return "hold"
        rsi = features.rsi.iloc[i]
        if not _finite(rsi):
            return "hold"
        if float(rsi) < rsi_below:
            return "long"
        if float(rsi) > exit_above:
            return "exit"
        return "hold"

    return fn


def make_previous_day_breakout(min_gap: float = 0.0) -> DecisionFn:
    if min_gap < 0:
        raise ValueError("min_gap must be nonnegative")

    def fn(features: Features, i: int) -> str:
        if i == 0:
            return "hold"
        previous = features.previous
        if previous is None:
            return "hold"
        close = float(features.frame["close"].iloc[i])
        breakout = close > previous.high * (1.0 + min_gap)
        return "long" if breakout else "hold"

    return fn


def make_volume_breakout(opening_minutes: int = 30, volume_mult: float = 2.0) -> DecisionFn:
    if opening_minutes <= 0 or volume_mult <= 0:
        raise ValueError("opening_minutes and volume_mult must be positive")

    def fn(features: Features, i: int) -> str:
        if i == 0:
            return "hold"
        close = float(features.frame["close"].iloc[i])
        high = features.opening_range_high
        volume = float(features.frame["volume"].iloc[i])
        avg = features.volume_avg.iloc[i]
        if high is None or not _finite_positive(high, avg) or volume < 0:
            return "hold"
        if close > high and volume >= volume_mult * float(avg):
            return "long"
        return "hold"

    return fn


def make_buy_hold() -> DecisionFn:
    def fn(_features: Features, i: int) -> str:
        return "long" if i == 0 else "hold"

    return fn


def _hold_at_least(features: Features, i: int, minutes: int) -> bool:
    return int(features.frame["minute_of_day"].iloc[i]) >= minutes


# --- registry ----------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class StrategyFn:
    name: str
    describe: str
    params: dict[str, ParamValue]
    fn: DecisionFn


@dataclass(frozen=True, slots=True)
class StrategySpec:
    name: str
    describe: str
    defaults: dict[str, ParamValue]
    factory: Callable[..., DecisionFn]


SPECS: dict[str, StrategySpec] = {
    name: spec
    for name, spec in (
        (
            "opening-range-breakout",
            StrategySpec(
                "opening-range-breakout",
                "long break of the opening range high, optional early exit at exit_minutes",
                {"opening_minutes": 30, "exit_minutes": 9999},
                make_opening_breakout,
            ),
        ),
        (
            "vwap-mean-reversion",
            StrategySpec(
                "vwap-mean-reversion",
                "long when close is far below VWAP, exit at close",
                {"deviation_threshold": 0.005},
                make_vwap_reversion,
            ),
        ),
        (
            "intraday-momentum",
            StrategySpec(
                "intraday-momentum",
                "long when the first 30 minutes were positive",
                {"window_minutes": 30},
                make_intraday_momentum,
            ),
        ),
        (
            "intraday-reversal",
            StrategySpec(
                "intraday-reversal",
                "long when the first 30 minutes were negative",
                {"window_minutes": 30, "direction": -1},
                make_intraday_momentum,
            ),
        ),
        (
            "sma-crossover",
            StrategySpec(
                "sma-crossover",
                "long while fast SMA exceeds slow SMA",
                {"fast": 5, "slow": 20},
                make_sma_crossover,
            ),
        ),
        (
            "rsi-pullback",
            StrategySpec(
                "rsi-pullback",
                "long when RSI dips below rsi_below, exit above exit_above",
                {"rsi_period": 14, "rsi_below": 30.0, "exit_above": 55.0},
                make_rsi_pullback,
            ),
        ),
        (
            "previous-day-breakout",
            StrategySpec(
                "previous-day-breakout",
                "long when close exceeds the previous session high",
                {"min_gap": 0.0},
                make_previous_day_breakout,
            ),
        ),
        (
            "volume-breakout",
            StrategySpec(
                "volume-breakout",
                "long when close breaks the opening range with volume_mult volume",
                {"opening_minutes": 30, "volume_mult": 2.0},
                make_volume_breakout,
            ),
        ),
        (
            "buy-and-hold",
            StrategySpec(
                "buy-and-hold",
                "enter long at the first bar, exit at session close",
                {},
                make_buy_hold,
            ),
        ),
    )
}


def strategy_names() -> list[str]:
    return sorted(SPECS)


def build_strategy(
    name: str, params: dict[str, ParamValue] | None = None
) -> tuple[StrategyFn, dict[str, ParamValue]]:
    """Build a strategy with optional parameter overrides, validating keys."""
    spec = SPECS.get(name)
    if spec is None:
        raise KeyError(f"unknown strategy {name!r}; available: {', '.join(strategy_names())}")
    overrides = dict(params or {})
    unknown = set(overrides) - set(spec.defaults)
    if unknown:
        raise ValueError(f"unknown parameters for {name}: {sorted(unknown)}")
    merged = {**spec.defaults, **overrides}
    fn = spec.factory(**merged)
    return StrategyFn(name=name, describe=spec.describe, params=merged, fn=fn), merged


STRATEGIES: dict[str, StrategyFn] = {name: build_strategy(name)[0] for name in SPECS}


def resolve(name: str) -> StrategyFn:
    if name not in STRATEGIES:
        raise KeyError(f"unknown strategy {name!r}; available: {', '.join(strategy_names())}")
    return STRATEGIES[name]


def decisions_for(fn: DecisionFn, features: Features) -> list[str]:
    """Evaluate a strategy over every bar, returning one decision per bar."""
    return [fn(features, i) for i in range(len(features.frame))]
