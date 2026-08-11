from __future__ import annotations

from .engine import CostConfig, SimulationResult, Simulator
from .features import (
    PreviousSession,
    compute_features,
)
from .loader import SymbolDay, day_frame, to_float
from .strategies import StrategyFn, decisions_for

BUY_HOLD_STRATEGY = "buy-and-hold"

# Strategy parameter keys that change feature computation, mapped to
# compute_features keyword arguments. Decision-only keys (exit_minutes,
# deviation_threshold, volume_mult, min_gap, direction) are intentionally
# absent: they are consumed by the decision function, not the features.
FEATURE_PARAM_KEYS: dict[str, str] = {
    "opening_minutes": "opening_minutes",
    "window_minutes": "first_window_minutes",
    "fast": "sma_fast",
    "slow": "sma_slow",
    "rsi_period": "rsi_period",
}


def feature_kwargs(strategy: StrategyFn) -> dict[str, int]:
    """Feature-computation overrides for a strategy's merged parameters."""
    kwargs: dict[str, int] = {}
    for strategy_key, feature_key in FEATURE_PARAM_KEYS.items():
        if strategy_key in strategy.params:
            kwargs[feature_key] = int(strategy.params[strategy_key])
    return kwargs


def run_day(
    day: SymbolDay,
    strategy: StrategyFn,
    costs: CostConfig,
    *,
    previous: PreviousSession | None = None,
) -> SimulationResult:
    """Run one strategy over one symbol-day with next-bar fills."""
    frame = to_float(day_frame(day))
    features = compute_features(frame, previous_session=previous, **feature_kwargs(strategy))
    decisions = decisions_for(strategy.fn, features)
    initial_decision = None
    if strategy.name == BUY_HOLD_STRATEGY:
        decisions = ["hold"] * len(decisions)
        initial_decision = "long"
    sim = Simulator(
        features,
        symbol=day.symbol,
        session_date=day.session_date,
        costs=costs,
        decisions=decisions,
        initial_decision=initial_decision,
    )
    return sim.run()


def run_buy_hold(day: SymbolDay, costs: CostConfig) -> SimulationResult:
    """Equal-weight buy-and-hold control with the same fill and cost model."""
    from .strategies import resolve

    strategy = resolve(BUY_HOLD_STRATEGY)
    return run_day(day, strategy, costs)


def previous_session_map(
    days: tuple[SymbolDay, ...],
) -> dict[tuple[str, str], PreviousSession | None]:
    """Map (symbol, session_date) to the prior eligible session for that symbol."""
    result: dict[tuple[str, str], PreviousSession | None] = {}
    by_symbol: dict[str, list[SymbolDay]] = {}
    for day in days:
        by_symbol.setdefault(day.symbol, []).append(day)
    for _, symbol_days in by_symbol.items():
        ordered = sorted(symbol_days, key=lambda d: d.session_date)
        for position, day in enumerate(ordered):
            previous = ordered[position - 1] if position > 0 else None
            if previous is None:
                result[(day.symbol, day.session_date)] = None
                continue
            frame = to_float(day_frame(previous))
            result[(day.symbol, day.session_date)] = PreviousSession(
                close=float(frame["close"].iloc[-1]),
                high=float(frame["high"].max()),
                low=float(frame["low"].min()),
                session_date=previous.session_date,
            )
    return result
