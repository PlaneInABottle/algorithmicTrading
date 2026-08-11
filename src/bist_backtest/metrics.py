from __future__ import annotations

import math
from typing import cast

import pandas as pd

from .engine import SimulationResult

TRADING_DAYS_PER_YEAR = 252.0


def metrics_for(
    results: list[SimulationResult], *, starting_equity: float = 100_000.0
) -> dict[str, float | int | None]:
    """Aggregate metrics across symbol-day simulations.

    Returns plain primitives only, so the report can serialize them directly.
    """
    if not results:
        return {
            "symbol_days": 0,
            "total_return": 0.0,
            "profit_factor": 0.0,
            "trade_count": 0,
            "hit_rate": 0.0,
            "avg_trade_return": 0.0,
            "avg_win_return": 0.0,
            "avg_loss_return": 0.0,
            "expectancy_per_trade": 0.0,
            "sharpe": 0.0,
            "sortino": 0.0,
            "exposure": 0.0,
            "max_drawdown": 0.0,
            "forced_closes": 0,
            "skipped_entries": 0,
        }
    non_empty = [r.trades for r in results if not r.trades.empty]
    trades = pd.concat(non_empty, ignore_index=True) if non_empty else pd.DataFrame()
    if trades.empty:
        trades = pd.DataFrame({"pnl": pd.Series(dtype=float), "return_pct": pd.Series(dtype=float)})
    equity_series = _pooled_equity(results, starting_equity)
    winners = trades.loc[trades["pnl"] > 0, "pnl"]
    losers = trades.loc[trades["pnl"] < 0, "pnl"]
    gross_profit = float(winners.sum()) if len(winners) else 0.0
    gross_loss = float(-losers.sum()) if len(losers) else 0.0
    session_returns = _portfolio_session_returns(results)
    sharpe, sortino = _risk_metrics(session_returns)
    return {
        "symbol_days": len(results),
        "total_return": float(cast(float, (1.0 + session_returns).prod()) - 1.0),
        "profit_factor": _profit_factor(gross_profit, gross_loss),
        "trade_count": len(trades),
        "hit_rate": float((trades["pnl"] > 0).mean()) if len(trades) else 0.0,
        "avg_trade_return": (float(trades["return_pct"].mean()) if len(trades) else 0.0),
        "avg_win_return": (
            float(trades.loc[trades["pnl"] > 0, "return_pct"].mean()) if len(winners) else 0.0
        ),
        "avg_loss_return": (
            float(trades.loc[trades["pnl"] < 0, "return_pct"].mean()) if len(losers) else 0.0
        ),
        "expectancy_per_trade": float(trades["pnl"].mean()) if len(trades) else 0.0,
        "sharpe": sharpe,
        "sortino": sortino,
        "exposure": _exposure(results),
        "max_drawdown": float(_max_drawdown(equity_series)),
        "forced_closes": sum(r.forced_closes for r in results),
        "skipped_entries": sum(r.skipped_entries for r in results),
    }


def _profit_factor(gross_profit: float, gross_loss: float) -> float | None:
    if gross_profit == 0.0 and gross_loss == 0.0:
        return None
    if gross_loss == 0.0:
        return None
    return gross_profit / gross_loss


def _risk_metrics(session_returns: pd.Series) -> tuple[float, float]:
    """Annualized Sharpe and Sortino from per-session returns."""
    if len(session_returns) < 2:
        return 0.0, 0.0
    mean = float(session_returns.mean())
    std = float(session_returns.std(ddof=1))
    if std == 0.0:
        return 0.0, 0.0
    downside = float(session_returns.clip(upper=0.0).pow(2).mean())
    downside_dev = math.sqrt(downside) if downside > 0.0 else 0.0
    denominator = std if std > 0.0 else 0.0
    sharpe = mean / denominator if denominator else 0.0
    sortino = mean / downside_dev * math.sqrt(TRADING_DAYS_PER_YEAR) if downside_dev else 0.0
    return sharpe * math.sqrt(TRADING_DAYS_PER_YEAR), sortino


def _exposure(results: list[SimulationResult]) -> float:
    """Fraction of tracked bars spent holding a position."""
    in_market = 0
    total = 0
    for r in results:
        positions = r.ledger["position"]
        in_market += int((positions != 0).sum())
        total += len(positions)
    return float(in_market / total) if total else 0.0


def _portfolio_session_returns(results: list[SimulationResult]) -> pd.Series:
    """Mean per-session return across symbols, sorted by session date."""
    values = pd.Series(
        [result.total_return for result in results],
        index=pd.Index([result.session_date for result in results], name="session_date"),
        name="return",
        dtype=float,
    )
    return values.groupby(level=0, sort=True).mean()


def _pooled_equity(results: list[SimulationResult], starting_equity: float) -> pd.Series:
    by_session: dict[str, list[SimulationResult]] = {}
    for result in results:
        by_session.setdefault(result.session_date, []).append(result)

    capital = 1.0
    curves: list[pd.Series[float]] = []
    for session_date in sorted(by_session):
        normalized = []
        for result in by_session[session_date]:
            frame = result.ledger[["bar_open_utc", "equity"]].copy()
            frame["equity"] = frame["equity"] / starting_equity
            normalized.append(frame)
        combined = pd.concat(normalized, ignore_index=True)
        session_curve = combined.groupby("bar_open_utc")["equity"].mean().sort_index() * capital
        curves.append(session_curve)
        capital = float(session_curve.iloc[-1])
    return pd.concat(curves, ignore_index=True)


def _max_drawdown(equity: pd.Series) -> float:
    if equity.empty:
        return 0.0
    running = equity.cummax()
    drawdown = (equity - running) / running
    return -float(drawdown.min())
