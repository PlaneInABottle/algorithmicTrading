from __future__ import annotations

from dataclasses import replace

import pytest

from bist_backtest.engine import CostConfig, Simulator
from bist_backtest.metrics import metrics_for
from tests.support.backtest import features_from_closes


def run(closes: list[float], decisions: list[str]) -> Simulator:
    features = features_from_closes(closes)
    return Simulator(
        features,
        symbol="TEST.IS",
        session_date="2026-08-03",
        costs=CostConfig(
            commission_bps=0,
            slippage_bps=0,
            half_spread_bps=0,
        ),
        decisions=decisions,
    )


def test_metrics_include_profit_and_risk_fields() -> None:
    sim = run([100.0, 101.0, 99.0, 102.0, 100.0], ["long", "hold", "exit", "long", "exit"])
    metrics = metrics_for([sim.run()])
    for key in (
        "profit_factor",
        "expectancy_per_trade",
        "sharpe",
        "sortino",
        "exposure",
    ):
        assert key in metrics


def test_profit_factor_and_expectancy_math() -> None:
    # long at 101 -> exit 99 (loss -1980); long at 100 -> exit 102 (win +1960)
    sim = run([100.0, 101.0, 99.0, 100.0, 102.0], ["long", "exit", "long", "exit", "hold"])
    metrics = metrics_for([sim.run()])
    assert metrics["profit_factor"] is not None
    assert metrics["profit_factor"] == pytest.approx(1960.0 / 1980.0)
    assert metrics["expectancy_per_trade"] == pytest.approx(-10.0)


def test_exposure_counts_only_bars_in_position() -> None:
    trades = ["long"] + ["hold"] * 3 + ["exit"] + ["hold"] * 4
    sim = run([100.0] * 9, trades)
    exposure = metrics_for([sim.run()])["exposure"]
    assert exposure == pytest.approx(4 / 9)


def test_risk_metrics_annualized_from_session_returns() -> None:
    bigger = run([100.0, 110.0, 115.0], ["long", "hold", "hold"])
    smaller = run([100.0, 102.0, 104.0], ["long", "hold", "hold"])
    day_one = bigger.run()
    day_two = replace(smaller.run(), session_date="2026-08-04")
    metrics = metrics_for([day_one, day_two])
    assert metrics["sharpe"] is not None and metrics["sharpe"] > 0.0
    assert metrics["sortino"] is not None and metrics["sortino"] >= 0.0


def test_symbol_returns_are_averaged_by_session_before_compounding() -> None:
    winner = run([100.0, 100.0, 110.0], ["long", "hold", "hold"]).run()
    loser = replace(
        run([100.0, 100.0, 90.0], ["long", "hold", "hold"]).run(), symbol="OTHER.IS"
    )
    next_winner = replace(winner, session_date="2026-08-04")
    next_winner_two = replace(winner, symbol="OTHER.IS", session_date="2026-08-04")

    first_day_return = (winner.total_return + loser.total_return) / 2.0
    second_day_return = (next_winner.total_return + next_winner_two.total_return) / 2.0
    expected = (1.0 + first_day_return) * (1.0 + second_day_return) - 1.0

    metrics = metrics_for([winner, loser, next_winner, next_winner_two])
    assert metrics["total_return"] == pytest.approx(expected)


def test_empty_results_keep_all_keys() -> None:
    metrics = metrics_for([])
    for key in ("profit_factor", "expectancy_per_trade", "sharpe", "sortino", "exposure"):
        assert metrics[key] == 0.0


def test_zero_trade_run_does_not_crash() -> None:
    sim = run([100.0, 101.0, 102.0], ["hold", "hold", "hold"])
    metrics = metrics_for([sim.run()])
    assert metrics["trade_count"] == 0
    assert metrics["profit_factor"] is None
    assert metrics["avg_win_return"] == 0.0
    assert metrics["avg_loss_return"] == 0.0
    assert metrics["expectancy_per_trade"] == 0.0
    assert metrics["max_drawdown"] == 0.0


def test_all_winners_yield_no_nan_loss_average() -> None:
    sim = run([100.0, 110.0, 110.0], ["long", "hold", "hold"])
    metrics = metrics_for([sim.run()])
    assert metrics["avg_loss_return"] == 0.0
    assert metrics["profit_factor"] is None
    assert metrics["hit_rate"] == 0.0  # zero-pnl forced close is not a win
