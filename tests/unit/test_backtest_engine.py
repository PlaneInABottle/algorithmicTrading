from __future__ import annotations

import pytest

from bist_backtest.engine import CostConfig, Simulator
from tests.support.backtest import features_from_closes


def run(closes: list[float], decisions: list[str], **cost_kwargs: object) -> Simulator:
    features = features_from_closes(closes)
    return Simulator(
        features,
        symbol="TEST.IS",
        session_date="2026-08-03",
        costs=CostConfig(**cost_kwargs),  # type: ignore[arg-type]
        decisions=decisions,
    )


def test_entry_fills_at_next_bar_open() -> None:
    sim = run(
        [100.0, 102.0, 104.0],
        ["long", "hold", "hold"],
        commission_bps=0,
        slippage_bps=0,
        half_spread_bps=0,
    )
    result = sim.run()
    fills = result.fills
    assert len(fills) == 2  # entry + forced exit
    entry = fills.iloc[0]
    assert entry["kind"] == "entry"
    assert entry["bar_index"] == 1  # next bar, not the signal bar
    assert float(entry["reference"]) == pytest.approx(102.0)
    assert float(entry["fill"]) == pytest.approx(102.0)


def test_initial_decision_fills_at_first_bar_open() -> None:
    features = features_from_closes([100.0, 102.0, 104.0])
    result = Simulator(
        features,
        symbol="TEST.IS",
        session_date="2026-08-03",
        costs=CostConfig(commission_bps=0, slippage_bps=0, half_spread_bps=0),
        decisions=["hold", "hold", "hold"],
        initial_decision="long",
    ).run()
    assert int(result.fills.iloc[0]["bar_index"]) == 0
    assert float(result.fills.iloc[0]["fill"]) == pytest.approx(100.0)


def test_forced_close_at_session_end() -> None:
    sim = run(
        [100.0, 102.0, 104.0],
        ["long", "hold", "hold"],
        commission_bps=0,
        slippage_bps=0,
        half_spread_bps=0,
    )
    result = sim.run()
    assert result.trade_count == 1
    assert result.forced_closes == 1
    trade = result.trades.iloc[0]
    assert trade["side"] == "long"
    assert float(trade["entry_fill"]) == pytest.approx(102.0)
    assert float(trade["exit_fill"]) == pytest.approx(104.0)
    quantity = int(100_000.0 / 102.0)
    assert float(trade["pnl"]) == pytest.approx(2.0 * quantity)


def test_costs_bleed_both_sides() -> None:
    sim = run([100.0, 100.0], ["long", "hold"], commission_bps=10, slippage_bps=10)
    result = sim.run()
    fills = result.fills
    entry = fills.iloc[0]
    exit_row = fills.iloc[1]
    assert float(entry["fill"]) > 100.0
    assert float(exit_row["fill"]) < 100.0
    assert float(exit_row["fill"]) < float(entry["fill"])


def test_shorts_disabled_by_default() -> None:
    sim = run([100.0, 98.0, 96.0], ["short", "hold", "hold"])
    result = sim.run()
    assert result.trade_count == 0
    assert result.ledger["position"].max() == 0


def test_shorts_allowed_when_enabled() -> None:
    sim = run([100.0, 98.0, 96.0], ["short", "hold", "hold"], allow_short=True)
    result = sim.run()
    assert result.trade_count == 1
    trade = result.trades.iloc[0]
    assert trade["side"] == "short"
    assert float(trade["pnl"]) > 0


def test_short_costs_use_bid_for_entry_and_ask_for_exit() -> None:
    sim = run(
        [100.0, 100.0],
        ["short", "hold"],
        allow_short=True,
        commission_bps=10,
        slippage_bps=10,
    )
    result = sim.run()
    entry = result.fills.iloc[0]
    exit_row = result.fills.iloc[1]
    assert float(entry["fill"]) < 100.0
    assert float(exit_row["fill"]) > 100.0
    assert float(result.trades.iloc[0]["pnl"]) < 0.0


def test_decision_count_must_match_bars() -> None:
    with pytest.raises(ValueError):
        run([100.0, 101.0], ["hold"])


def test_position_lifecycle_in_ledger() -> None:
    sim = run([100.0, 101.0, 102.0, 103.0], ["long", "hold", "exit", "hold"])
    result = sim.run()
    ledger = result.ledger
    assert int(ledger.iloc[0]["position"]) == 0
    assert int(ledger.iloc[1]["position"]) > 0
    assert int(ledger.iloc[-1]["position"]) == 0
    assert result.trade_count == 1


def test_flat_session_keeps_equity() -> None:
    sim = run([100.0, 100.0, 100.0], ["hold", "hold", "hold"])
    result = sim.run()
    assert result.final_equity == pytest.approx(100_000.0)
    assert result.total_return == pytest.approx(0.0)
