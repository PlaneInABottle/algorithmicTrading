from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from bist_backtest.engine import CostConfig, Simulator
from bist_backtest.report import write_report
from tests.support.backtest import features_from_closes


def run(closes: list[float]) -> Simulator:
    features = features_from_closes(closes)
    return Simulator(
        features,
        symbol="TEST.IS",
        session_date="2026-08-03",
        costs=CostConfig(commission_bps=0, slippage_bps=0, half_spread_bps=0),
        decisions=["hold"] * len(closes),  # never trades: reports must not crash
    )


def test_float_param_manifest_keeps_full_filename(tmp_path: Path) -> None:
    result = run([100.0, 101.0, 102.0])
    started = datetime(2026, 8, 8, 17, 30, 45, tzinfo=UTC)
    manifest_path = write_report(
        tmp_path,
        strategy="vwap-mean-reversion",
        description="desc",
        symbols=["TEST.IS"],
        session_dates=["2026-08-03"],
        started_at=started,
        costs=CostConfig(commission_bps=10, slippage_bps=5, half_spread_bps=1),
        params={"deviation_threshold": 0.005},
        results=[result.run()],
        metrics={"total_return": 0.0},
        benchmark=None,
    )
    assert (
        manifest_path.name == "vwap-mean-reversion-deviation_threshold=0.005-20260808T173045Z.json"
    )
    assert (
        tmp_path / "vwap-mean-reversion-deviation_threshold=0.005-20260808T173045Z-trades.csv"
    ).exists()
    payload = json.loads(manifest_path.read_text())
    assert payload["params"] == {"deviation_threshold": 0.005}
    assert payload["session_dates"] == ["2026-08-03"]
    assert payload["dataset"].endswith("2026-08-03..2026-08-03")


def test_zero_trade_run_writes_valid_manifest(tmp_path: Path) -> None:
    started = datetime(2026, 8, 8, 12, 0, 0, tzinfo=UTC)
    manifest_path = write_report(
        tmp_path,
        strategy="opening-range-breakout",
        description="or",
        symbols=["TEST.IS"],
        session_dates=["2026-08-03"],
        started_at=started,
        costs=CostConfig(commission_bps=10, slippage_bps=5, half_spread_bps=1),
        params={"opening_minutes": 30, "exit_minutes": 9999},
        results=[run([100.0, 101.0, 102.0]).run()],
        metrics={"total_return": 0.0, "trade_count": 0},
        benchmark=None,
    )
    json.loads(manifest_path.read_text())  # must serialize without NaN/KeyError
