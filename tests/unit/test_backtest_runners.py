from __future__ import annotations

from pathlib import Path

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from bist_backtest.loader import load_archive
from bist_backtest.runners import previous_session_map, run_buy_hold
from tests.support.backtest import synthetic_frame


def _write_partition(data_root: Path, session_date: str, symbol: str, closes: list[float]) -> None:
    frame = synthetic_frame(closes)
    frame["provider_symbol"] = symbol
    table = pa.Table.from_pandas(frame).replace_schema_metadata(
        {b"collector_schema": b"bist-minute-bars-v2"}
    )
    part_dir = (
        data_root
        / "archive"
        / "source=yahoo_chart"
        / "interval=60"
        / f"session_date={session_date}"
    )
    part_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, part_dir / "part-test.parquet")


def test_previous_session_map_links_consecutive_days(tmp_path: Path) -> None:
    _write_partition(tmp_path, "2026-08-03", "ASELS.IS", [100.0, 101.0, 102.0])
    _write_partition(tmp_path, "2026-08-04", "ASELS.IS", [103.0, 104.0])
    days = load_archive(tmp_path)
    mapping = previous_session_map(days)
    assert mapping[("ASELS.IS", "2026-08-03")] is None
    previous = mapping[("ASELS.IS", "2026-08-04")]
    assert previous is not None
    assert previous.close == 102.0
    assert previous.high == 102.0 * 1.001
    assert previous.low == 100.0 * 0.999
    assert previous.session_date == "2026-08-03"


def test_previous_session_map_ignores_unrelated_symbols(tmp_path: Path) -> None:
    _write_partition(tmp_path, "2026-08-03", "ASELS.IS", [100.0])
    _write_partition(tmp_path, "2026-08-04", "THYAO.IS", [50.0])
    days = load_archive(tmp_path)
    mapping = previous_session_map(days)
    assert mapping[("THYAO.IS", "2026-08-04")] is None


def _momentum_day_closes() -> list[float]:
    """First 30 minutes rise, then fall back to the open: 30-min window is up,
    the 60-min window return is exactly zero."""
    up = [100.0 + k * (3.0 / 29) for k in range(30)]
    down = [103.0 - k * (3.0 / 29) for k in range(30)]
    return up + down


def test_window_minutes_param_reaches_feature_computation(tmp_path: Path) -> None:
    from bist_backtest.engine import CostConfig
    from bist_backtest.runners import run_day
    from bist_backtest.strategies import build_strategy

    _write_partition(tmp_path, "2026-08-03", "TEST.IS", _momentum_day_closes())
    (day,) = load_archive(tmp_path)
    costs = CostConfig(commission_bps=0, slippage_bps=0, half_spread_bps=0)

    assert not run_day(day, build_strategy("intraday-momentum")[0], costs).trades.empty
    no_break = build_strategy("intraday-momentum", {"window_minutes": 60})[0]
    assert run_day(day, no_break, costs).trades.empty


def test_buy_hold_enters_at_the_first_session_open(tmp_path: Path) -> None:
    from bist_backtest.engine import CostConfig

    _write_partition(tmp_path, "2026-08-03", "TEST.IS", [100.0, 101.0, 102.0])
    (day,) = load_archive(tmp_path)
    costs = CostConfig(commission_bps=0, slippage_bps=0, half_spread_bps=0)

    result = run_buy_hold(day, costs)

    assert int(result.fills.iloc[0]["bar_index"]) == 0
    assert float(result.fills.iloc[0]["fill"]) == 100.0


def test_cli_accepts_explicit_session_dates() -> None:
    from bist_backtest.cli import build_parser

    args = build_parser().parse_args(
        [
            "run",
            "--strategy",
            "buy-and-hold",
            "--session-dates",
            "2026-08-03",
            "2026-08-07",
        ]
    )

    assert args.session_dates == ["2026-08-03", "2026-08-07"]
