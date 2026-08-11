from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from .engine import CostConfig, SimulationResult


def write_report(
    output_dir: Path,
    *,
    strategy: str,
    description: str,
    symbols: list[str],
    session_dates: list[str],
    started_at: datetime,
    costs: CostConfig,
    params: dict[str, float | int | str],
    results: list[SimulationResult],
    metrics: dict[str, float | int | None],
    benchmark: dict[str, float | int | None] | None = None,
    coverage: pd.DataFrame | None = None,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = started_at.strftime("%Y%m%dT%H%M%SZ")
    safe_name = strategy.replace("/", "_")
    param_suffix = (
        "-".join(f"{key}={value}" for key, value in sorted(params.items())) if params else ""
    )
    file_stem = f"{safe_name}-{param_suffix}-{stamp}" if param_suffix else f"{safe_name}-{stamp}"
    first_day = session_dates[0] if session_dates else "?"
    last_day = session_dates[-1] if session_dates else "?"
    manifest = {
        "strategy": strategy,
        "description": description,
        "params": params,
        "symbols": symbols,
        "session_dates": session_dates,
        "started_at_utc": started_at.astimezone(UTC).isoformat(),
        "dataset": f"bist-minute-bars-v2 archive, {first_day}..{last_day}",
        "scope_note": f"Exploratory engineering backtest over {len(session_dates)} sessions; "
        "not evidence of a tradable edge (see BIST_BACKTEST_SYSTEM_RESEARCH.md section 7/9).",
        "metrics": metrics,
        "benchmark_buy_hold": benchmark or {},
        "costs": {
            "commission_bps": costs.commission_bps,
            "slippage_bps": costs.slippage_bps,
            "half_spread_bps": costs.half_spread_bps,
            "position_fraction": costs.position_fraction,
            "allow_short": costs.allow_short,
            "starting_equity": costs.starting_equity,
        },
    }
    manifest_path = output_dir / f"{file_stem}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    if results:
        non_empty = [r.trades for r in results if not r.trades.empty]
        trades = pd.concat(non_empty, ignore_index=True) if non_empty else pd.DataFrame()
        ledger = pd.concat([r.ledger for r in results], ignore_index=True)
        trades.to_csv(output_dir / f"{file_stem}-trades.csv", index=False)
        ledger.to_csv(output_dir / f"{file_stem}-ledger.csv", index=False)
    if coverage is not None:
        coverage.to_csv(output_dir / f"{file_stem}-coverage.csv", index=False)
    return manifest_path


def print_summary(
    strategy: str,
    description: str,
    metrics: dict[str, float | int | None],
    benchmark: dict[str, float | int | None] | None,
    *,
    params: dict[str, float | int | str] | None = None,
) -> None:
    print(f"strategy: {strategy} — {description}")
    if params:
        print(f"  params: {params}")
    ordered = [
        "symbol_days",
        "total_return",
        "profit_factor",
        "trade_count",
        "hit_rate",
        "avg_trade_return",
        "avg_win_return",
        "avg_loss_return",
        "expectancy_per_trade",
        "sharpe",
        "sortino",
        "exposure",
        "max_drawdown",
        "forced_closes",
        "skipped_entries",
    ]
    for key in ordered:
        if key not in metrics:
            continue
        value = metrics[key]
        if value is None:
            print(f"  {key}: n/a")
        elif isinstance(value, float):
            print(f"  {key}: {value:g}")
        else:
            print(f"  {key}: {value}")
    for key, value in metrics.items():
        if key not in ordered:
            print(f"  {key}: {value:g}" if isinstance(value, float) else f"  {key}: {value}")
    if benchmark:
        print("  vs buy-and-hold:")
        print(f"    total_return: {benchmark.get('total_return', 0.0):+.4f}")
