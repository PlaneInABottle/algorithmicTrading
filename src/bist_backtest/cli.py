from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path

from .engine import CostConfig
from .ledger import already_tried, append_attempt, print_attempts
from .loader import ArchiveError, load_archive
from .metrics import metrics_for
from .report import print_summary, write_report
from .runners import previous_session_map, run_buy_hold, run_day
from .strategies import SPECS, StrategyFn, build_strategy

DEFAULT_DATA_ROOT = Path.home() / ".local/share/bist-minute-collector/data"
DEFAULT_OUTPUT = Path("backtest-results")

ENGINEERING_SYMBOLS = [
    "ASELS.IS",
    "THYAO.IS",
    "GARAN.IS",
    "AKBNK.IS",
    "KCHOL.IS",
    "SISE.IS",
    "EREGL.IS",
    "TUPRS.IS",
    "BIMAS.IS",
    "SAHOL.IS",
]


def _param(value: str) -> tuple[str, object]:
    if "=" not in value:
        raise argparse.ArgumentTypeError(f"expected key=value, got {value!r}")
    key, raw = value.split("=", 1)
    if not key:
        raise argparse.ArgumentTypeError("parameter key cannot be empty")
    try:
        parsed: object = int(raw)
    except ValueError:
        try:
            parsed = float(raw)
        except ValueError:
            parsed = raw
    return key, parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bist-backtest",
        description="Deterministic intraday backtests over the BIST minute-bar archive",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="run one strategy over the archive")
    run.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT)
    run.add_argument("--strategy", required=True, choices=sorted(SPECS))
    run.add_argument("--param", type=_param, action="append", default=[], metavar="KEY=VALUE")
    run.add_argument("--symbols", nargs="*", default=ENGINEERING_SYMBOLS)
    run.add_argument("--all-symbols", action="store_true")
    run.add_argument(
        "--session-dates",
        nargs="+",
        metavar="YYYY-MM-DD",
        help="run only the listed archive session dates",
    )
    run.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    run.add_argument(
        "--commission-bps", type=float, default=10.0, help="broker commission in basis points"
    )
    run.add_argument(
        "--slippage-bps", type=float, default=5.0, help="adverse slippage in basis points"
    )
    run.add_argument(
        "--half-spread-bps", type=float, default=1.0, help="half spread in basis points"
    )
    run.add_argument(
        "--position-fraction",
        type=float,
        default=1.0,
        help="fraction of equity deployed per entry (0..1)",
    )
    run.add_argument("--allow-short", action="store_true")
    run.add_argument("--starting-equity", type=float, default=100_000.0)
    run.add_argument("--force", action="store_true", help="run even if already tried")

    attempts = sub.add_parser("attempts", help="list previously tried parameter combinations")
    attempts.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "run":
        return run_command(args)
    if args.command == "attempts":
        return print_attempts(args.output)
    parser.error(f"unknown command {args.command!r}")


def run_command(args: argparse.Namespace) -> int:
    try:
        return _run(args)
    except (ArchiveError, ValueError) as error:
        print(f"error: {error}")
        return 1


def _run(args: argparse.Namespace) -> int:
    params: dict[str, float | int | str] = {key: value for key, value in args.param}
    strategy, merged = build_strategy(args.strategy, params)
    days = load_archive(args.data_root)
    if args.all_symbols:
        selected_symbols = sorted({day.symbol for day in days})
    else:
        available = {day.symbol for day in days}
        selected_symbols = [s for s in args.symbols if s in available]
        missing = [s for s in args.symbols if s not in available]
        if missing:
            parser = build_parser()
            parser.error(f"symbols not present in archive: {missing}")
    symbol_days = tuple(day for day in days if day.symbol in selected_symbols)
    selected_days = symbol_days
    if args.session_dates:
        requested_dates = set(args.session_dates)
        available_dates = {day.session_date for day in selected_days}
        missing_dates = sorted(requested_dates - available_dates)
        if missing_dates:
            raise ValueError(f"session dates not present for selected symbols: {missing_dates}")
        selected_days = tuple(day for day in selected_days if day.session_date in requested_dates)
    if not selected_days:
        print("error: no bars found for the selected symbols; check --symbols / --data-root")
        return 1
    session_dates = sorted({day.session_date for day in selected_days})
    if not session_dates:
        print("error: archive contains no session partitions")
        return 1

    if (
        already_tried(
            args.output,
            strategy=args.strategy,
            params=merged_params(strategy),
            symbols=selected_symbols,
            session_dates=session_dates,
        )
        and not args.force
    ):
        print(
            f"skipping {args.strategy} with {sort_params(merged_params(strategy))} "
            f"over {session_dates[0]}..{session_dates[-1]}: already tried "
            "(see `bist-backtest attempts`; use --force to rerun)"
        )
        return 0

    costs = CostConfig(
        commission_bps=args.commission_bps,
        slippage_bps=args.slippage_bps,
        half_spread_bps=args.half_spread_bps,
        position_fraction=args.position_fraction,
        allow_short=args.allow_short,
        starting_equity=args.starting_equity,
    )
    previous_map = previous_session_map(symbol_days)
    results = [
        run_day(day, strategy, costs, previous=previous_map[(day.symbol, day.session_date)])
        for day in selected_days
    ]
    metrics = metrics_for(results, starting_equity=costs.starting_equity)
    benchmark = None
    if args.strategy != "buy-and-hold":
        benchmark_results = [run_buy_hold(day, costs) for day in selected_days]
        benchmark = metrics_for(benchmark_results, starting_equity=costs.starting_equity)
    started_at = datetime.now(UTC)
    manifest_path = write_report(
        args.output,
        strategy=args.strategy,
        description=strategy.describe,
        symbols=selected_symbols,
        session_dates=session_dates,
        started_at=started_at,
        costs=costs,
        params=merged,
        results=results,
        metrics=metrics,
        benchmark=benchmark,
    )
    append_attempt(
        args.output,
        strategy=args.strategy,
        params=merged,
        symbols=selected_symbols,
        session_dates=session_dates,
        metrics=metrics,
        benchmark=benchmark,
        manifest_path=manifest_path,
    )
    print_summary(args.strategy, strategy.describe, metrics, benchmark, params=merged)
    print(f"manifest: {manifest_path}")
    print(f"session dates: {session_dates[0]}..{session_dates[-1]}")
    return 0


def merged_params(strategy: StrategyFn) -> dict[str, float | int | str]:
    return dict(strategy.params)


def sort_params(params: dict[str, float | int | str]) -> str:
    return ", ".join(f"{k}={v}" for k, v in sorted(params.items()))
