from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import NoReturn

from .archive import ArchiveCompactor, ArchiveError
from .collector import Collector, CollectorConfig, SymbolRoute
from .provider import YahooChartProvider
from .snapshots import SnapshotValidationError, load_universe_snapshot
from .store import SCHEMA_VERSION, StoreError, WriterStore


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="bist-minute-collector")
    commands = parser.add_subparsers(dest="command", required=True)
    doctor = commands.add_parser("doctor", help="validate an existing Phase 2 data root")
    doctor.add_argument("--data-root", type=Path, required=True)
    compact = commands.add_parser("compact", help="reconcile and compact an existing data root")
    compact.add_argument("--data-root", type=Path, required=True)
    collect_once = commands.add_parser(
        "collect-once", help="run one bounded foreground collection cycle"
    )
    collect_once.add_argument(
        "--data-root",
        type=Path,
        default=(
            Path(os.environ["BIST_MINUTE_COLLECTOR_DATA_ROOT"])
            if "BIST_MINUTE_COLLECTOR_DATA_ROOT" in os.environ
            else None
        ),
    )
    collect_once.add_argument("--symbols", required=True)
    collect_once.add_argument("--dry-run", action="store_true")
    return parser


def _fail(message: str) -> NoReturn:
    print(json.dumps({"error": message, "ok": False}, sort_keys=True))
    raise SystemExit(1)


def _doctor(data_root: Path) -> int:
    database = data_root.resolve() / "state" / "collector.sqlite3"
    if not database.is_file():
        _fail(f"collector database does not exist: {database}")
    try:
        with WriterStore.open_reader(data_root) as reader:
            quick_check = reader.quick_check()
            version = reader.integer_scalar("PRAGMA user_version")
            active = reader.integer_scalar(
                "SELECT COUNT(*) FROM compaction_manifests WHERE state='active'"
            )
            pending = reader.integer_scalar(
                "SELECT COUNT(*) FROM compaction_manifests WHERE state IN ('writing','pending')"
            )
    except (OSError, StoreError) as error:
        _fail(str(error))
    ok = quick_check == "ok" and version == SCHEMA_VERSION and pending == 0
    print(
        json.dumps(
            {
                "active_partitions": active,
                "ok": ok,
                "pending_publications": pending,
                "quick_check": quick_check,
                "schema_version": version,
            },
            sort_keys=True,
        )
    )
    return 0 if ok else 1


def _compact(data_root: Path) -> int:
    database = data_root.resolve() / "state" / "collector.sqlite3"
    if not database.is_file():
        _fail(f"collector database does not exist: {database}")
    try:
        with WriterStore(data_root) as store:
            compactor = ArchiveCompactor(data_root, store)
            reconciled = compactor.reconcile()
            published = compactor.compact_all()
            print(
                json.dumps(
                    {
                        "failed_reconciliations": reconciled.failed,
                        "ok": reconciled.failed == 0,
                        "partitions": [
                            {
                                "content_sha256": item.content_sha256,
                                "path": item.path.relative_to(data_root.resolve()).as_posix(),
                                "row_count": item.row_count,
                                "session_date": item.session_date,
                            }
                            for item in published
                        ],
                        "cleaned_failed_temps": reconciled.cleaned_failed_temps,
                        "recovered_publications": reconciled.recovered,
                    },
                    sort_keys=True,
                )
            )
            return 0 if reconciled.failed == 0 else 1
    except (OSError, StoreError, ArchiveError) as error:
        _fail(str(error))


def _routes(raw_symbols: str) -> tuple[SymbolRoute, ...]:
    symbols = tuple(item.strip() for item in raw_symbols.split(",") if item.strip())
    if not symbols or len(symbols) > 50 or len(set(symbols)) != len(symbols):
        _fail("symbols must contain between one and 50 distinct aliases")
    universe_path = (
        Path(__file__).resolve().parents[2] / "config" / "universes" / "bist50" / "2026q3.json"
    )
    try:
        snapshot = load_universe_snapshot(universe_path)
    except SnapshotValidationError as error:
        _fail(str(error))
    aliases = {member.alias_for("yahoo_chart"): member.exchange_code for member in snapshot.members}
    unknown = tuple(symbol for symbol in symbols if symbol not in aliases)
    if unknown:
        _fail(f"symbols are not in the active immutable universe: {','.join(unknown)}")
    return tuple(SymbolRoute(aliases[symbol], symbol) for symbol in symbols)


def _collect_once(data_root: Path | None, raw_symbols: str, *, dry_run: bool) -> int:
    routes = _routes(raw_symbols)
    symbols = [route.provider_symbol for route in routes]
    if dry_run:
        print(
            json.dumps(
                {
                    "dry_run": True,
                    "max_concurrency": 2,
                    "planned_requests": len(routes),
                    "symbols": symbols,
                },
                sort_keys=True,
            )
        )
        return 0
    if data_root is None:
        _fail("--data-root or BIST_MINUTE_COLLECTOR_DATA_ROOT is required")
    try:
        with WriterStore(data_root) as store:
            report = asyncio.run(
                Collector(
                    YahooChartProvider(timeout_seconds=15),
                    store,
                    routes,
                    config=CollectorConfig(max_concurrency=2),
                ).collect_once(now_utc=datetime.now(UTC))
            )
            accounted = len(report.completed_symbols) + len(report.denied_symbols)
            ok = (
                accounted == len(routes)
                and len(report.successful_symbols) == len(routes)
                and report.cancelled_requests == 0
            )
            print(
                json.dumps(
                    {
                        "adapter_paused_reason": report.adapter_paused_reason,
                        "cancelled_requests": report.cancelled_requests,
                        "completed_symbols": list(report.completed_symbols),
                        "denied_symbols": list(report.denied_symbols),
                        "dry_run": False,
                        "isolated_symbols": list(report.isolated_symbols),
                        "max_fetch_concurrency": report.max_fetch_concurrency,
                        "max_writer_concurrency": report.max_writer_concurrency,
                        "ok": ok,
                        "queue_full_events": report.queue_full_events,
                        "request_attempts": report.request_attempts,
                        "successful_symbols": list(report.successful_symbols),
                        "symbols": symbols,
                    },
                    sort_keys=True,
                )
            )
            return 0 if ok else 1
    except (OSError, StoreError, ValueError) as error:
        _fail(str(error))


def main(argv: list[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    if arguments.command == "doctor":
        return _doctor(arguments.data_root)
    if arguments.command == "compact":
        return _compact(arguments.data_root)
    if arguments.command == "collect-once":
        return _collect_once(arguments.data_root, arguments.symbols, dry_run=arguments.dry_run)
    raise AssertionError(f"unhandled command: {arguments.command}")
