from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import NoReturn

from .archive import ArchiveCompactor, ArchiveError
from .calendar_engine import BistCalendar
from .collector import Collector, CollectorConfig, SymbolRoute
from .gaps import GapState, RecoveryPlanner
from .provider import YahooChartProvider
from .recovery import RecoveryEngine
from .snapshots import (
    SnapshotValidationError,
    load_calendar_snapshot,
    load_universe_snapshot,
)
from .store import SCHEMA_VERSION, StoreError, WriterStore

ROOT = Path(__file__).resolve().parents[2]
UNIVERSE_PATH = ROOT / "config" / "universes" / "bist50" / "2026q3.json"
CALENDAR_PATH = ROOT / "config" / "calendars" / "bist" / "2026.json"
DEFAULT_DATA_ROOT = Path.home() / ".local" / "share" / "bist-minute-collector" / "data"


def _default_data_root() -> Path:
    return Path(os.environ.get("BIST_MINUTE_COLLECTOR_DATA_ROOT", DEFAULT_DATA_ROOT))


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
    backfill = commands.add_parser(
        "backfill", help="plan bounded historical recovery; execution is opt-in"
    )
    backfill.add_argument("--data-root", type=Path, default=_default_data_root())
    backfill.add_argument("--symbols", required=True)
    backfill.add_argument("--from", dest="from_utc", required=True)
    backfill.add_argument("--to", dest="to_utc", required=True)
    mode = backfill.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--execute", action="store_true")
    gaps = commands.add_parser("gaps", help="report the append-only current gap ledger")
    gaps.add_argument("--data-root", type=Path, default=_default_data_root())
    gaps.add_argument("--state")
    gaps.add_argument("--json", action="store_true")
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
    try:
        snapshot = load_universe_snapshot(UNIVERSE_PATH)
    except SnapshotValidationError as error:
        _fail(str(error))
    aliases = {member.alias_for("yahoo_chart"): member.exchange_code for member in snapshot.members}
    unknown = tuple(symbol for symbol in symbols if symbol not in aliases)
    if unknown:
        _fail(f"symbols are not in the active immutable universe: {','.join(unknown)}")
    return tuple(SymbolRoute(aliases[symbol], symbol) for symbol in symbols)


def _parse_utc(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        _fail(f"{field} must be an ISO-8601 timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        _fail(f"{field} must be timezone-aware UTC")
    return parsed.astimezone(UTC)


def _backfill(
    data_root: Path,
    raw_symbols: str,
    raw_from: str,
    raw_to: str,
    *,
    execute: bool,
) -> int:
    routes = _routes(raw_symbols)
    start = _parse_utc(raw_from, "--from")
    end = _parse_utc(raw_to, "--to")
    if end <= start:
        _fail("--to must be after --from")
    try:
        calendar = BistCalendar(load_calendar_snapshot(CALENDAR_PATH))
        snapshot = load_universe_snapshot(UNIVERSE_PATH)
    except SnapshotValidationError as error:
        _fail(str(error))
    now = datetime.now(UTC)
    plans = tuple(
        (
            route,
            RecoveryPlanner(calendar).plan(
                source="yahoo_chart",
                exchange_code=route.exchange_code,
                start_utc=start,
                end_utc=end,
                now_utc=now,
            ),
        )
        for route in routes
    )
    if not execute:
        state_counts = Counter(item.state.value for _, plan in plans for item in plan.gaps)
        print(
            json.dumps(
                {
                    "dry_run": True,
                    "from_utc": start.isoformat(),
                    "gap_states": dict(sorted(state_counts.items())),
                    "planned_requests": sum(len(plan.tasks) for _, plan in plans),
                    "symbols": [route.provider_symbol for route, _ in plans],
                    "to_utc": end.isoformat(),
                },
                sort_keys=True,
            )
        )
        return 0
    reports = []
    try:
        with WriterStore(data_root) as store:
            store.activate_universe(snapshot, activated_at_utc=now)
            provider = YahooChartProvider(timeout_seconds=15)
            for route, _ in plans:
                reports.append(
                    asyncio.run(
                        RecoveryEngine(store, provider, calendar).recover(
                            route,
                            start_utc=start,
                            end_utc=end,
                            now_utc=now,
                        )
                    )
                )
    except (OSError, StoreError, ValueError) as error:
        _fail(str(error))
    print(
        json.dumps(
            {
                "denied": [item.denied_reason for item in reports if item.denied_reason],
                "dry_run": False,
                "pending_gaps": sum(item.pending_gaps for item in reports),
                "provider_empty_gaps": sum(item.provider_empty_gaps for item in reports),
                "recovered_gaps": sum(item.recovered_gaps for item in reports),
                "requests": sum(item.requests for item in reports),
                "unrecoverable_gaps": sum(item.unrecoverable_gaps for item in reports),
            },
            sort_keys=True,
        )
    )
    return 0 if not any(item.denied_reason for item in reports) else 1


def _gaps(data_root: Path, raw_states: str | None) -> int:
    wanted: frozenset[str] | None = None
    if raw_states:
        values = frozenset(item.strip() for item in raw_states.split(",") if item.strip())
        allowed = frozenset(state.value for state in GapState)
        unknown = values - allowed
        if unknown:
            _fail(f"unknown gap states: {','.join(sorted(unknown))}")
        wanted = values
    try:
        with WriterStore.open_reader(data_root) as reader:
            rows = reader.gap_current_rows(wanted)
    except (OSError, StoreError) as error:
        _fail(str(error))
    counts = Counter(str(row["state"]) for row in rows)
    print(
        json.dumps(
            {
                "counts": dict(sorted(counts.items())),
                "gaps": rows,
                "ok": True,
            },
            sort_keys=True,
        )
    )
    return 0


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
    if arguments.command == "backfill":
        return _backfill(
            arguments.data_root,
            arguments.symbols,
            arguments.from_utc,
            arguments.to_utc,
            execute=arguments.execute,
        )
    if arguments.command == "gaps":
        return _gaps(arguments.data_root, arguments.state)
    raise AssertionError(f"unhandled command: {arguments.command}")
