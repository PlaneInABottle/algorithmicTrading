from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import NoReturn

from .archive import ArchiveCompactor, ArchiveError
from .store import SCHEMA_VERSION, StoreError, WriterStore


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="bist-minute-collector")
    commands = parser.add_subparsers(dest="command", required=True)
    doctor = commands.add_parser("doctor", help="validate an existing Phase 2 data root")
    doctor.add_argument("--data-root", type=Path, required=True)
    compact = commands.add_parser("compact", help="reconcile and compact an existing data root")
    compact.add_argument("--data-root", type=Path, required=True)
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
                        "recovered_publications": reconciled.recovered,
                    },
                    sort_keys=True,
                )
            )
            return 0 if reconciled.failed == 0 else 1
    except (OSError, StoreError, ArchiveError) as error:
        _fail(str(error))


def main(argv: list[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    if arguments.command == "doctor":
        return _doctor(arguments.data_root)
    if arguments.command == "compact":
        return _compact(arguments.data_root)
    raise AssertionError(f"unhandled command: {arguments.command}")
