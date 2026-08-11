from __future__ import annotations

import sqlite3
from pathlib import Path

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from bist_backtest.loader import coverage_report, day_frame, load_archive
from tests.support.backtest import synthetic_frame


def _write_partition(data_root: Path, session_date: str, symbol: str, closes: list[float]) -> None:
    _write_partition_named(data_root, session_date, symbol, closes, "part-test.parquet")


def _write_partition_named(
    data_root: Path, session_date: str, symbol: str, closes: list[float], name: str
) -> None:
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
    pq.write_table(table, part_dir / name)


def test_load_archive_partitions_by_symbol_day(tmp_path: Path) -> None:
    _write_partition(tmp_path, "2026-08-03", "ASELS.IS", [100.0, 101.0])
    _write_partition(tmp_path, "2026-08-04", "ASELS.IS", [102.0])
    days = load_archive(tmp_path)
    assert len(days) == 2
    assert (days[0].symbol, days[0].session_date) == ("ASELS.IS", "2026-08-03")
    assert days[0].observed_slots == 2
    assert days[1].observed_slots == 1


def test_coverage_report_marks_expected_slots(tmp_path: Path) -> None:
    _write_partition(tmp_path, "2026-08-03", "ASELS.IS", [100.0, 101.0, 102.0])
    report = coverage_report(load_archive(tmp_path))
    assert len(report) == 1
    row = report.iloc[0]
    assert row["expected_slots"] == 3
    assert bool(row["complete"]) is True


def test_day_frame_adds_istanbul_clock(tmp_path: Path) -> None:
    _write_partition(tmp_path, "2026-08-03", "ASELS.IS", [100.0])
    day = load_archive(tmp_path)[0]
    frame = day_frame(day)
    assert "session_datetime_ist" in frame.columns
    assert "minute_of_day" in frame.columns


def test_load_archive_prefers_active_manifest_partitions(tmp_path: Path) -> None:
    # Two parts for the same session: the older superseded one (subset) and the
    # newer active one (full set). The loader must read only the active part.
    _write_partition_named(tmp_path, "2026-08-03", "ASELS.IS", [100.0], "part-old.parquet")
    _write_partition_named(
        tmp_path, "2026-08-03", "THYAO.IS", [50.0], "part-new.parquet"
    )
    state = tmp_path / "state"
    state.mkdir()
    database = state / "collector.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE compaction_manifests ("
            "attempt_id TEXT PRIMARY KEY, source TEXT NOT NULL, "
            "interval_seconds INTEGER NOT NULL, session_date TEXT NOT NULL, "
            "state TEXT NOT NULL, temp_path TEXT NOT NULL, final_path TEXT, "
            "content_sha256 TEXT, row_count INTEGER, error TEXT, "
            "created_at_utc TEXT NOT NULL, activated_at_utc TEXT, "
            "temp_removed_at_utc TEXT, temp_removal_reason TEXT)"
        )
        connection.execute(
            "INSERT INTO compaction_manifests VALUES "
            "('old','yahoo_chart',60,'2026-08-03','superseded','t','"
            "archive/source=yahoo_chart/interval=60/session_date=2026-08-03/part-old.parquet',"
            "'s',1,NULL,'2026-08-03',NULL,NULL,NULL), "
            "('new','yahoo_chart',60,'2026-08-03','active','t','"
            "archive/source=yahoo_chart/interval=60/session_date=2026-08-03/part-new.parquet',"
            "'s',1,NULL,'2026-08-03',NULL,NULL,NULL)"
        )
    days = load_archive(tmp_path)
    assert [d.symbol for d in days] == ["THYAO.IS"]
