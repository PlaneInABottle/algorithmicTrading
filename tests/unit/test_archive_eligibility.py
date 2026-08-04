from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

from bist_minute_collector.archive import ArchiveCompactor
from bist_minute_collector.store import IngestEnvelope, WriterStore
from tests.helpers import fetch_result, minute_bar


def test_compaction_through_never_publishes_future_session(tmp_path: Path) -> None:
    old = minute_bar(bar_open_utc=datetime(2026, 8, 3, 7, 0, tzinfo=UTC))
    future = minute_bar(bar_open_utc=datetime(2026, 8, 5, 7, 0, tzinfo=UTC))
    with WriterStore(tmp_path) as store:
        store.ingest(
            IngestEnvelope(
                "old",
                "ASELS.E",
                fetch_result(bar=old, response_sha256="1" * 64),
            )
        )
        store.ingest(
            IngestEnvelope(
                "future",
                "ASELS.E",
                fetch_result(bar=future, response_sha256="2" * 64),
            )
        )
        published = ArchiveCompactor(tmp_path, store, min_free_bytes=0).compact_through(
            date(2026, 8, 4)
        )

        assert [item.session_date for item in published] == ["2026-08-03"]
        assert (
            store.integer_scalar(
                "SELECT COUNT(*) FROM compaction_manifests WHERE session_date='2026-08-05'"
            )
            == 0
        )
