from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from bist_minute_collector.archive import ArchiveCompactor
from bist_minute_collector.store import IngestEnvelope, WriterStore
from tests.helpers import corrected_result, fetch_result


def seed(root: Path) -> None:
    with WriterStore(root) as store:
        store.ingest(
            IngestEnvelope(
                fetch_id="seed",
                exchange_code="ASELS.E",
                result=fetch_result(),
                retained_response=b'{"seed":true}',
            )
        )


@pytest.mark.parametrize(
    "kill_point",
    ["after_temp_write", "after_pending_manifest", "after_final_rename"],
)
def test_reopen_reconciles_each_publication_crash_boundary(tmp_path: Path, kill_point: str) -> None:
    seed(tmp_path)

    def kill(point: str) -> None:
        if point == kill_point:
            raise RuntimeError(f"simulated process death at {point}")

    with WriterStore(tmp_path) as store:
        with pytest.raises(RuntimeError, match="simulated process death"):
            ArchiveCompactor(tmp_path, store, fault_injector=kill).compact_all()
        assert store.active_manifest_count() == 0

    with WriterStore(tmp_path) as reopened:
        recovered = ArchiveCompactor(tmp_path, reopened).reconcile()
        assert recovered.recovered == 1
        assert recovered.failed == 0
        assert reopened.active_manifest_count() == 1
        active = reopened.active_manifests()[0]
        assert active.final_path is not None
        path = tmp_path / active.final_path
        assert path.is_file()
        assert hashlib.sha256(path.read_bytes()).hexdigest() == active.content_sha256
        assert reopened.quick_check() == "ok"


@pytest.mark.parametrize("corruption", ["truncated", "wrong-hash"])
def test_corrupt_pending_publication_fails_closed_and_preserves_prior_active(
    tmp_path: Path, corruption: str
) -> None:
    seed(tmp_path)
    with WriterStore(tmp_path) as store:
        original = ArchiveCompactor(tmp_path, store).compact_all()[0]
        store.ingest(
            IngestEnvelope(
                fetch_id="correction",
                exchange_code="ASELS.E",
                result=corrected_result(close="101.75", observed_minutes=20, response_byte="c"),
                retained_response=b'{"corrected":true}',
            )
        )

        def kill(point: str) -> None:
            if point == "after_pending_manifest":
                raise RuntimeError("leave pending")

        with pytest.raises(RuntimeError, match="leave pending"):
            ArchiveCompactor(tmp_path, store, fault_injector=kill).compact_all(force_rewrite=True)
        pending = store.pending_manifests()[0]
        temp = tmp_path / pending.temp_path
        if corruption == "truncated":
            temp.write_bytes(temp.read_bytes()[:16])
        else:
            store.override_manifest_hash_for_test(pending.attempt_id, "0" * 64)

    with WriterStore(tmp_path) as reopened:
        result = ArchiveCompactor(tmp_path, reopened).reconcile()
        assert result.failed == 1
        assert reopened.active_partition_hashes() == [original.content_sha256]
        assert original.path.read_bytes()
        assert temp.exists()
        assert reopened.quick_check() == "ok"
