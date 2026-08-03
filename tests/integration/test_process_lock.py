from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from bist_minute_collector.store import WriterAlreadyOpenError, WriterStore


def start_probe(
    mode: str,
    data_root: Path,
    ready: Path,
    *,
    stopped: Path | None = None,
) -> subprocess.Popen[str]:
    command = [
        sys.executable,
        "-m",
        "tests.support.phase3_process",
        mode,
        "--data-root",
        str(data_root),
        "--ready",
        str(ready),
    ]
    if stopped is not None:
        command.extend(("--stopped", str(stopped)))
    return subprocess.Popen(
        command,
        cwd=Path.cwd(),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def wait_for_file(path: Path, process: subprocess.Popen[str], timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.is_file():
            return
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise AssertionError(
                f"probe exited before readiness: rc={process.returncode} stdout={stdout!r} "
                f"stderr={stderr!r}"
            )
        time.sleep(0.01)
    raise AssertionError(f"probe did not create {path} within {timeout} seconds")


def terminate_probe(process: subprocess.Popen[str]) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.communicate(timeout=2)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate(timeout=2)


@pytest.mark.skipif(os.name != "posix", reason="POSIX process locking/signals required")
def test_second_process_cannot_open_writer(tmp_path: Path) -> None:
    ready = tmp_path / "ready"
    process = start_probe("hold-lock", tmp_path / "data", ready)
    try:
        wait_for_file(ready, process)
        with pytest.raises(WriterAlreadyOpenError):
            WriterStore(tmp_path / "data")
    finally:
        terminate_probe(process)
    assert process.returncode == 0


@pytest.mark.skipif(os.name != "posix", reason="POSIX process locking/signals required")
def test_real_sigterm_cancels_fetch_and_closes_cleanly_without_cursor(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    ready = tmp_path / "ready"
    stopped = tmp_path / "stopped.json"
    process = start_probe("signal-collector", data_root, ready, stopped=stopped)
    try:
        wait_for_file(ready, process)
        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=3)
    finally:
        terminate_probe(process)

    assert process.returncode == 0, (stdout, stderr)
    evidence = json.loads(stopped.read_text(encoding="utf-8"))
    assert evidence == {"cancelled_requests": 1, "quick_check": "ok"}
    with WriterStore(data_root) as reopened:
        assert reopened.cursor("yahoo-chart", "ASELS.E", 60) is None
        assert reopened.provenance_count() == 0
        assert reopened.request_attempt_state(1) == "cancelled"


@pytest.mark.skipif(os.name != "posix", reason="POSIX SIGKILL required")
def test_real_sigkill_rolls_back_uncommitted_bar_cursor_and_provenance(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    ready = tmp_path / "ready"
    process = start_probe("killed-transaction", data_root, ready)
    try:
        wait_for_file(ready, process)
        process.kill()
        process.communicate(timeout=3)
    finally:
        terminate_probe(process)

    assert process.returncode == -signal.SIGKILL
    with WriterStore(data_root) as reopened:
        assert reopened.quick_check() == "ok"
        assert reopened.active_bar_count() == 0
        assert reopened.provenance_count() == 0
        assert reopened.cursor("yahoo-chart", "ASELS.E", 60) is None
