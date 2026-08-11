from __future__ import annotations

from pathlib import Path

import pytest

from bist_backtest.ledger import (
    already_tried,
    append_attempt,
    attempt_id,
    load_attempts,
    print_attempts,
)


def test_attempt_id_is_deterministic_and_sensitive() -> None:
    base = {
        "strategy": "sma-crossover",
        "params": {"fast": 3},
        "symbols": ["A.IS"],
        "session_dates": ["2026-08-03"],
    }
    first = attempt_id(**base)  # type: ignore[arg-type]
    assert first == attempt_id(**base)  # type: ignore[arg-type]
    changed = attempt_id("sma-crossover", {"fast": 5}, ["A.IS"], ["2026-08-03"])
    assert changed != first


def test_append_and_reload(tmp_path: Path) -> None:
    record_id = append_attempt(
        tmp_path,
        strategy="sma-crossover",
        params={"fast": 3},
        symbols=["A.IS"],
        session_dates=["2026-08-03"],
        metrics={"total_return": 0.01},
        benchmark={"total_return": 0.02},
        manifest_path=tmp_path / "m.json",
    )
    assert (tmp_path / "attempts.jsonl").exists()
    records = load_attempts(tmp_path)
    assert len(records) == 1
    assert records[0]["attempt_id"] == record_id
    assert already_tried(
        tmp_path,
        strategy="sma-crossover",
        params={"fast": 3},
        symbols=["A.IS"],
        session_dates=["2026-08-03"],
    )
    assert not already_tried(
        tmp_path,
        strategy="sma-crossover",
        params={"fast": 5},
        symbols=["A.IS"],
        session_dates=["2026-08-03"],
    )


def test_print_attempts_empty_and_populated(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert print_attempts(tmp_path) == 0
    assert "no attempts" in capsys.readouterr().out
    append_attempt(
        tmp_path,
        strategy="sma-crossover",
        params={"fast": 3},
        symbols=["A.IS"],
        session_dates=["2026-08-03"],
        metrics={"total_return": 0.01, "trade_count": 4, "hit_rate": 0.5},
        benchmark=None,
        manifest_path=tmp_path / "m.json",
    )
    print_attempts(tmp_path)
    assert "sma-crossover" in capsys.readouterr().out
