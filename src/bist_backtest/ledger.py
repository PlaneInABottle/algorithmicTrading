from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

LEDGER_NAME = "attempts.jsonl"


def attempt_id(
    strategy: str,
    params: dict[str, float | int | str],
    symbols: list[str],
    session_dates: list[str],
) -> str:
    canonical = json.dumps(
        {
            "strategy": strategy,
            "params": params,
            "symbols": symbols,
            "session_dates": session_dates,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def load_attempts(output_dir: Path) -> list[dict[str, object]]:
    path = output_dir / LEDGER_NAME
    if not path.exists():
        return []
    records: list[dict[str, object]] = []
    for line in path.read_text().splitlines():
        if line.strip():
            records.append(json.loads(line))
    return records


def already_tried(
    output_dir: Path,
    *,
    strategy: str,
    params: dict[str, float | int | str],
    symbols: list[str],
    session_dates: list[str],
) -> bool:
    wanted = attempt_id(strategy, params, symbols, session_dates)
    return any(record.get("attempt_id") == wanted for record in load_attempts(output_dir))


def append_attempt(
    output_dir: Path,
    *,
    strategy: str,
    params: dict[str, float | int | str],
    symbols: list[str],
    session_dates: list[str],
    metrics: dict[str, float | int | None],
    benchmark: dict[str, float | int | None] | None,
    manifest_path: Path,
) -> str:
    output_dir.mkdir(parents=True, exist_ok=True)
    record_id = attempt_id(strategy, params, symbols, session_dates)
    record = {
        "attempt_id": record_id,
        "tried_at_utc": datetime.now(UTC).isoformat(),
        "strategy": strategy,
        "params": params,
        "symbols": symbols,
        "session_dates": session_dates,
        "metrics": metrics,
        "benchmark_buy_hold": benchmark,
        "manifest": str(manifest_path),
    }
    with (output_dir / LEDGER_NAME).open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")
    return record_id


def print_attempts(output_dir: Path) -> int:
    records = load_attempts(output_dir)
    if not records:
        print("no attempts recorded yet")
        return 0
    print(f"{len(records)} attempts in {output_dir / LEDGER_NAME}")
    print(
        f"{'tried_at_utc':<26} {'strategy':<28} {'params':<46} {'ret':>8} {'trades':>6} {'hit%':>6}"
    )
    for record in records:
        params = cast(dict[str, float | int | str], record["params"])
        metrics = cast(dict[str, float | int], record["metrics"])
        printed_params = ", ".join(f"{k}={v}" for k, v in sorted(params.items()))
        tried_at = cast(str, record.get("tried_at_utc", "?"))
        print(
            f"{tried_at[:25]:<26} "
            f"{record['strategy']!s:<28} {printed_params:<46} "
            f"{float(metrics['total_return']):>8.4f} "
            f"{int(metrics['trade_count']):>6} "
            f"{100.0 * float(metrics['hit_rate']):>6.1f}"
        )
    return 0
