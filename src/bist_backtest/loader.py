from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow.parquet as pq  # type: ignore[import-untyped]

ISTANBUL = ZoneInfo("Europe/Istanbul")

MINUTE_BARS_INTERVAL = 60
COLLECTOR_SCHEMA = "bist-minute-bars-v2"


@dataclass(frozen=True, slots=True)
class SymbolDay:
    symbol: str
    session_date: str
    bars: pd.DataFrame
    expected_slots: int
    observed_slots: int
    first_bar_utc: pd.Timestamp
    last_bar_utc: pd.Timestamp

    @property
    def complete(self) -> bool:
        return bool(self.observed_slots == self.expected_slots)

    @property
    def coverage(self) -> float:
        return float(self.observed_slots / self.expected_slots)


class ArchiveError(RuntimeError):
    """Raised when the archive cannot be read or validated."""


def load_archive(data_root: Path) -> tuple[SymbolDay, ...]:
    """Load every active session partition into per-symbol-day frames.

    When the collector database exists, only partitions listed as ``active``
    in its compaction_manifests table are read, so superseded partitions from
    a previous publication are ignored. Without a database (e.g. synthetic
    test archives) every part-*.parquet under the archive is used.
    """
    archive = Path(data_root) / "archive"
    partitions = _active_partitions(data_root) or sorted(
        archive.glob("source=*/interval=*/session_date=*/part-*.parquet")
    )
    if not partitions:
        raise ArchiveError(f"no archive partitions under {archive}")
    days: list[SymbolDay] = []
    seen: set[tuple[str, str]] = set()
    for partition in partitions:
        table = pq.read_table(partition)
        metadata = table.schema.metadata or {}
        if metadata.get(b"collector_schema") != COLLECTOR_SCHEMA.encode():
            raise ArchiveError(f"unexpected collector schema in {partition}")
        frame = table.to_pandas()
        for symbol in sorted(frame["provider_symbol"].dropna().unique()):
            sub = (
                frame[frame["provider_symbol"] == symbol]
                .sort_values("bar_open_utc")
                .drop_duplicates("bar_open_utc", keep="last")
                .reset_index(drop=True)
            )
            session_date = _session_date_of(partition)
            key = (symbol, session_date)
            if key in seen:
                raise ArchiveError(f"duplicate partition for {symbol} on {session_date}")
            seen.add(key)
            if sub.empty:
                continue
            first_utc = pd.Timestamp(sub["bar_open_utc"].iloc[0])
            last_utc = pd.Timestamp(sub["bar_open_utc"].iloc[-1])
            expected = _expected_minute_slots(first_utc, last_utc)
            observed = sub["bar_open_utc"].nunique()
            days.append(
                SymbolDay(
                    symbol=symbol,
                    session_date=session_date,
                    bars=sub,
                    expected_slots=expected,
                    observed_slots=observed,
                    first_bar_utc=first_utc,
                    last_bar_utc=last_utc,
                )
            )
    return tuple(sorted(days, key=lambda d: (d.symbol, d.session_date)))


def _active_partitions(data_root: Path) -> list[Path] | None:
    """Return active partition paths from the collector database, if present."""
    database = Path(data_root) / "state" / "collector.sqlite3"
    if not database.is_file():
        return None
    try:
        with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
            rows = connection.execute(
                "SELECT final_path FROM compaction_manifests "
                "WHERE state='active' AND final_path IS NOT NULL"
            ).fetchall()
    except sqlite3.Error:
        return None
    partitions: list[Path] = []
    for (final_path,) in rows:
        candidate = Path(str(final_path))
        if not candidate.is_absolute():
            candidate = data_root / candidate
        if candidate.is_file():
            partitions.append(candidate)
    return sorted(partitions) if partitions else None


def _session_date_of(partition: Path) -> str:
    for segment in partition.parts:
        if segment.startswith("session_date="):
            value = segment.removeprefix("session_date=")
            if len(value) == 10:
                return value
    raise ArchiveError(f"partition without session_date: {partition}")


def _expected_minute_slots(first: pd.Timestamp, last: pd.Timestamp) -> int:
    span = int((last - first).total_seconds() // MINUTE_BARS_INTERVAL) + 1
    return max(span, 1)


def coverage_report(days: tuple[SymbolDay, ...]) -> pd.DataFrame:
    rows = [
        {
            "symbol": day.symbol,
            "session_date": day.session_date,
            "observed_bars": day.observed_slots,
            "expected_slots": day.expected_slots,
            "coverage": day.coverage,
            "complete": day.complete,
        }
        for day in days
    ]
    return pd.DataFrame(rows)


def day_frame(day: SymbolDay) -> pd.DataFrame:
    """Return the intraday frame with Istanbul session clock columns."""
    frame = day.bars.copy()
    frame["session_datetime_ist"] = frame["bar_open_utc"].dt.tz_convert(ISTANBUL)
    frame["minute_of_day"] = (
        frame["session_datetime_ist"].dt.hour * 60 + frame["session_datetime_ist"].dt.minute
    )
    return frame.reset_index(drop=True)


def to_float(frame: pd.DataFrame) -> pd.DataFrame:
    """Widen the decimal OHLC columns to float64 for simulation."""
    result = frame.copy()
    for column in ("open", "high", "low", "close"):
        if result[column].dtype != "float64":
            result[column] = result[column].astype("float64")
    return result
