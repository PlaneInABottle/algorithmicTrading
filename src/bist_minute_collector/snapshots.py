from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
EXCHANGE_CODE_PATTERN = re.compile(r"^[A-Z0-9]+\.E$")
YAHOO_ALIAS_PATTERN = re.compile(r"^[A-Z0-9]+\.IS$")
ALLOWED_PROVIDER_ALIASES = frozenset({"yahoo_chart"})


class SnapshotValidationError(ValueError):
    """Raised when an immutable source snapshot fails its declared contract."""


@dataclass(frozen=True, slots=True)
class SourceEvidence:
    name: str
    url: str
    retrieved_at_utc: datetime
    sha256: str
    role: str


@dataclass(frozen=True, slots=True)
class UniverseMember:
    exchange_code: str
    provider_aliases: Mapping[str, str]

    def alias_for(self, provider: str) -> str:
        try:
            return self.provider_aliases[provider]
        except KeyError as exc:
            raise SnapshotValidationError(f"unknown provider alias: {provider}") from exc


@dataclass(frozen=True, slots=True)
class UniverseSnapshot:
    schema_version: int
    snapshot_id: str
    index_code: str
    effective_from: date
    effective_to: date
    retrieved_at_utc: datetime
    sources: tuple[SourceEvidence, ...]
    limitations: tuple[str, ...]
    members: tuple[UniverseMember, ...]
    canonical_exchange_sha256: str
    canonical_yahoo_sha256: str

    def compute_exchange_sha256(self) -> str:
        return _symbol_checksum(member.exchange_code for member in self.members)

    def compute_alias_sha256(self, provider: str) -> str:
        return _symbol_checksum(member.alias_for(provider) for member in self.members)


@dataclass(frozen=True, slots=True)
class SourceListComparison:
    agrees: bool
    missing_from_source: tuple[str, ...]
    unexpected_in_source: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SessionTransition:
    state: str
    at: time


@dataclass(frozen=True, slots=True)
class HolidayEvent:
    date: date
    name: str
    state: str


@dataclass(frozen=True, slots=True)
class CalendarSnapshot:
    schema_version: int
    snapshot_id: str
    exchange: str
    timezone: str
    effective_from: date
    effective_to: date
    retrieved_at_utc: datetime
    sources: tuple[SourceEvidence, ...]
    transcription_sha256: str
    session_scope: str
    full_day_transitions: tuple[SessionTransition, ...]
    half_day_transitions: tuple[SessionTransition, ...]
    holiday_events: tuple[HolidayEvent, ...]
    limitations: tuple[str, ...]

    def compute_transcription_sha256(self) -> str:
        payload = {
            "session_scope": self.session_scope,
            "full_day_transitions": [
                {"state": item.state, "at": item.at.isoformat()}
                for item in self.full_day_transitions
            ],
            "half_day_transitions": [
                {"state": item.state, "at": item.at.isoformat()}
                for item in self.half_day_transitions
            ],
            "holiday_events": [
                {"date": item.date.isoformat(), "name": item.name, "state": item.state}
                for item in self.holiday_events
            ],
            "limitations": list(self.limitations),
        }
        encoded = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode()
        return hashlib.sha256(encoded).hexdigest()


def _read_object(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SnapshotValidationError(f"cannot read snapshot {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise SnapshotValidationError("snapshot root must be an object")
    return cast(dict[str, Any], raw)


def _required(mapping: Mapping[str, Any], field: str) -> Any:
    if field not in mapping or mapping[field] in (None, ""):
        raise SnapshotValidationError(f"missing required field: {field}")
    return mapping[field]


def _parse_datetime(value: Any, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise SnapshotValidationError(f"invalid {field}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise SnapshotValidationError(f"{field} must be timezone-aware UTC")
    return parsed.astimezone(UTC)


def _parse_date(value: Any, field: str) -> date:
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise SnapshotValidationError(f"invalid {field}") from exc


def _parse_sha256(value: Any, field: str) -> str:
    parsed = str(value)
    if not SHA256_PATTERN.fullmatch(parsed):
        raise SnapshotValidationError(f"invalid {field}: expected lowercase SHA-256")
    return parsed


def _parse_sources(value: Any) -> tuple[SourceEvidence, ...]:
    if not isinstance(value, list) or not value:
        raise SnapshotValidationError("sources must be a non-empty list")
    sources: list[SourceEvidence] = []
    for raw_source in value:
        if not isinstance(raw_source, dict):
            raise SnapshotValidationError("source must be an object")
        source = cast(dict[str, Any], raw_source)
        url = str(_required(source, "url"))
        if not url.startswith("https://"):
            raise SnapshotValidationError("source url must use HTTPS")
        sources.append(
            SourceEvidence(
                name=str(_required(source, "name")),
                url=url,
                retrieved_at_utc=_parse_datetime(
                    _required(source, "retrieved_at_utc"), "retrieved_at_utc"
                ),
                sha256=_parse_sha256(_required(source, "sha256"), "sha256"),
                role=str(_required(source, "role")),
            )
        )
    return tuple(sources)


def _symbol_checksum(symbols: Any) -> str:
    canonical = "".join(f"{symbol}\n" for symbol in sorted(symbols)).encode()
    return hashlib.sha256(canonical).hexdigest()


def load_universe_snapshot(path: Path) -> UniverseSnapshot:
    raw = _read_object(path)
    effective_from = _parse_date(_required(raw, "effective_from"), "effective_from")
    effective_to = _parse_date(_required(raw, "effective_to"), "effective_to")
    if effective_to < effective_from:
        raise SnapshotValidationError("effective_to must not be before effective_from")

    raw_members = _required(raw, "members")
    if not isinstance(raw_members, list):
        raise SnapshotValidationError("members must be a list")
    members: list[UniverseMember] = []
    exchange_codes: set[str] = set()
    provider_values: dict[str, set[str]] = {
        provider: set() for provider in ALLOWED_PROVIDER_ALIASES
    }
    for item in raw_members:
        if not isinstance(item, dict):
            raise SnapshotValidationError("member must be an object")
        member = cast(dict[str, Any], item)
        exchange_code = str(_required(member, "exchange_code"))
        if not EXCHANGE_CODE_PATTERN.fullmatch(exchange_code):
            raise SnapshotValidationError(f"invalid exchange code: {exchange_code}")
        if exchange_code in exchange_codes:
            raise SnapshotValidationError(f"duplicate exchange code: {exchange_code}")
        exchange_codes.add(exchange_code)

        raw_aliases = _required(member, "provider_aliases")
        if not isinstance(raw_aliases, dict):
            raise SnapshotValidationError("provider_aliases must be an object")
        aliases = {str(key): str(value) for key, value in raw_aliases.items()}
        unknown = set(aliases) - ALLOWED_PROVIDER_ALIASES
        if unknown:
            raise SnapshotValidationError(f"unknown provider alias: {sorted(unknown)}")
        if set(aliases) != ALLOWED_PROVIDER_ALIASES:
            raise SnapshotValidationError("missing yahoo_chart provider alias")
        yahoo_alias = aliases["yahoo_chart"]
        if not YAHOO_ALIAS_PATTERN.fullmatch(yahoo_alias):
            raise SnapshotValidationError(f"invalid yahoo_chart alias: {yahoo_alias}")
        if yahoo_alias in provider_values["yahoo_chart"]:
            raise SnapshotValidationError(f"duplicate provider alias: {yahoo_alias}")
        expected_alias = f"{exchange_code.removesuffix('.E')}.IS"
        if yahoo_alias != expected_alias:
            raise SnapshotValidationError(
                f"yahoo_chart alias does not match versioned suffix rule: {exchange_code}"
            )
        provider_values["yahoo_chart"].add(yahoo_alias)
        members.append(UniverseMember(exchange_code, MappingProxyType(aliases)))

    expected_count = int(_required(raw, "expected_member_count"))
    if len(members) != expected_count:
        raise SnapshotValidationError(
            f"expected_member_count={expected_count}, observed={len(members)}"
        )
    if [member.exchange_code for member in members] != sorted(exchange_codes):
        raise SnapshotValidationError("members must be ASCII-sorted by exchange_code")

    snapshot = UniverseSnapshot(
        schema_version=int(_required(raw, "schema_version")),
        snapshot_id=str(_required(raw, "snapshot_id")),
        index_code=str(_required(raw, "index_code")),
        effective_from=effective_from,
        effective_to=effective_to,
        retrieved_at_utc=_parse_datetime(_required(raw, "retrieved_at_utc"), "retrieved_at_utc"),
        sources=_parse_sources(_required(raw, "sources")),
        limitations=tuple(str(item) for item in _required(raw, "limitations")),
        members=tuple(members),
        canonical_exchange_sha256=_parse_sha256(
            _required(raw, "canonical_exchange_sha256"), "canonical_exchange_sha256"
        ),
        canonical_yahoo_sha256=_parse_sha256(
            _required(raw, "canonical_yahoo_sha256"), "canonical_yahoo_sha256"
        ),
    )
    if snapshot.compute_exchange_sha256() != snapshot.canonical_exchange_sha256:
        raise SnapshotValidationError("canonical_exchange_sha256 mismatch")
    if snapshot.compute_alias_sha256("yahoo_chart") != snapshot.canonical_yahoo_sha256:
        raise SnapshotValidationError("canonical_yahoo_sha256 mismatch")
    return snapshot


def compare_source_members(
    snapshot: UniverseSnapshot, source_exchange_codes: set[str]
) -> SourceListComparison:
    snapshot_codes = {member.exchange_code for member in snapshot.members}
    missing = tuple(sorted(snapshot_codes - source_exchange_codes))
    unexpected = tuple(sorted(source_exchange_codes - snapshot_codes))
    return SourceListComparison(not missing and not unexpected, missing, unexpected)


def _parse_transitions(value: Any, field: str) -> tuple[SessionTransition, ...]:
    if not isinstance(value, list) or not value:
        raise SnapshotValidationError(f"{field} must be a non-empty list")
    transitions: list[SessionTransition] = []
    for item in value:
        if not isinstance(item, dict):
            raise SnapshotValidationError(f"{field} item must be an object")
        transition = cast(dict[str, Any], item)
        try:
            at = time.fromisoformat(str(_required(transition, "at")))
        except ValueError as exc:
            raise SnapshotValidationError(f"invalid {field} time") from exc
        transitions.append(SessionTransition(str(_required(transition, "state")), at))
    if [item.at for item in transitions] != sorted(item.at for item in transitions):
        raise SnapshotValidationError(f"{field} must be chronological")
    return tuple(transitions)


def load_calendar_snapshot(path: Path) -> CalendarSnapshot:
    raw = _read_object(path)
    effective_from = _parse_date(_required(raw, "effective_from"), "effective_from")
    effective_to = _parse_date(_required(raw, "effective_to"), "effective_to")
    if effective_to < effective_from:
        raise SnapshotValidationError("effective_to must not be before effective_from")
    timezone = str(_required(raw, "timezone"))
    try:
        ZoneInfo(timezone)
    except ZoneInfoNotFoundError as exc:
        raise SnapshotValidationError(f"unknown timezone: {timezone}") from exc

    raw_holidays = _required(raw, "holiday_events")
    if not isinstance(raw_holidays, list):
        raise SnapshotValidationError("holiday_events must be a list")
    holidays: list[HolidayEvent] = []
    for item in raw_holidays:
        if not isinstance(item, dict):
            raise SnapshotValidationError("holiday event must be an object")
        event = cast(dict[str, Any], item)
        holidays.append(
            HolidayEvent(
                date=_parse_date(_required(event, "date"), "holiday date"),
                name=str(_required(event, "name")),
                state=str(_required(event, "state")),
            )
        )

    snapshot = CalendarSnapshot(
        schema_version=int(_required(raw, "schema_version")),
        snapshot_id=str(_required(raw, "snapshot_id")),
        exchange=str(_required(raw, "exchange")),
        timezone=timezone,
        effective_from=effective_from,
        effective_to=effective_to,
        retrieved_at_utc=_parse_datetime(_required(raw, "retrieved_at_utc"), "retrieved_at_utc"),
        sources=_parse_sources(_required(raw, "sources")),
        transcription_sha256=_parse_sha256(
            _required(raw, "transcription_sha256"), "transcription_sha256"
        ),
        session_scope=str(_required(raw, "session_scope")),
        full_day_transitions=_parse_transitions(
            _required(raw, "full_day_transitions"), "full_day_transitions"
        ),
        half_day_transitions=_parse_transitions(
            _required(raw, "half_day_transitions"), "half_day_transitions"
        ),
        holiday_events=tuple(holidays),
        limitations=tuple(str(item) for item in _required(raw, "limitations")),
    )
    if snapshot.compute_transcription_sha256() != snapshot.transcription_sha256:
        raise SnapshotValidationError("transcription_sha256 mismatch")
    return snapshot
