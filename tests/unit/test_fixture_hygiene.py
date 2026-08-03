from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

FIXTURES = Path(__file__).parents[1] / "fixtures/yahoo_chart"
FORBIDDEN_KEYS = {
    "authorization",
    "cookie",
    "cookies",
    "set-cookie",
    "api_key",
    "apikey",
    "access_token",
    "refresh_token",
}


def _walk_keys(value: Any) -> set[str]:
    if isinstance(value, dict):
        result_keys = {str(key).lower() for key in value}
        for child in value.values():
            result_keys.update(_walk_keys(child))
        return result_keys
    if isinstance(value, list):
        list_keys: set[str] = set()
        for child in value:
            list_keys.update(_walk_keys(child))
        return list_keys
    return set()


def test_sanitized_json_fixtures_have_no_secrets_personal_paths_or_volatile_headers() -> None:
    fixtures = sorted(path for path in FIXTURES.iterdir() if path.is_file())

    assert len(fixtures) == 8
    for path in fixtures:
        text = path.read_text(encoding="utf-8")
        assert "/home/" not in text
        assert "mirza" not in text.lower()
        assert not re.search(r"(?i)(bearer\s+[a-z0-9._-]+|yahoo_consumer_key|crumb)", text)
        if path.name.endswith(".sanitized.json"):
            payload = json.loads(text)
            assert not (_walk_keys(payload) & FORBIDDEN_KEYS)


def test_success_fixture_capture_metadata_is_explicitly_sanitized() -> None:
    fixtures = sorted(FIXTURES.glob("*.IS-1d-1m-2026-08-03.sanitized.json"))

    assert len(fixtures) == 5
    for path in fixtures:
        payload = json.loads(path.read_text(encoding="utf-8"))
        capture = payload["fixture_capture"]
        assert capture["http_status"] == 200
        assert len(capture["raw_response_sha256"]) == 64
        assert "removed transport headers" in capture["sanitization"].lower()
