from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from bist_minute_collector.service import (
    SERVICE_NAME,
    TIMER_NAME,
    ServicePaths,
    exact_systemctl,
    install_user_units,
    render_user_units,
    status_is_healthy,
)

ROOT = Path(__file__).resolve().parents[2]


def _paths(tmp_path: Path) -> ServicePaths:
    return ServicePaths(
        project_root=ROOT,
        executable=ROOT / ".venv" / "bin" / "bist-minute-collector",
        data_root=Path("/home/mirza/.local/share/bist-minute-collector/data"),
        user_unit_directory=tmp_path / ".config" / "systemd" / "user",
    )


def test_rendered_service_and_timer_freeze_daily_oneshot_contract(tmp_path: Path) -> None:
    rendered = render_user_units(_paths(tmp_path))

    assert set(rendered) == {SERVICE_NAME, TIMER_NAME}
    service = rendered[SERVICE_NAME]
    timer = rendered[TIMER_NAME]
    assert "Type=oneshot" in service
    assert "Restart=" not in service
    assert "TimeoutStartSec=45min" in service
    assert "TimeoutStopSec=45s" in service
    assert "CPUQuota=50%" in service
    assert "NoNewPrivileges=true" in service
    assert "ProtectSystem=strict" in service
    assert (
        "ExecStart=/home/mirza/Documents/Projects/algorithmicTrading/.venv/bin/"
        "bist-minute-collector run-once --data-root "
        "/home/mirza/.local/share/bist-minute-collector/data" in service
    )
    assert "OnCalendar=Mon..Fri *-*-* 19:30:00 Europe/Istanbul" in timer
    assert "Persistent=true" in timer
    assert "WantedBy=timers.target" in timer
    combined = service + timer
    assert "python3" not in combined
    assert "EnvironmentFile" not in combined
    assert "password" not in combined.lower()
    assert "token" not in combined.lower()


def test_install_dry_run_writes_nothing_and_actual_install_only_writes_two_units(
    tmp_path: Path,
) -> None:
    paths = _paths(tmp_path)
    calls: list[tuple[str, ...]] = []

    def command(args: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        return subprocess.CompletedProcess(args, 0, "", "")

    dry = install_user_units(paths, dry_run=True, command=command)
    assert dry.installed is False
    assert not paths.user_unit_directory.exists()
    assert calls and calls[0][:3] == ("systemd-analyze", "--user", "verify")
    assert not any("enable" in item or "start" in item for call in calls for item in call)

    calls.clear()
    installed = install_user_units(paths, dry_run=False, command=command)
    assert installed.installed is True
    assert sorted(path.name for path in paths.user_unit_directory.iterdir()) == [
        SERVICE_NAME,
        TIMER_NAME,
    ]
    assert calls[-1] == ("systemctl", "--user", "daemon-reload")
    assert not any("enable" in item or "start" in item for call in calls for item in call)


def test_inactive_successful_oneshot_with_waiting_timer_is_healthy() -> None:
    assert status_is_healthy(
        service_active="inactive",
        service_sub="dead",
        service_result="success",
        timer_active="active",
        timer_sub="waiting",
        timer_enabled=True,
    )
    assert not status_is_healthy(
        service_active="failed",
        service_sub="failed",
        service_result="exit-code",
        timer_active="active",
        timer_sub="waiting",
        timer_enabled=True,
    )


def test_partial_unit_install_rolls_back_without_touching_other_files(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    paths.user_unit_directory.mkdir(parents=True)
    unrelated = paths.user_unit_directory / "unrelated.service"
    unrelated.write_text("preserve me", encoding="utf-8")

    def command(args: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args, 0, "", "")

    def fault(point: str) -> None:
        if point == "after-unit-1":
            raise OSError("injected partial install")

    with pytest.raises(OSError, match="injected partial install"):
        install_user_units(paths, dry_run=False, command=command, fault=fault)

    assert unrelated.read_text(encoding="utf-8") == "preserve me"
    assert not (paths.user_unit_directory / SERVICE_NAME).exists()
    assert not (paths.user_unit_directory / TIMER_NAME).exists()


def test_operator_actions_are_restricted_to_exact_units() -> None:
    calls: list[tuple[str, ...]] = []

    def command(args: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        return subprocess.CompletedProcess(args, 0, "", "")

    exact_systemctl("start-now", command)
    exact_systemctl("enable-timer", command)
    exact_systemctl("disable-timer", command)
    assert calls == [
        ("systemctl", "--user", "start", SERVICE_NAME),
        ("systemctl", "--user", "enable", "--now", TIMER_NAME),
        ("systemctl", "--user", "disable", "--now", TIMER_NAME),
    ]
