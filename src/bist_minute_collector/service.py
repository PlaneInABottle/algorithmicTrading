from __future__ import annotations

import os
import subprocess
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

SERVICE_NAME = "bist-minute-collector.service"
TIMER_NAME = "bist-minute-collector.timer"
UNIT_NAMES = (SERVICE_NAME, TIMER_NAME)

CommandRunner = Callable[[tuple[str, ...]], subprocess.CompletedProcess[str]]
FaultInjector = Callable[[str], None]


class ServiceError(RuntimeError):
    """Raised when exact user-unit validation or installation fails."""


@dataclass(frozen=True, slots=True)
class ServicePaths:
    project_root: Path
    executable: Path
    data_root: Path
    user_unit_directory: Path

    @classmethod
    def current(cls) -> ServicePaths:
        project = Path(__file__).resolve().parents[2]
        return cls(
            project,
            project / ".venv" / "bin" / "bist-minute-collector",
            Path.home() / ".local" / "share" / "bist-minute-collector" / "data",
            Path.home() / ".config" / "systemd" / "user",
        )


@dataclass(frozen=True, slots=True)
class InstallReport:
    installed: bool
    unit_paths: tuple[Path, Path]


def _run(args: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, check=False, capture_output=True, text=True)


def render_user_units(paths: ServicePaths) -> dict[str, str]:
    definitions = paths.project_root / "systemd"
    rendered: dict[str, str] = {}
    for name in UNIT_NAMES:
        source = definitions / name
        try:
            body = source.read_text(encoding="utf-8")
        except OSError as error:
            raise ServiceError(f"cannot read repository unit {source}: {error}") from error
        rendered[name] = body
    service = rendered[SERVICE_NAME]
    expected = (
        f"WorkingDirectory={paths.project_root}",
        f"ExecStart={paths.executable} run-once --data-root {paths.data_root}",
        f"ReadWritePaths={paths.data_root}",
    )
    missing = tuple(item for item in expected if item not in service)
    if missing:
        raise ServiceError(f"repository service does not match exact runtime paths: {missing}")
    return rendered


def _verify(paths: ServicePaths, command: CommandRunner) -> None:
    sources = tuple(str(paths.project_root / "systemd" / name) for name in UNIT_NAMES)
    result = command(("systemd-analyze", "--user", "verify", *sources))
    if result.returncode != 0:
        message = (result.stderr or result.stdout).strip()
        raise ServiceError(f"systemd unit verification failed: {message}")


def _atomic_write(path: Path, body: bytes) -> None:
    descriptor, raw_temp = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(raw_temp)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(body)
            output.flush()
            os.fsync(output.fileno())
        temporary.chmod(0o644)
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def install_user_units(
    paths: ServicePaths,
    *,
    dry_run: bool,
    command: CommandRunner = _run,
    fault: FaultInjector = lambda _point: None,
) -> InstallReport:
    rendered = render_user_units(paths)
    _verify(paths, command)
    targets = (
        paths.user_unit_directory / SERVICE_NAME,
        paths.user_unit_directory / TIMER_NAME,
    )
    if dry_run:
        return InstallReport(False, targets)

    paths.user_unit_directory.mkdir(parents=True, exist_ok=True, mode=0o755)
    prior = {target: (target.read_bytes() if target.exists() else None) for target in targets}
    changed: list[Path] = []
    try:
        for index, target in enumerate(targets):
            _atomic_write(target, rendered[target.name].encode())
            changed.append(target)
            fault(f"after-unit-{index + 1}")
        reload_result = command(("systemctl", "--user", "daemon-reload"))
        if reload_result.returncode != 0:
            message = (reload_result.stderr or reload_result.stdout).strip()
            raise ServiceError(f"user daemon-reload failed: {message}")
    except BaseException:
        for target in reversed(changed):
            original = prior[target]
            if original is None:
                target.unlink(missing_ok=True)
            else:
                _atomic_write(target, original)
        raise
    return InstallReport(True, targets)


def status_is_healthy(
    *,
    service_active: str,
    service_sub: str,
    service_result: str,
    timer_active: str,
    timer_sub: str,
    timer_enabled: bool,
) -> bool:
    service_ok = (
        service_active == "inactive" and service_sub == "dead" and service_result == "success"
    ) or service_active == "activating"
    return service_ok and timer_active == "active" and timer_sub == "waiting" and timer_enabled


def exact_systemctl(action: str, command: CommandRunner = _run) -> subprocess.CompletedProcess[str]:
    commands = {
        "start-now": ("systemctl", "--user", "start", SERVICE_NAME),
        "enable-timer": ("systemctl", "--user", "enable", "--now", TIMER_NAME),
        "disable-timer": ("systemctl", "--user", "disable", "--now", TIMER_NAME),
    }
    try:
        args = commands[action]
    except KeyError as error:
        raise ValueError(f"unsupported systemd action: {action}") from error
    return command(args)


def _properties(result: subprocess.CompletedProcess[str]) -> dict[str, str]:
    if result.returncode != 0:
        message = (result.stderr or result.stdout).strip()
        raise ServiceError(f"systemctl status query failed: {message}")
    return {
        key: value
        for line in result.stdout.splitlines()
        if "=" in line
        for key, value in (line.split("=", 1),)
    }


def systemd_status(command: CommandRunner = _run) -> dict[str, object]:
    service = _properties(
        command(
            (
                "systemctl",
                "--user",
                "show",
                SERVICE_NAME,
                "--property=ActiveState,SubState,Result,NRestarts",
                "--no-pager",
            )
        )
    )
    timer = _properties(
        command(
            (
                "systemctl",
                "--user",
                "show",
                TIMER_NAME,
                "--property=ActiveState,SubState,NextElapseUSecRealtime",
                "--no-pager",
            )
        )
    )
    enabled_result = command(("systemctl", "--user", "is-enabled", TIMER_NAME))
    enabled = enabled_result.returncode == 0 and enabled_result.stdout.strip() == "enabled"
    healthy = status_is_healthy(
        service_active=service.get("ActiveState", "unknown"),
        service_sub=service.get("SubState", "unknown"),
        service_result=service.get("Result", "unknown"),
        timer_active=timer.get("ActiveState", "unknown"),
        timer_sub=timer.get("SubState", "unknown"),
        timer_enabled=enabled,
    )
    return {
        "healthy": healthy,
        "service": {
            "active": service.get("ActiveState", "unknown"),
            "sub": service.get("SubState", "unknown"),
            "result": service.get("Result", "unknown"),
            "restarts": int(service.get("NRestarts", "0") or 0),
        },
        "timer": {
            "active": timer.get("ActiveState", "unknown"),
            "sub": timer.get("SubState", "unknown"),
            "enabled": enabled,
            "next_trigger": timer.get("NextElapseUSecRealtime", ""),
        },
    }


def journal_logs(command: CommandRunner = _run) -> subprocess.CompletedProcess[str]:
    return command(
        (
            "journalctl",
            f"--user-unit={SERVICE_NAME}",
            "--since=-7days",
            "--lines=200",
            "--no-pager",
        )
    )
