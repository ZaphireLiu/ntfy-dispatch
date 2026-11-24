"""TOML configuration loading and validation."""

from __future__ import annotations

import math
import os
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}


class ConfigError(ValueError):
    """Raised when the configuration file is missing or invalid."""


@dataclass(frozen=True, slots=True)
class AuthConfig:
    type: str = "none"
    token_env: str | None = None
    username_env: str | None = None
    password_env: str | None = None


@dataclass(frozen=True, slots=True)
class NtfyConfig:
    name: str = "default"
    base_url: str = "https://ntfy.sh"
    auth: AuthConfig = field(default_factory=AuthConfig)
    read_timeout: float = 120.0
    connect_timeout: float = 10.0
    min_backoff: float = 1.0
    max_backoff: float = 60.0
    resume: bool = True
    resume_overlap_seconds: int = 120
    tls_verify: bool = True
    ca_file: Path | None = None


@dataclass(frozen=True, slots=True)
class CoreConfig:
    log_level: str = "INFO"
    state_dir: Path = Path("state")
    handler_packages: tuple[str, ...] = ("handlers",)
    queue_size: int = 1000
    stats_interval: float = 300.0
    shutdown_drain_seconds: float = 10.0
    http_timeout: float = 10.0


@dataclass(frozen=True, slots=True)
class DedupeConfig:
    path: Path
    retention_days: int = 7
    max_rows: int = 200_000
    memory_cache: int = 10_000
    prune_interval_seconds: float = 3600.0


@dataclass(frozen=True, slots=True)
class AppConfig:
    core: CoreConfig
    ntfy: NtfyConfig
    dedupe: DedupeConfig
    handler_configs: Mapping[str, Mapping[str, Any]]


def _table(data: Mapping[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key, {})
    if not isinstance(value, dict):
        raise ConfigError(f"{key!r} must be a table")
    return value


def _unknown_keys(table: Mapping[str, Any], allowed: set[str], where: str) -> None:
    unknown = sorted(set(table) - allowed)
    if unknown:
        raise ConfigError(f"{where}: unknown key(s): {', '.join(unknown)}")


def _str(table: Mapping[str, Any], key: str, default: str, where: str) -> str:
    value = table.get(key, default)
    if not isinstance(value, str) or not value:
        raise ConfigError(f"{where}.{key} must be a non-empty string")
    return value


def _optional_str(table: Mapping[str, Any], key: str, where: str) -> str | None:
    value = table.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ConfigError(f"{where}.{key} must be a non-empty string")
    return value


def _bool(table: Mapping[str, Any], key: str, default: bool, where: str) -> bool:
    value = table.get(key, default)
    if not isinstance(value, bool):
        raise ConfigError(f"{where}.{key} must be a boolean")
    return value


def _int(
    table: Mapping[str, Any],
    key: str,
    default: int,
    where: str,
    *,
    minimum: int | None = None,
) -> int:
    value = table.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"{where}.{key} must be an integer")
    if isinstance(value, float) and not value.is_integer():
        raise ConfigError(f"{where}.{key} must be an integer")
    result = int(value)
    if minimum is not None and result < minimum:
        raise ConfigError(f"{where}.{key} must be >= {minimum}")
    return result


def _float(
    table: Mapping[str, Any],
    key: str,
    default: float,
    where: str,
    *,
    minimum: float | None = None,
) -> float:
    value = table.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"{where}.{key} must be a number")
    result = float(value)
    if not math.isfinite(result):
        raise ConfigError(f"{where}.{key} must be a finite number")
    if minimum is not None and result < minimum:
        raise ConfigError(f"{where}.{key} must be >= {minimum}")
    return result


def _str_list(
    table: Mapping[str, Any], key: str, default: list[str], where: str
) -> tuple[str, ...]:
    value = table.get(key, default)
    if not isinstance(value, list):
        raise ConfigError(f"{where}.{key} must be a list of strings")
    result: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item:
            raise ConfigError(f"{where}.{key} must contain non-empty strings")
        result.append(item)
    return tuple(result)


def _resolve_path(base_dir: Path, value: str | Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return path


def _validate_base_url(value: str) -> str:
    value = value.rstrip("/")
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ConfigError("ntfy.base_url must be an absolute http(s) URL")
    if parsed.query or parsed.fragment:
        raise ConfigError("ntfy.base_url must not contain a query string or fragment")
    return value


def load_config(path: str | Path) -> AppConfig:
    """Load and validate a TOML configuration file."""

    config_path = Path(path)
    try:
        with config_path.open("rb") as handle:
            data = tomllib.load(handle)
    except FileNotFoundError as exc:
        raise ConfigError(f"config file not found: {config_path}") from exc
    except OSError as exc:
        raise ConfigError(f"cannot read config file {config_path}: {exc}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"invalid TOML in {config_path}: {exc}") from exc

    if not isinstance(data, dict):
        raise ConfigError("top-level TOML value must be a table")

    _unknown_keys(data, {"core", "ntfy", "dedupe", "handlers"}, "top level")
    base_dir = config_path.resolve().parent

    core_table = _table(data, "core")
    _unknown_keys(
        core_table,
        {
            "log_level",
            "state_dir",
            "handler_packages",
            "queue_size",
            "stats_interval",
            "shutdown_drain_seconds",
            "http_timeout",
        },
        "[core]",
    )
    log_level = _str(core_table, "log_level", "INFO", "[core]").upper()
    if log_level not in LOG_LEVELS:
        raise ConfigError(f"[core].log_level must be one of {', '.join(sorted(LOG_LEVELS))}")
    state_dir = _resolve_path(base_dir, _str(core_table, "state_dir", "state", "[core]"))
    handler_packages = _str_list(core_table, "handler_packages", ["handlers"], "[core]")
    if not handler_packages:
        raise ConfigError("[core].handler_packages must not be empty")
    core = CoreConfig(
        log_level=log_level,
        state_dir=state_dir,
        handler_packages=handler_packages,
        queue_size=_int(core_table, "queue_size", 1000, "[core]", minimum=1),
        stats_interval=_float(core_table, "stats_interval", 300.0, "[core]", minimum=0.001),
        shutdown_drain_seconds=_float(
            core_table, "shutdown_drain_seconds", 10.0, "[core]", minimum=0.0
        ),
        http_timeout=_float(core_table, "http_timeout", 10.0, "[core]", minimum=0.001),
    )

    ntfy_table = _table(data, "ntfy")
    _unknown_keys(
        ntfy_table,
        {
            "name",
            "base_url",
            "auth",
            "token_env",
            "username_env",
            "password_env",
            "read_timeout",
            "connect_timeout",
            "min_backoff",
            "max_backoff",
            "resume",
            "resume_overlap_seconds",
            "tls_verify",
            "ca_file",
        },
        "[ntfy]",
    )
    auth_type = _str(ntfy_table, "auth", "none", "[ntfy]").lower()
    if auth_type not in {"none", "token", "basic"}:
        raise ConfigError("[ntfy].auth must be one of: none, token, basic")
    token_env = _optional_str(ntfy_table, "token_env", "[ntfy]")
    username_env = _optional_str(ntfy_table, "username_env", "[ntfy]")
    password_env = _optional_str(ntfy_table, "password_env", "[ntfy]")
    if auth_type == "token" and token_env is None:
        raise ConfigError("[ntfy].token_env is required when auth = 'token'")
    if auth_type == "basic" and (username_env is None or password_env is None):
        raise ConfigError(
            "[ntfy].username_env and [ntfy].password_env are required when auth = 'basic'"
        )

    tls_verify = _bool(ntfy_table, "tls_verify", True, "[ntfy]")
    ca_file_value = _optional_str(ntfy_table, "ca_file", "[ntfy]")
    ca_file = _resolve_path(base_dir, ca_file_value) if ca_file_value else None
    if ca_file is not None and not tls_verify:
        raise ConfigError("[ntfy].ca_file requires tls_verify = true")

    min_backoff = _float(ntfy_table, "min_backoff", 1.0, "[ntfy]", minimum=0.001)
    max_backoff = _float(ntfy_table, "max_backoff", 60.0, "[ntfy]", minimum=0.001)
    if min_backoff > max_backoff:
        raise ConfigError("[ntfy].min_backoff must be <= max_backoff")

    ntfy = NtfyConfig(
        name=_str(ntfy_table, "name", "default", "[ntfy]"),
        base_url=_validate_base_url(_str(ntfy_table, "base_url", "https://ntfy.sh", "[ntfy]")),
        auth=AuthConfig(
            type=auth_type,
            token_env=token_env,
            username_env=username_env,
            password_env=password_env,
        ),
        read_timeout=_float(ntfy_table, "read_timeout", 120.0, "[ntfy]", minimum=0.001),
        connect_timeout=_float(ntfy_table, "connect_timeout", 10.0, "[ntfy]", minimum=0.001),
        min_backoff=min_backoff,
        max_backoff=max_backoff,
        resume=_bool(ntfy_table, "resume", True, "[ntfy]"),
        resume_overlap_seconds=_int(ntfy_table, "resume_overlap_seconds", 120, "[ntfy]", minimum=0),
        tls_verify=tls_verify,
        ca_file=ca_file,
    )

    dedupe_table = _table(data, "dedupe")
    _unknown_keys(
        dedupe_table,
        {"path", "retention_days", "max_rows", "memory_cache", "prune_interval_seconds"},
        "[dedupe]",
    )
    dedupe_path_value = _optional_str(dedupe_table, "path", "[dedupe]")
    dedupe_path = (
        _resolve_path(base_dir, dedupe_path_value)
        if dedupe_path_value
        else state_dir / "dedupe.sqlite3"
    )
    dedupe = DedupeConfig(
        path=dedupe_path,
        retention_days=_int(dedupe_table, "retention_days", 7, "[dedupe]", minimum=1),
        max_rows=_int(dedupe_table, "max_rows", 200_000, "[dedupe]", minimum=0),
        memory_cache=_int(dedupe_table, "memory_cache", 10_000, "[dedupe]", minimum=0),
        prune_interval_seconds=_float(
            dedupe_table, "prune_interval_seconds", 3600.0, "[dedupe]", minimum=1.0
        ),
    )

    handlers_table = _table(data, "handlers")
    handler_configs: dict[str, dict[str, Any]] = {}
    for name, value in handlers_table.items():
        if not isinstance(value, dict):
            raise ConfigError(f"[handlers.{name}] must be a table")
        handler_configs[name] = dict(value)

    return AppConfig(
        core=core,
        ntfy=ntfy,
        dedupe=dedupe,
        handler_configs=handler_configs,
    )


def missing_secret_envs(config: NtfyConfig) -> list[str]:
    """Return required secret environment variables that are not set."""

    missing: list[str] = []
    if config.auth.type == "token" and config.auth.token_env:
        if not os.environ.get(config.auth.token_env):
            missing.append(config.auth.token_env)
    elif config.auth.type == "basic":
        for env_name in (config.auth.username_env, config.auth.password_env):
            if env_name and os.environ.get(env_name) is None:
                missing.append(env_name)
    return missing


def validate_runtime_config(config: AppConfig) -> None:
    missing = missing_secret_envs(config.ntfy)
    if missing:
        raise ConfigError(
            "missing required environment variable(s): " + ", ".join(sorted(set(missing)))
        )
