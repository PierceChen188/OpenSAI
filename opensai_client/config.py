"""
OpenSAI SDK configuration loading, validation and redaction.

Priority (highest first): ``overrides`` > environment variables > config file
> built-in defaults. Unknown keys are warned about; type or range errors raise
:class:`~opensai_contracts.errors.ConfigError`.

The resulting config is an immutable :class:`~opensai_contracts.schema.OpenSAIConfig`.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Mapping

from opensai_contracts.errors import ConfigError, ErrorCode
from opensai_contracts.schema import OpenSAIConfig

__all__ = ["Config", "ConfigError"]

log = logging.getLogger(__name__)

# field -> (env var name, expected python type)
_ENV_MAP: dict[str, tuple[str, type]] = {
    "server_host": ("OPENSAI_SERVER_HOST", str),
    "server_port": ("OPENSAI_SERVER_PORT", int),
    "redis_host": ("OPENSAI_REDIS_HOST", str),
    "redis_port": ("OPENSAI_REDIS_PORT", int),
    "redis_db": ("OPENSAI_REDIS_DB", int),
    "redis_username": ("OPENSAI_REDIS_USERNAME", str),
    "redis_password": ("OPENSAI_REDIS_PASSWORD", str),
    "redis_ssl": ("OPENSAI_REDIS_SSL", bool),
    "timeout": ("OPENSAI_TIMEOUT", float),
    "poll_interval": ("OPENSAI_POLL_INTERVAL", float),
    "max_frame_bytes": ("OPENSAI_MAX_FRAME_BYTES", int),
    "tls": ("OPENSAI_TLS", bool),
    "log_level": ("OPENSAI_LOG_LEVEL", str),
}

_VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}


def _coerce(value: Any, typ: type, field: str) -> Any:
    """Coerce ``value`` to ``typ``; raise ConfigError on failure."""
    if value is None:
        return None
    if typ is bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            if value.strip().lower() in ("1", "true", "yes", "on"):
                return True
            if value.strip().lower() in ("0", "false", "no", "off"):
                return False
        if isinstance(value, int) and value in (0, 1):
            return bool(value)
        raise ConfigError(
            f"config field '{field}' must be a boolean, got {value!r}",
            code=ErrorCode.CONFIG_ERROR,
            details={"field": field},
        )
    try:
        return typ(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(
            f"config field '{field}' must be {typ.__name__}, got {value!r}",
            code=ErrorCode.CONFIG_ERROR,
            details={"field": field, "expected": typ.__name__},
        ) from exc


class Config:
    """Immutable config loader / validator / redactor."""

    # ── load ─────────────────────────────────────────────────────────────

    @staticmethod
    def load(
        path: str | os.PathLike | None = None,
        env: Mapping[str, str] | None = None,
        overrides: Mapping[str, Any] | None = None,
    ) -> OpenSAIConfig:
        """Build a config from defaults < file < env < overrides.

        Parameters
        ----------
        path:
            Optional JSON/YAML config file.
        env:
            Optional environment mapping (defaults to ``os.environ``).
        overrides:
            Highest-priority explicit values.

        Raises
        ------
        ConfigError
            On unknown-typed / out-of-range values.
        """
        env = env if env is not None else os.environ

        merged: dict[str, Any] = {}
        for field in _ENV_MAP:
            merged[field] = getattr(OpenSAIConfig(), field)

        # 2. config file
        file_cfg = _read_config_file(path) if path is not None else {}
        _apply(merged, file_cfg, "file")

        # 3. environment variables
        env_cfg = _read_env(env)
        _apply(merged, env_cfg, "env")

        # 4. overrides
        if overrides:
            _apply(merged, overrides, "overrides")

        # coerce types and validate ranges
        coerced = {}
        for field, value in merged.items():
            coerced[field] = _coerce(value, _ENV_MAP[field][1], field)
        return Config.validate(OpenSAIConfig(**coerced))

    # ── validate ─────────────────────────────────────────────────────────

    @staticmethod
    def validate(config: OpenSAIConfig) -> OpenSAIConfig:
        """Validate address/port/timeout/poll/redis/frame/tls/log-level ranges."""
        if not isinstance(config, OpenSAIConfig):
            raise ConfigError(
                "config must be an OpenSAIConfig",
                code=ErrorCode.CONFIG_ERROR,
            )
        if not config.server_host or not isinstance(config.server_host, str):
            raise ConfigError("server_host must be a non-empty string", code=ErrorCode.CONFIG_ERROR)
        _check_port(config.server_port, "server_port")
        if config.redis_host is not None and (not isinstance(config.redis_host, str) or not config.redis_host):
            raise ConfigError("redis_host must be a non-empty string", code=ErrorCode.CONFIG_ERROR)
        if config.redis_port is not None:
            _check_port(config.redis_port, "redis_port")
        if config.redis_db is not None and not (0 <= config.redis_db <= 15):
            raise ConfigError(
                f"redis_db must be 0..15, got {config.redis_db}",
                code=ErrorCode.CONFIG_ERROR,
                details={"field": "redis_db", "value": config.redis_db},
            )
        if config.timeout is not None and config.timeout <= 0:
            raise ConfigError(f"timeout must be > 0, got {config.timeout}", code=ErrorCode.CONFIG_ERROR)
        if config.poll_interval is not None and config.poll_interval <= 0:
            raise ConfigError(f"poll_interval must be > 0, got {config.poll_interval}", code=ErrorCode.CONFIG_ERROR)
        if config.max_frame_bytes is not None and config.max_frame_bytes <= 0:
            raise ConfigError(f"max_frame_bytes must be > 0, got {config.max_frame_bytes}", code=ErrorCode.CONFIG_ERROR)
        if config.log_level is not None and config.log_level.upper() not in _VALID_LOG_LEVELS:
            raise ConfigError(
                f"log_level must be one of {sorted(_VALID_LOG_LEVELS)}, got {config.log_level}",
                code=ErrorCode.CONFIG_ERROR,
                details={"field": "log_level", "value": config.log_level},
            )
        return config

    # ── redact ───────────────────────────────────────────────────────────

    @staticmethod
    def redacted(config: OpenSAIConfig) -> dict:
        """Return a loggable dict with credentials replaced by ``***``."""
        return config.redacted()


# ── internal helpers ────────────────────────────────────────────────────────

def _check_port(value: int | None, field: str) -> None:
    if value is not None and not (1 <= value <= 65535):
        raise ConfigError(
            f"{field} must be 1..65535, got {value}",
            code=ErrorCode.CONFIG_ERROR,
            details={"field": field, "value": value},
        )


def _apply(target: dict, source: Mapping, source_name: str) -> None:
    for key, value in source.items():
        if key not in _ENV_MAP:
            log.warning("ignoring unknown config key '%s' from %s", key, source_name)
            continue
        target[key] = value


def _read_env(env: Mapping[str, str]) -> dict:
    out: dict[str, Any] = {}
    for field, (var, _typ) in _ENV_MAP.items():
        if var in env:
            out[field] = env[var]
    return out


def _read_config_file(path: str | os.PathLike) -> dict:
    p = Path(path)
    try:
        text = p.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise ConfigError(f"config file not found: {p}", code=ErrorCode.CONFIG_ERROR)
    if p.suffix.lower() in (".yaml", ".yml"):
        try:
            import yaml

            data = yaml.safe_load(text)
        except Exception as exc:  # pragma: no cover - yaml optional
            raise ConfigError(f"invalid YAML config {p}: {exc}", code=ErrorCode.CONFIG_ERROR)
    else:
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ConfigError(f"invalid JSON config {p}: {exc}", code=ErrorCode.CONFIG_ERROR)
    if not isinstance(data, Mapping):
        raise ConfigError(f"config file {p} must be a mapping", code=ErrorCode.CONFIG_ERROR)
    return dict(data)
