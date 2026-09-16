"""
Unified structured error contract for OpenSAI.

Every error surfaced across the SDK, Agent, CLI and server carries the same
machine-readable shape::

    { "code": str, "message": str, "retryable": bool, "details": dict }

This module is the *only* authoritative definition of the error shape and of
the stable machine-readable error codes. It is part of the neutral
``opensai_contracts`` package and must never import from ``opensai_client``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

__all__ = [
    "ErrorCode",
    "ErrorDetail",
    "ContractError",
    "ContractParseError",
    "ContractValidationError",
    "SerializationError",
    "ConfigError",
    "JobNotFoundError",
    "TerminalStateError",
    "IdempotencyConflictError",
    "AlgorithmNotRegisteredError",
]


class ErrorCode:
    """Stable machine-readable error codes shared across all components.

    These values are part of the frozen contract: SDK, Agent and CLI must keep
    the same machine codes so callers can branch on them without string
    matching against human-readable messages.
    """

    # ── contract parsing / validation ──────────────────────────────────────
    INVALID_ARGUMENT = "INVALID_ARGUMENT"
    MISSING_FIELD = "MISSING_FIELD"
    UNKNOWN_FIELD = "UNKNOWN_FIELD"
    TYPE_ERROR = "TYPE_ERROR"
    VALUE_ERROR = "VALUE_ERROR"
    DAG_INVALID = "DAG_INVALID"
    CYCLE_DETECTED = "CYCLE_DETECTED"
    DUPLICATE_ID = "DUPLICATE_ID"
    SELF_REFERENCE = "SELF_REFERENCE"
    DUPLICATE_EDGE = "DUPLICATE_EDGE"
    ALGORITHM_NOT_REGISTERED = "ALGORITHM_NOT_REGISTERED"
    ALGORITHM_NOT_SUBMITTABLE = "ALGORITHM_NOT_SUBMITTABLE"
    RESERVED_KEY = "RESERVED_KEY"
    DAG_TOO_LARGE = "DAG_TOO_LARGE"
    PAYLOAD_TOO_LARGE = "PAYLOAD_TOO_LARGE"
    SERIALIZATION_ERROR = "SERIALIZATION_ERROR"

    # ── resource validation (shared with ADP) ──────────────────────────────
    RESOURCE_TYPE = "RESOURCE_TYPE"
    RESOURCE_NOT_POSITIVE = "RESOURCE_NOT_POSITIVE"
    RESOURCE_EXCEEDS_LIMIT = "RESOURCE_EXCEEDS_LIMIT"
    UNKNOWN_RESOURCE_FIELD = "UNKNOWN_RESOURCE_FIELD"
    DAG_NODES_TYPE = "DAG_NODES_TYPE"

    # ── job lifecycle ──────────────────────────────────────────────────────
    JOB_NOT_FOUND = "JOB_NOT_FOUND"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
    TERMINAL_STATE_IMMUTABLE = "TERMINAL_STATE_IMMUTABLE"
    WAIT_TIMEOUT = "WAIT_TIMEOUT"

    # ── transport / config ─────────────────────────────────────────────────
    CONNECTION_ERROR = "CONNECTION_ERROR"
    TIMEOUT = "TIMEOUT"
    CONFIG_ERROR = "CONFIG_ERROR"
    CLIENT_CLOSED = "CLIENT_CLOSED"
    PROTOCOL_ERROR = "PROTOCOL_ERROR"

    # ── uncategorized ──────────────────────────────────────────────────────
    INTERNAL_ERROR = "INTERNAL_ERROR"


@dataclass(frozen=True)
class ErrorDetail:
    """The unified structured error.

    Attributes
    ----------
    code:
        Stable machine code from :class:`ErrorCode`.
    message:
        Human-readable diagnostic (not to be branched on).
    retryable:
        Whether the caller may safely retry the operation.
    details:
        Optional machine-readable context (e.g. ``{"path": "$.nodes[0].id"}``).
    """

    code: str
    message: str
    retryable: bool = False
    details: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Return the canonical ``{code,message,retryable,details}`` mapping."""
        return {
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
            "details": dict(self.details),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ErrorDetail":
        """Build an ``ErrorDetail`` from a mapping (lenient about extras).

        Unlike the strict DAG parsers, error payloads are read defensively:
        unknown keys are ignored so a newer server can still be understood by
        an older client.
        """
        if not isinstance(data, Mapping):
            raise ContractParseError(
                "error detail must be an object",
                code=ErrorCode.TYPE_ERROR,
            )
        code = data.get("code", ErrorCode.INTERNAL_ERROR)
        if not isinstance(code, str) or not code:
            code = ErrorCode.INTERNAL_ERROR
        message = data.get("message", "")
        if not isinstance(message, str):
            message = str(message)
        retryable = bool(data.get("retryable", False))
        details = data.get("details", {})
        if not isinstance(details, Mapping):
            details = {}
        return cls(
            code=code,
            message=message,
            retryable=retryable,
            details=dict(details),
        )

    def __bool__(self) -> bool:
        """An error detail is truthy when it carries a non-empty code."""
        return bool(self.code)


class ContractError(Exception):
    """Base exception carrying a structured :class:`ErrorDetail`.

    ``str(exc)`` returns the human message; ``exc.error`` exposes the
    structured payload for machine consumers.
    """

    default_code = ErrorCode.INVALID_ARGUMENT

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        retryable: bool = False,
        details: dict | None = None,
    ) -> None:
        self.error = ErrorDetail(
            code=code or self.default_code,
            message=message,
            retryable=retryable,
            details=details or {},
        )
        super().__init__(self.error.message)

    @property
    def code(self) -> str:
        return self.error.code

    @property
    def message(self) -> str:
        return self.error.message

    @property
    def retryable(self) -> bool:
        return self.error.retryable

    def to_dict(self) -> dict:
        return self.error.to_dict()


class ContractParseError(ContractError):
    """Raised when a contract structure cannot be parsed (unknown/missing/typed)."""

    default_code = ErrorCode.INVALID_ARGUMENT


class ContractValidationError(ContractError):
    """Raised when a DAG fails shared validation."""

    default_code = ErrorCode.DAG_INVALID


class SerializationError(ContractError):
    """Raised when a value cannot be serialized to a public JSON-compatible form."""

    default_code = ErrorCode.SERIALIZATION_ERROR


class ConfigError(ContractError):
    """Raised for invalid configuration values or ranges."""

    default_code = ErrorCode.CONFIG_ERROR


class JobNotFoundError(ContractError):
    """Raised when a ``job_id`` is unknown or its record has expired."""

    default_code = ErrorCode.JOB_NOT_FOUND


class TerminalStateError(ContractError):
    """Raised when an attempt is made to overwrite a terminal job state."""

    default_code = ErrorCode.TERMINAL_STATE_IMMUTABLE


class IdempotencyConflictError(ContractError):
    """Raised when an idempotency key is re-used with a different payload."""

    default_code = ErrorCode.IDEMPOTENCY_CONFLICT


class AlgorithmNotRegisteredError(ContractError):
    """Raised when a DAG references an algorithm absent from the registry."""

    default_code = ErrorCode.ALGORITHM_NOT_REGISTERED
