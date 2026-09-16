"""
OpenSAI SDK exceptions (unified, carry ``opensai_contracts.ErrorDetail``).

Every SDK exception is a :class:`~opensai_contracts.errors.ContractError`
subclass, so it carries the same ``{code, message, retryable, details}`` shape
as every other layer. The stable machine codes come from
``opensai_contracts.ErrorCode``.
"""

from __future__ import annotations

from opensai_contracts.errors import ContractError, ErrorCode

__all__ = [
    "OpenSAIClientError",
    "TransportError",
    "TransportTimeoutError",
    "TimeoutError",
    "TransportClosedError",
    "ProtocolFrameTooLarge",
    "ClientClosedError",
    "MonitorUnavailableError",
    "RemoteError",
    "AgentControlError",
    "AGENT_DENIAL_CODES",
    "ConfigError",
]

# ── Agent-control denial codes (§2.8.3) ─────────────────────────────────────
# Stable machine codes emitted by ``OpenSAIAgentControl`` when a tool call is
# denied by policy (permission) or rate limiting. The CLI maps these to exit
# code 6 (``EXIT_AGENT_DENIED``). Keep in sync with
# ``skills/opensai/agent_control.py`` (the emitter).
AGENT_DENIAL_CODES = frozenset({
    "TOOL_DENIED",
    "TOOL_NOT_ALLOWED",
    "MAX_DAG_NODES",
    "ARBITRARY_CODE",
    "DIRECT_ALLOCATION_FORBIDDEN",
    "RESOURCE_LIMIT",
    "MAX_WAIT_EXCEEDED",
    "RATE_LIMITED",
})


class OpenSAIClientError(ContractError):
    """Base exception for all OpenSAI SDK errors."""

    default_code = ErrorCode.INTERNAL_ERROR


class TransportError(OpenSAIClientError):
    """Raised for control-plane transport failures."""

    default_code = ErrorCode.CONNECTION_ERROR


class TransportTimeoutError(TransportError):
    """Raised when a receive / response wait exceeds its timeout."""

    default_code = ErrorCode.TIMEOUT


# Backward-compatible alias (opensai_client.TimeoutError).
TimeoutError = TransportTimeoutError


class TransportClosedError(TransportError):
    """Raised when an operation is attempted on a closed transport."""

    default_code = ErrorCode.CONNECTION_ERROR


class ProtocolFrameTooLarge(TransportError):
    """Raised when a frame exceeds ``max_frame_bytes``."""

    default_code = ErrorCode.PAYLOAD_TOO_LARGE


class ClientClosedError(OpenSAIClientError):
    """Raised when a business call is made after ``close()``."""

    default_code = ErrorCode.CLIENT_CLOSED


class MonitorUnavailableError(OpenSAIClientError):
    """Raised when monitor data cannot be read (e.g. Redis unavailable)."""

    default_code = ErrorCode.CONNECTION_ERROR


class RemoteError(OpenSAIClientError):
    """Raised for a structured error returned by the server."""

    default_code = ErrorCode.INTERNAL_ERROR


class AgentControlError(OpenSAIClientError):
    """Raised when the Agent-control runtime denies a tool call (§2.8.3).

    Covers policy (permission) and rate-limit rejections. Carries the stable
    denial code (``TOOL_DENIED`` / ``RATE_LIMITED`` / …) and maps to CLI exit
    code 6 (``EXIT_AGENT_DENIED``).
    """

    default_code = "TOOL_DENIED"

    def __init__(self, message: str = "", *, code: str | None = None,
                 retryable: bool = False, details: dict | None = None,
                 retry_after_s: float | None = None) -> None:
        super().__init__(message or code or "agent control denied",
                         code=code, retryable=retryable, details=details)
        self.retry_after_s = retry_after_s


# Re-export the contract ConfigError (used by config.py) for convenience.
from opensai_contracts.errors import ConfigError  # noqa: E402
