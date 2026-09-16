"""
Versioned control-plane wire envelopes (neutral, cross APP/COM).

Separated from ``serializer.py`` (which owns the ``DAGSpec`` <-> ``dagData``
mapping) — this module owns the request / response / event / error envelopes
and re-exports the unified :class:`~opensai_contracts.errors.ErrorDetail`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from .errors import ErrorDetail
from .schema import PROTOCOL_VERSION, public_structure_to_dict

__all__ = [
    "PROTOCOL_VERSION",
    "ErrorDetail",
    "RequestEnvelope",
    "ResponseEnvelope",
    "EventEnvelope",
]


@dataclass
class RequestEnvelope:
    """Versioned request envelope; legacy codec maps it to top-level messages."""

    command: str
    payload: dict = field(default_factory=dict)
    protocol_version: str = PROTOCOL_VERSION
    request_id: str | None = None
    idempotency_key: str | None = None

    def to_dict(self) -> dict:
        return public_structure_to_dict(self)


@dataclass
class ResponseEnvelope:
    """Synchronous response envelope; ``request_id`` must be echoed back."""

    request_id: str
    ok: bool = True
    protocol_version: str = PROTOCOL_VERSION
    data: dict | None = None
    error: ErrorDetail | None = None

    @classmethod
    def success(cls, request_id: str, data: dict | None = None) -> "ResponseEnvelope":
        return cls(request_id=request_id, ok=True, data=data)

    @classmethod
    def failure(cls, request_id: str, error: ErrorDetail) -> "ResponseEnvelope":
        return cls(request_id=request_id, ok=False, error=error)

    def to_dict(self) -> dict:
        return public_structure_to_dict(self)


@dataclass
class EventEnvelope:
    """Asynchronous ``dagResult`` / decision-event envelope."""

    event_type: str
    data: dict = field(default_factory=dict)
    protocol_version: str = PROTOCOL_VERSION
    job_id: str | None = None

    def to_dict(self) -> dict:
        return public_structure_to_dict(self)


def parse_response(data: Mapping[str, Any]) -> ResponseEnvelope:
    """Best-effort parse of a wire response into a :class:`ResponseEnvelope`."""
    request_id = str(data.get("request_id", ""))
    ok = bool(data.get("ok", True))
    error = (
        ErrorDetail.from_dict(data["error"])
        if isinstance(data.get("error"), Mapping) else None
    )
    payload = data.get("data")
    return ResponseEnvelope(
        request_id=request_id,
        ok=ok,
        protocol_version=str(data.get("protocol_version", PROTOCOL_VERSION)),
        data=dict(payload) if isinstance(payload, Mapping) else None,
        error=error,
    )
