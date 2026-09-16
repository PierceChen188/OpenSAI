"""
OpenSAI Client SDK — framework-agnostic control-plane client.

Reuses the neutral ``opensai_contracts`` package for all data structures, wire
conversion and shared validation. Construction is lazy (no network until the
first business call or context entry).

Usage::

    from opensai_client import OpenSAIClient

    with OpenSAIClient() as client:
        algorithms = client.list_algorithms()
        job = client.submit_dag(spec)
        result = client.wait_result(job.job_id)
"""

from .client import OpenSAIClient
from .config import Config, ConfigError
from .errors import (
    OpenSAIClientError,
    TransportError,
    TransportTimeoutError,
    TimeoutError,
    TransportClosedError,
    ProtocolFrameTooLarge,
    ClientClosedError,
    MonitorUnavailableError,
    RemoteError,
    AgentControlError,
    AGENT_DENIAL_CODES,
)
from .redis_bridge import RedisBridge
from .transport import Transport, OpenSAITransport
from .validator import ClientValidator, ValidationError

__version__ = "0.1.0"

__all__ = [
    "OpenSAIClient",
    "Transport",
    "OpenSAITransport",
    "ClientValidator",
    "ValidationError",
    "RedisBridge",
    "Config",
    "ConfigError",
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
]
