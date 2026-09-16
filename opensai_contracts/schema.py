"""
Neutral OpenSAI contract data structures (cross APP/COM).

This module is the single authoritative home for the public data structures
shared between the application layer (APP) and the compute-distributed layer
(COM). It is part of the neutral ``opensai_contracts`` package:

* It MUST NOT import ``opensai_client`` (neutrality invariant).
* It defines strict ``from_dict`` parsers that reject unknown fields and
  preserve a locatable JSON-path on every error.
* It defines ``single_task_dag`` and ``public_structure_to_dict`` per the
  design document (§2.2.1 / §2.2.2).

Wire-format (``dagData``) conversion lives in ``serializer.py``; validation
lives in ``validator.py``.
"""

from __future__ import annotations

import dataclasses
import datetime as _dt
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping

from .errors import (
    ContractParseError,
    ErrorCode,
    SerializationError,
)

__all__ = [
    "SCHEMA_VERSION",
    "PROTOCOL_VERSION",
    "JobStatus",
    "Policy",
    "Severity",
    "is_terminal",
    "utc_now",
    "ResourceHint",
    "ResourceLimits",
    "AllocationInfo",
    "AlgorithmInfo",
    "DAGNode",
    "DAGSpec",
    "OpenSAIConfig",
    "ValidationIssue",
    "ValidationReport",
    "JobHandle",
    "NodeResult",
    "JobResult",
    "JobRecord",
    "IdempotencyRecord",
    "AlgorithmRegistry",
    "ParameterSpec",
    "ValidationLimits",
    "ToolResponse",
    "single_task_dag",
    "public_structure_to_dict",
]

# ── frozen contract versions ────────────────────────────────────────────────

SCHEMA_VERSION = "1.0.0"
"""Version of the public DAG contract (``DAGSpec`` / ``DAGNode``)."""

PROTOCOL_VERSION = "1.0"
"""Version of the control-plane wire protocol envelope."""

# The four resource dimensions (frozen, in canonical order).
RESOURCE_FIELDS = ("cores", "mem_mb", "bw_mbps", "storage_mb")

# System keys algorithms must never use as business parameters.
RESERVED_PARAM_KEYS = frozenset({"_alloc", "_artifact_in", "_artifact_out"})

# Resource keys that belong in ``resources`` rather than ``params``.
RESOURCE_PARAM_KEYS = frozenset(RESOURCE_FIELDS)


# ── enums ───────────────────────────────────────────────────────────────────

class JobStatus(str, Enum):
    """Job lifecycle state (frozen).

    Terminal states are ``SUCCEEDED``, ``FAILED`` and ``CANCELLED`` and must
    never be overwritten. ``EARLY_EXIT`` and ``WAIT_TIMEOUT`` are intermediate
    / synthetic states that map onto terminal states at the job level.
    """

    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    EARLY_EXIT = "EARLY_EXIT"
    WAIT_TIMEOUT = "WAIT_TIMEOUT"


_TERMINAL_STATUSES = frozenset(
    {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED}
)


def is_terminal(status: JobStatus | str) -> bool:
    """Return True when ``status`` is a terminal state.

    Accepts a :class:`JobStatus` or a raw string; unknown strings are treated
    as non-terminal (conservative for polling).
    """
    if isinstance(status, JobStatus):
        return status in _TERMINAL_STATUSES
    try:
        return JobStatus(str(status)) in _TERMINAL_STATUSES
    except (ValueError, TypeError):
        return False


class Policy(str, Enum):
    """Placement policy for a DAG (frozen)."""

    PLAN = "plan"
    SMART = "smart"
    RANDOM = "random"


class Severity(str, Enum):
    """Severity of a single validation issue."""

    ERROR = "ERROR"
    WARNING = "WARNING"


def utc_now() -> str:
    """Return the current UTC time as an RFC3339 string (public timestamps)."""
    return (
        _dt.datetime.now(_dt.timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


# ── internal coercion helpers ───────────────────────────────────────────────

def _typename(value: Any) -> str:
    return type(value).__name__


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _path(parent: str, key: str) -> str:
    return f"{parent}.{key}" if parent else key


def _require_mapping(data: Any, path: str) -> None:
    if not isinstance(data, Mapping):
        raise ContractParseError(
            f"expected object at '{path}', got {_typename(data)}",
            code=ErrorCode.TYPE_ERROR,
            details={"path": path or "$"},
        )


def _reject_unknown(data: Mapping, allowed: set, path: str) -> None:
    unknown = sorted(str(k) for k in data.keys() if k not in allowed)
    if unknown:
        raise ContractParseError(
            f"unknown field(s) at '{path or '$'}': {', '.join(unknown)}",
            code=ErrorCode.UNKNOWN_FIELD,
            details={"path": path or "$", "fields": unknown},
        )


def _coerce_str(value: Any, path: str) -> str:
    if not isinstance(value, str):
        raise ContractParseError(
            f"expected string at '{path}', got {_typename(value)}",
            code=ErrorCode.TYPE_ERROR,
            details={"path": path},
        )
    return value


def _coerce_int(value: Any, path: str) -> int:
    if not _is_int(value):
        raise ContractParseError(
            f"expected integer at '{path}', got {_typename(value)}",
            code=ErrorCode.TYPE_ERROR,
            details={"path": path},
        )
    return value


def _coerce_number(value: Any, path: str) -> float:
    if not _is_number(value):
        raise ContractParseError(
            f"expected number at '{path}', got {_typename(value)}",
            code=ErrorCode.TYPE_ERROR,
            details={"path": path},
        )
    return float(value)


def _coerce_bool(value: Any, path: str) -> bool:
    if not isinstance(value, bool):
        raise ContractParseError(
            f"expected boolean at '{path}', got {_typename(value)}",
            code=ErrorCode.TYPE_ERROR,
            details={"path": path},
        )
    return value


def _coerce_dict(value: Any, path: str) -> dict:
    if not isinstance(value, Mapping):
        raise ContractParseError(
            f"expected object at '{path}', got {_typename(value)}",
            code=ErrorCode.TYPE_ERROR,
            details={"path": path},
        )
    return dict(value)


def _coerce_int_list(value: Any, path: str) -> list:
    if not isinstance(value, (list, tuple)):
        raise ContractParseError(
            f"expected array at '{path}', got {_typename(value)}",
            code=ErrorCode.TYPE_ERROR,
            details={"path": path},
        )
    out = []
    for i, item in enumerate(value):
        out.append(_coerce_int(item, _path(path, str(i))))
    return out


def _coerce_str_list(value: Any, path: str) -> list:
    if not isinstance(value, (list, tuple)):
        raise ContractParseError(
            f"expected array at '{path}', got {_typename(value)}",
            code=ErrorCode.TYPE_ERROR,
            details={"path": path},
        )
    return [_coerce_str(item, _path(path, str(i))) for i, item in enumerate(value)]


def _coerce_enum(value: Any, enum_cls: type[Enum], path: str) -> Enum:
    if isinstance(value, enum_cls):
        return value
    if isinstance(value, str):
        try:
            return enum_cls(value)
        except ValueError:
            raise ContractParseError(
                f"invalid {enum_cls.__name__} value '{value}' at '{path}'",
                code=ErrorCode.VALUE_ERROR,
                details={"path": path, "allowed": [e.value for e in enum_cls]},
            )
    raise ContractParseError(
        f"expected {enum_cls.__name__} at '{path}', got {_typename(value)}",
        code=ErrorCode.TYPE_ERROR,
        details={"path": path},
    )


# ── public serialization ────────────────────────────────────────────────────

def public_structure_to_dict(value: Any) -> Any:
    """Recursively serialize a public structure into a JSON-compatible value.

    Handles ``Enum``, ``dataclass`` and datetime/date fields. Rejects ``bytes``
    and any internal object it does not recognize — public output must never
    leak binary or runtime internals.
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (_dt.datetime, _dt.date)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise SerializationError(
            "bytes are not allowed in public output",
            details={"type": _typename(value)},
        )
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            f.name: public_structure_to_dict(getattr(value, f.name))
            for f in dataclasses.fields(value)
            if not f.name.startswith("_")
        }
    if isinstance(value, Mapping):
        return {
            str(k): public_structure_to_dict(v) for k, v in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [public_structure_to_dict(v) for v in value]
    if isinstance(value, (set, frozenset)):
        return sorted(public_structure_to_dict(v) for v in value)
    raise SerializationError(
        f"unsupported public type {_typename(value)}",
        details={"type": _typename(value)},
    )


# ── resource structures ─────────────────────────────────────────────────────

@dataclass(frozen=True)
class ResourceHint:
    """Caller resource declaration (four dimensions).

    Each non-null value must be strictly greater than zero. Units (frozen):
    ``cores`` = cores, ``mem_mb`` / ``storage_mb`` = MiB, ``bw_mbps`` = MB/s.
    """

    cores: int | None = None
    mem_mb: int | None = None
    bw_mbps: float | None = None
    storage_mb: int | None = None

    _FIELDS = ("cores", "mem_mb", "bw_mbps", "storage_mb")

    @classmethod
    def from_dict(cls, data: Mapping[str, Any], path: str = "resources") -> "ResourceHint":
        _require_mapping(data, path)
        _reject_unknown(data, set(cls._FIELDS), path)
        kwargs: dict[str, Any] = {}
        for key in cls._FIELDS:
            if key not in data or data[key] is None:
                kwargs[key] = None
                continue
            p = _path(path, key)
            if key == "bw_mbps":
                value = _coerce_number(data[key], p)
            else:
                value = _coerce_int(data[key], p)
            if value <= 0:
                raise ContractParseError(
                    f"'{p}' must be greater than 0, got {value}",
                    code=ErrorCode.VALUE_ERROR,
                    details={"path": p, "value": value},
                )
            kwargs[key] = value
        return cls(**kwargs)

    def to_dict(self) -> dict:
        return public_structure_to_dict(self)


@dataclass(frozen=True)
class ResourceLimits:
    """Static upper bounds for resource declarations (ADP-shared field names).

    Field names follow the ``<dim>_max`` convention so they compose
    mechanically with :func:`opensai_contracts.validate_resource_hints`.
    """

    cores_max: int | None = None
    mem_mb_max: int | None = None
    bw_mbps_max: float | None = None
    storage_mb_max: int | None = None


@dataclass(frozen=True)
class AllocationInfo:
    """Device-side final execution allocation (bandwidth is not part of this)."""

    cores: list | int | None = None
    mem_budget_mb: int | None = None
    est_mem_mb: int | None = None
    storage_budget_mb: int | None = None

    @classmethod
    def from_dict(cls, data: Mapping[str, Any], path: str = "allocation") -> "AllocationInfo":
        _require_mapping(data, path)
        _reject_unknown(data, {"cores", "mem_budget_mb", "est_mem_mb", "storage_budget_mb"}, path)
        cores = data.get("cores")
        if cores is not None:
            if isinstance(cores, (list, tuple)):
                cores = _coerce_int_list(cores, _path(path, "cores"))
            else:
                cores = _coerce_int(cores, _path(path, "cores"))
        mem_budget_mb = _coerce_int(data["mem_budget_mb"], _path(path, "mem_budget_mb")) if data.get("mem_budget_mb") is not None else None
        est_mem_mb = _coerce_int(data["est_mem_mb"], _path(path, "est_mem_mb")) if data.get("est_mem_mb") is not None else None
        storage_budget_mb = _coerce_int(data["storage_budget_mb"], _path(path, "storage_budget_mb")) if data.get("storage_budget_mb") is not None else None
        return cls(cores=cores, mem_budget_mb=mem_budget_mb, est_mem_mb=est_mem_mb, storage_budget_mb=storage_budget_mb)

    def to_dict(self) -> dict:
        return public_structure_to_dict(self)


# ── application-layer structures ────────────────────────────────────────────

@dataclass(frozen=True)
class AlgorithmInfo:
    """Algorithm name, version and parameter schema metadata."""

    name: str
    version: str = "1.0"
    parameters: dict = field(default_factory=dict)
    schema_available: bool = False
    submittable: bool = True

    @classmethod
    def from_dict(cls, data: Mapping[str, Any], path: str = "algorithm") -> "AlgorithmInfo":
        _require_mapping(data, path)
        _reject_unknown(data, {"name", "version", "parameters", "schema_available", "submittable"}, path)
        name = _coerce_str(data["name"], _path(path, "name"))
        version = _coerce_str(data.get("version", "1.0"), _path(path, "version"))
        parameters = _coerce_dict(data.get("parameters", {}), _path(path, "parameters"))
        schema_available = _coerce_bool(data.get("schema_available", False), _path(path, "schema_available"))
        submittable = _coerce_bool(data.get("submittable", True), _path(path, "submittable"))
        return cls(name=name, version=version, parameters=parameters,
                   schema_available=schema_available, submittable=submittable)

    def to_dict(self) -> dict:
        return public_structure_to_dict(self)


@dataclass
class DAGNode:
    """A single computation node inside a DAG.

    ``id`` must be an integer starting at 1; before/after dependencies must be
    symmetric once the DAG is normalized.
    """

    id: int
    name: str = ""
    algorithm: str = ""
    version: str = "1.0"
    params: dict = field(default_factory=dict)
    before_nodes: list = field(default_factory=list)
    after_nodes: list = field(default_factory=list)
    resources: ResourceHint | None = None
    direct_allocation_id: int = -1

    _FIELDS = (
        "id", "name", "algorithm", "version", "params",
        "before_nodes", "after_nodes", "resources", "direct_allocation_id",
    )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any], path: str = "node") -> "DAGNode":
        _require_mapping(data, path)
        _reject_unknown(data, set(cls._FIELDS), path)
        node_id = _coerce_int(data["id"], _path(path, "id"))
        name = _coerce_str(data.get("name", ""), _path(path, "name"))
        algorithm = _coerce_str(data.get("algorithm", ""), _path(path, "algorithm"))
        version = _coerce_str(data.get("version", "1.0"), _path(path, "version"))
        params = _coerce_dict(data.get("params", {}), _path(path, "params"))
        before_nodes = _coerce_int_list(data.get("before_nodes", []), _path(path, "before_nodes"))
        after_nodes = _coerce_int_list(data.get("after_nodes", []), _path(path, "after_nodes"))
        resources = (
            ResourceHint.from_dict(data["resources"], _path(path, "resources"))
            if data.get("resources") is not None else None
        )
        direct_allocation_id = _coerce_int(
            data.get("direct_allocation_id", -1), _path(path, "direct_allocation_id")
        )
        return cls(
            id=node_id, name=name, algorithm=algorithm, version=version,
            params=params, before_nodes=before_nodes, after_nodes=after_nodes,
            resources=resources, direct_allocation_id=direct_allocation_id,
        )

    def to_dict(self) -> dict:
        return public_structure_to_dict(self)


@dataclass
class DAGSpec:
    """Public SDK DAG structure (not identical to the wire ``dagData``)."""

    name: str
    policy: Policy | str = Policy.SMART
    nodes: list = field(default_factory=list)
    metadata: dict = field(default_factory=dict)
    schema_version: str = SCHEMA_VERSION

    _FIELDS = ("name", "policy", "nodes", "metadata", "schema_version")

    @classmethod
    def from_dict(cls, data: Mapping[str, Any], path: str = "$") -> "DAGSpec":
        _require_mapping(data, path)
        _reject_unknown(data, set(cls._FIELDS), path)
        name = _coerce_str(data["name"], _path(path, "name"))
        policy = _coerce_enum(data.get("policy", Policy.SMART.value), Policy, _path(path, "policy"))
        nodes_raw = data.get("nodes", [])
        if not isinstance(nodes_raw, (list, tuple)):
            raise ContractParseError(
                f"expected array at '{_path(path, 'nodes')}', got {_typename(nodes_raw)}",
                code=ErrorCode.TYPE_ERROR,
                details={"path": _path(path, "nodes")},
            )
        nodes = [
            DAGNode.from_dict(n, _path(path, f"nodes[{i}]"))
            for i, n in enumerate(nodes_raw)
        ]
        metadata = _coerce_dict(data.get("metadata", {}), _path(path, "metadata"))
        schema_version = _coerce_str(
            data.get("schema_version", SCHEMA_VERSION), _path(path, "schema_version")
        )
        return cls(name=name, policy=policy, nodes=nodes,
                   metadata=metadata, schema_version=schema_version)

    def to_dict(self) -> dict:
        return public_structure_to_dict(self)

    def node_map(self) -> dict:
        return {n.id: n for n in self.nodes}


@dataclass(frozen=True)
class OpenSAIConfig:
    """Immutable configuration for the SDK client.

    Sensitive fields (passwords/tokens) must never appear in ``repr`` or logs;
    use :meth:`redacted` for a loggable view.
    """

    server_host: str = "127.0.0.1"
    server_port: int = 8888
    redis_host: str = "127.0.0.1"
    redis_port: int = 6379
    redis_db: int = 0
    redis_username: str | None = None
    redis_password: str | None = None
    redis_ssl: bool = False
    timeout: float = 30.0
    poll_interval: float = 1.0
    max_frame_bytes: int = 8388608
    tls: bool = False
    log_level: str = "INFO"

    _SENSITIVE = ("redis_password", "redis_username", "tls")

    def __repr__(self) -> str:
        return (
            f"OpenSAIConfig(server={self.server_host}:{self.server_port}, "
            f"redis={self.redis_host}:{self.redis_port}/db{self.redis_db}, "
            f"timeout={self.timeout}, poll_interval={self.poll_interval}, "
            f"max_frame_bytes={self.max_frame_bytes}, tls={self.tls}, "
            f"log_level={self.log_level})"
        )

    def redacted(self) -> dict:
        """Return a loggable dict with credentials replaced by ``***``."""
        out = public_structure_to_dict(self)
        for key in self._SENSITIVE:
            if out.get(key) is not None:
                out[key] = "***"
        return out

    def to_dict(self) -> dict:
        return public_structure_to_dict(self)


# ── validation structures ───────────────────────────────────────────────────

@dataclass(frozen=True)
class ValidationIssue:
    """A single, stable, machine-assertable validation problem."""

    code: str
    path: str
    message: str
    severity: Severity | str = Severity.ERROR
    hint: str | None = None
    details: dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any], path: str = "issue") -> "ValidationIssue":
        _require_mapping(data, path)
        _reject_unknown(data, {"code", "path", "message", "severity", "hint", "details"}, path)
        code = _coerce_str(data["code"], _path(path, "code"))
        ipath = _coerce_str(data.get("path", ""), _path(path, "path"))
        message = _coerce_str(data["message"], _path(path, "message"))
        severity = _coerce_enum(data.get("severity", Severity.ERROR.value), Severity, _path(path, "severity"))
        hint = _coerce_str(data["hint"], _path(path, "hint")) if data.get("hint") is not None else None
        details = _coerce_dict(data.get("details", {}), _path(path, "details"))
        return cls(code=code, path=ipath, message=message, severity=severity,
                   hint=hint, details=details)

    def to_dict(self) -> dict:
        return public_structure_to_dict(self)


@dataclass
class ValidationReport:
    """Shared validation result for client hints and server adjudication."""

    valid: bool
    errors: list = field(default_factory=list)
    warnings: list = field(default_factory=list)
    normalized_dag: DAGSpec | None = None
    source: str = ""

    @property
    def is_valid(self) -> bool:
        return bool(self.valid) and not self.errors

    def to_dict(self) -> dict:
        return public_structure_to_dict(self)


# ── job / result structures ─────────────────────────────────────────────────

@dataclass(frozen=True)
class JobHandle:
    """Typed representation of a submission ACK.

    ``accepted`` does **not** mean the job has completed.
    """

    job_id: str
    legacy_task_id: str
    status: JobStatus | str = JobStatus.QUEUED
    accepted_at: str = ""
    request_id: str | None = None

    @classmethod
    def from_dict(cls, data: Mapping[str, Any], path: str = "job") -> "JobHandle":
        _require_mapping(data, path)
        _reject_unknown(data, {"job_id", "legacy_task_id", "status", "accepted_at", "request_id"}, path)
        job_id = _coerce_str(data["job_id"], _path(path, "job_id"))
        legacy_task_id = _coerce_str(data["legacy_task_id"], _path(path, "legacy_task_id"))
        status = _coerce_enum(data.get("status", JobStatus.QUEUED.value), JobStatus, _path(path, "status"))
        accepted_at = _coerce_str(data.get("accepted_at", ""), _path(path, "accepted_at"))
        request_id = _coerce_str(data["request_id"], _path(path, "request_id")) if data.get("request_id") is not None else None
        return cls(job_id=job_id, legacy_task_id=legacy_task_id, status=status,
                   accepted_at=accepted_at, request_id=request_id)

    def to_dict(self) -> dict:
        return public_structure_to_dict(self)


@dataclass
class NodeResult:
    """Per-node execution result and resource-allocation snapshot."""

    node_id: int
    status: JobStatus | str = JobStatus.QUEUED
    device_id: str | None = None
    allocation: AllocationInfo | None = None
    result: Any = None
    error: Any = None  # ErrorDetail | None (kept loose to allow lazy import)

    def to_dict(self) -> dict:
        return public_structure_to_dict(self)


@dataclass
class JobResult:
    """One query/wait snapshot of a job; terminal failure uses status + error."""

    job_id: str
    status: JobStatus | str = JobStatus.QUEUED
    submitted_at: str | None = None
    started_at: str | None = None
    completed_at: str | None = None
    duration_s: float | None = None
    nodes: list = field(default_factory=list)
    error: Any = None  # ErrorDetail | None

    def to_dict(self) -> dict:
        return public_structure_to_dict(self)


@dataclass
class JobRecord:
    """COM-internal authoritative record — NOT a public SDK structure.

    Must never be returned directly to SDK consumers; map to
    :class:`JobResult` first.
    """

    job_id: str
    legacy_task_id: str
    owner: str = ""
    status: JobStatus | str = JobStatus.QUEUED
    request_id: str | None = None
    idempotency_key: str | None = None
    dag_digest: str | None = None
    submitted_at: str | None = None
    started_at: str | None = None
    completed_at: str | None = None
    duration_s: float | None = None
    nodes: list = field(default_factory=list)
    error: Any = None  # ErrorDetail | None
    expires_at: str | None = None


@dataclass(frozen=True)
class IdempotencyRecord:
    """Guarantees one job per (client_scope, idempotency_key)."""

    client_scope: str
    idempotency_key: str
    job_id: str
    created_at: str

    @classmethod
    def from_dict(cls, data: Mapping[str, Any], path: str = "idempotency") -> "IdempotencyRecord":
        _require_mapping(data, path)
        _reject_unknown(data, {"client_scope", "idempotency_key", "job_id", "created_at"}, path)
        client_scope = _coerce_str(data["client_scope"], _path(path, "client_scope"))
        idempotency_key = _coerce_str(data["idempotency_key"], _path(path, "idempotency_key"))
        job_id = _coerce_str(data["job_id"], _path(path, "job_id"))
        created_at = _coerce_str(data["created_at"], _path(path, "created_at"))
        return cls(client_scope=client_scope, idempotency_key=idempotency_key,
                   job_id=job_id, created_at=created_at)

    def to_dict(self) -> dict:
        return public_structure_to_dict(self)


# ── shared registry / limits ────────────────────────────────────────────────

@dataclass(frozen=True)
class ParameterSpec:
    """JSON-Schema-like constraint for a single algorithm parameter."""

    type: str = "string"
    required: bool = False
    default: Any = None
    enum: list | None = None
    minimum: float | None = None
    maximum: float | None = None
    description: str | None = None


@dataclass
class AlgorithmRegistry:
    """Shared algorithm-metadata snapshot used by both APP and COM."""

    schema_version: str = SCHEMA_VERSION
    algorithms: dict = field(default_factory=dict)  # dict[str, AlgorithmInfo]
    digest: str | None = None
    loaded_at: str | None = None

    def get(self, name: str) -> AlgorithmInfo | None:
        return self.algorithms.get(name)

    def names(self) -> list:
        return list(self.algorithms.keys())


@dataclass
class ValidationLimits:
    """Static upper bounds shared by client hints and server adjudication."""

    max_nodes: int = 100
    max_edges: int = 500
    max_payload_bytes: int = 8388608
    max_depth: int = 50
    resource_limits: ResourceLimits | None = None


# ── Agent tool output ───────────────────────────────────────────────────────

@dataclass
class ToolResponse:
    """Stable output structure for every Agent tool."""

    ok: bool = True
    data: dict | None = None
    error: Any = None  # ErrorDetail | None
    meta: dict = field(default_factory=dict)

    @classmethod
    def success(cls, data: dict | None = None, meta: dict | None = None) -> "ToolResponse":
        return cls(ok=True, data=data, error=None, meta=meta or {})

    @classmethod
    def failure(cls, error: Any, meta: dict | None = None) -> "ToolResponse":
        return cls(ok=False, data=None, error=error, meta=meta or {})

    def to_dict(self) -> dict:
        return public_structure_to_dict(self)


# ── constructors ────────────────────────────────────────────────────────────

def single_task_dag(
    algorithm: str,
    params: dict | None = None,
    resources: ResourceHint | None = None,
    direct_allocation_id: int = -1,
    name: str | None = None,
) -> DAGSpec:
    """Construct a single-node DAG (ID=1, no dependencies) for ``submit_task``.

    Path fields are completed with the default data directory so the resulting
    DAG is submission-ready.
    """
    if not algorithm:
        raise ContractParseError(
            "algorithm name must be a non-empty string",
            code=ErrorCode.INVALID_ARGUMENT,
            details={"path": "algorithm"},
        )
    if params is not None and not isinstance(params, Mapping):
        raise ContractParseError(
            "params must be an object",
            code=ErrorCode.TYPE_ERROR,
            details={"path": "params"},
        )
    if resources is not None and not isinstance(resources, ResourceHint):
        raise ContractParseError(
            "resources must be a ResourceHint or None",
            code=ErrorCode.TYPE_ERROR,
            details={"path": "resources"},
        )
    merged_params = dict(params or {})
    merged_params.setdefault("inDataPath", "./data/")
    merged_params.setdefault("outDataPath", "./data/")

    node_name = name or algorithm
    node = DAGNode(
        id=1,
        name=node_name,
        algorithm=algorithm,
        version="1.0",
        params=merged_params,
        before_nodes=[],
        after_nodes=[],
        resources=resources,
        direct_allocation_id=direct_allocation_id,
    )
    return DAGSpec(
        name=name or f"{algorithm}_task",
        policy=Policy.SMART,
        nodes=[node],
        metadata={},
        schema_version=SCHEMA_VERSION,
    )
