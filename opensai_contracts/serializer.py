"""
SDK <-> wire conversion (neutral, cross APP/COM).

Implements strict parsing of the public SDK DAG (:class:`~opensai_contracts.schema.DAGSpec`)
and the legacy wire ``dagData``, plus the mapping back to wire format. Unknown
fields are rejected with a locatable JSON path.

Ownership note: the wire *envelope* (request/response/event) lives in
``protocol.py``; this module focuses on the ``DAGSpec`` <-> ``dagData`` mapping
and the typed conversion helpers defined by §2.7.3.
"""

from __future__ import annotations

import time
from typing import Any, Mapping

from .errors import (
    ContractParseError,
    ErrorCode,
    ErrorDetail,
)
from .schema import (
    AllocationInfo,
    DAGNode,
    DAGSpec,
    JobHandle,
    JobResult,
    JobStatus,
    NodeResult,
    Policy,
    ResourceHint,
    _coerce_bool,
    _coerce_int,
    _coerce_number,
    _coerce_str,
    _path,
    _reject_unknown,
    _require_mapping,
    public_structure_to_dict,
)

__all__ = [
    "parse_dag",
    "dag_spec_to_dag_data",
    "node_to_wire",
    "wire_to_dag_spec",
    "job_ack_from_wire",
    "remote_error_from_wire",
    "node_result_from_wire",
    "dag_result_to_job_result",
    "encode_legacy_request",
]

_RESOURCE_KEYS = ("cores", "mem_mb", "storage_mb", "bw_mbps")

# wire -> JobStatus mapping for sub-task status values (config.sub_task_status)
_SUBTASK_STATUS_MAP = {
    4: JobStatus.SUCCEEDED,   # FINISH
    5: JobStatus.EARLY_EXIT,  # EARLY_EXIT
    6: JobStatus.FAILED,      # DROPPED
}


# ── format detection ────────────────────────────────────────────────────────

def _looks_like_wire(data: Mapping) -> bool:
    if "name" in data:
        return False
    if "taskName" in data or "taskId" in data or "taskCreateTime" in data:
        return True
    nodes = data.get("nodes")
    if isinstance(nodes, (list, tuple)) and nodes and isinstance(nodes[0], Mapping):
        return "form" in nodes[0]
    return False


# ── parse ───────────────────────────────────────────────────────────────────

def parse_dag(raw: Mapping[str, Any], allow_wire_format: bool = True) -> DAGSpec:
    """Strictly parse an SDK DAG or legacy ``dagData`` into a :class:`DAGSpec`.

    Parameters
    ----------
    raw:
        Either the public SDK DAG mapping (``name``/``policy``/``nodes``) or a
        wire ``dagData`` envelope.
    allow_wire_format:
        When False, wire format is rejected (SDK-only parsing).

    Raises
    ------
    ContractParseError
        On unknown fields, missing required fields, or type errors. The error
        ``details["path"]`` carries a locatable JSON path.
    """
    _require_mapping(raw, "$")

    if "dagData" in raw:
        if not allow_wire_format:
            raise ContractParseError(
                "wire 'dagData' format not allowed",
                code=ErrorCode.UNKNOWN_FIELD,
                details={"path": "$.dagData"},
            )
        # envelope may carry request_id / idempotency_key alongside dagData
        _reject_unknown(raw, {"dagData", "request_id", "idempotency_key"}, "$")
        return _parse_wire_dag(raw["dagData"], "$.dagData")

    if _looks_like_wire(raw):
        if not allow_wire_format:
            raise ContractParseError(
                "wire format not allowed",
                code=ErrorCode.UNKNOWN_FIELD,
                details={"path": "$"},
            )
        return _parse_wire_dag(raw, "$")

    return DAGSpec.from_dict(raw, path="$")


def _map_policy(raw_policy: Any, path: str) -> Policy:
    if raw_policy is None or raw_policy == "" or raw_policy == "default":
        return Policy.SMART
    if isinstance(raw_policy, Policy):
        return raw_policy
    if isinstance(raw_policy, str):
        try:
            return Policy(raw_policy)
        except ValueError:
            raise ContractParseError(
                f"invalid policy '{raw_policy}' at '{path}'",
                code=ErrorCode.VALUE_ERROR,
                details={"path": path, "allowed": [e.value for e in Policy]},
            )
    raise ContractParseError(
        f"expected policy string at '{path}', got {type(raw_policy).__name__}",
        code=ErrorCode.TYPE_ERROR,
        details={"path": path},
    )


def _parse_wire_dag(inner: Mapping, path: str) -> DAGSpec:
    _require_mapping(inner, path)
    _reject_unknown(
        inner,
        {"taskName", "taskId", "taskDesc", "taskCreateTime", "taskPolicy", "nodes"},
        path,
    )
    name = _coerce_str(inner.get("taskName", ""), _path(path, "taskName"))
    policy = _map_policy(inner.get("taskPolicy"), _path(path, "taskPolicy"))

    nodes_raw = inner.get("nodes")
    if nodes_raw is None:
        raise ContractParseError(
            f"missing required field 'nodes' at '{_path(path, 'nodes')}'",
            code=ErrorCode.MISSING_FIELD,
            details={"path": _path(path, "nodes")},
        )
    if not isinstance(nodes_raw, (list, tuple)):
        raise ContractParseError(
            f"expected array at '{_path(path, 'nodes')}', got {type(nodes_raw).__name__}",
            code=ErrorCode.TYPE_ERROR,
            details={"path": _path(path, "nodes")},
        )

    nodes = [
        _parse_wire_node(n, _path(path, f"nodes[{i}]"))
        for i, n in enumerate(nodes_raw)
    ]

    metadata: dict = {}
    if inner.get("taskId") is not None:
        metadata["task_id"] = inner["taskId"]
    if inner.get("taskDesc") is not None:
        metadata["description"] = inner["taskDesc"]
    if inner.get("taskCreateTime") is not None:
        metadata["task_create_time"] = inner["taskCreateTime"]

    return DAGSpec(
        name=name,
        policy=policy,
        nodes=nodes,
        metadata=metadata,
        schema_version="1.0.0",
    )


def _parse_wire_node(node: Mapping, path: str) -> DAGNode:
    _require_mapping(node, path)
    _reject_unknown(node, {"id", "name", "form", "result"}, path)

    node_id = _coerce_int(node.get("id", 0), _path(path, "id"))
    name = _coerce_str(node.get("name", ""), _path(path, "name"))

    form = node.get("form")
    if form is None:
        raise ContractParseError(
            f"missing required field 'form' at '{_path(path, 'form')}'",
            code=ErrorCode.MISSING_FIELD,
            details={"path": _path(path, "form")},
        )
    _require_mapping(form, _path(path, "form"))
    _reject_unknown(
        form,
        {
            "beforeNodes", "afterNodes", "taskAlgorithm", "taskVersion",
            "taskInputInfo", "details", "directAllocationId", "deviceParam",
        },
        _path(path, "form"),
    )

    algorithm = _coerce_str(
        form.get("taskAlgorithm", ""), _path(path, "form.taskAlgorithm")
    )
    version = _coerce_str(
        form.get("taskVersion", "1.0"), _path(path, "form.taskVersion")
    )
    before_nodes = list(form.get("beforeNodes", []) or [])
    after_nodes = list(form.get("afterNodes", []) or [])
    for key, val in (("beforeNodes", before_nodes), ("afterNodes", after_nodes)):
        if not all(isinstance(x, int) and not isinstance(x, bool) for x in val):
            raise ContractParseError(
                f"'{key}' must be an array of integers at '{_path(path, 'form.' + key)}'",
                code=ErrorCode.TYPE_ERROR,
                details={"path": _path(path, "form." + key)},
            )

    direct_allocation_id = _coerce_int(
        form.get("directAllocationId", -1), _path(path, "form.directAllocationId")
    )

    task_input = form.get("taskInputInfo", {})
    _require_mapping(task_input, _path(path, "form.taskInputInfo"))
    params, resources = _split_params_and_resources(
        task_input, _path(path, "form.taskInputInfo")
    )

    return DAGNode(
        id=node_id,
        name=name,
        algorithm=algorithm,
        version=version,
        params=params,
        before_nodes=before_nodes,
        after_nodes=after_nodes,
        resources=resources,
        direct_allocation_id=direct_allocation_id,
    )


def _split_params_and_resources(task_input: Mapping, path: str):
    """Split ``taskInputInfo`` into algorithm params + ResourceHint."""
    params = dict(task_input)
    res: dict = {}
    for key in _RESOURCE_KEYS:
        if key in params:
            value = params.pop(key)
            if value is not None and value > 0:
                res[key] = value
    resources = ResourceHint.from_dict(res, path) if res else None
    return params, resources


# ── serialize to wire ───────────────────────────────────────────────────────

def node_to_wire(node: DAGNode, node_count: int = 1) -> dict:
    """Map a :class:`DAGNode` to the wire ``{id,name,form,result}`` shape."""
    task_input = dict(node.params or {})
    if node.resources is not None:
        for key in _RESOURCE_KEYS:
            value = getattr(node.resources, key)
            if value is not None:
                task_input[key] = value

    return {
        "id": node.id,
        "name": node.name or node.algorithm or f"node_{node.id}",
        "form": {
            "beforeNodes": list(node.before_nodes),
            "afterNodes": list(node.after_nodes),
            "taskAlgorithm": node.algorithm,
            "taskVersion": node.version or "1.0",
            "taskInputInfo": task_input,
            "details": "",
            "directAllocationId": node.direct_allocation_id,
            "deviceParam": {},
        },
        "result": {
            "taskResultInfo": "",
            "status": 1,
            "delay": 0,
            "deviceId": 0,
        },
    }


def dag_spec_to_dag_data(
    dag: DAGSpec,
    task_create_time: float | None = None,
    idempotency_key: str | None = None,
    request_id: str | None = None,
) -> dict:
    """Build a legacy-compatible ``dagData`` envelope from a :class:`DAGSpec`.

    Fills taskName/time/policy, per-node form/path and initial result stubs.
    """
    if not isinstance(dag, DAGSpec):
        raise ContractParseError(
            f"expected DAGSpec, got {type(dag).__name__}",
            code=ErrorCode.TYPE_ERROR,
            details={"path": "$"},
        )
    policy = dag.policy.value if isinstance(dag.policy, Policy) else str(dag.policy)
    task_id = str(dag.metadata.get("task_id", "1")) if dag.metadata else "1"
    description = (dag.metadata.get("description", "") if dag.metadata else "") or ""

    payload = {
        "dagData": {
            "taskName": dag.name,
            "taskId": task_id,
            "taskDesc": description,
            "taskCreateTime": task_create_time if task_create_time is not None else time.time(),
            "taskPolicy": policy,
            "nodes": [node_to_wire(n, len(dag.nodes)) for n in dag.nodes],
        }
    }
    if request_id is not None:
        payload["request_id"] = request_id
    if idempotency_key is not None:
        payload["idempotency_key"] = idempotency_key
    return payload


def wire_to_dag_spec(wire: Mapping[str, Any]) -> DAGSpec:
    """Convenience alias: strictly parse a wire ``dagData`` into ``DAGSpec``."""
    return parse_dag(wire, allow_wire_format=True)


# ── typed wire conversions (§2.7.3) ────────────────────────────────────────

def job_ack_from_wire(payload: Mapping[str, Any]) -> JobHandle:
    """Parse a ``dagData`` ACK into a :class:`JobHandle`.

    Raises ``ContractParseError`` when ``dagRspStatus`` is false (carrying the
    structured server error as ``details``).
    """
    _require_mapping(payload, "$")
    dag_rsp_status = payload.get("dagRspStatus", True)
    if dag_rsp_status is False:
        err = remote_error_from_wire(payload)
        raise ContractParseError(
            err.message or "DAG rejected by server",
            code=err.code,
            retryable=err.retryable,
            details=err.details,
        )

    job_id = payload.get("job_id")
    legacy_task_id = payload.get("legacy_task_id")
    if job_id is None:
        raise ContractParseError(
            "ACK missing 'job_id'",
            code=ErrorCode.MISSING_FIELD,
            details={"path": "$.job_id"},
        )
    if legacy_task_id is None:
        raise ContractParseError(
            "ACK missing 'legacy_task_id'",
            code=ErrorCode.MISSING_FIELD,
            details={"path": "$.legacy_task_id"},
        )
    return JobHandle(
        job_id=_coerce_str(job_id, "$.job_id"),
        legacy_task_id=_coerce_str(legacy_task_id, "$.legacy_task_id"),
        status=_coerce_str(payload.get("status", JobStatus.QUEUED.value), "$.status"),
        accepted_at=_coerce_str(payload.get("accepted_at", ""), "$.accepted_at"),
        request_id=(
            _coerce_str(payload["request_id"], "$.request_id")
            if payload.get("request_id") is not None else None
        ),
    )


def remote_error_from_wire(payload: Mapping[str, Any]) -> ErrorDetail:
    """Extract a structured :class:`ErrorDetail` from an error payload."""
    _require_mapping(payload, "$")
    if isinstance(payload.get("error"), Mapping):
        return ErrorDetail.from_dict(payload["error"])
    if "code" in payload:
        return ErrorDetail.from_dict(payload)
    # Legacy string error
    message = payload.get("error", payload.get("message", "unknown server error"))
    return ErrorDetail(
        code=ErrorCode.INTERNAL_ERROR,
        message=str(message),
        retryable=False,
        details={},
    )


def node_result_from_wire(node_payload: Mapping[str, Any]) -> NodeResult:
    """Normalize a wire node (with ``result``) into a :class:`NodeResult`."""
    _require_mapping(node_payload, "$")
    node_id = _coerce_int(node_payload.get("id", 0), "$.id")
    result = node_payload.get("result", {})
    _require_mapping(result, "$.result")

    raw_status = result.get("status")
    status = _SUBTASK_STATUS_MAP.get(raw_status, JobStatus.RUNNING)

    device_id = result.get("deviceId")
    device_id_str = str(device_id) if device_id is not None else None

    task_result_info = result.get("taskResultInfo", "")
    error = _error_from_result_info(task_result_info)

    allocation = None
    form = node_payload.get("form", {})
    if isinstance(form, Mapping):
        alloc_raw = (form.get("taskInputInfo") or {}).get("_alloc") if isinstance(form.get("taskInputInfo"), Mapping) else None
        if isinstance(alloc_raw, Mapping):
            allocation = AllocationInfo.from_dict(alloc_raw, "$.form.taskInputInfo._alloc")

    return NodeResult(
        node_id=node_id,
        status=status,
        device_id=device_id_str,
        allocation=allocation,
        result=task_result_info,
        error=error,
    )


def _error_from_result_info(task_result_info: Any) -> ErrorDetail | None:
    if not isinstance(task_result_info, str) or not task_result_info:
        return None
    if task_result_info.startswith("ERROR"):
        return ErrorDetail(
            code=ErrorCode.INTERNAL_ERROR,
            message=task_result_info,
            retryable=False,
            details={},
        )
    return None


def dag_result_to_job_result(
    payload: Mapping[str, Any],
    job_record: Any | None = None,
) -> JobResult:
    """Normalize a wire ``dagResult`` into a :class:`JobResult`."""
    _require_mapping(payload, "$")
    if "dagResult" not in payload:
        raise ContractParseError(
            "missing 'dagResult' key",
            code=ErrorCode.MISSING_FIELD,
            details={"path": "$.dagResult"},
        )
    result = payload["dagResult"]
    _require_mapping(result, "$.dagResult")

    legacy_task_id = _coerce_str(result.get("taskName", ""), "$.dagResult.taskName")
    duration_s = result.get("finishedTime")
    if duration_s is not None:
        duration_s = _coerce_number(duration_s, "$.dagResult.finishedTime")

    nodes_raw = result.get("nodes", [])
    nodes = [node_result_from_wire(n) for n in nodes_raw]

    status = JobStatus.SUCCEEDED
    error = None
    if job_record is not None:
        job_id = getattr(job_record, "job_id", legacy_task_id)
        status = getattr(job_record, "status", status)
        error = getattr(job_record, "error", None)
        completed_at = getattr(job_record, "completed_at", None)
    else:
        job_id = legacy_task_id
        completed_at = None

    return JobResult(
        job_id=str(job_id),
        status=status,
        completed_at=completed_at,
        duration_s=duration_s,
        nodes=nodes,
        error=error,
    )


def encode_legacy_request(command: str, payload: Mapping[str, Any]) -> dict:
    """Map a versioned ``command`` to a legacy top-level message.

    Used by the legacy codec; raises for commands it does not know.
    """
    known = {
        "dagData": "dagData",
        "jobQuery": "jobQuery",
        "devicesInfo": "devicesInfo",
        "algorithmsInfo": "algorithmsInfo",
        "policiesInfo": "policiesInfo",
        "eventsInfo": "eventsInfo",
        "user_beat": "user_beat",
    }
    if command not in known:
        raise ContractParseError(
            f"unknown legacy command '{command}'",
            code=ErrorCode.INVALID_ARGUMENT,
            details={"command": command, "known": sorted(known)},
        )
    return {known[command]: dict(payload)}
