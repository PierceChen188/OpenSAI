"""
Shared DAG validation core (cross APP/COM).

This module is the *single* authoritative validation implementation used by
both the client-side validator (APP) and the server-side validator (COM). It
operates purely on neutral contract structures and never performs dynamic
imports or executes user text.

Checks performed (in order) by :meth:`DAGValidator.validate`:

1. structure     — mapping, node count, payload size, JSON-serializability
2. parse         — strict parse into ``DAGSpec`` (unknown/missing/type rejection)
3. graph         — ID=1..N, references, no self-loop/duplicate edge, acyclic
4. algorithms    — registered + submittable + version
5. params        — per-algorithm JSON-Schema constraints + reserved keys
6. resources     — positive values + static limits（仅当注入 ADP resource_validator 时执行）
7. placement     —（仅当注入 CLU placement_validator 时执行）
8. normalize     — fills name/version/paths/after_nodes (no reordering)

Ownership boundary: ``validate_resource_hints`` (ADP, WP-06) is implemented in
``adaptation.py``; ``validate_placement_targets`` (CLU, WP-05) is implemented
in the CLU ``cluster_manager.py``. ``DAGValidator.validate`` only orchestrates
these cross-Owner checks by calling the injected ``resource_validator`` /
``placement_validator`` — it never inlines another Owner's logic.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from .errors import (
    ContractError,
    ErrorCode,
)
from .schema import (
    SCHEMA_VERSION,
    AlgorithmInfo,
    AlgorithmRegistry,
    DAGNode,
    DAGSpec,
    JobStatus,
    ParameterSpec,
    Policy,
    RESERVED_PARAM_KEYS,
    RESOURCE_PARAM_KEYS,
    ResourceHint,
    ResourceLimits,
    Severity,
    ValidationIssue,
    ValidationLimits,
    ValidationReport,
)

__all__ = [
    "DAGValidator",
    "ValidationLimits",
    "AlgorithmRegistry",
    "ParameterSpec",
    "ResourceLimits",
    "load_algorithm_registry",
    "merge_runtime_registry",
    "validate_dag_request",
    "find_cycle_path",
    "topological_order",
    "is_acyclic",
]

# Keys that algorithms must not use as business parameters, and resource keys
# that belong in ``resources`` rather than ``params``, are defined once in
# ``schema.py`` (imported above) so client and server share a single source.

_DEFAULT_LIMITS = ValidationLimits()


def _issue(code, path, message, severity=Severity.ERROR, hint=None, details=None):
    return ValidationIssue(
        code=code, path=path or "$", message=message,
        severity=severity, hint=hint, details=details or {},
    )


# ── graph helpers ───────────────────────────────────────────────────────────

def topological_order(nodes: list[DAGNode]) -> list[int]:
    """Return node ids in topological order; raise ``ValueError`` on cycle."""
    node_map = {n.id: n for n in nodes}
    if not node_map:
        return []
    in_degree = {nid: 0 for nid in node_map}
    adj: dict[int, list[int]] = {nid: [] for nid in node_map}
    for n in nodes:
        for dep in n.before_nodes:
            if dep in node_map:
                adj[dep].append(n.id)
                in_degree[n.id] += 1

    from collections import deque

    queue = deque(nid for nid, d in in_degree.items() if d == 0)
    order: list[int] = []
    while queue:
        nid = queue.popleft()
        order.append(nid)
        for succ in adj[nid]:
            in_degree[succ] -= 1
            if in_degree[succ] == 0:
                queue.append(succ)
    if len(order) != len(node_map):
        remaining = sorted(set(node_map) - set(order))
        raise ValueError(f"DAG contains a cycle involving node(s): {remaining}")
    return order


def is_acyclic(nodes: list[DAGNode]) -> bool:
    try:
        topological_order(nodes)
        return True
    except ValueError:
        return False


def find_cycle_path(nodes: list[DAGNode]) -> list[int]:
    """Return a list of node ids forming one cycle, or ``[]`` if acyclic."""
    node_map = {n.id: n for n in nodes}
    adj: dict[int, list[int]] = {nid: [] for nid in node_map}
    for n in nodes:
        for dep in n.before_nodes:
            if dep in node_map:
                adj[dep].append(n.id)

    WHITE, GRAY, BLACK = 0, 1, 2
    color = {nid: WHITE for nid in node_map}
    parent: dict[int, int] = {}
    cycle: list[int] = []

    def dfs(u: int) -> None:
        nonlocal cycle
        color[u] = GRAY
        for v in adj.get(u, []):
            if cycle:
                return
            if color.get(v) == WHITE:
                parent[v] = u
                dfs(v)
            elif color.get(v) == GRAY:
                path = [v]
                cur = u
                while cur != v and cur in parent:
                    path.append(cur)
                    cur = parent[cur]
                path.append(v)
                path.reverse()
                cycle = path
                return
        color[u] = BLACK

    for nid in node_map:
        if color[nid] == WHITE and not cycle:
            dfs(nid)
    return cycle


# ── registry loading ────────────────────────────────────────────────────────

def load_algorithm_registry(path, expected_schema_version: str = SCHEMA_VERSION) -> AlgorithmRegistry:
    """Load a versioned ``algorithms.yaml`` into an :class:`AlgorithmRegistry`.

    Validates duplicate algorithms and parameter schemas; never imports
    algorithm code.
    """
    import yaml  # lazy: yaml is only needed for manifest loading

    p = Path(path)
    try:
        raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ContractError(
            f"algorithm manifest not found: {p}",
            code=ErrorCode.INVALID_ARGUMENT,
        )
    if not isinstance(raw, Mapping):
        raise ContractError(
            f"algorithm manifest must be a mapping: {p}",
            code=ErrorCode.INVALID_ARGUMENT,
        )

    schema_version = raw.get("schema_version", SCHEMA_VERSION)
    algorithms: dict[str, AlgorithmInfo] = {}
    for name, spec in (raw.get("algorithms") or {}).items():
        if not isinstance(spec, Mapping):
            raise ContractError(
                f"algorithm '{name}' entry must be a mapping",
                code=ErrorCode.INVALID_ARGUMENT,
            )
        params = _parse_parameter_specs(spec.get("parameters") or {}, name)
        algorithms[name] = AlgorithmInfo(
            name=str(name),
            version=str(spec.get("version", "1.0")),
            parameters=params,
            schema_available=bool(spec.get("schema_available", bool(params))),
            submittable=bool(spec.get("submittable", True)),
        )

    return AlgorithmRegistry(
        schema_version=str(schema_version),
        algorithms=algorithms,
        digest=raw.get("digest"),
        loaded_at=raw.get("loaded_at"),
    )


def _parse_parameter_specs(params: Mapping, algo_name: str) -> dict:
    out: dict[str, ParameterSpec] = {}
    for pname, pspec in params.items():
        if not isinstance(pspec, Mapping):
            raise ContractError(
                f"algorithm '{algo_name}' param '{pname}' must be a mapping",
                code=ErrorCode.INVALID_ARGUMENT,
            )
        out[str(pname)] = ParameterSpec(
            type=str(pspec.get("type", "string")),
            required=bool(pspec.get("required", False)),
            default=pspec.get("default"),
            enum=list(pspec["enum"]) if pspec.get("enum") is not None else None,
            minimum=pspec.get("minimum"),
            maximum=pspec.get("maximum"),
            description=pspec.get("description"),
        )
    return out


def merge_runtime_registry(runtime_names, manifest: AlgorithmRegistry | None) -> list[AlgorithmInfo]:
    """Merge runtime algorithm names with authoritative manifest metadata.

    Algorithms missing from the manifest are marked non-submittable.
    """
    out: list[AlgorithmInfo] = []
    manifest_map = manifest.algorithms if manifest else {}
    for name in runtime_names:
        if name in manifest_map:
            out.append(manifest_map[name])
        else:
            out.append(AlgorithmInfo(name=str(name), schema_available=False, submittable=False))
    return out


# ── validator ───────────────────────────────────────────────────────────────

@dataclass
class DAGValidator:
    """Shared DAG validator; does not touch the network."""

    algorithm_registry: AlgorithmRegistry | None = None
    limits: ValidationLimits | None = None
    _limits: ValidationLimits = field(default_factory=lambda: _DEFAULT_LIMITS, init=False, repr=False)

    def __post_init__(self):
        if self.limits is not None:
            self._limits = self.limits

    # ── orchestration ────────────────────────────────────────────────────

    def validate(
        self,
        dag,
        strict: bool = True,
        available_devices=None,
        resource_validator=None,
        placement_validator=None,
    ) -> ValidationReport:
        """Validate ``dag`` (DAGSpec or raw mapping) and return a report.

        Never modifies the input. ``strict`` currently controls unknown-field
        rejection at parse time (always on for the shared contract); it is kept
        for API stability.

        Cross-Owner orchestration (ADR D6): resource validation (ADP) and
        placement validation (CLU) are *not* inlined here. The orchestrator
        injects the authoritative implementations via ``resource_validator``
        and ``placement_validator``; when omitted, those checks are skipped.
        """
        errors: list[ValidationIssue] = []
        warnings: list[ValidationIssue] = []
        spec: DAGSpec | None = None
        source = "dag_spec" if isinstance(dag, DAGSpec) else "dict"

        if isinstance(dag, DAGSpec):
            spec = dag
        elif isinstance(dag, Mapping):
            errors.extend(self.validate_structure(dag))
            try:
                spec = _parse_dag_strict(dag)
            except ContractError as exc:
                errors.append(_issue(
                    exc.code,
                    exc.error.details.get("path", "$"),
                    exc.error.message,
                    details=exc.error.details,
                ))
        else:
            errors.append(_issue(
                ErrorCode.TYPE_ERROR, "$",
                f"dag must be a DAGSpec or mapping, got {type(dag).__name__}",
            ))

        if spec is not None:
            errors.extend(self.validate_graph(spec))
            if self.algorithm_registry is not None:
                errors.extend(self.validate_algorithms(spec, self.algorithm_registry))
                errors.extend(self.validate_params_all(spec, self.algorithm_registry))
            # Resource validation: ADP-owned, injected by the orchestrator.
            if resource_validator is not None:
                errors.extend(resource_validator(spec, self._limits.resource_limits))
            # Placement validation: CLU-owned, injected by the orchestrator.
            if placement_validator is not None:
                errors.extend(placement_validator(spec, available_devices))

        normalized = self.normalize(spec) if spec is not None else None
        return ValidationReport(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            normalized_dag=normalized,
            source=source,
        )

    # ── structure ────────────────────────────────────────────────────────

    def validate_structure(self, dag: dict) -> list[ValidationIssue]:
        """Structural + limit checks on the raw mapping (does not throw)."""
        issues: list[ValidationIssue] = []
        if not isinstance(dag, Mapping):
            return [_issue(ErrorCode.TYPE_ERROR, "$", "dag must be an object")]

        nodes = _extract_nodes(dag)
        if nodes is not None:
            if len(nodes) == 0:
                issues.append(_issue(ErrorCode.DAG_INVALID, "$.nodes", "DAG must contain at least one node"))
            elif len(nodes) > self._limits.max_nodes:
                issues.append(_issue(
                    ErrorCode.DAG_TOO_LARGE, "$.nodes",
                    f"node count {len(nodes)} exceeds limit {self._limits.max_nodes}",
                    details={"count": len(nodes), "limit": self._limits.max_nodes},
                ))
        else:
            issues.append(_issue(
                ErrorCode.MISSING_FIELD, "$",
                "missing required field 'nodes' (or 'dagData.nodes')",
            ))

        # JSON-serializable + payload size
        try:
            payload = json.dumps(dag, ensure_ascii=False)
        except (TypeError, ValueError):
            issues.append(_issue(ErrorCode.SERIALIZATION_ERROR, "$", "dag is not JSON-serializable"))
        else:
            size = len(payload.encode("utf-8"))
            if size > self._limits.max_payload_bytes:
                issues.append(_issue(
                    ErrorCode.PAYLOAD_TOO_LARGE, "$",
                    f"payload {size} bytes exceeds limit {self._limits.max_payload_bytes}",
                    details={"bytes": size, "limit": self._limits.max_payload_bytes},
                ))
        return issues

    # ── graph ────────────────────────────────────────────────────────────

    def validate_graph(self, dag: DAGSpec) -> list[ValidationIssue]:
        """ID contiguity, references, self-loop, duplicate edge, acyclicity."""
        issues: list[ValidationIssue] = []
        nodes = dag.nodes
        node_ids = [n.id for n in nodes]

        # ID = 1..N contiguous ascending
        expected = list(range(1, len(nodes) + 1))
        if node_ids != expected:
            issues.append(_issue(
                ErrorCode.VALUE_ERROR, "$.nodes",
                f"node ids must be contiguous 1..N; got {node_ids}",
            ))

        id_set = set(node_ids)
        seen_edges = set()
        for n in nodes:
            for dep in n.before_nodes:
                if dep not in id_set:
                    issues.append(_issue(
                        ErrorCode.VALUE_ERROR, f"$.nodes[{node_ids.index(n.id) if n.id in node_ids else '?'}].before_nodes",
                        f"node {n.id} references non-existent node id {dep}",
                        details={"node": n.id, "dependency": dep},
                    ))
                if dep == n.id:
                    issues.append(_issue(
                        ErrorCode.SELF_REFERENCE, f"$.nodes[{n.id - 1}].before_nodes",
                        f"node {n.id} depends on itself",
                        details={"node": n.id},
                    ))
                edge = (dep, n.id)
                if edge in seen_edges:
                    issues.append(_issue(
                        ErrorCode.DUPLICATE_EDGE, f"$.nodes[{n.id - 1}].before_nodes",
                        f"duplicate edge {dep} -> {n.id}",
                        details={"edge": list(edge)},
                    ))
                seen_edges.add(edge)

        # symmetry (only when after_nodes was explicitly supplied)
        for n in nodes:
            if n.after_nodes:
                expected_after = sorted(
                    p.id for p in nodes if n.id in p.before_nodes
                )
                if sorted(n.after_nodes) != expected_after:
                    issues.append(_issue(
                        ErrorCode.VALUE_ERROR, f"$.nodes[{n.id - 1}].after_nodes",
                        f"after_nodes of node {n.id} is not symmetric with before_nodes",
                        details={"node": n.id},
                    ))

        cycle = find_cycle_path(nodes)
        if cycle:
            issues.append(_issue(
                ErrorCode.CYCLE_DETECTED, "$.nodes",
                f"DAG contains a cycle: {' -> '.join(map(str, cycle))}",
                details={"cycle": cycle},
            ))
        return issues

    # ── algorithms ───────────────────────────────────────────────────────

    def validate_algorithms(self, dag: DAGSpec, registry: AlgorithmRegistry) -> list[ValidationIssue]:
        """Check every node algorithm is registered and submittable."""
        issues: list[ValidationIssue] = []
        for i, n in enumerate(dag.nodes):
            path = f"$.nodes[{i}].algorithm"
            if not n.algorithm:
                issues.append(_issue(
                    ErrorCode.MISSING_FIELD, path, f"node {n.id} has no algorithm",
                ))
                continue
            info = registry.get(n.algorithm)
            if info is None:
                issues.append(_issue(
                    ErrorCode.ALGORITHM_NOT_REGISTERED, path,
                    f"algorithm '{n.algorithm}' is not registered",
                    details={"algorithm": n.algorithm},
                ))
                continue
            if not info.submittable:
                issues.append(_issue(
                    ErrorCode.ALGORITHM_NOT_SUBMITTABLE, path,
                    f"algorithm '{n.algorithm}' is not submittable (missing manifest metadata)",
                    details={"algorithm": n.algorithm},
                ))
            if n.version and info.version and n.version != info.version:
                issues.append(_issue(
                    ErrorCode.VALUE_ERROR, f"$.nodes[{i}].version",
                    f"node {n.id} pins version '{n.version}' but registry has '{info.version}'",
                    severity=Severity.WARNING,
                    details={"algorithm": n.algorithm, "pinned": n.version, "registered": info.version},
                ))
        return issues

    def validate_params_all(self, dag: DAGSpec, registry: AlgorithmRegistry) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        for i, n in enumerate(dag.nodes):
            info = registry.get(n.algorithm)
            if info is not None:
                issues.extend(self.validate_params(n, info))
        return issues

    def validate_params(self, node: DAGNode, algorithm: AlgorithmInfo) -> list[ValidationIssue]:
        """Validate a node's params against its algorithm schema + reserved keys."""
        issues: list[ValidationIssue] = []
        params = node.params or {}

        for key in params:
            if key in RESERVED_PARAM_KEYS:
                issues.append(_issue(
                    ErrorCode.RESERVED_KEY, f"$.nodes[{node.id - 1}].params.{key}",
                    f"param '{key}' is a reserved system key",
                    details={"key": key},
                ))
            elif key in RESOURCE_PARAM_KEYS:
                issues.append(_issue(
                    ErrorCode.RESERVED_KEY, f"$.nodes[{node.id - 1}].params.{key}",
                    f"param '{key}' is a resource key; declare it under 'resources' instead",
                    severity=Severity.WARNING,
                    details={"key": key},
                ))

        specs = algorithm.parameters or {}
        for pname, pspec in specs.items():
            ps = pspec if isinstance(pspec, ParameterSpec) else ParameterSpec(**pspec)
            present = pname in params
            if ps.required and not present:
                issues.append(_issue(
                    ErrorCode.MISSING_FIELD, f"$.nodes[{node.id - 1}].params",
                    f"missing required parameter '{pname}' for algorithm '{algorithm.name}'",
                    details={"parameter": pname},
                ))
                continue
            if not present:
                continue
            value = params[pname]
            issues.extend(_check_param_value(value, ps, pname, node.id))
        return issues

    # ── normalize ────────────────────────────────────────────────────────

    def normalize(self, dag: DAGSpec) -> DAGSpec:
        """Fill name/version/paths/after_nodes without reordering or mutating.

        Returns a new :class:`DAGSpec` (the input is left untouched).
        """
        nodes: list[DAGNode] = []
        for n in dag.nodes:
            params = dict(n.params)
            params.setdefault("inDataPath", "./data/")
            params.setdefault("outDataPath", "./data/")
            before = list(n.before_nodes)
            after = list(n.after_nodes)
            if not after:
                after = sorted(p.id for p in dag.nodes if n.id in p.before_nodes)
            nodes.append(DAGNode(
                id=n.id,
                name=n.name or n.algorithm or f"node_{n.id}",
                algorithm=n.algorithm,
                version=n.version or "1.0",
                params=params,
                before_nodes=before,
                after_nodes=after,
                resources=n.resources,
                direct_allocation_id=n.direct_allocation_id,
            ))
        return DAGSpec(
            name=dag.name or "unnamed_dag",
            policy=dag.policy if isinstance(dag.policy, Policy) else Policy(dag.policy),
            nodes=nodes,
            metadata=dict(dag.metadata),
            schema_version=dag.schema_version or SCHEMA_VERSION,
        )


# ── server-side entry ───────────────────────────────────────────────────────

def validate_dag_request(
    dag_data: dict,
    registry: AlgorithmRegistry | None = None,
    limits: ValidationLimits | None = None,
) -> ValidationReport:
    """Server-side second validation of a raw wire ``dagData`` request.

    Must run before any task/queue/ledger write.
    """
    return DAGValidator(algorithm_registry=registry, limits=limits).validate(dag_data)


# ── internal helpers ────────────────────────────────────────────────────────

def _extract_nodes(dag: Mapping) -> list | None:
    if "dagData" in dag and isinstance(dag["dagData"], Mapping):
        inner = dag["dagData"]
        if "nodes" in inner:
            return inner["nodes"] if isinstance(inner["nodes"], (list, tuple)) else None
    if "nodes" in dag:
        return dag["nodes"] if isinstance(dag["nodes"], (list, tuple)) else None
    return None


def _parse_dag_strict(dag: Mapping) -> DAGSpec:
    # Local import avoids a serializer->validator cycle at module import time.
    from .serializer import parse_dag

    return parse_dag(dag, allow_wire_format=True)


def _check_param_value(value, pspec: ParameterSpec, pname: str, node_id: int) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    path = f"$.nodes[{node_id - 1}].params.{pname}"
    typ = pspec.type or "string"
    if typ in ("integer", "int"):
        if not isinstance(value, int) or isinstance(value, bool):
            issues.append(_issue(ErrorCode.TYPE_ERROR, path, f"param '{pname}' must be an integer"))
    elif typ in ("number", "float"):
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            issues.append(_issue(ErrorCode.TYPE_ERROR, path, f"param '{pname}' must be a number"))
    elif typ == "boolean":
        if not isinstance(value, bool):
            issues.append(_issue(ErrorCode.TYPE_ERROR, path, f"param '{pname}' must be a boolean"))
    elif typ == "string":
        if not isinstance(value, str):
            issues.append(_issue(ErrorCode.TYPE_ERROR, path, f"param '{pname}' must be a string"))
    elif typ in ("array", "list"):
        if not isinstance(value, (list, tuple)):
            issues.append(_issue(ErrorCode.TYPE_ERROR, path, f"param '{pname}' must be an array"))
    elif typ in ("object", "dict"):
        if not isinstance(value, Mapping):
            issues.append(_issue(ErrorCode.TYPE_ERROR, path, f"param '{pname}' must be an object"))

    if pspec.enum is not None and value not in pspec.enum:
        issues.append(_issue(ErrorCode.VALUE_ERROR, path, f"param '{pname}' must be one of {pspec.enum}"))
    if pspec.minimum is not None and isinstance(value, (int, float)) and not isinstance(value, bool) and value < pspec.minimum:
        issues.append(_issue(ErrorCode.VALUE_ERROR, path, f"param '{pname}' must be >= {pspec.minimum}"))
    if pspec.maximum is not None and isinstance(value, (int, float)) and not isinstance(value, bool) and value > pspec.maximum:
        issues.append(_issue(ErrorCode.VALUE_ERROR, path, f"param '{pname}' must be <= {pspec.maximum}"))
    return issues
