"""
DAG Validator — schema validation, acyclicity check, algorithm whitelist.

Reads an Agent-friendly DAG specification (conforming to schema/dag_spec.json)
and validates it for submission to the OpenSAI control plane.

Supports input from:
  - A Python dict  (validate(spec))
  - A JSON file    (validate_file(path))
  - stdin           (validate_stdin())

Public API
----------
  validate(spec, algorithm_whitelist=None) -> (bool, str, dict|None)
      Returns (valid, error_message, internal_dagdata).

  validate_file(path, algorithm_whitelist=None) -> (bool, str, dict|None)
      Loads and validates a JSON DAG file.

  validate_stdin(algorithm_whitelist=None) -> (bool, str, dict|None)
      Reads JSON from stdin and validates.

  to_dagdata(spec, task_id='1') -> dict
      Converts a validated spec to the internal dagData format understood
      by OpenSAIClient.submit_dag().

  is_acyclic(nodes) -> bool
      Check if the DAG is acyclic.

  topological_order(nodes) -> list
      Return nodes in topological order. Raises ValueError on cycle.

Validation Steps
----------------
  1. Structural: check required fields, types, id uniqueness.
  2. Dependency: check all dependsOn references refer to existing node ids.
  3. Cyclicity: topological sort to detect cycles.
  4. Algorithm whitelist: if provided, reject unknown algorithms.
"""

__all__ = [
    'validate', 'validate_file', 'validate_stdin',
    'to_dagdata', 'is_acyclic', 'topological_order',
    'DAGValidationError',
]

import json
import sys
import time
from collections import defaultdict, deque
from pathlib import Path


class DAGValidationError(ValueError):
    """Raised when DAG validation fails with a structured error message."""


# ── schema loading ────────────────────────────────────────────────────────

def _load_schema():
    """Load the bundled JSON Schema for reference."""
    schema_path = Path(__file__).resolve().parent / 'dag_spec.json'
    with open(schema_path, encoding='utf-8') as f:
        return json.load(f)


# ── structural validation ─────────────────────────────────────────────────

def _check_required_fields(spec: dict) -> list[str]:
    """Return list of missing required top-level fields."""
    errors = []
    if not isinstance(spec, dict):
        return ['specification must be a JSON object']
    for field in ('taskName', 'nodes'):
        if field not in spec:
            errors.append(f"missing required field: '{field}'")
    if 'nodes' in spec:
        if not isinstance(spec['nodes'], list):
            errors.append("'nodes' must be an array")
        elif len(spec['nodes']) == 0:
            errors.append("'nodes' must contain at least one node")
    return errors


def _check_nodes(spec: dict) -> list[str]:
    """Validate individual node structure."""
    errors = []
    nodes = spec.get('nodes', [])
    seen_ids = set()

    for i, node in enumerate(nodes):
        prefix = f"node[{i}]"
        if not isinstance(node, dict):
            errors.append(f"{prefix}: must be an object")
            continue

        # Required fields
        if 'id' not in node:
            errors.append(f"{prefix}: missing required field 'id'")
        elif not isinstance(node['id'], int):
            errors.append(f"{prefix}: 'id' must be an integer")

        if 'algorithm' not in node:
            errors.append(f"{prefix}: missing required field 'algorithm'")
        elif not isinstance(node['algorithm'], str) or not node['algorithm'].strip():
            errors.append(f"{prefix}: 'algorithm' must be a non-empty string")

        # Optional fields — type checks
        if 'params' in node and not isinstance(node['params'], dict):
            errors.append(f"{prefix}: 'params' must be an object")
        if 'dependsOn' in node:
            deps = node['dependsOn']
            if not isinstance(deps, list):
                errors.append(f"{prefix}: 'dependsOn' must be an array")
            elif not all(isinstance(d, int) for d in deps):
                errors.append(f"{prefix}: all 'dependsOn' entries must be integers")
        if 'resource_hint' in node:
            rh = node['resource_hint']
            if not isinstance(rh, dict):
                errors.append(f"{prefix}: 'resource_hint' must be an object")
        if 'directAllocationId' in node and not isinstance(node['directAllocationId'], int):
            errors.append(f"{prefix}: 'directAllocationId' must be an integer")

        # ID uniqueness
        nid = node.get('id')
        if nid is not None:
            if nid in seen_ids:
                errors.append(f"{prefix}: duplicate node id {nid}")
            seen_ids.add(nid)

    return errors


def _check_dependencies(spec: dict) -> list[str]:
    """Check that all dependsOn references point to existing node ids."""
    errors = []
    nodes = spec.get('nodes', [])
    node_ids = {n.get('id') for n in nodes if isinstance(n, dict) and 'id' in n}

    for i, node in enumerate(nodes):
        if not isinstance(node, dict):
            continue
        deps = node.get('dependsOn', [])
        if not isinstance(deps, list):
            continue
        for dep in deps:
            if dep not in node_ids:
                errors.append(
                    f"node[{i}] (id={node.get('id')}): dependsOn references "
                    f"non-existent node id {dep}"
                )
    return errors


# ── cyclicity ─────────────────────────────────────────────────────────────

def is_acyclic(nodes: list[dict]) -> bool:
    """Return True if the node dependency graph has no cycles."""
    try:
        topological_order(nodes)
        return True
    except ValueError:
        return False


def topological_order(nodes: list[dict]) -> list[dict]:
    """Return nodes in topological order. Raises ValueError on cycle.

    Uses Kahn's algorithm (BFS with in-degree tracking).
    """
    if not nodes:
        return []

    # Build adjacency and in-degree
    node_map = {}
    for n in nodes:
        nid = n.get('id')
        if nid is not None:
            node_map[nid] = n

    in_degree = {nid: 0 for nid in node_map}
    adj = defaultdict(list)

    for n in nodes:
        nid = n.get('id')
        if nid is None:
            continue
        deps = n.get('dependsOn', [])
        if not isinstance(deps, list):
            deps = []
        for dep in deps:
            if dep in node_map:
                adj[dep].append(nid)
                in_degree[nid] = in_degree.get(nid, 0) + 1

    # Queue nodes with zero in-degree
    queue = deque(nid for nid, deg in in_degree.items() if deg == 0)
    result = []

    while queue:
        nid = queue.popleft()
        result.append(node_map[nid])
        for succ in adj[nid]:
            in_degree[succ] -= 1
            if in_degree[succ] == 0:
                queue.append(succ)

    if len(result) != len(node_map):
        # Cycle detected — find nodes remaining
        remaining = set(node_map.keys()) - {n.get('id') for n in result}
        raise ValueError(
            f"DAG contains a cycle involving node(s): "
            f"{sorted(remaining)}"
        )

    return result


# ── algorithm whitelist ───────────────────────────────────────────────────

def _check_algorithms(spec: dict, whitelist: set | None) -> list[str]:
    """Check that all node algorithms are in the whitelist (if provided)."""
    if whitelist is None:
        return []
    errors = []
    nodes = spec.get('nodes', [])
    for i, node in enumerate(nodes):
        if not isinstance(node, dict):
            continue
        algo = node.get('algorithm', '')
        if algo and algo not in whitelist:
            errors.append(
                f"node[{i}] (id={node.get('id')}): algorithm '{algo}' "
                f"is not registered"
            )
    return errors


# ── main validation ───────────────────────────────────────────────────────

def validate(spec: dict, algorithm_whitelist: list[str] | set | None = None) -> tuple[bool, str, dict | None]:
    """Validate a DAG specification dict.

    Parameters
    ----------
    spec : dict
        Agent-friendly DAG specification.
    algorithm_whitelist : list or set, optional
        Set of allowed algorithm names. If None, skip algorithm check.

    Returns
    -------
    (bool, str, dict|None)
        (valid, error_message, internal_dagdata_or_None)
    """
    whitelist_set = set(algorithm_whitelist) if algorithm_whitelist is not None else None

    # Step 1: structural
    errors = _check_required_fields(spec)
    errors += _check_nodes(spec)
    if errors:
        return False, '; '.join(errors), None

    # Step 2: dependencies
    errors = _check_dependencies(spec)
    if errors:
        return False, '; '.join(errors), None

    # Step 3: cyclicity
    try:
        topological_order(spec.get('nodes', []))
    except ValueError as e:
        return False, str(e), None

    # Step 4: algorithm whitelist
    errors = _check_algorithms(spec, whitelist_set)
    if errors:
        return False, '; '.join(errors), None

    # All checks passed — produce dagData
    try:
        dagdata = to_dagdata(spec)
    except Exception as e:
        return False, f"Failed to convert to dagData: {e}", None

    return True, '', dagdata


def validate_file(path: str, algorithm_whitelist: list[str] | set | None = None) -> tuple[bool, str, dict | None]:
    """Load a JSON DAG file and validate it.

    Parameters
    ----------
    path : str
        Path to a JSON file containing the DAG specification.
    algorithm_whitelist : list or set, optional
        Allowed algorithm names.

    Returns
    -------
    (bool, str, dict|None)
    """
    try:
        with open(path, encoding='utf-8') as f:
            spec = json.load(f)
    except FileNotFoundError:
        return False, f"File not found: {path}", None
    except json.JSONDecodeError as e:
        return False, f"Invalid JSON in {path}: {e}", None
    except Exception as e:
        return False, f"Failed to read {path}: {e}", None

    return validate(spec, algorithm_whitelist)


def validate_stdin(algorithm_whitelist: list[str] | set | None = None) -> tuple[bool, str, dict | None]:
    """Read DAG specification from stdin and validate it.

    Parameters
    ----------
    algorithm_whitelist : list or set, optional
        Allowed algorithm names.

    Returns
    -------
    (bool, str, dict|None)
    """
    try:
        raw = sys.stdin.read()
        spec = json.loads(raw)
    except json.JSONDecodeError as e:
        return False, f"Invalid JSON from stdin: {e}", None
    except Exception as e:
        return False, f"Failed to read stdin: {e}", None

    return validate(spec, algorithm_whitelist)


# ── dagData conversion ────────────────────────────────────────────────────

def to_dagdata(spec: dict, task_id: str = '1') -> dict:
    """Convert agent-friendly DAG spec to internal dagData format.

    Parameters
    ----------
    spec : dict
        Validated agent-friendly DAG specification.
    task_id : str
        Task ID for the dagData envelope.

    Returns
    -------
    dict
        Internal dagData format, ready for OpenSAIClient.submit_dag().
    """
    nodes_out = []
    node_ids = set()

    for node in spec.get('nodes', []):
        nid = node['id']
        name = node.get('name', node.get('algorithm', f'node_{nid}'))
        algo = node['algorithm']
        deps = node.get('dependsOn', [])

        # params — merge inDataPath/outDataPath defaults
        params = dict(node.get('params', {}))
        params.setdefault('inDataPath', './data/')
        params.setdefault('outDataPath', './data/')

        # resource_hint merged into params
        resource_hint = node.get('resource_hint', {})
        if resource_hint:
            for key, val in resource_hint.items():
                params[key] = val  # overwrite params with resource hints

        direct = node.get('directAllocationId', -1)

        nodes_out.append({
            'id': nid,
            'name': name,
            'form': {
                'beforeNodes': deps,
                'afterNodes': [],  # populated below
                'taskAlgorithm': algo,
                'taskVersion': '1.0',
                'taskInputInfo': params,
                'details': '',
                'directAllocationId': direct,
                'deviceParam': {},
            },
            'result': {
                'taskResultInfo': '',
                'status': 1,
                'delay': 0,
                'deviceId': 0,
            },
        })
        node_ids.add(nid)

    # Populate afterNodes (reverse of dependsOn)
    for node in nodes_out:
        nid = node['id']
        for dep in node['form']['beforeNodes']:
            # Find the predecessor node and add nid to its afterNodes
            for pre in nodes_out:
                if pre['id'] == dep:
                    pre['form']['afterNodes'].append(nid)
                    break

    return {
        'dagData': {
            'taskName': spec.get('taskName', 'unnamed_dag'),
            'taskId': task_id,
            'taskDesc': spec.get('taskDesc', ''),
            'taskCreateTime': time.time(),
            'taskPolicy': spec.get('taskPolicy', 'default'),
            'nodes': nodes_out,
        }
    }
