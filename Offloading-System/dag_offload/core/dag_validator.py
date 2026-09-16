"""
Server-side DAG validator (COM-1.4).

Defence-in-depth: validates DAG submissions at the server protocol layer
before they enter the scheduling pipeline. Mirrors the client-side validator
(opensai_client/validator.py) to prevent bypass attacks.

Checks:
  1. Structural validity (required fields, types, id uniqueness)
  2. Dependency integrity (all dependsOn / beforeNodes refs valid)
  3. Acyclicity (no dependency cycles)
  4. Algorithm whitelist (only registered algorithms)
  5. Parameter legality (JSON-serializable dicts)

Usage in server.py (dagData handler):
    from core.dag_validator import ServerDAGValidator

    ok, err = ServerDAGValidator.validate(data_decode)
    if not ok:
        NetworkUtils.send_msg(client_sock, {'dagRspStatus': False, 'error': err})
        return
"""

__all__ = ['ServerDAGValidator']


class ServerDAGValidator:
    """Server-side DAG validation (COM-1.4).

    All validation methods are static so they can be called without
    instantiating the server class.
    """

    @staticmethod
    def validate(dag_payload: dict, algorithm_whitelist: set | None = None) -> tuple[bool, str]:
        """Validate a DAG payload received from a client.

        Parameters
        ----------
        dag_payload : dict
            The full NDJSON message received from the client (must contain 'dagData').
        algorithm_whitelist : set, optional
            Set of registered algorithm names. If None, skips the algorithm check.

        Returns
        -------
        (bool, str)
            (valid, error_message)
        """
        if not isinstance(dag_payload, dict):
            return False, 'Message must be a JSON object'

        if 'dagData' not in dag_payload:
            return False, "Missing 'dagData' key in payload"

        dag = dag_payload['dagData']
        if not isinstance(dag, dict):
            return False, "'dagData' must be an object"

        # Step 1: structural
        err = ServerDAGValidator._check_structure(dag)
        if err:
            return False, err

        # Step 2: dependency integrity
        err = ServerDAGValidator._check_dependencies(dag)
        if err:
            return False, err

        # Step 3: acyclicity
        err = ServerDAGValidator._check_acyclicity(dag)
        if err:
            return False, err

        # Step 4: algorithm whitelist
        if algorithm_whitelist is not None:
            err = ServerDAGValidator._check_algorithms(dag, algorithm_whitelist)
            if err:
                return False, err

        # Step 5: parameters
        err = ServerDAGValidator._check_params(dag)
        if err:
            return False, err

        return True, ''

    # ── step implementations ──────────────────────────────────────────

    @staticmethod
    def _check_structure(dag: dict) -> str | None:
        """Check required top-level and per-node fields."""
        if 'taskName' not in dag:
            return "Missing required field: 'taskName'"

        if 'nodes' not in dag:
            return "Missing required field: 'nodes'"

        nodes = dag['nodes']
        if not isinstance(nodes, list):
            return "'nodes' must be an array"
        if len(nodes) == 0:
            return "DAG must contain at least one node"

        seen_ids = set()
        for i, node in enumerate(nodes):
            if not isinstance(node, dict):
                return f"node[{i}]: must be an object"

            if 'id' not in node:
                return f"node[{i}]: missing required field 'id'"
            if not isinstance(node['id'], int):
                return f"node[{i}]: 'id' must be an integer"

            if 'form' not in node:
                return f"node[{i}] (id={node['id']}): missing 'form'"

            form = node['form']
            if not isinstance(form, dict):
                return f"node[{i}] (id={node['id']}): 'form' must be an object"

            if 'taskAlgorithm' not in form:
                return f"node[{i}] (id={node['id']}): missing 'form.taskAlgorithm'"
            if not isinstance(form['taskAlgorithm'], str) or not form['taskAlgorithm'].strip():
                return f"node[{i}] (id={node['id']}): 'form.taskAlgorithm' must be a non-empty string"

            # beforeNodes / afterNodes type check
            for key in ('beforeNodes', 'afterNodes'):
                val = form.get(key, [])
                if not isinstance(val, list):
                    return f"node[{i}] (id={node['id']}): 'form.{key}' must be an array"
                if not all(isinstance(v, int) for v in val):
                    return f"node[{i}] (id={node['id']}): all '{key}' entries must be integers"

            # ID uniqueness
            nid = node['id']
            if nid in seen_ids:
                return f"node[{i}]: duplicate node id {nid}"
            seen_ids.add(nid)

        return None

    @staticmethod
    def _check_dependencies(dag: dict) -> str | None:
        """Verify all beforeNodes references point to existing node ids."""
        nodes = dag.get('nodes', [])
        node_ids = {n.get('id') for n in nodes if isinstance(n, dict) and 'id' in n}

        for i, node in enumerate(nodes):
            if not isinstance(node, dict):
                continue
            form = node.get('form', {})
            if not isinstance(form, dict):
                continue
            deps = form.get('beforeNodes', [])
            if not isinstance(deps, list):
                continue
            for dep in deps:
                if dep not in node_ids:
                    return (
                        f"node[{i}] (id={node.get('id')}): "
                        f"beforeNodes references non-existent node id {dep}"
                    )

        return None

    @staticmethod
    def _check_acyclicity(dag: dict) -> str | None:
        """Check that the DAG has no cycles (Kahn's algorithm)."""
        nodes = dag.get('nodes', [])
        if len(nodes) <= 1:
            return None

        node_map = {}
        for n in nodes:
            if isinstance(n, dict) and 'id' in n:
                node_map[n['id']] = n

        from collections import defaultdict, deque

        in_degree = {nid: 0 for nid in node_map}
        adj = defaultdict(list)

        for n in nodes:
            if not isinstance(n, dict):
                continue
            nid = n.get('id')
            if nid is None:
                continue
            form = n.get('form', {})
            deps = form.get('beforeNodes', [])
            if not isinstance(deps, list):
                deps = []
            for dep in deps:
                if dep in node_map:
                    adj[dep].append(nid)
                    in_degree[nid] = in_degree.get(nid, 0) + 1

        queue = deque(nid for nid, deg in in_degree.items() if deg == 0)
        result_count = 0

        while queue:
            nid = queue.popleft()
            result_count += 1
            for succ in adj[nid]:
                in_degree[succ] -= 1
                if in_degree[succ] == 0:
                    queue.append(succ)

        if result_count != len(node_map):
            remaining = sorted(nid for nid, deg in in_degree.items() if deg > 0)
            if not remaining:
                remaining = sorted(node_map.keys())
            return f"DAG contains a cycle involving node(s): {remaining}"

        return None

    @staticmethod
    def _check_algorithms(dag: dict, whitelist: set) -> str | None:
        """Check all node algorithms are in the whitelist."""
        nodes = dag.get('nodes', [])
        for i, node in enumerate(nodes):
            if not isinstance(node, dict):
                continue
            form = node.get('form', {})
            algo = form.get('taskAlgorithm', '')
            if algo and algo not in whitelist:
                return (
                    f"node[{i}] (id={node.get('id')}): "
                    f"algorithm '{algo}' is not registered"
                )
        return None

    @staticmethod
    def _check_params(dag: dict) -> str | None:
        """Check that taskInputInfo params are dicts (JSON-serializable)."""
        nodes = dag.get('nodes', [])
        for i, node in enumerate(nodes):
            if not isinstance(node, dict):
                continue
            form = node.get('form', {})
            if not isinstance(form, dict):
                continue
            params = form.get('taskInputInfo', {})
            if not isinstance(params, dict):
                return (
                    f"node[{i}] (id={node.get('id')}): "
                    "'form.taskInputInfo' must be an object"
                )
        return None
