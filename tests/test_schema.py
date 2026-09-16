"""
Schema and validator tests (APP-1.10).

Covers: APP-1.3 (DAG Schema), APP-1.4 (Client Validator),
        schema validation, algorithm whitelist rejection, acyclicity.
"""

import sys
import json
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
_offloading = _project_root / 'Offloading-System'
if _offloading.is_dir() and str(_offloading) not in sys.path:
    sys.path.insert(0, str(_offloading))


# ── valid spec for reuse ───────────────────────────────────────────────────

VALID_SPEC = {
    'taskName': 'test_dag',
    'nodes': [
        {'id': 1, 'algorithm': 'Algorithm_Pi_Calculation',
         'params': {'count_num': 100000}, 'dependsOn': []},
        {'id': 2, 'algorithm': 'Algorithm_Sleep_1',
         'params': {'sleep_time': 1}, 'dependsOn': [1]},
    ],
}

# Public SDK-format DAG (snake_case) used by the shared DAGValidator tests.
_SDK_SPEC = {
    'name': 'test_dag',
    'policy': 'smart',
    'nodes': [
        {'id': 1, 'algorithm': 'Algorithm_Pi_Calculation',
         'params': {'count_num': 100000}, 'before_nodes': []},
        {'id': 2, 'algorithm': 'Algorithm_Sleep_1',
         'params': {'sleep_time': 1}, 'before_nodes': [1]},
    ],
}


# ── schema validator tests ─────────────────────────────────────────────────

def test_schema_imports():
    from schema import dag_validator
    from schema.dag_validator import (
        validate, validate_file, to_dagdata,
        is_acyclic, topological_order, DAGValidationError,
    )
    assert dag_validator is not None


def test_validate_valid_dag():
    from schema.dag_validator import validate
    ok, err, dagdata = validate(VALID_SPEC)
    assert ok, f'Expected valid: {err}'
    assert dagdata is not None
    assert 'dagData' in dagdata
    assert dagdata['dagData']['taskName'] == 'test_dag'


def test_validate_missing_taskname():
    from schema.dag_validator import validate
    ok, err, dagdata = validate({'nodes': [{'id': 1, 'algorithm': 'Algo'}]})
    assert not ok
    assert 'taskName' in err


def test_validate_empty_nodes():
    from schema.dag_validator import validate
    ok, err, dagdata = validate({'taskName': 't', 'nodes': []})
    assert not ok
    assert 'at least one node' in err


def test_validate_missing_algorithm():
    from schema.dag_validator import validate
    ok, err, dagdata = validate({'taskName': 't', 'nodes': [{'id': 1}]})
    assert not ok
    assert 'algorithm' in err


def test_validate_duplicate_id():
    from schema.dag_validator import validate
    ok, err, dagdata = validate({
        'taskName': 't',
        'nodes': [{'id': 1, 'algorithm': 'A'}, {'id': 1, 'algorithm': 'B'}],
    })
    assert not ok
    assert 'duplicate' in err


def test_validate_bad_dependency():
    from schema.dag_validator import validate
    ok, err, dagdata = validate({
        'taskName': 't',
        'nodes': [{'id': 1, 'algorithm': 'A', 'dependsOn': [99]}],
    })
    assert not ok
    assert 'non-existent' in err


def test_validate_cycle():
    from schema.dag_validator import validate
    ok, err, dagdata = validate({
        'taskName': 't',
        'nodes': [
            {'id': 1, 'algorithm': 'A', 'dependsOn': [2]},
            {'id': 2, 'algorithm': 'B', 'dependsOn': [1]},
        ],
    })
    assert not ok
    assert 'cycle' in err.lower()


def test_validate_algorithm_whitelist():
    from schema.dag_validator import validate
    whitelist = {'Algorithm_Pi_Calculation', 'Algorithm_Sleep_1'}

    ok, err, dagdata = validate(VALID_SPEC, algorithm_whitelist=whitelist)
    assert ok, f'Expected valid: {err}'

    ok, err, dagdata = validate(VALID_SPEC, algorithm_whitelist={'Bad_Only'})
    assert not ok
    assert 'not registered' in err


def test_is_acyclic():
    from schema.dag_validator import is_acyclic
    assert is_acyclic(VALID_SPEC['nodes'])
    cycle_nodes = [
        {'id': 1, 'algorithm': 'A', 'dependsOn': [2]},
        {'id': 2, 'algorithm': 'B', 'dependsOn': [1]},
    ]
    assert not is_acyclic(cycle_nodes)


def test_topological_order():
    from schema.dag_validator import topological_order
    order = topological_order(VALID_SPEC['nodes'])
    ids = [n['id'] for n in order]
    assert ids == [1, 2]


def test_to_dagdata():
    from schema.dag_validator import to_dagdata
    dagdata = to_dagdata(VALID_SPEC)
    assert 'dagData' in dagdata
    nodes = dagdata['dagData']['nodes']
    assert len(nodes) == 2
    # Node 1 afterNodes should be [2]
    assert nodes[0]['form']['afterNodes'] == [2]
    assert nodes[1]['form']['beforeNodes'] == [1]
    # params merged
    assert nodes[0]['form']['taskInputInfo']['count_num'] == 100000
    # result stubs
    assert nodes[0]['result']['status'] == 1
    assert nodes[0]['result']['deviceId'] == 0


def test_to_dagdata_resource_hint():
    from schema.dag_validator import to_dagdata
    spec = {
        'taskName': 'with_hints',
        'nodes': [{
            'id': 1,
            'algorithm': 'Algorithm_MatMul',
            'resource_hint': {'cores': 4, 'mem_mb': 2048},
            'directAllocationId': 2,
        }],
    }
    dagdata = to_dagdata(spec)
    n = dagdata['dagData']['nodes'][0]
    assert n['form']['directAllocationId'] == 2
    assert n['form']['taskInputInfo']['cores'] == 4
    assert n['form']['taskInputInfo']['mem_mb'] == 2048


# ── client validator tests ─────────────────────────────────────────────────

def test_client_validator_imports():
    from opensai_client.validator import ClientValidator, ValidationError
    assert ClientValidator is not None
    assert ValidationError is not None


def test_client_validator_validate():
    from opensai_contracts import AlgorithmInfo, AlgorithmRegistry
    from opensai_client.validator import ClientValidator

    reg = AlgorithmRegistry(algorithms={
        'Algorithm_Pi_Calculation': AlgorithmInfo(name='Algorithm_Pi_Calculation'),
        'Algorithm_Sleep_1': AlgorithmInfo(name='Algorithm_Sleep_1'),
    })
    v = ClientValidator(algorithm_registry=reg)
    report = v.validate(_SDK_SPEC)
    assert report.valid, [e.message for e in report.errors]
    assert report.normalized_dag is not None


def test_client_validator_whitelist_reject():
    from opensai_contracts import AlgorithmInfo, AlgorithmRegistry
    from opensai_client.validator import ClientValidator

    reg = AlgorithmRegistry(algorithms={'Only_This': AlgorithmInfo(name='Only_This')})
    v = ClientValidator(algorithm_registry=reg)
    report = v.validate(_SDK_SPEC)
    assert not report.valid
    assert any(e.code == 'ALGORITHM_NOT_REGISTERED' for e in report.errors)


def test_client_validator_validate_or_raise():
    from opensai_contracts import AlgorithmInfo, AlgorithmRegistry
    from opensai_client.validator import ClientValidator, ValidationError

    reg = AlgorithmRegistry(algorithms={
        'Algorithm_Pi_Calculation': AlgorithmInfo(name='Algorithm_Pi_Calculation'),
        'Algorithm_Sleep_1': AlgorithmInfo(name='Algorithm_Sleep_1'),
    })
    v = ClientValidator(algorithm_registry=reg)
    dag = v.validate_or_raise(_SDK_SPEC)
    assert dag is not None

    bad = ClientValidator(algorithm_registry=AlgorithmRegistry(
        algorithms={'Bad': AlgorithmInfo(name='Bad')}))
    try:
        bad.validate_or_raise(_SDK_SPEC)
        assert False, 'Should have raised'
    except ValidationError:
        pass


def test_client_validator_delegates_to_dag_validator():
    from opensai_contracts import DAGValidator
    from opensai_client.validator import ClientValidator

    v = ClientValidator()
    assert isinstance(v.validator, DAGValidator)
    # No registry -> no algorithm whitelist check, but structure still validated.
    report = v.validate(_SDK_SPEC)
    assert report.valid, [e.message for e in report.errors]


# ── JSON I/O tests ─────────────────────────────────────────────────────────

def test_dag_spec_json_loads():
    """dag_spec.json is valid JSON."""
    spec_path = _project_root / 'schema' / 'dag_spec.json'
    with open(spec_path, encoding='utf-8') as f:
        schema = json.load(f)
    assert schema['title'] == 'OpenSAI DAG Specification'
    assert 'nodes' in schema['properties']


def test_validate_via_json_file(tmp_path):
    """validate_file works with a JSON file."""
    from schema.dag_validator import validate_file

    dag_file = tmp_path / 'test_dag.json'
    dag_file.write_text(json.dumps(VALID_SPEC), encoding='utf-8')

    ok, err, dagdata = validate_file(str(dag_file))
    assert ok, f'Expected valid: {err}'


def test_validate_via_json_file_missing():
    from schema.dag_validator import validate_file
    ok, err, dagdata = validate_file('/nonexistent/path.json')
    assert not ok
    assert 'not found' in err.lower() or 'File' in err
