"""
OpenSAI neutral contract package tests (WP-01).

Covers the acceptance criteria for WP-01:
  1. Contract examples are committed and reusable.
  2. Client/server validators share a single validation core (DAGValidator).
  3. opensai_contracts is neutral (does not import opensai_client).
  4. Unit tests: schema validation, unknown-field rejection, cycle detection,
     algorithm whitelist.

Runs under both `tests/run_all.py` and pytest (no pytest-only fixtures).
"""

import json
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

_FIXTURES = _project_root / "opensai_contracts" / "fixtures"
_SCHEMA = _project_root / "opensai_contracts" / "schemas" / "dag_spec.schema.json"
_MANIFEST = _project_root / "opensai_contracts" / "algorithms.yaml"


def _load_json(name):
    return json.loads((_FIXTURES / name).read_text(encoding="utf-8"))


# ── neutrality ──────────────────────────────────────────────────────────────

def test_contracts_imports():
    import opensai_contracts as c
    for attr in (
        "DAGSpec", "DAGNode", "ResourceHint", "AlgorithmInfo",
        "ValidationIssue", "ValidationReport", "JobHandle", "NodeResult",
        "JobResult", "JobRecord", "IdempotencyRecord", "OpenSAIConfig",
        "ErrorDetail", "ErrorCode", "DAGValidator", "parse_dag",
        "single_task_dag", "public_structure_to_dict", "dag_spec_to_dag_data",
    ):
        assert hasattr(c, attr), f"missing export: {attr}"


def test_contracts_is_neutral():
    """opensai_contracts must not import opensai_client (acceptance ③)."""
    import re
    pkg_dir = _project_root / "opensai_contracts"
    import_re = re.compile(r"^\s*(?:from\s+opensai_client|import\s+opensai_client)\b")
    for py in pkg_dir.rglob("*.py"):
        for lineno, line in enumerate(py.read_text(encoding="utf-8").splitlines(), 1):
            assert not import_re.match(line), (
                f"{py.relative_to(_project_root)}:{lineno} imports opensai_client"
            )
    # And importing the package must not pull opensai_client into sys.modules
    # (guard against transitive import at runtime).
    before = set(sys.modules)
    import opensai_contracts  # noqa: F401
    after = set(sys.modules)
    pulled = {m for m in after - before if m == "opensai_client" or m.startswith("opensai_client.")}
    assert not pulled, f"opensai_contracts transitively imported: {pulled}"


# ── data structures ─────────────────────────────────────────────────────────

def test_dag_spec_from_dict_roundtrip():
    from opensai_contracts import DAGSpec, ResourceHint
    spec = DAGSpec.from_dict(_load_json("valid_sdk_dag.json"))
    assert spec.name == "pi_pipeline"
    assert len(spec.nodes) == 2
    assert spec.nodes[0].id == 1
    assert spec.nodes[0].algorithm == "Algorithm_Pi_Calculation"
    assert isinstance(spec.nodes[0].resources, ResourceHint)
    assert spec.nodes[0].resources.cores == 2
    assert spec.nodes[1].before_nodes == [1]
    assert spec.nodes[0].after_nodes == [2]


def test_dag_spec_rejects_unknown_field():
    from opensai_contracts import DAGSpec, ContractParseError
    try:
        DAGSpec.from_dict({"name": "x", "nodes": [{"id": 1, "algorithm": "A"}], "junk": 1})
        assert False, "expected unknown-field rejection"
    except ContractParseError as e:
        assert e.code == "UNKNOWN_FIELD"
        assert "junk" in e.message


def test_dag_node_rejects_unknown_field():
    from opensai_contracts import DAGNode, ContractParseError
    try:
        DAGNode.from_dict({"id": 1, "algorithm": "A", "bogus": 1})
        assert False, "expected unknown-field rejection"
    except ContractParseError as e:
        assert e.code == "UNKNOWN_FIELD"
        assert "bogus" in e.message
        assert e.error.details.get("path")


def test_resource_hint_rejects_non_positive():
    from opensai_contracts import ResourceHint, ContractParseError
    assert ResourceHint(cores=2, mem_mb=2048).cores == 2
    for bad in ({"cores": 0}, {"mem_mb": -1}, {"bw_mbps": 0.0}):
        try:
            ResourceHint.from_dict(bad)
            assert False, f"expected reject {bad}"
        except ContractParseError as e:
            assert e.code == "VALUE_ERROR"


def test_error_detail_shape():
    from opensai_contracts import ErrorDetail
    e = ErrorDetail(code="X", message="m", retryable=True, details={"a": 1})
    d = e.to_dict()
    assert set(d.keys()) == {"code", "message", "retryable", "details"}
    assert d == {"code": "X", "message": "m", "retryable": True, "details": {"a": 1}}
    assert ErrorDetail.from_dict(d) == e


def test_job_status_terminal():
    from opensai_contracts import JobStatus, is_terminal
    assert is_terminal(JobStatus.SUCCEEDED)
    assert is_terminal(JobStatus.FAILED)
    assert is_terminal(JobStatus.CANCELLED)
    assert not is_terminal(JobStatus.QUEUED)
    assert not is_terminal(JobStatus.RUNNING)
    assert is_terminal("SUCCEEDED")
    assert not is_terminal("QUEUED")


def test_openai_config_redacts_secrets():
    from opensai_contracts import OpenSAIConfig
    cfg = OpenSAIConfig(redis_password="s3cret", redis_username="admin")
    assert "s3cret" not in repr(cfg)
    redacted = cfg.redacted()
    assert redacted["redis_password"] == "***"
    assert redacted["redis_username"] == "***"
    # full dict still carries real value for internal use
    assert cfg.to_dict()["redis_password"] == "s3cret"


# ── parse_dag ───────────────────────────────────────────────────────────────

def test_parse_dag_sdk_format():
    from opensai_contracts import parse_dag, DAGSpec
    spec = parse_dag(_load_json("valid_sdk_dag.json"))
    assert isinstance(spec, DAGSpec)
    assert spec.name == "pi_pipeline"


def test_parse_dag_wire_format():
    from opensai_contracts import parse_dag, DAGSpec
    spec = parse_dag(_load_json("valid_wire_dag.json"), allow_wire_format=True)
    assert isinstance(spec, DAGSpec)
    assert spec.name == "pi_pipeline"
    assert spec.nodes[0].algorithm == "Algorithm_Pi_Calculation"
    assert spec.nodes[1].before_nodes == [1]
    assert spec.nodes[0].after_nodes == [2]


def test_parse_dag_rejects_wire_when_disabled():
    from opensai_contracts import parse_dag, ContractParseError
    try:
        parse_dag(_load_json("valid_wire_dag.json"), allow_wire_format=False)
        assert False, "expected wire-format rejection"
    except ContractParseError as e:
        assert e.code == "UNKNOWN_FIELD"


def test_parse_dag_rejects_unknown_wire_field():
    from opensai_contracts import parse_dag, ContractParseError
    wire = _load_json("valid_wire_dag.json")
    wire["dagData"]["bogus"] = 1
    try:
        parse_dag(wire)
        assert False, "expected unknown-field rejection"
    except ContractParseError as e:
        assert e.code == "UNKNOWN_FIELD"
        assert "bogus" in e.message


def test_parse_dag_extracts_resources_from_wire():
    from opensai_contracts import parse_dag
    wire = _load_json("valid_wire_dag.json")
    wire["dagData"]["nodes"][0]["form"]["taskInputInfo"]["cores"] = 4
    spec = parse_dag(wire)
    assert spec.nodes[0].resources is not None
    assert spec.nodes[0].resources.cores == 4
    assert "cores" not in spec.nodes[0].params


# ── constructors / serialization ────────────────────────────────────────────

def test_single_task_dag():
    from opensai_contracts import single_task_dag, ResourceHint
    dag = single_task_dag("Algorithm_Pi_Calculation", {"count_num": 5},
                          resources=ResourceHint(cores=2))
    assert len(dag.nodes) == 1
    n = dag.nodes[0]
    assert n.id == 1
    assert n.before_nodes == []
    assert n.after_nodes == []
    assert n.algorithm == "Algorithm_Pi_Calculation"
    assert n.params["count_num"] == 5
    assert n.params["inDataPath"] == "./data/"
    assert n.params["outDataPath"] == "./data/"
    assert n.resources.cores == 2


def test_public_structure_to_dict():
    import datetime as dt
    from opensai_contracts import public_structure_to_dict, single_task_dag
    dag = single_task_dag("Algorithm_Sleep_1", {"sleep_time": 1})
    d = public_structure_to_dict(dag)
    assert d["name"] == "Algorithm_Sleep_1_task"
    assert d["nodes"][0]["id"] == 1
    # enum + datetime handling
    from opensai_contracts import JobStatus
    assert public_structure_to_dict(JobStatus.QUEUED) == "QUEUED"
    ts = dt.datetime(2026, 1, 1, 12, 0, 0)
    assert public_structure_to_dict(ts) == "2026-01-01T12:00:00"


def test_public_structure_to_dict_rejects_bytes():
    from opensai_contracts import public_structure_to_dict, SerializationError
    try:
        public_structure_to_dict(b"\x00")
        assert False, "expected bytes rejection"
    except SerializationError:
        pass
    try:
        public_structure_to_dict({"bin": b"\x00"})
        assert False, "expected nested bytes rejection"
    except SerializationError:
        pass


def test_dag_spec_to_dag_data_roundtrip():
    from opensai_contracts import parse_dag, dag_spec_to_dag_data
    spec = parse_dag(_load_json("valid_sdk_dag.json"))
    wire = dag_spec_to_dag_data(spec, task_create_time=1700000000.0)
    assert "dagData" in wire
    assert wire["dagData"]["taskName"] == "pi_pipeline"
    n0 = wire["dagData"]["nodes"][0]
    assert n0["form"]["taskAlgorithm"] == "Algorithm_Pi_Calculation"
    # resource hint merged into taskInputInfo root
    assert n0["form"]["taskInputInfo"]["cores"] == 2
    assert n0["form"]["taskInputInfo"]["mem_mb"] == 2048
    assert n0["result"]["status"] == 1
    # round-trip back
    back = parse_dag(wire)
    assert back.nodes[0].resources.cores == 2
    assert back.nodes[1].before_nodes == [1]


# ── validation ──────────────────────────────────────────────────────────────

def _registry():
    from opensai_contracts import load_algorithm_registry
    return load_algorithm_registry(str(_MANIFEST))


def test_validator_valid():
    from opensai_contracts import DAGValidator
    v = DAGValidator(algorithm_registry=_registry())
    report = v.validate(_load_json("valid_sdk_dag.json"))
    assert report.valid, [e.message for e in report.errors]
    assert report.normalized_dag is not None


def test_validator_cycle_detection():
    from opensai_contracts import DAGValidator
    v = DAGValidator(algorithm_registry=_registry())
    report = v.validate(_load_json("invalid_cycle.json"))
    assert not report.valid
    codes = {e.code for e in report.errors}
    assert "CYCLE_DETECTED" in codes
    cycle_issue = next(e for e in report.errors if e.code == "CYCLE_DETECTED")
    assert cycle_issue.details.get("cycle")


def test_validator_algorithm_whitelist():
    from opensai_contracts import DAGValidator
    v = DAGValidator(algorithm_registry=_registry())
    report = v.validate(_load_json("invalid_algorithm.json"))
    assert not report.valid
    assert any(e.code == "ALGORITHM_NOT_REGISTERED" for e in report.errors)


def test_validator_unknown_field():
    from opensai_contracts import DAGValidator
    v = DAGValidator(algorithm_registry=_registry())
    report = v.validate(_load_json("invalid_unknown_field.json"))
    assert not report.valid
    assert any(e.code == "UNKNOWN_FIELD" for e in report.errors)


def test_validator_normalize_fills_after_nodes():
    from opensai_contracts import DAGValidator, parse_dag
    v = DAGValidator(algorithm_registry=_registry())
    # SDK spec has empty after_nodes on node 1 -> normalize must fill it
    raw = _load_json("valid_sdk_dag.json")
    raw["nodes"][0]["after_nodes"] = []
    report = v.validate(raw)
    assert report.valid
    normalized = report.normalized_dag
    assert normalized.nodes[0].after_nodes == [2]
    assert normalized.nodes[1].before_nodes == [1]


def test_validator_shared_core_client_and_server():
    """Acceptance ②: the same DAGValidator validates both a public SDK DAG
    (client perspective) and a wire dagData (server perspective)."""
    from opensai_contracts import DAGValidator
    v = DAGValidator(algorithm_registry=_registry())
    sdk = v.validate(_load_json("valid_sdk_dag.json"))
    wire = v.validate(_load_json("valid_wire_dag.json"))
    assert sdk.valid
    assert wire.valid
    assert sdk.normalized_dag.name == wire.normalized_dag.name
    assert [n.algorithm for n in sdk.normalized_dag.nodes] == \
        [n.algorithm for n in wire.normalized_dag.nodes]


def test_validate_dag_request():
    from opensai_contracts import validate_dag_request
    report = validate_dag_request(_load_json("valid_wire_dag.json"), registry=_registry())
    assert report.valid


# ── wire conversions ────────────────────────────────────────────────────────

def test_job_ack_from_wire():
    from opensai_contracts import job_ack_from_wire, JobHandle
    h = job_ack_from_wire({
        "dagRspStatus": True,
        "job_id": "job_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "legacy_task_id": "task123",
        "status": "QUEUED",
        "accepted_at": "2026-09-05T00:00:00.000Z",
        "request_id": "req-1",
    })
    assert isinstance(h, JobHandle)
    assert h.job_id.startswith("job_")
    assert h.status == "QUEUED"


def test_job_ack_error_raises_structured():
    from opensai_contracts import job_ack_from_wire, ContractParseError
    try:
        job_ack_from_wire({
            "dagRspStatus": False,
            "error": {"code": "DAG_INVALID", "message": "bad", "retryable": False, "details": {}},
        })
        assert False, "expected rejection"
    except ContractParseError as e:
        assert e.code == "DAG_INVALID"


def test_remote_error_from_wire():
    from opensai_contracts import remote_error_from_wire
    e = remote_error_from_wire({"error": {"code": "JOB_NOT_FOUND", "message": "gone", "retryable": False, "details": {"job_id": "x"}}})
    assert e.code == "JOB_NOT_FOUND"
    assert e.details["job_id"] == "x"


def test_node_result_from_wire():
    from opensai_contracts import node_result_from_wire
    nr = node_result_from_wire({
        "id": 2,
        "result": {"taskResultInfo": "42", "status": 4, "delay": 1.5, "deviceId": 3},
    })
    assert nr.node_id == 2
    assert nr.status == "SUCCEEDED"
    assert nr.device_id == "3"
    assert nr.result == "42"


# ── sample JSON Schema + fixtures ───────────────────────────────────────────

def test_fixtures_present():
    for name in (
        "valid_sdk_dag.json", "valid_wire_dag.json", "invalid_cycle.json",
        "invalid_unknown_field.json", "invalid_algorithm.json",
    ):
        assert (_FIXTURES / name).exists(), f"missing fixture {name}"


def test_sample_schema_validates_fixtures():
    import jsonschema
    schema = json.loads(_SCHEMA.read_text(encoding="utf-8"))
    # valid SDK DAG passes
    jsonschema.validate(_load_json("valid_sdk_dag.json"), schema)
    # unknown field must be rejected (additionalProperties: false)
    from jsonschema import ValidationError
    try:
        jsonschema.validate(_load_json("invalid_unknown_field.json"), schema)
        assert False, "expected schema to reject unknown field"
    except ValidationError:
        pass


def test_algorithm_registry_manifest():
    from opensai_contracts import load_algorithm_registry
    reg = load_algorithm_registry(str(_MANIFEST))
    assert reg.schema_version == "1.0.0"
    assert "Algorithm_Pi_Calculation" in reg.algorithms
    info = reg.algorithms["Algorithm_Pi_Calculation"]
    assert info.submittable
    assert info.schema_available
    assert "count_num" in info.parameters
