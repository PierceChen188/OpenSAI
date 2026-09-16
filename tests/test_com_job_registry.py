"""WP-02 (COM) 接入与作业查询 — 单元测试。

覆盖验收标准：
  1. dagData 向后兼容（ACK 保留 dagRspStatus）。
  2. job_id 回执（ACK 带 job_id / legacy_task_id / status / accepted_at）。
  3. 幂等（同 idempotency_key + 同 DAG → 同 job_id；同 key 不同 DAG → 冲突）。
  4. 终态不可覆盖（SUCCEEDED/FAILED/CANCELLED 不可转移为不同状态）。
  5. 服务端校验钩子（共享 DAGValidator + resource/placement 编排）。
  6. jobQuery 映射（JobRecord → JobResult）。

不依赖运行中的 server / Redis（JobRegistry 为纯 Python；校验钩子用共享 DAGValidator）。
"""

import json
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
_offloading = _project_root / 'Offloading-System'
if _offloading.is_dir() and str(_offloading) not in sys.path:
    sys.path.insert(0, str(_offloading))
_dag_offload = _offloading / 'dag_offload'
if _dag_offload.is_dir() and str(_dag_offload) not in sys.path:
    sys.path.insert(0, str(_dag_offload))

_FIXTURES = _project_root / "opensai_contracts" / "fixtures"


def _load_fixture(name):
    return json.loads((_FIXTURES / name).read_text(encoding="utf-8"))


def _wire_dag(**overrides):
    """构造一个最小合法 wire dagData（单节点 Algorithm_Sleep_1）。"""
    dag = {
        "dagData": {
            "taskName": "sleep_job",
            "taskCreateTime": 1700000000.0,
            "taskPolicy": "smart",
            "nodes": [
                {
                    "id": 1,
                    "name": "sleep",
                    "form": {
                        "beforeNodes": [],
                        "afterNodes": [],
                        "taskAlgorithm": "Algorithm_Sleep_1",
                        "taskVersion": "1.0",
                        "taskInputInfo": {"sleep_time": 1},
                        "details": "",
                        "directAllocationId": -1,
                        "deviceParam": {},
                    },
                    "result": {"taskResultInfo": "", "status": 1, "delay": 0, "deviceId": 0},
                }
            ],
        }
    }
    for k, v in overrides.items():
        dag[k] = v
    return dag


_WHITELIST = {"Algorithm_Sleep_1", "Algorithm_Pi_Calculation"}


# ── ULID / digest / legacy ──────────────────────────────────────────────────

def test_new_ulid_format():
    from core.job_registry import new_ulid
    u = new_ulid()
    assert len(u) == 26
    allowed = set("0123456789ABCDEFGHJKMNPQRSTVWXYZ")
    assert all(c in allowed for c in u), f"invalid ULID charset: {u}"
    # 128-bit：最高 2 bit 为 0，首字符必 ≤ '7'
    assert u[0] in "01234567"


def test_new_ulid_unique():
    from core.job_registry import new_ulid
    assert len({new_ulid() for _ in range(2000)}) == 2000


def test_new_ulid_monotonic_prefix():
    from core.job_registry import new_ulid
    prev = new_ulid()
    for _ in range(100):
        cur = new_ulid()
        # 前 10 字符（48bit 毫秒时间戳）单调不减
        assert cur[:10] >= prev[:10]
        prev = cur


def test_compute_dag_digest_stable():
    from core.job_registry import compute_dag_digest
    a = {"nodes": [{"id": 1}], "taskName": "x"}
    b = {"taskName": "x", "nodes": [{"id": 1}]}   # 键序不同
    assert compute_dag_digest(a) == compute_dag_digest(b)
    c = {"nodes": [{"id": 2}], "taskName": "x"}    # 内容不同
    assert compute_dag_digest(a) != compute_dag_digest(c)


def test_derive_legacy_task_id():
    from core.job_registry import derive_legacy_task_id
    # float 时间戳
    assert derive_legacy_task_id({"dagData": {"taskName": "pi", "taskCreateTime": 1700000000.0}}) == "pi1700000000.0"
    # int 时间戳
    assert derive_legacy_task_id({"dagData": {"taskName": "pi", "taskCreateTime": 1700000000}}) == "pi1700000000"


# ── JobRegistry 注册 / 查询 ─────────────────────────────────────────────────

def test_register_creates_record():
    from core.job_registry import JobRegistry
    from opensai_contracts import JobStatus
    reg = JobRegistry()
    rec, created = reg.register("task1", dag_data={"nodes": [{"id": 1}]}, owner="u1")
    assert created is True
    assert rec.job_id.startswith("job_")
    assert rec.legacy_task_id == "task1"
    assert rec.status == JobStatus.QUEUED
    assert rec.dag_digest
    assert rec.submitted_at


def test_get_and_get_by_legacy():
    from core.job_registry import JobRegistry
    from opensai_contracts.errors import JobNotFoundError
    reg = JobRegistry()
    rec, _ = reg.register("task9", dag_data={"nodes": [{"id": 1}]})
    assert reg.get(rec.job_id).legacy_task_id == "task9"
    assert reg.get_by_legacy("task9").job_id == rec.job_id
    try:
        reg.get("job_nonexistent")
        assert False, "expected JobNotFoundError"
    except JobNotFoundError as e:
        assert e.code == "JOB_NOT_FOUND"


# ── 幂等 ────────────────────────────────────────────────────────────────────

def test_idempotency_same_key_same_payload():
    from core.job_registry import JobRegistry
    reg = JobRegistry()
    rec1, created1 = reg.register("task1", dag_data={"nodes": [{"id": 1}]},
                                  owner="u1", idempotency_key="k1")
    rec2, created2 = reg.register("task1", dag_data={"nodes": [{"id": 1}]},
                                  owner="u1", idempotency_key="k1")
    assert created1 is True
    assert created2 is False
    assert rec1.job_id == rec2.job_id


def test_idempotency_same_key_different_payload_conflict():
    from core.job_registry import JobRegistry
    from opensai_contracts.errors import IdempotencyConflictError
    reg = JobRegistry()
    reg.register("task1", dag_data={"nodes": [{"id": 1}]}, owner="u1", idempotency_key="k1")
    try:
        reg.register("task1", dag_data={"nodes": [{"id": 2}]}, owner="u1", idempotency_key="k1")
        assert False, "expected IdempotencyConflictError"
    except IdempotencyConflictError as e:
        assert e.code == "IDEMPOTENCY_CONFLICT"


def test_idempotency_scoped_by_owner():
    from core.job_registry import JobRegistry
    reg = JobRegistry()
    rec1, _ = reg.register("t", dag_data={"nodes": [{"id": 1}]}, owner="u1", idempotency_key="k")
    rec2, created2 = reg.register("t", dag_data={"nodes": [{"id": 1}]}, owner="u2", idempotency_key="k")
    assert created2 is True           # 不同 owner 独立幂等域
    assert rec1.job_id != rec2.job_id


# ── 终态不可覆盖 ────────────────────────────────────────────────────────────

def test_terminal_immutable():
    from core.job_registry import JobRegistry
    from opensai_contracts import JobStatus
    from opensai_contracts.errors import TerminalStateError
    reg = JobRegistry()
    rec, _ = reg.register("task1", dag_data={"nodes": [{"id": 1}]})
    reg.transition(rec.job_id, JobStatus.SUCCEEDED)
    assert reg.get(rec.job_id).status == JobStatus.SUCCEEDED
    # 终态 → 任何不同状态（含另一终态）都拒绝
    for target in (JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.RUNNING, JobStatus.QUEUED):
        try:
            reg.transition(rec.job_id, target)
            assert False, f"expected TerminalStateError for {target}"
        except TerminalStateError as e:
            assert e.code == "TERMINAL_STATE_IMMUTABLE"


def test_terminal_idempotent_same_state_allowed():
    from core.job_registry import JobRegistry
    from opensai_contracts import JobStatus
    reg = JobRegistry()
    rec, _ = reg.register("task1", dag_data={"nodes": [{"id": 1}]})
    reg.transition(rec.job_id, JobStatus.FAILED)
    # 幂等重复设置同一终态（无害）
    reg.transition(rec.job_id, JobStatus.FAILED)
    assert reg.get(rec.job_id).status == JobStatus.FAILED


def test_transition_by_legacy_and_unknown():
    from core.job_registry import JobRegistry
    from opensai_contracts import JobStatus
    reg = JobRegistry()
    rec, _ = reg.register("task1", dag_data={"nodes": [{"id": 1}]})
    reg.transition_by_legacy("task1", JobStatus.SUCCEEDED)
    assert reg.get(rec.job_id).status == JobStatus.SUCCEEDED
    # 未注册的旧任务安全跳过（返回 None，不抛异常）
    assert reg.transition_by_legacy("unknown_task", JobStatus.SUCCEEDED) is None


# ── jobQuery 映射 ───────────────────────────────────────────────────────────

def test_to_job_result_maps():
    from core.job_registry import JobRegistry
    from opensai_contracts import JobStatus
    reg = JobRegistry()
    rec, _ = reg.register("task1", dag_data={"nodes": [{"id": 1}]})
    jr = reg.to_job_result(rec.job_id)
    assert jr.job_id == rec.job_id
    assert jr.status == JobStatus.QUEUED
    assert jr.submitted_at == rec.submitted_at


def test_jobquery_client_server_field_alignment():
    """X11: 客户端 ``_parse_job_query`` 与服务端 ``{"job": JobResult}`` 字段级对齐。

    服务端 jobQuery 回 ``{"job": JobResult.to_dict(), "request_id": ...}``；客户端
    ``get_result`` 经 ``_parse_job_query`` 逐字段还原，两者字段一一对应、无漂移。
    """
    from core.job_registry import JobRegistry
    from opensai_contracts import JobStatus
    from opensai_client.client import _parse_job_query

    reg = JobRegistry()
    rec, _ = reg.register("task1", dag_data={"nodes": [{"id": 1}]})
    reg.transition(rec.job_id, JobStatus.RUNNING, started_at="2026-09-05T00:00:01Z",
                   nodes=[{"node_id": 1, "status": "RUNNING"}])

    server_job = reg.to_job_result(rec.job_id)                 # 服务端公开映射
    wire = {"job": server_job.to_dict(), "request_id": "req-9"}  # 服务端 jobQuery 响应

    parsed = _parse_job_query(wire, rec.job_id)                 # 客户端解析
    assert parsed.job_id == server_job.job_id
    assert str(parsed.status) == str(server_job.status)
    assert parsed.submitted_at == server_job.submitted_at
    assert parsed.started_at == server_job.started_at
    assert parsed.completed_at == server_job.completed_at
    assert parsed.duration_s == server_job.duration_s
    assert parsed.nodes == server_job.nodes
    assert parsed.error == server_job.error


def test_jobquery_response_uses_job_key():
    """X11: jobQuery 响应顶层键必须是 ``job``（非 ``dagResult``/``jobResult``）。"""
    from opensai_contracts import JobResult
    jr = JobResult(job_id="job_x", status="QUEUED")
    assert "job" in {"job": jr.to_dict()}
    # 客户端正是按 ``job`` 键读取，legacy dagResult 仅作兜底
    from opensai_client.client import _parse_job_query
    parsed = _parse_job_query({"job": jr.to_dict()}, "job_x")
    assert parsed.job_id == "job_x"
    assert str(parsed.status) == "QUEUED"


def test_resolve_by_job_id_and_legacy():
    from core.job_registry import JobRegistry
    reg = JobRegistry()
    rec, _ = reg.register("task1", dag_data={"nodes": [{"id": 1}]})
    assert reg.resolve(job_id=rec.job_id).job_id == rec.job_id
    assert reg.resolve(legacy_task_id="task1").job_id == rec.job_id


# ── dagData ACK 回执（向后兼容 + job_id）────────────────────────────────────

def test_ack_payload_backward_compatible():
    from core.job_registry import JobRegistry
    reg = JobRegistry()
    rec, _ = reg.register("task1", dag_data={"nodes": [{"id": 1}]})
    ack = reg.ack_payload(rec, request_id="req-1")
    # 旧客户端兼容字段
    assert ack["dagRspStatus"] is True
    # 新回执字段
    assert ack["job_id"] == rec.job_id
    assert ack["legacy_task_id"] == "task1"
    assert ack["status"] == "QUEUED"
    assert ack["accepted_at"] == rec.submitted_at
    assert ack["request_id"] == "req-1"


def test_ack_payload_without_request_id():
    from core.job_registry import JobRegistry
    reg = JobRegistry()
    rec, _ = reg.register("task1", dag_data={"nodes": [{"id": 1}]})
    ack = reg.ack_payload(rec)
    assert "request_id" not in ack
    assert ack["dagRspStatus"] is True


# ── 服务端校验钩子 ──────────────────────────────────────────────────────────

def test_validate_server_dag_valid():
    from core.server_dag_validator import validate_server_dag
    ok, err = validate_server_dag(_load_fixture("valid_wire_dag.json"), algorithm_whitelist=_WHITELIST)
    assert ok is True and err is None


def test_validate_server_dag_algorithm_not_registered():
    from core.server_dag_validator import validate_server_dag
    ok, err = validate_server_dag(_load_fixture("valid_wire_dag.json"), algorithm_whitelist={"Algorithm_MatMul"})
    assert ok is False
    assert err["code"] == "ALGORITHM_NOT_REGISTERED"
    assert err["retryable"] is False
    assert err["details"]["issues"]


def test_validate_server_dag_cycle():
    from core.server_dag_validator import validate_server_dag
    ok, err = validate_server_dag(_load_fixture("invalid_cycle.json"), algorithm_whitelist=_WHITELIST)
    assert ok is False
    assert err["code"] == "CYCLE_DETECTED"


def test_validate_server_dag_unknown_field():
    from core.server_dag_validator import validate_server_dag
    ok, err = validate_server_dag(_load_fixture("invalid_unknown_field.json"), algorithm_whitelist=_WHITELIST)
    assert ok is False
    assert err["code"] == "UNKNOWN_FIELD"


def test_validate_server_dag_placement_target_invalid():
    from core.server_dag_validator import validate_server_dag
    dag = _wire_dag()
    dag["dagData"]["nodes"][0]["form"]["directAllocationId"] = 0   # 非正整数
    ok, err = validate_server_dag(dag, algorithm_whitelist=_WHITELIST)
    assert ok is False
    assert err["code"] == "INVALID_DIRECT_ALLOCATION"


def test_validate_server_dag_resource_hint_positive():
    from core.server_dag_validator import validate_server_dag
    # cores=2（合法正值）应通过
    dag = _wire_dag()
    dag["dagData"]["nodes"][0]["form"]["taskInputInfo"]["cores"] = 2
    ok, err = validate_server_dag(dag, algorithm_whitelist=_WHITELIST)
    assert ok is True


def test_validate_server_dag_structured_error_shape():
    from core.server_dag_validator import validate_server_dag
    ok, err = validate_server_dag(_load_fixture("invalid_cycle.json"), algorithm_whitelist=_WHITELIST)
    assert ok is False
    assert set(err.keys()) == {"code", "message", "retryable", "details"}
    assert isinstance(err["details"]["issues"], list)
