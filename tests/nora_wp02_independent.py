"""Nora 独立复核脚本（WP-02-TEST）：不依赖 Ethan 测试，直接对 JobRegistry / 校验钩子 / server.py 接线做独立断言。

核验验收：dagData 向后兼容、job_id 回执、幂等、终态不可覆盖、服务端校验钩子、jobQuery 映射。
"""
import ast
import json
import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))
sys.path.insert(0, str(root / "Offloading-System"))
sys.path.insert(0, str(root / "Offloading-System" / "dag_offload"))

PASS = []
FAIL = []


def check(name, cond, detail=""):
    if cond:
        PASS.append(name)
    else:
        FAIL.append((name, detail))


# ── ULID / digest / legacy ──────────────────────────────────────────────

from core.job_registry import new_ulid, compute_dag_digest, derive_legacy_task_id

_CROCKFORD = set("0123456789ABCDEFGHJKMNPQRSTVWXYZ")
u = new_ulid()
check("ULID: 长度 26", len(u) == 26)
check("ULID: Crockford 字符集", all(c in _CROCKFORD for c in u))
check("ULID: 首字符 ≤ '7'（128bit 最高 2bit=0）", u[0] in "01234567")
check("ULID: 唯一性（2000 个不重复）", len({new_ulid() for _ in range(2000)}) == 2000)

check("digest: 键序无关", compute_dag_digest({"b": 1, "a": 2}) == compute_dag_digest({"a": 2, "b": 1}))
check("digest: 内容敏感", compute_dag_digest({"a": 1}) != compute_dag_digest({"a": 2}))
check("digest: 64 位 hex", len(compute_dag_digest({"a": 1})) == 64)

check("legacy: float 时间戳", derive_legacy_task_id({"dagData": {"taskName": "pi", "taskCreateTime": 1700000000.0}}) == "pi1700000000.0")
check("legacy: int 时间戳", derive_legacy_task_id({"dagData": {"taskName": "pi", "taskCreateTime": 1700000000}}) == "pi1700000000")

# ── JobRegistry：注册 / ACK / 幂等 / 终态 / jobQuery ─────────────────────

from core.job_registry import JobRegistry
from opensai_contracts import JobStatus
from opensai_contracts.errors import (
    JobNotFoundError, IdempotencyConflictError, TerminalStateError,
)

reg = JobRegistry()
rec, created = reg.register("task1", dag_data={"nodes": [{"id": 1}]}, owner="u1")
check("register: 新建 created=True", created is True)
check("register: job_id 前缀 job_", rec.job_id.startswith("job_"))
check("register: 初始状态 QUEUED", rec.status == JobStatus.QUEUED)
check("register: 有 dag_digest + submitted_at", bool(rec.dag_digest) and bool(rec.submitted_at))

# ACK 向后兼容 + job_id 回执
ack = reg.ack_payload(rec, request_id="req-1")
check("ACK: 向后兼容 dagRspStatus=True", ack["dagRspStatus"] is True)
check("ACK: job_id 回执", ack["job_id"] == rec.job_id)
check("ACK: legacy_task_id 回执", ack["legacy_task_id"] == "task1")
check("ACK: status=QUEUED", ack["status"] == "QUEUED")
check("ACK: accepted_at", ack["accepted_at"] == rec.submitted_at)
check("ACK: request_id 回显", ack["request_id"] == "req-1")
check("ACK: 无 request_id 时不含该键", "request_id" not in reg.ack_payload(rec))

# 幂等
r1, c1 = reg.register("t", dag_data={"nodes": [{"id": 1}]}, owner="u", idempotency_key="k")
r2, c2 = reg.register("t", dag_data={"nodes": [{"id": 1}]}, owner="u", idempotency_key="k")
check("幂等: 同 key 同 payload -> 同 job_id 且 created=False", c1 is True and c2 is False and r1.job_id == r2.job_id)
try:
    reg.register("t", dag_data={"nodes": [{"id": 2}]}, owner="u", idempotency_key="k")
    check("幂等: 同 key 异 payload -> 冲突", False, "未抛错")
except IdempotencyConflictError as e:
    check("幂等: 同 key 异 payload -> 冲突", e.code == "IDEMPOTENCY_CONFLICT")
# 幂等域按 owner 隔离
ra, _ = reg.register("t", dag_data={"nodes": [{"id": 1}]}, owner="alice", idempotency_key="k2")
rb, cb = reg.register("t", dag_data={"nodes": [{"id": 1}]}, owner="bob", idempotency_key="k2")
check("幂等: owner 隔离（不同 owner 同 key 新建）", cb is True and ra.job_id != rb.job_id)

# 终态不可覆盖
rt, _ = reg.register("term", dag_data={"nodes": [{"id": 1}]})
reg.transition(rt.job_id, JobStatus.SUCCEEDED)
check("终态: SUCCEEDED 生效", reg.get(rt.job_id).status == JobStatus.SUCCEEDED)
for target in (JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.RUNNING, JobStatus.QUEUED):
    try:
        reg.transition(rt.job_id, target)
        check(f"终态: SUCCEEDED -> {target} 拒绝", False, "未抛错")
    except TerminalStateError as e:
        check(f"终态: SUCCEEDED -> {target} 拒绝", e.code == "TERMINAL_STATE_IMMUTABLE")
# 同终态幂等重复允许
reg.transition(rt.job_id, JobStatus.SUCCEEDED)
check("终态: 同终态幂等重复允许", reg.get(rt.job_id).status == JobStatus.SUCCEEDED)

# jobQuery 映射（不暴露内部记录）
jr = reg.to_job_result(rt.job_id)
check("jobQuery: JobResult 映射 job_id", jr.job_id == rt.job_id)
check("jobQuery: 状态透传", jr.status == JobStatus.SUCCEEDED)

# resolve / get_by_legacy / 未注册
check("resolve: job_id 解析", reg.resolve(job_id=rt.job_id).job_id == rt.job_id)
check("resolve: legacy 解析", reg.resolve(legacy_task_id="term").job_id == rt.job_id)
try:
    reg.get("job_unknown")
    check("get: 未注册 -> JobNotFoundError", False, "未抛错")
except JobNotFoundError as e:
    check("get: 未注册 -> JobNotFoundError", e.code == "JOB_NOT_FOUND")

# ── 服务端校验钩子 ──────────────────────────────────────────────────────

from core.server_dag_validator import validate_server_dag

_FIX = root / "opensai_contracts" / "fixtures"
def load_fixture(name):
    return json.loads((_FIX / name).read_text(encoding="utf-8"))

WHITELIST = {"Algorithm_Sleep_1", "Algorithm_Pi_Calculation"}

ok, err = validate_server_dag(load_fixture("valid_wire_dag.json"), algorithm_whitelist=WHITELIST)
check("校验钩子: 合法 wire 通过", ok is True and err is None)

ok, err = validate_server_dag(load_fixture("valid_wire_dag.json"), algorithm_whitelist={"Algorithm_MatMul"})
check("校验钩子: 未注册算法拒绝", ok is False and err["code"] == "ALGORITHM_NOT_REGISTERED", str(err))

ok, err = validate_server_dag(load_fixture("invalid_cycle.json"), algorithm_whitelist=WHITELIST)
check("校验钩子: 环路拒绝 CYCLE_DETECTED", ok is False and err["code"] == "CYCLE_DETECTED")

ok, err = validate_server_dag(load_fixture("invalid_unknown_field.json"), algorithm_whitelist=WHITELIST)
check("校验钩子: 未知字段拒绝 UNKNOWN_FIELD", ok is False and err["code"] == "UNKNOWN_FIELD")

# 结构化错误 shape
check("校验钩子: 错误四键", set(err.keys()) == {"code", "message", "retryable", "details"})
check("校验钩子: retryable=False", err["retryable"] is False)
check("校验钩子: details.issues 列表", isinstance(err["details"]["issues"], list))

# 放置目标校验（directAllocationId=0 非法，X13 相关）
def wire_dag():
    return {
        "dagData": {
            "taskName": "sleep_job", "taskCreateTime": 1700000000.0, "taskPolicy": "smart",
            "nodes": [{"id": 1, "name": "s", "form": {
                "beforeNodes": [], "afterNodes": [], "taskAlgorithm": "Algorithm_Sleep_1",
                "taskVersion": "1.0", "taskInputInfo": {"sleep_time": 1}, "details": "",
                "directAllocationId": -1, "deviceParam": {}},
                "result": {"taskResultInfo": "", "status": 1, "delay": 0, "deviceId": 0}}],
        }
    }

dag = wire_dag()
dag["dagData"]["nodes"][0]["form"]["directAllocationId"] = 0
ok, err = validate_server_dag(dag, algorithm_whitelist=WHITELIST)
check("校验钩子: directAllocationId=0 拒绝（placement）", ok is False and err["code"] == "INVALID_DIRECT_ALLOCATION", str(err))

# ── server.py 接线（AST 结构 + ast.parse 语法）──────────────────────────

_SERVER = root / "Offloading-System" / "dag_offload" / "server.py"
src = _SERVER.read_text(encoding="utf-8")
try:
    ast.parse(src)
    check("server.py: ast.parse 语法合法", True)
except SyntaxError as e:
    check("server.py: ast.parse 语法合法", False, str(e))

check("server.py: import JobRegistry", "from core.job_registry import JobRegistry" in src)
check("server.py: import validate_server_dag", "from core.server_dag_validator import validate_server_dag" in src)
check("server.py: job_registry 类属性", "job_registry = JobRegistry()" in src)
check("server.py: dagData 分支接入校验钩子", "validate_server_dag(" in src)
check("server.py: 注册（幂等）", ".register(" in src and "idempotency_key" in src)
check("server.py: ACK 回执", "ack_payload" in src)
check("server.py: jobQuery 分支", "'jobQuery' in data_decode" in src)
check("server.py: to_job_result 映射", "to_job_result" in src)
check("server.py: _mark_job_terminal 定义", "def _mark_job_terminal" in src)
check("server.py: transition_by_legacy 终态标记", "transition_by_legacy" in src)
# 三处终态标记（早退/丢弃/全完成）
import re
mark_calls = re.findall(r"_mark_job_terminal\(task_id,\s*[\"'](SUCCEEDED|FAILED)[\"']\)", src)
check("server.py: 终态标记覆盖 SUCCEEDED + FAILED", set(mark_calls) >= {"SUCCEEDED", "FAILED"}, str(mark_calls))

print("=" * 60)
print(f"独立复核：{len(PASS)} PASS, {len(FAIL)} FAIL")
for name, detail in FAIL:
    print(f"  FAIL {name} — {detail}")
print("=" * 60)
sys.exit(0 if not FAIL else 1)
