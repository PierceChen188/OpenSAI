"""Nora 独立复核脚本（WP-04-TEST）：不依赖 Leo 测试，直接对 CLI/AgentControl/工具注册做独立断言。

核验验收条件 #1（稳定退出码 + JSON 输出）、#2（AgentControl 三层拦截之 AgentControl 层与客户端层）、
#3（register_tools 唯一入口 + 8 工具 + 兼容别名）。
"""
import io
import json
import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))

PASS = []
FAIL = []


def check(name, cond, detail=""):
    if cond:
        PASS.append(name)
    else:
        FAIL.append((name, detail))


# ── #1 稳定退出码 + JSON 输出 ────────────────────────────────────────────

from opensai_client.cli import (
    EXIT_SUCCESS, EXIT_USAGE, EXIT_NETWORK, EXIT_SERVER,
    EXIT_TIMEOUT, EXIT_AGENT_DENIED, EXIT_INTERNAL, EXIT_INTERRUPT,
    emit_output, emit_error, _classify,
)

# 八个退出码常量定义且数值正确（§2.2.5）
expected = {0, 2, 3, 4, 5, 6, 10, 130}
actual = {EXIT_SUCCESS, EXIT_USAGE, EXIT_NETWORK, EXIT_SERVER, EXIT_TIMEOUT,
          EXIT_AGENT_DENIED, EXIT_INTERNAL, EXIT_INTERRUPT}
check("#1: 八个退出码常量齐备且数值正确", actual == expected, str(actual))

from opensai_contracts.errors import ConfigError, ContractValidationError
from opensai_client.errors import TransportError, TransportTimeoutError, RemoteError, ClientClosedError

# _classify 映射到稳定退出码
check("#1: ConfigError -> 2", _classify(ConfigError("x"))[0] == EXIT_USAGE)
check("#1: ContractValidationError -> 2", _classify(ContractValidationError("x"))[0] == EXIT_USAGE)
check("#1: ValueError -> 2", _classify(ValueError("x"))[0] == EXIT_USAGE)
check("#1: WAIT_TIMEOUT -> 5", _classify(TransportTimeoutError("w", code="WAIT_TIMEOUT"))[0] == EXIT_TIMEOUT)
check("#1: RemoteError -> 4", _classify(RemoteError("x"))[0] == EXIT_SERVER)
check("#1: ClientClosedError -> 10", _classify(ClientClosedError("x"))[0] == EXIT_INTERNAL)
check("#1: TransportError -> 3", _classify(TransportError("x"))[0] == EXIT_NETWORK)
check("#1: KeyboardInterrupt -> 130", _classify(KeyboardInterrupt())[0] == EXIT_INTERRUPT)
check("#1: 未知异常 -> 10", _classify(RuntimeError("x"))[0] == EXIT_INTERNAL)

# stdout 单 JSON
buf = io.StringIO()
emit_output({"algorithms": ["A", "B"]}, stdout=buf)
out_lines = [l for l in buf.getvalue().splitlines() if l.strip()]
check("#1: stdout 单 JSON 行", len(out_lines) == 1 and json.loads(out_lines[0]) == {"algorithms": ["A", "B"]})

# stderr 错误 JSON 带 request_id + exit_code
ebuf = io.StringIO()
ecode = emit_error(ConfigError("bad cfg"), stderr=ebuf)
epayload = json.loads(ebuf.getvalue().splitlines()[-1])
check("#1: stderr 错误 JSON 带 request_id", bool(epayload["error"].get("request_id")))
check("#1: stderr 错误 JSON 带 exit_code", epayload["error"].get("exit_code") == 2, str(epayload))
check("#1: 错误 JSON code 字段", epayload["error"]["code"] == "CONFIG_ERROR")

# ── #2 AgentControl 层拦截 ────────────────────────────────────────────────

from skills.opensai.agent_control import OpenSAIAgentControl, AgentControl, AgentControlPolicy, AgentContext

check("#3: AgentControl 是 OpenSAIAgentControl 别名", AgentControl is OpenSAIAgentControl)

# 默认放行
check("#2: 默认放行", OpenSAIAgentControl().check_policy("t", {}, AgentContext(actor="a")).allowed)

# 七类拒绝码逐一独立核验
ac = OpenSAIAgentControl(policy=AgentControlPolicy(denied_tools=("d",)))
check("#2: TOOL_DENIED", ac.check_policy("d", {}, AgentContext(actor="a")).code == "TOOL_DENIED")

ac = OpenSAIAgentControl(policy=AgentControlPolicy(allowed_tools=("ok",)))
check("#2: TOOL_NOT_ALLOWED", ac.check_policy("other", {}, AgentContext(actor="a")).code == "TOOL_NOT_ALLOWED")

ac = OpenSAIAgentControl(policy=AgentControlPolicy(max_dag_nodes=2))
dag = {"nodes": [{"id": 1}, {"id": 2}, {"id": 3}]}
check("#2: MAX_DAG_NODES", ac.check_policy("opensai_submit_dag", {"dag_spec": dag}, AgentContext(actor="a")).code == "MAX_DAG_NODES")

ac = OpenSAIAgentControl()
dag = {"nodes": [{"id": 1, "code": "os.system('x')"}]}
check("#2: ARBITRARY_CODE", ac.check_policy("opensai_submit_dag", {"dag_spec": dag}, AgentContext(actor="a")).code == "ARBITRARY_CODE")

ac = OpenSAIAgentControl(policy=AgentControlPolicy(allow_direct_allocation=False))
dag = {"nodes": [{"id": 1, "direct_allocation_id": 3}]}
check("#2: DIRECT_ALLOCATION_FORBIDDEN", ac.check_policy("opensai_submit_dag", {"dag_spec": dag}, AgentContext(actor="a")).code == "DIRECT_ALLOCATION_FORBIDDEN")

ac = OpenSAIAgentControl(policy=AgentControlPolicy(resource_limits={"cores_max": 8}))
dag = {"nodes": [{"id": 1, "resources": {"cores": 16}}]}
check("#2: RESOURCE_LIMIT", ac.check_policy("opensai_submit_dag", {"dag_spec": dag}, AgentContext(actor="a")).code == "RESOURCE_LIMIT")

ac = OpenSAIAgentControl(policy=AgentControlPolicy(max_wait_s=30))
check("#2: MAX_WAIT_EXCEEDED", ac.check_policy("opensai_wait_result", {"job_id": "j", "timeout": 120}, AgentContext(actor="a")).code == "MAX_WAIT_EXCEEDED")

ac = OpenSAIAgentControl(policy=AgentControlPolicy(rate_limits={"t": {"max_per_second": 1}}))
ac.bind_handlers({"t": lambda **kw: "ok"})
r1 = ac.invoke("t", {}, AgentContext(actor="a"))
r2 = ac.invoke("t", {}, AgentContext(actor="a"))
check("#2: RATE_LIMITED（invoke 第二次触发限频）", r1.ok and (not r2.ok) and r2.error["code"] == "RATE_LIMITED")

# 审计事件：拒绝/成功均记录，且脱敏
events = []
ac = OpenSAIAgentControl(policy=AgentControlPolicy(audit_redact_paths=("token",)), audit_sink=events.append)
ac.bind_handlers({"e": lambda **kw: "ok"})
ac.invoke("e", {"token": "s3cret", "x": 1}, AgentContext(actor="alice", request_id="r1"))
check("#2: 审计记录脱敏（secret 不入 digest）", events and "s3cret" not in events[0].redacted_digest)
check("#2: 审计记录 actor/tool/request_id 齐全", events[0].actor == "alice" and events[0].tool == "e" and events[0].request_id == "r1")

# ── #2 客户端层拦截（ClientValidator -> DAGValidator 拒未注册算法）───────

from opensai_client.validator import ClientValidator
from opensai_contracts import AlgorithmInfo, AlgorithmRegistry

reg = AlgorithmRegistry(algorithms={"Only_This": AlgorithmInfo(name="Only_This")})
v = ClientValidator(algorithm_registry=reg)
report = v.validate({"name": "x", "nodes": [{"id": 1, "algorithm": "Unregistered_Algo"}]})
check("#2: 客户端层拒未注册算法（ALGORITHM_NOT_REGISTERED）",
      not report.valid and any(e.code == "ALGORITHM_NOT_REGISTERED" for e in report.errors),
      str([e.code for e in report.errors]))

# ── #3 register_tools 唯一入口 + 8 工具 ────────────────────────────────────

from skills.opensai.tools import register_tools, shutdown, ToolRegistry, TOOL_DEFINITIONS

EXPECTED_TOOLS = [
    "opensai_list_algorithms", "opensai_list_devices", "opensai_cluster_status",
    "opensai_validate_dag", "opensai_submit_dag", "opensai_submit_task",
    "opensai_get_result", "opensai_wait_result",
]
check("#3: 8 个 opensai_* 工具定义", [t["name"] for t in TOOL_DEFINITIONS] == EXPECTED_TOOLS)

class FakeClient:
    def list_algorithms(self, refresh=False):
        return ["A", "B"]

reg2 = ToolRegistry()
control = OpenSAIAgentControl()
names = register_tools(registry=reg2, client_factory=FakeClient, control=control)
check("#3: register_tools 返回 8 个工具名", names == EXPECTED_TOOLS)
check("#3: ToolRegistry 注册 8 个", sorted(reg2.names()) == sorted(EXPECTED_TOOLS))
resp = control.invoke("opensai_list_algorithms", {}, AgentContext(actor="a"))
check("#3: 经 AgentControl 调用工具成功", resp.ok and resp.result == ["A", "B"])
shutdown()

print("=" * 60)
print(f"独立复核：{len(PASS)} PASS, {len(FAIL)} FAIL")
for name, detail in FAIL:
    print(f"  FAIL {name} — {detail}")
print("=" * 60)
sys.exit(0 if not FAIL else 1)
