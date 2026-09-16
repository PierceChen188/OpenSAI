"""Nora 独立复核脚本（WP-08-G4）：不复用 Ethan 测试，独立核验退出码 6 / X11 交叉校准 / P3-a fixture 跳过 / X8 真实 socket E2E。
"""
import io
import json
import socket
import sys
import threading
import time
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


# ── 退出码 6（独立）───────────────────────────────────────────────────────

from opensai_client.cli import (
    _classify, emit_error, EXIT_AGENT_DENIED, EXIT_SERVER, EXIT_INTERNAL,
)
from opensai_client.errors import AgentControlError, RemoteError, AGENT_DENIAL_CODES

check("退出码6: AGENT_DENIAL_CODES 为 8 个", AGENT_DENIAL_CODES == {
    "TOOL_DENIED", "TOOL_NOT_ALLOWED", "MAX_DAG_NODES", "ARBITRARY_CODE",
    "DIRECT_ALLOCATION_FORBIDDEN", "RESOURCE_LIMIT", "MAX_WAIT_EXCEEDED", "RATE_LIMITED",
})

check("退出码6: AgentControlError(TOOL_DENIED) -> 6",
      _classify(AgentControlError("d", code="TOOL_DENIED"))[0] == EXIT_AGENT_DENIED)
check("退出码6: AgentControlError(RATE_LIMITED) -> 6",
      _classify(AgentControlError("r", code="RATE_LIMITED", retryable=True))[0] == EXIT_AGENT_DENIED)
check("退出码6: RemoteError 携带拒绝码 -> 6",
      _classify(RemoteError("a", code="ARBITRARY_CODE"))[0] == EXIT_AGENT_DENIED)
check("退出码6: RemoteError 非拒绝码 -> 4（不误升为 6）",
      _classify(RemoteError("s", code="INTERNAL_ERROR"))[0] == EXIT_SERVER)
check("退出码6: 未知异常 -> 10", _classify(RuntimeError("x"))[0] == EXIT_INTERNAL)

# 顺序：拒绝码检查必须先于 RemoteError 检查（源码级确认）
import inspect
src = inspect.getsource(_classify)
check("退出码6: 拒绝码判断位于 RemoteError 判断之前",
      src.index("AGENT_DENIAL_CODES") < src.index("isinstance(error, RemoteError)"))

# emit_error JSON 带 exit_code=6
buf = io.StringIO()
ec = emit_error(AgentControlError("denied", code="TOOL_DENIED"), stderr=buf)
payload = json.loads(buf.getvalue())
check("退出码6: emit_error 返回 6", ec == 6)
check("退出码6: JSON error.exit_code == 6", payload["error"]["exit_code"] == 6)
check("退出码6: JSON error.code == TOOL_DENIED", payload["error"]["code"] == "TOOL_DENIED")

# ── X11 交叉校准（独立，逐字段）────────────────────────────────────────────

from core.job_registry import JobRegistry
from opensai_contracts import JobStatus
from opensai_client.client import _parse_job_query

reg = JobRegistry()
rec, _ = reg.register("task1", dag_data={"nodes": [{"id": 1}]})
reg.transition(rec.job_id, JobStatus.RUNNING, started_at="2026-09-05T00:00:01Z",
               nodes=[{"node_id": 1, "status": "RUNNING"}], duration_s=1.5)
server_job = reg.to_job_result(rec.job_id)
wire = {"job": server_job.to_dict(), "request_id": "req-9"}

parsed = _parse_job_query(wire, rec.job_id)
check("X11: 顶层键为 job（非 dagResult）", "job" in wire and "dagResult" not in wire)
check("X11: job_id 一致", parsed.job_id == server_job.job_id)
check("X11: status 一致", str(parsed.status) == str(server_job.status))
check("X11: submitted_at 一致", parsed.submitted_at == server_job.submitted_at)
check("X11: started_at 一致", parsed.started_at == server_job.started_at)
check("X11: completed_at 一致", parsed.completed_at == server_job.completed_at)
check("X11: duration_s 一致", parsed.duration_s == server_job.duration_s)
check("X11: nodes 一致", parsed.nodes == server_job.nodes)
check("X11: error 一致", parsed.error == server_job.error)

# ── P3-a fixture 跳过（独立）───────────────────────────────────────────────

from tests.run_all import _requires_fixtures
import tests.test_schema as ts
check("P3-a: test_validate_via_json_file 判定为需 fixture",
      _requires_fixtures(ts.test_validate_via_json_file) is True)
check("P3-a: test_validate_valid_dag 判定为不需 fixture",
      _requires_fixtures(ts.test_validate_valid_dag) is False)

# ── X8 真实 socket E2E（Transport，独立）───────────────────────────────────

from opensai_client.transport import Transport

# 真实 TCP echo：dagData ACK 单帧往返
srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.bind(("127.0.0.1", 0))
srv.listen(1)
port = srv.getsockname()[1]


def echo_loop():
    conn, _ = srv.accept()
    buf = b""
    try:
        while True:
            chunk = conn.recv(65536)
            if not chunk:
                break
            buf += chunk
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                if not line.strip():
                    continue
                msg = json.loads(line.decode("utf-8"))
                if "user_beat" in msg:
                    continue
                rid = msg.get("request_id")
                conn.sendall((json.dumps({"dagRspStatus": True, "request_id": rid,
                                          "job_id": "job_N", "legacy_task_id": "t",
                                          "status": "QUEUED", "accepted_at": "2026-09-05T00:00:00Z"}) + "\n").encode())
    except OSError:
        pass
    finally:
        try:
            conn.close()
        except OSError:
            pass


threading.Thread(target=echo_loop, daemon=True).start()
t = Transport("127.0.0.1", port, timeout=5)
t.connect("nora-g4")
res = t.send_command("dagData", {"taskName": "x"}, timeout=5)
check("X8: 真实 socket dagData ACK 往返 dagRspStatus=True", res.get("dagRspStatus") is True, str(res))
check("X8: ACK 携带 job_id", res.get("job_id") == "job_N")
t.close()
srv.close()

# 乱序 request_id 分发（独立）
srv2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv2.bind(("127.0.0.1", 0))
srv2.listen(1)
port2 = srv2.getsockname()[1]


def out_of_order_loop():
    conn, _ = srv2.accept()
    buf = b""
    try:
        while True:
            chunk = conn.recv(65536)
            if not chunk:
                break
            buf += chunk
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                if not line.strip():
                    continue
                msg = json.loads(line.decode())
                if "user_beat" in msg:
                    continue
                rid = msg["request_id"]
                marker = msg.get("cmd", {}).get("marker")
                time.sleep(0.05 if marker == "first" else 0.0)
                conn.sendall((json.dumps({"ok": True, "request_id": rid, "marker": marker}) + "\n").encode())
    except OSError:
        pass


threading.Thread(target=out_of_order_loop, daemon=True).start()
t2 = Transport("127.0.0.1", port2, timeout=5)
t2.connect()
r_first = t2.send_command("cmd", {"marker": "first"}, timeout=5)
r_second = t2.send_command("cmd", {"marker": "second"}, timeout=5)
check("X8: 乱序 request_id 关联正确（first/second 各回其主）",
      r_first.get("marker") == "first" and r_second.get("marker") == "second")
t2.close()
srv2.close()

print("=" * 60)
print(f"独立复核：{len(PASS)} PASS, {len(FAIL)} FAIL")
for name, detail in FAIL:
    print(f"  FAIL {name} — {detail}")
print("=" * 60)
sys.exit(0 if not FAIL else 1)
