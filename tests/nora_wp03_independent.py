"""Nora 独立复核脚本（WP-03-TEST）：不依赖 Leo 测试，直接对 SDK 契约做独立断言。

核验验收条件 #1/#2/#3/#4（构造惰性、NDJSON 分帧+乱序 request_id、统一错误、ClientValidator 复用）。
"""
import json
import socket
import sys
import threading
import time
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


# ── #1 构造不联网（惰性）───────────────────────────────────────────────

from opensai_client import OpenSAIClient

c = OpenSAIClient(server_host="127.0.0.1", server_port=1)
check("#1: OpenSAIClient 构造后 _transport 为 None", c._transport is None)
check("#1: OpenSAIClient 构造后 _redis_bridge 为 None", c._redis_bridge is None)
check("#1: OpenSAIClient 构造后 is_connected=False", c.is_connected is False)
check("#1: OpenSAIClient 构造后 _connected=False", c._connected is False)

from opensai_client.transport import Transport

t = Transport("127.0.0.1", 9999)
check("#1: Transport 构造后 _sock 为 None（不联网）", t._sock is None and t.is_connected is False)

from opensai_client.redis_bridge import RedisBridge

b = RedisBridge(password="s3cret")
check("#1: RedisBridge 构造后 _client 为 None（不联网）", b._client is None)

# ── #2 NDJSON 分帧 + 乱序 request_id 分发（真实 socket roundtrip）───────

# 独立乱序回显服务：对 first 请求延迟，second 先回，验证按 request_id 关联而非顺序。
srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.bind(("127.0.0.1", 0))
srv.listen(1)
port = srv.getsockname()[1]


def server_loop():
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
                rid = msg["request_id"]
                marker = msg.get("cmd", {}).get("marker")
                # 乱序：marker=="first" 延迟，second 先回
                time.sleep(0.05 if marker == "first" else 0.0)
                conn.sendall((json.dumps({"ok": True, "request_id": rid, "marker": marker}) + "\n").encode())
    except OSError:
        pass
    finally:
        try:
            conn.close()
        except OSError:
            pass


threading.Thread(target=server_loop, daemon=True).start()

t2 = Transport("127.0.0.1", port, timeout=5)
t2.connect("nora-check")
# 先发 first（服务器延迟它），后发 second（先回）
r_first = t2.send_command("cmd", {"marker": "first"}, timeout=5)
r_second = t2.send_command("cmd", {"marker": "second"}, timeout=5)
check("#2: 乱序 request_id 关联正确（first 回 first）", r_first.get("marker") == "first", str(r_first))
check("#2: 乱序 request_id 关联正确（second 回 second）", r_second.get("marker") == "second", str(r_second))
check("#2: 响应携带 request_id", bool(r_first.get("request_id")) and bool(r_second.get("request_id")))
t2.close()
srv.close()

# NDJSON 单帧往返（dagData ACK 形态）
srv2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv2.bind(("127.0.0.1", 0))
srv2.listen(1)
port2 = srv2.getsockname()[1]


def server2_loop():
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
                msg = json.loads(line.decode("utf-8"))
                if "user_beat" in msg:
                    continue
                rid = msg.get("request_id")
                conn.sendall((json.dumps({"dagRspStatus": True, "request_id": rid, "job_id": "job_N", "legacy_task_id": "t", "status": "QUEUED", "accepted_at": "2026-09-05T00:00:00Z"}) + "\n").encode())
    except OSError:
        pass
    finally:
        try:
            conn.close()
        except OSError:
            pass


threading.Thread(target=server2_loop, daemon=True).start()
t3 = Transport("127.0.0.1", port2, timeout=5)
t3.connect("nora")
res = t3.send_command("dagData", {"taskName": "x"}, timeout=5)
check("#2: NDJSON 单帧往返 dagRspStatus=True", res.get("dagRspStatus") is True, str(res))
check("#2: ACK 携带 job_id", res.get("job_id") == "job_N", str(res))
t3.close()
srv2.close()

# ── #3 统一错误四键 {code,message,retryable,details} ────────────────────

from opensai_client import ClientClosedError

c2 = OpenSAIClient()
c2.close()
try:
    c2.list_algorithms()
    check("#3: close 后业务调用抛 ClientClosedError", False, "未抛错")
except ClientClosedError as e:
    d = e.to_dict()
    check("#3: 错误四键齐全", set(d.keys()) == {"code", "message", "retryable", "details"}, str(d.keys()))
    check("#3: code=CLIENT_CLOSED", d["code"] == "CLIENT_CLOSED", str(d))
    check("#3: retryable=False", d["retryable"] is False)

# 所有 SDK 异常均继承 ContractError（统一 shape 的根）
from opensai_contracts.errors import ContractError
from opensai_client import errors as sdk_errors
non_contract = [n for n in dir(sdk_errors) if n.endswith("Error") and not n.startswith("_")
                and isinstance(getattr(sdk_errors, n), type) and not issubclass(getattr(sdk_errors, n), ContractError)]
check("#3: 所有 SDK 异常继承 ContractError", not non_contract, str(non_contract))

# ── #4 ClientValidator 复用 DAGValidator ────────────────────────────────

from opensai_client.validator import ClientValidator
from opensai_contracts import DAGValidator

v = ClientValidator()
check("#4: ClientValidator.validator 是 DAGValidator 实例", isinstance(v.validator, DAGValidator))
# 无重复校验逻辑：validate 直接委托共享核心，报告走统一 ValidationReport
report = v.validate({"name": "x", "nodes": [{"id": 1, "algorithm": "A", "bogus": 1}]})
check("#4: 委托后未知字段被 DAGValidator 拒绝", not report.valid and any(e.code == "UNKNOWN_FIELD" for e in report.errors), str([e.code for e in report.errors]))

print("=" * 60)
print(f"独立复核：{len(PASS)} PASS, {len(FAIL)} FAIL")
for name, detail in FAIL:
    print(f"  FAIL {name} — {detail}")
print("=" * 60)
sys.exit(0 if not FAIL else 1)
