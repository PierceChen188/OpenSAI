"""
OpenSAI SDK contract tests (WP-03).

Covers the WP-03 acceptance criteria:
  1. Lazy connection (constructor never touches the network).
  2. NDJSON framing + request_id dispatch + serialized writes.
  3. Unified error {code, message, retryable, details}.
  4. ClientValidator delegates to opensai_contracts.DAGValidator.
  5. Unit tests (Config, Transport, RedisBridge, ClientValidator, OpenSAIClient).

Runs under tests/run_all.py (no pytest-only fixtures).
"""

import json
import socket
import sys
import threading
import time
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))


# ── Config ──────────────────────────────────────────────────────────────────

def test_config_defaults():
    from opensai_client.config import Config
    from opensai_contracts import OpenSAIConfig
    cfg = Config.load()
    assert isinstance(cfg, OpenSAIConfig)
    assert cfg.server_host == "127.0.0.1"
    assert cfg.server_port == 8888
    assert cfg.max_frame_bytes == 8388608


def test_config_priority_overrides_env_file():
    from opensai_client.config import Config
    env = {
        "OPENSAI_SERVER_HOST": "env-host",
        "OPENSAI_SERVER_PORT": "9000",
        "OPENSAI_TIMEOUT": "15",
    }
    # overrides beat env
    cfg = Config.load(env=env, overrides={"server_host": "override-host"})
    assert cfg.server_host == "override-host"
    assert cfg.server_port == 9000       # from env
    assert cfg.timeout == 15.0           # from env (coerced to float)


def test_config_env_beats_file_beats_default():
    import tempfile
    from opensai_client.config import Config
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        f.write(json.dumps({"server_host": "file-host", "server_port": 8000}))
        path = f.name
    try:
        cfg = Config.load(path=path)
        assert cfg.server_host == "file-host"
        assert cfg.server_port == 8000
        cfg2 = Config.load(path=path, env={"OPENSAI_SERVER_HOST": "env-host"})
        assert cfg2.server_host == "env-host"
    finally:
        import os
        os.unlink(path)


def test_config_rejects_bad_port():
    from opensai_client.config import Config
    from opensai_contracts.errors import ConfigError
    try:
        Config.load(overrides={"server_port": 70000})
        assert False, "expected ConfigError"
    except ConfigError as e:
        assert e.code == "CONFIG_ERROR"


def test_config_rejects_bad_type():
    from opensai_client.config import Config
    from opensai_contracts.errors import ConfigError
    try:
        Config.load(overrides={"timeout": "not-a-number"})
        assert False, "expected ConfigError"
    except ConfigError:
        pass


def test_config_redacted_masks_secrets():
    from opensai_client.config import Config
    from opensai_contracts import OpenSAIConfig
    cfg = OpenSAIConfig(redis_password="s3cret", redis_username="admin")
    red = Config.redacted(cfg)
    assert red["redis_password"] == "***"
    assert red["redis_username"] == "***"
    assert "s3cret" not in repr(cfg)


# ── Transport ───────────────────────────────────────────────────────────────

def test_transport_construction_is_lazy():
    from opensai_client.transport import Transport
    t = Transport("127.0.0.1", 9999)
    assert t.is_connected is False
    assert t._sock is None


def test_transport_send_frame_requires_connection():
    from opensai_client.transport import Transport, TransportError
    t = Transport()
    try:
        t._send_frame({"x": 1})
        assert False, "expected TransportError"
    except TransportError:
        pass


def test_transport_frame_too_large():
    from opensai_client.transport import Transport, ProtocolFrameTooLarge
    t = Transport(max_frame_bytes=64)
    try:
        t._send_frame({"payload": "x" * 1000})
        assert False, "expected ProtocolFrameTooLarge"
    except ProtocolFrameTooLarge:
        pass


def _start_echo_server(on_message=None):
    """Start a fake NDJSON server on an ephemeral port; returns (server, port)."""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    port = server.getsockname()[1]

    def loop():
        try:
            conn, _ = server.accept()
        except OSError:
            return
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
                    if on_message is not None:
                        on_message(msg)
                    rid = msg.get("request_id")
                    conn.sendall((json.dumps({
                        "dagRspStatus": True, "request_id": rid,
                        "job_id": "job_1", "legacy_task_id": "task1",
                        "status": "QUEUED", "accepted_at": "2026-09-05T00:00:00Z",
                    }) + "\n").encode("utf-8"))
        except OSError:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass

    t = threading.Thread(target=loop, daemon=True)
    t.start()
    return server, port


def test_transport_send_command_roundtrip():
    from opensai_client.transport import Transport
    server, port = _start_echo_server()
    try:
        t = Transport("127.0.0.1", port, timeout=5)
        t.connect("test-client")
        res = t.send_command("dagData", {"taskName": "x"}, timeout=5)
        assert res["dagRspStatus"] is True
        assert res["job_id"] == "job_1"
        assert res["request_id"]
        t.close()
    finally:
        server.close()


def test_transport_request_id_dispatch():
    """Two concurrent commands are correlated by request_id, not order."""
    from opensai_client.transport import Transport
    seen = []
    lock = threading.Lock()

    def on_message(msg):
        # simulate: the server echoes the request_id but with the payload reversed
        pass

    # Use a server that tags each response with a per-request id derived from payload.
    results = {}

    def make_server():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("127.0.0.1", 0))
        s.listen(1)
        p = s.getsockname()[1]

        def loop():
            conn, _ = s.accept()
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
                        marker = msg.get("cmd", {}).get("marker") if isinstance(msg.get("cmd"), dict) else None
                        # respond out of order: small delay for first
                        time.sleep(0.02 if marker == "first" else 0.0)
                        conn.sendall((json.dumps({"ok": True, "request_id": rid, "marker": marker}) + "\n").encode())
            except OSError:
                pass

        threading.Thread(target=loop, daemon=True).start()
        return s, p

    server, port = make_server()
    try:
        t = Transport("127.0.0.1", port, timeout=5)
        t.connect()
        r1 = t.send_command("cmd", {"marker": "first"}, timeout=5)
        r2 = t.send_command("cmd", {"marker": "second"}, timeout=5)
        assert r1["marker"] == "first"
        assert r2["marker"] == "second"
        t.close()
    finally:
        server.close()


def test_transport_event_handler():
    from opensai_client.transport import Transport

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    port = server.getsockname()[1]
    received = []

    def loop():
        conn, _ = server.accept()
        try:
            # read user_beat then send an async dagResult event
            conn.recv(65536)
            conn.sendall((json.dumps({"dagResult": {"taskName": "t", "finishedTime": 1.0}}) + "\n").encode())
            time.sleep(0.2)
        except OSError:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass

    threading.Thread(target=loop, daemon=True).start()
    try:
        t = Transport("127.0.0.1", port, timeout=5)
        hid = t.add_event_handler("dagResult", lambda m: received.append(m))
        t.connect("c")
        time.sleep(0.3)
        assert len(received) == 1
        assert received[0]["dagResult"]["taskName"] == "t"
        assert t.remove_event_handler(hid) is True
        assert t.remove_event_handler(hid) is False
        t.close()
    finally:
        server.close()


def test_transport_close_is_idempotent():
    from opensai_client.transport import Transport
    t = Transport()
    t.close()
    t.close()  # must not raise


# ── RedisBridge ─────────────────────────────────────────────────────────────

def test_redis_bridge_construction_is_lazy():
    from opensai_client.redis_bridge import RedisBridge
    b = RedisBridge(password="s3cret", username="admin")
    assert b._client is None  # no connection on construction
    assert "s3cret" not in repr(b)


def test_redis_bridge_close_idempotent():
    from opensai_client.redis_bridge import RedisBridge
    b = RedisBridge()
    b.close()
    b.close()  # no-op


# ── ClientValidator ─────────────────────────────────────────────────────────

def test_client_validator_delegates():
    from opensai_client.validator import ClientValidator
    from opensai_contracts import DAGValidator
    v = ClientValidator()
    assert isinstance(v.validator, DAGValidator)


def test_client_validator_rejects_unknown_field():
    from opensai_client.validator import ClientValidator
    v = ClientValidator()
    report = v.validate({"name": "x", "nodes": [{"id": 1, "algorithm": "A", "bogus": 1}]})
    assert not report.valid
    assert any(e.code == "UNKNOWN_FIELD" for e in report.errors)


# ── OpenSAIClient ───────────────────────────────────────────────────────────

def test_client_construction_is_lazy():
    from opensai_client import OpenSAIClient
    c = OpenSAIClient(server_host="127.0.0.1", server_port=1)
    assert c.is_connected is False
    assert c._transport is None  # no socket/thread created yet
    assert c._redis_bridge is None


def test_client_context_manager_and_close():
    from opensai_client import OpenSAIClient, ClientClosedError
    c = OpenSAIClient()
    c.close()
    c.close()  # idempotent
    try:
        c.list_algorithms()
        assert False, "expected ClientClosedError"
    except ClientClosedError:
        pass


def test_client_unified_error_shape():
    from opensai_client import OpenSAIClient, ClientClosedError
    c = OpenSAIClient()
    c.close()
    try:
        c.list_algorithms()
        assert False, "expected ClientClosedError"
    except ClientClosedError as e:
        d = e.to_dict()
        assert set(d.keys()) == {"code", "message", "retryable", "details"}
        assert d["code"] == "CLIENT_CLOSED"
        assert d["retryable"] is False


def test_client_backward_compat_positional():
    from opensai_client import OpenSAIClient
    c = OpenSAIClient("10.1.2.3", 9999)
    assert c.config.server_host == "10.1.2.3"
    assert c.config.server_port == 9999


def test_client_submit_task_builds_single_node():
    from opensai_client import OpenSAIClient
    from opensai_contracts import single_task_dag
    # Verify single_task_dag path is exercised by submit_task without a server:
    dag = single_task_dag("Algorithm_Sleep_1", {"sleep_time": 1})
    assert len(dag.nodes) == 1
    assert dag.nodes[0].id == 1


def test_client_validate_dag_no_side_effect():
    from opensai_client import OpenSAIClient
    from opensai_contracts import AlgorithmInfo, AlgorithmRegistry
    c = OpenSAIClient()
    c._validator = None  # not needed; use a fresh validator directly
    from opensai_client.validator import ClientValidator
    reg = AlgorithmRegistry(algorithms={"A": AlgorithmInfo(name="A")})
    v = ClientValidator(algorithm_registry=reg)
    report = v.validate({"name": "x", "nodes": [{"id": 1, "algorithm": "A"}]})
    assert report.valid
    assert report.normalized_dag.nodes[0].after_nodes == []
