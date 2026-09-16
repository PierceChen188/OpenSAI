"""
OpenSAI CLI contract tests (WP-04, §2.2.3 + §2.2.5).

Covers the CLI public functions (load_dag / parse_params / emit_output /
emit_error) and stable exit codes, plus the machine-readable JSON output shape.
"""

import io
import json
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))


def test_load_dag_from_stdin():
    from opensai_client.cli import load_dag
    from opensai_contracts import DAGSpec
    dag = load_dag(stdin=io.StringIO(json.dumps({
        "name": "t", "nodes": [{"id": 1, "algorithm": "Algorithm_Pi_Calculation"}]})))
    assert isinstance(dag, DAGSpec)
    assert len(dag.nodes) == 1


def test_load_dag_rejects_non_object():
    from opensai_client.cli import load_dag
    try:
        load_dag(stdin=io.StringIO("[1, 2, 3]"))
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_load_dag_missing_input():
    from opensai_client.cli import load_dag
    try:
        load_dag(stdin=io.StringIO(""))
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_parse_params_inline():
    from opensai_client.cli import parse_params
    assert parse_params('{"count_num": 100}') == {"count_num": 100}


def test_parse_params_rejects_array():
    from opensai_client.cli import parse_params
    try:
        parse_params("[1, 2, 3]")
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_parse_params_at_file(tmpdir=None):
    import tempfile
    from opensai_client.cli import parse_params
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        f.write('{"a": 1}')
        path = f.name
    try:
        assert parse_params(f"@{path}") == {"a": 1}
    finally:
        import os
        os.unlink(path)


def test_emit_output_json():
    from opensai_client.cli import emit_output
    buf = io.StringIO()
    emit_output({"algorithms": ["A", "B"]}, stdout=buf)
    assert json.loads(buf.getvalue()) == {"algorithms": ["A", "B"]}


def test_emit_error_exit_codes():
    from opensai_client.cli import emit_error, EXIT_USAGE, EXIT_NETWORK, EXIT_SERVER, EXIT_TIMEOUT
    from opensai_contracts.errors import ConfigError
    from opensai_client.errors import TransportError, RemoteError, TimeoutError

    buf = io.StringIO()
    assert emit_error(ConfigError("bad config"), stderr=buf) == EXIT_USAGE
    assert emit_error(TransportError("conn refused"), stderr=buf) == EXIT_NETWORK
    assert emit_error(RemoteError("rejected"), stderr=buf) == EXIT_SERVER
    assert emit_error(TimeoutError("wait", code="WAIT_TIMEOUT"), stderr=buf) == EXIT_TIMEOUT


def test_emit_error_agent_denied_exit_6():
    """§2.2.5: Agent permission / rate-limit rejection maps to exit code 6."""
    from opensai_client.cli import emit_error, EXIT_AGENT_DENIED, EXIT_SERVER
    from opensai_client.errors import AgentControlError, RemoteError

    buf = io.StringIO()
    # policy / permission denial
    assert emit_error(AgentControlError("tool denied", code="TOOL_DENIED"),
                      stderr=buf) == EXIT_AGENT_DENIED
    # rate-limit denial (retryable)
    assert emit_error(AgentControlError("rate limited", code="RATE_LIMITED",
                                        retryable=True, retry_after_s=1.0),
                      stderr=buf) == EXIT_AGENT_DENIED
    # any denial code carried by a generic remote error still maps to 6
    assert emit_error(RemoteError("arbitrary code", code="ARBITRARY_CODE"),
                      stderr=buf) == EXIT_AGENT_DENIED
    # regression: non-denial server rejection still maps to 4
    assert emit_error(RemoteError("server rejected", code="INTERNAL_ERROR"),
                      stderr=buf) == EXIT_SERVER


def test_emit_error_agent_denied_json_shape():
    from opensai_client.cli import emit_error
    from opensai_client.errors import AgentControlError

    buf = io.StringIO()
    code = emit_error(AgentControlError("denied", code="TOOL_DENIED"), stderr=buf)
    payload = json.loads(buf.getvalue())
    assert code == 6
    assert payload["error"]["code"] == "TOOL_DENIED"
    assert payload["error"]["exit_code"] == 6


def test_agent_denial_codes_complete():
    from opensai_client.errors import AGENT_DENIAL_CODES
    assert AGENT_DENIAL_CODES == {
        "TOOL_DENIED", "TOOL_NOT_ALLOWED", "MAX_DAG_NODES", "ARBITRARY_CODE",
        "DIRECT_ALLOCATION_FORBIDDEN", "RESOURCE_LIMIT", "MAX_WAIT_EXCEEDED",
        "RATE_LIMITED",
    }


def test_emit_error_json_shape():
    from opensai_client.cli import emit_error
    from opensai_contracts.errors import ConfigError
    buf = io.StringIO()
    code = emit_error(ConfigError("bad"), stderr=buf)
    payload = json.loads(buf.getvalue())
    assert "request_id" in payload["error"]
    assert payload["error"]["code"] == "CONFIG_ERROR"
    assert payload["error"]["message"] == "bad"


def test_main_validate_missing_input_exit_2():
    from opensai_client.cli import main, EXIT_USAGE
    old_err = sys.stderr
    sys.stderr = io.StringIO()
    try:
        code = main(["dag", "validate"])
    finally:
        sys.stderr = old_err
    assert code == EXIT_USAGE


def test_main_validate_file_not_found_exit_2():
    from opensai_client.cli import main, EXIT_USAGE
    old_err = sys.stderr
    sys.stderr = io.StringIO()
    try:
        code = main(["dag", "validate", "--file", "/nonexistent/dag.json"])
    finally:
        sys.stderr = old_err
    assert code == EXIT_USAGE
