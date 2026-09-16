"""
OpenSAI AgentSkill packaging and tools tests (APP-1.10, WP-04).

Covers §2.8.2 runtime: manifest, config, 8 ``opensai_*`` tool definitions,
``register_tools`` / ``shutdown``, and the legacy ``ResultWaiter``.
"""

import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
_offloading = _project_root / 'Offloading-System'
if _offloading.is_dir() and str(_offloading) not in sys.path:
    sys.path.insert(0, str(_offloading))

_TOOL_NAMES = [
    "opensai_list_algorithms",
    "opensai_list_devices",
    "opensai_cluster_status",
    "opensai_validate_dag",
    "opensai_submit_dag",
    "opensai_submit_task",
    "opensai_get_result",
    "opensai_wait_result",
]


def test_skills_package_import():
    import skills.opensai
    assert hasattr(skills.opensai, 'MANIFEST')
    assert hasattr(skills.opensai, 'get_config')
    assert hasattr(skills.opensai, 'create_client')
    assert hasattr(skills.opensai, 'register_tools')
    assert hasattr(skills.opensai, 'shutdown')


def test_manifest():
    import skills.opensai
    m = skills.opensai.MANIFEST
    assert m['name'] == 'opensai'
    assert m['version'] == '0.1.0'
    assert m['tool_count'] == 8
    assert 'config' in m
    assert len(m['config']) == 6


def test_get_config():
    import skills.opensai
    config = skills.opensai.get_config()
    assert config['host'] == '127.0.0.1'
    assert config['port'] == 8888
    assert config['timeout'] == 30
    assert 'log_level' in config


def test_tool_definitions():
    import skills.opensai
    td = skills.opensai.TOOL_DEFINITIONS
    assert len(td) == 8
    names = [t['name'] for t in td]
    assert names == _TOOL_NAMES


def test_tool_definitions_have_required_fields():
    import skills.opensai
    for t in skills.opensai.TOOL_DEFINITIONS:
        assert 'name' in t
        assert 'description' in t
        assert 'parameters' in t
        assert 'returns' in t
        assert t['parameters']['type'] == 'object'


def test_tool_registry():
    import skills.opensai
    tr = skills.opensai.TOOL_REGISTRY
    assert len(tr) == 8
    assert 'opensai_submit_dag' in tr


def test_make_handler():
    from skills.opensai.tools import make_handler
    h = make_handler('opensai_list_algorithms')
    assert callable(h)
    assert h.__name__ == 'handle_opensai_list_algorithms'
    try:
        make_handler('nonexistent')
        assert False
    except KeyError:
        pass


def test_register_tools_and_invoke():
    from skills.opensai.tools import register_tools, shutdown, ToolRegistry
    from skills.opensai.agent_control import OpenSAIAgentControl, AgentContext

    class FakeClient:
        def list_algorithms(self, refresh=False):
            return ['Algorithm_A', 'Algorithm_B']

    registry = ToolRegistry()
    control = OpenSAIAgentControl()
    names = register_tools(registry=registry, client_factory=FakeClient, control=control)
    assert names == _TOOL_NAMES
    assert set(registry.names()) == set(_TOOL_NAMES)

    resp = control.invoke('opensai_list_algorithms', {}, AgentContext(actor='agent'))
    assert resp.ok
    assert resp.result == ['Algorithm_A', 'Algorithm_B']
    shutdown()


def test_result_waiter():
    from opensai_client import OpenSAIClient
    from opensai_client.wait import ResultWaiter, ResultStatus

    c = OpenSAIClient()
    w = ResultWaiter(c, poll_interval=0.1)
    status = w.get_result('test_job')
    assert status['status'] == ResultStatus.ERROR
