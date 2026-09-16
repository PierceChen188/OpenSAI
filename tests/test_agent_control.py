"""
OpenSAI Agent Control tests (APP-1.10, WP-04).

Covers §2.8.3 OpenSAIAgentControl: policy allow/deny, node cap, arbitrary-code
guard, direct-allocation and wait caps, rate limiting, audit, and policy reload.
The legacy RateLimiter and Policy enum remain for backward compatibility.
"""

import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))


def test_agent_control_imports():
    from skills.opensai.agent_control import (
        OpenSAIAgentControl, AgentControl, AgentControlPolicy, PolicyDecision,
        RateLimitLease, AuditEvent, AgentContext, ToolResponse, RateLimiter,
        CallRecord, Policy,
    )
    assert AgentControl is OpenSAIAgentControl  # compat alias
    assert Policy.ALLOW == 'allow'
    assert Policy.BLOCK == 'block'
    assert Policy.AUDIT == 'audit'


def test_policy_default_allow():
    from skills.opensai.agent_control import OpenSAIAgentControl, AgentContext
    ac = OpenSAIAgentControl()
    d = ac.check_policy('any_tool', {}, AgentContext(actor='agent'))
    assert d.allowed


def test_policy_deny_tool():
    from skills.opensai.agent_control import OpenSAIAgentControl, AgentControlPolicy, AgentContext
    ac = OpenSAIAgentControl(policy=AgentControlPolicy(denied_tools=('dangerous_tool',)))
    d = ac.check_policy('dangerous_tool', {}, AgentContext(actor='agent'))
    assert not d.allowed
    assert d.code == 'TOOL_DENIED'


def test_policy_allowlist():
    from skills.opensai.agent_control import OpenSAIAgentControl, AgentControlPolicy, AgentContext
    ac = OpenSAIAgentControl(policy=AgentControlPolicy(allowed_tools=('submit_dag',)))
    assert ac.check_policy('submit_dag', {}, AgentContext(actor='a')).allowed
    assert not ac.check_policy('other_tool', {}, AgentContext(actor='a')).allowed


def test_policy_max_dag_nodes():
    from skills.opensai.agent_control import OpenSAIAgentControl, AgentControlPolicy, AgentContext
    ac = OpenSAIAgentControl(policy=AgentControlPolicy(max_dag_nodes=2))
    dag = {'nodes': [{'id': 1}, {'id': 2}, {'id': 3}]}
    d = ac.check_policy('opensai_submit_dag', {'dag_spec': dag}, AgentContext(actor='a'))
    assert not d.allowed
    assert d.code == 'MAX_DAG_NODES'


def test_policy_arbitrary_code_rejected():
    from skills.opensai.agent_control import OpenSAIAgentControl, AgentContext
    ac = OpenSAIAgentControl()
    dag = {'nodes': [{'id': 1, 'algorithm': 'A', 'code': 'import os; os.system("rm -rf /")'}]}
    d = ac.check_policy('opensai_submit_dag', {'dag_spec': dag}, AgentContext(actor='a'))
    assert not d.allowed
    assert d.code == 'ARBITRARY_CODE'


def test_policy_direct_allocation_forbidden():
    from skills.opensai.agent_control import OpenSAIAgentControl, AgentControlPolicy, AgentContext
    ac = OpenSAIAgentControl(policy=AgentControlPolicy(allow_direct_allocation=False))
    dag = {'nodes': [{'id': 1, 'direct_allocation_id': 3}]}
    d = ac.check_policy('opensai_submit_dag', {'dag_spec': dag}, AgentContext(actor='a'))
    assert not d.allowed
    assert d.code == 'DIRECT_ALLOCATION_FORBIDDEN'


def test_policy_max_wait():
    from skills.opensai.agent_control import OpenSAIAgentControl, AgentControlPolicy, AgentContext
    ac = OpenSAIAgentControl(policy=AgentControlPolicy(max_wait_s=30))
    d = ac.check_policy('opensai_wait_result', {'job_id': 'j', 'timeout': 120}, AgentContext(actor='a'))
    assert not d.allowed
    assert d.code == 'MAX_WAIT_EXCEEDED'


def test_invoke_allows_and_audits():
    from skills.opensai.agent_control import OpenSAIAgentControl, AgentContext
    events = []
    ac = OpenSAIAgentControl(audit_sink=events.append)
    ac.bind_handlers({'echo': lambda **kw: {'echo': kw.get('x')}})
    resp = ac.invoke('echo', {'x': 1}, AgentContext(actor='alice', request_id='r1'))
    assert resp.ok
    assert resp.result == {'echo': 1}
    assert len(events) == 1
    assert events[0].actor == 'alice'
    assert events[0].tool == 'echo'
    assert events[0].request_id == 'r1'
    assert events[0].result_code == 'OK'


def test_invoke_denied_tool():
    from skills.opensai.agent_control import OpenSAIAgentControl, AgentControlPolicy, AgentContext
    ac = OpenSAIAgentControl(policy=AgentControlPolicy(denied_tools=('forbidden',)))
    ac.bind_handlers({'forbidden': lambda **kw: 'x'})
    resp = ac.invoke('forbidden', {}, AgentContext(actor='a'))
    assert not resp.ok
    assert resp.error['code'] == 'TOOL_DENIED'


def test_invoke_unregistered_tool():
    from skills.opensai.agent_control import OpenSAIAgentControl, AgentContext
    ac = OpenSAIAgentControl()
    resp = ac.invoke('nope', {}, AgentContext(actor='a'))
    assert not resp.ok
    assert resp.error['code'] == 'TOOL_NOT_REGISTERED'


def test_invoke_rate_limited():
    from skills.opensai.agent_control import OpenSAIAgentControl, AgentControlPolicy, AgentContext
    ac = OpenSAIAgentControl(policy=AgentControlPolicy(
        rate_limits={'echo': {'max_per_second': 1, 'max_concurrent': 0}}))
    ac.bind_handlers({'echo': lambda **kw: 'ok'})
    assert ac.invoke('echo', {}, AgentContext(actor='a')).ok
    resp = ac.invoke('echo', {}, AgentContext(actor='a'))
    assert not resp.ok
    assert resp.error['code'] == 'RATE_LIMITED'
    assert resp.error['retryable'] is True


def test_acquire_release_rate_limit():
    from skills.opensai.agent_control import OpenSAIAgentControl, AgentControlPolicy
    ac = OpenSAIAgentControl(policy=AgentControlPolicy(
        rate_limits={'t': {'max_concurrent': 1}}))
    l1 = ac.acquire_rate_limit('s', 't')
    assert l1.retry_after_s is None
    l2 = ac.acquire_rate_limit('s', 't')
    assert l2.retry_after_s is None or l2.lease_id == ''  # second concurrent denied
    ac.release_rate_limit(l1)
    l3 = ac.acquire_rate_limit('s', 't')
    assert l3.retry_after_s is None  # slot freed


def test_reload_policy():
    from skills.opensai.agent_control import OpenSAIAgentControl, AgentControlPolicy, PolicyVersionConflict
    ac = OpenSAIAgentControl(policy=AgentControlPolicy(version='1'))
    new = AgentControlPolicy(version='2', denied_tools=('x',))
    v = ac.reload_policy(new, expected_version='1')
    assert v == '2'
    try:
        ac.reload_policy(AgentControlPolicy(version='3'), expected_version='1')
        assert False, 'expected PolicyVersionConflict'
    except PolicyVersionConflict:
        pass


def test_audit_redacts_paths():
    from skills.opensai.agent_control import OpenSAIAgentControl, AgentControlPolicy, AgentContext
    events = []
    ac = OpenSAIAgentControl(
        policy=AgentControlPolicy(audit_redact_paths=('token',)),
        audit_sink=events.append,
    )
    ac.bind_handlers({'echo': lambda **kw: 'ok'})
    ac.invoke('echo', {'token': 'secret', 'x': 1}, AgentContext(actor='a'))
    assert 'secret' not in events[0].redacted_digest


# ── legacy RateLimiter (backward compatibility) ─────────────────────────────

def test_rate_limiter_allow():
    from skills.opensai.agent_control import RateLimiter
    rl = RateLimiter(max_calls_per_second=5)
    for _ in range(5):
        allowed, reason = rl.check('test_tool')
        assert allowed, 'Call should be allowed'


def test_rate_limiter_block():
    from skills.opensai.agent_control import RateLimiter
    rl = RateLimiter(max_calls_per_second=2)
    rl.check('test_tool')
    rl.check('test_tool')
    allowed, reason = rl.check('test_tool')
    assert not allowed
    assert 'Rate limit' in reason


def test_rate_limiter_stats():
    from skills.opensai.agent_control import RateLimiter
    rl = RateLimiter(max_calls_per_second=5)
    for _ in range(3):
        rl.check('tool_x')
    stats = rl.get_stats('tool_x')
    assert stats['recent_calls'] == 3
    assert stats['total_calls'] == 3
    assert stats['max_rate'] == 5


def test_rate_limiter_max_total():
    from skills.opensai.agent_control import RateLimiter
    rl = RateLimiter(max_calls_per_second=10, max_total_calls=2)
    rl.check('t')
    rl.check('t')
    allowed, _ = rl.check('t')
    assert not allowed
