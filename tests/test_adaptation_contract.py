"""适配优化契约测试（WP-06 · 新开发 2 项）。

覆盖 §2.5.2 新增接口：
    validate_resource_hints  —— 四维 ResourceHint 类型/正值/静态上限校验
    resource_hint_to_wire    —— 资源字段根部展开与注入、禁止 details/保留键覆盖

复用 WP-01 契约类型（schema.ResourceLimits / schema.ValidationIssue / errors.ErrorCode）。
"""

import sys
from dataclasses import dataclass
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from opensai_contracts import (
    ResourceLimits,
    Severity,
    validate_resource_hints,
    resource_hint_to_wire,
    ContractError,
    ErrorCode,
)


# ── 测试用轻量 DAGSpec / DAGNode / ResourceHint（鸭子类型形态）────────────────

@dataclass
class ResourceHint:
    cores: int = None
    mem_mb: int = None
    bw_mbps: float = None
    storage_mb: int = None


@dataclass
class DAGNode:
    id: int
    algorithm: str
    resources: ResourceHint = None


@dataclass
class DAGSpec:
    nodes: list


def _dag(*hints):
    nodes = [DAGNode(id=i + 1, algorithm='Algo', resources=h) for i, h in enumerate(hints)]
    return DAGSpec(nodes=nodes)


def _codes(issues):
    return [i.code for i in issues]


# ── validate_resource_hints ─────────────────────────────────────────────────

def test_validate_resource_hints_valid():
    dag = _dag(ResourceHint(cores=4, mem_mb=2048, bw_mbps=100.5, storage_mb=512))
    issues = validate_resource_hints(dag)
    assert issues == [], f"expected valid, got {[i.to_dict() for i in issues]}"


def test_validate_resource_hints_none_resource_skipped():
    dag = _dag(None)
    assert validate_resource_hints(dag) == []


def test_validate_resource_hints_empty_rh_skipped():
    dag = _dag(ResourceHint())
    assert validate_resource_hints(dag) == []


def test_validate_resource_hints_accepts_dict_shape():
    dag = {'nodes': [{'id': 1, 'resources': {'cores': 2, 'mem_mb': 1024}}]}
    assert validate_resource_hints(dag) == []


def test_validate_resource_hints_legacy_resource_hint_alias():
    dag = {'nodes': [{'id': 1, 'resource_hint': {'cores': 2}}]}
    assert validate_resource_hints(dag) == []


def test_validate_resource_hints_type_errors():
    dag = _dag(ResourceHint(cores=2.5, mem_mb='x', bw_mbps='fast', storage_mb=True))
    issues = validate_resource_hints(dag)
    codes = _codes(issues)
    assert codes.count(ErrorCode.TYPE_ERROR) == 4, f"got {[i.to_dict() for i in issues]}"
    # 路径应定位到具体字段
    paths = {i.path for i in issues}
    assert any('cores' in p for p in paths)
    assert any('mem_mb' in p for p in paths)
    assert any('bw_mbps' in p for p in paths)
    assert any('storage_mb' in p for p in paths)


def test_validate_resource_hints_non_positive():
    dag = _dag(ResourceHint(cores=0, mem_mb=-5, storage_mb=0, bw_mbps=-1.5))
    issues = validate_resource_hints(dag)
    assert _codes(issues).count(ErrorCode.VALUE_ERROR) == 4, [i.to_dict() for i in issues]


def test_validate_resource_hints_upper_bound():
    limits = ResourceLimits(cores_max=8, mem_mb_max=4096, bw_mbps_max=100.0, storage_mb_max=1024)
    dag = _dag(ResourceHint(cores=16, mem_mb=8192, bw_mbps=500.0, storage_mb=2048))
    issues = validate_resource_hints(dag, limits=limits)
    assert _codes(issues).count(ErrorCode.VALUE_ERROR) == 4, [i.to_dict() for i in issues]
    # 每条的 details 都带 limit
    for i in issues:
        assert 'limit' in i.details


def test_validate_resource_hints_within_limit_ok():
    limits = ResourceLimits(cores_max=8, mem_mb_max=4096, bw_mbps_max=100.0, storage_mb_max=1024)
    dag = _dag(ResourceHint(cores=8, mem_mb=4096, bw_mbps=100.0, storage_mb=1024))
    assert validate_resource_hints(dag, limits=limits) == []


def test_validate_resource_hints_unknown_field_warning():
    dag = {'nodes': [{'id': 1, 'resources': {'cores': 2, 'gpu': 4}}]}
    issues = validate_resource_hints(dag)
    warnings = [i for i in issues if i.code == ErrorCode.UNKNOWN_FIELD]
    assert len(warnings) == 1
    assert warnings[0].severity == Severity.WARNING
    assert 'gpu' in warnings[0].path


def test_validate_resource_hints_no_nodes():
    assert validate_resource_hints({'nodes': []}) == []


def test_validate_resource_hints_bad_nodes_type():
    issues = validate_resource_hints({'nodes': 'not-a-list'})
    assert _codes(issues) == [ErrorCode.TYPE_ERROR]


# ── resource_hint_to_wire ───────────────────────────────────────────────────

def test_wire_params_expand_at_root():
    out = resource_hint_to_wire(None, {'count_num': 100000, 'sleep_time': 1})
    assert out == {'count_num': 100000, 'sleep_time': 1}


def test_wire_injects_resource_fields():
    out = resource_hint_to_wire(ResourceHint(cores=4, mem_mb=2048, bw_mbps=50.0, storage_mb=512),
                                {'count_num': 100})
    assert out['count_num'] == 100
    assert out['cores'] == 4
    assert out['mem_mb'] == 2048
    assert out['bw_mbps'] == 50.0
    assert out['storage_mb'] == 512


def test_wire_no_details_subobject():
    out = resource_hint_to_wire(ResourceHint(cores=2), {'a': 1})
    assert 'details' not in out
    # 资源字段在根部，而非任何子对象内
    assert out['cores'] == 2


def test_wire_none_resource_returns_params_copy():
    params = {'a': 1}
    out = resource_hint_to_wire(None, params)
    assert out == params
    assert out is not params  # 浅拷贝，不改调用方


def test_wire_does_not_mutate_params():
    params = {'a': 1}
    resource_hint_to_wire(ResourceHint(cores=3), params)
    assert 'cores' not in params  # 调用方 dict 未被污染


def test_wire_reserved_key_in_params_raises():
    try:
        resource_hint_to_wire(None, {'_alloc': {'cores': 1}})
        assert False, 'expected ContractError'
    except ContractError as e:
        assert e.code == ErrorCode.RESERVED_KEY


def test_wire_params_not_dict_raises():
    try:
        resource_hint_to_wire(None, 'not-a-dict')
        assert False, 'expected ContractError'
    except ContractError as e:
        assert e.code == ErrorCode.TYPE_ERROR


def test_wire_resource_dict_shape():
    out = resource_hint_to_wire({'cores': 2, 'mem_mb': 512}, {'x': 1})
    assert out == {'x': 1, 'cores': 2, 'mem_mb': 512}


def test_wire_contract_error_has_unified_fields():
    try:
        resource_hint_to_wire(None, {'_alloc': {}})
    except ContractError as e:
        d = e.to_dict()
        assert d['code'] == ErrorCode.RESERVED_KEY
        assert d['retryable'] is False
        assert 'details' in d
