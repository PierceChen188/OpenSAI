"""设备资源控制回归测试（WP-07 · 需更新 3 项）。

覆盖 §8.3「2.6」DeviceClient.process_task 的档位合并修复 + storage_budget_mb 贯通，
以及 §8.3「2.7」fetch_blob / fetch_range 的向后兼容更新。

环境说明：WSL 未装 aioquic / psutil，device_client.py 与 quic_client.py 无法直接
import（数据面依赖 aioquic），故：
    - 可导入的纯模块 dag_offload.resource_apply 做真实功能回归（apply_core_ids 复制语义）；
    - device_client.py / quic_client.py 采用 ast 结构校验 + 关键字段断言。
"""

import ast
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
_offloading = _project_root / 'Offloading-System'
if str(_offloading) not in sys.path:
    sys.path.insert(0, str(_offloading))
_dag_offload = _offloading / 'dag_offload'
if str(_dag_offload) not in sys.path:
    sys.path.insert(0, str(_dag_offload))

_DEVICE_CLIENT = _offloading / 'dag_offload' / 'device_client.py'
_QUIC_CLIENT = _offloading / 'network' / 'dataplane' / 'quic_client.py'


# ── AST 结构校验工具 ────────────────────────────────────────────────────────

def _func_source(path: Path, func_name: str, class_name: str = None) -> str:
    """从 path 的 AST 中取出函数/方法源码段；class_name 为空则取顶层函数。"""
    text = path.read_text(encoding='utf-8')
    tree = ast.parse(text)
    for node in ast.walk(tree):
        if class_name is not None:
            if isinstance(node, ast.ClassDef) and node.name == class_name:
                for item in node.body:
                    if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name == func_name:
                        return ast.get_source_segment(text, item) or ''
        else:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == func_name:
                return ast.get_source_segment(text, node) or ''
    return ''


# ── 功能回归：apply_core_ids（纯函数，可导入）───────────────────────────────

def test_apply_core_ids_preserves_allocation_fields():
    from dag_offload.resource_apply import apply_core_ids
    srv_alloc = {'cores': 4, 'mem_budget_mb': 512, 'est_mem_mb': 256, 'storage_budget_mb': 128}
    merged = apply_core_ids(srv_alloc, [0, 1, 2, 3])
    # 存储/估算字段不得丢失
    assert merged['cores'] == 4, merged
    assert merged['mem_budget_mb'] == 512, merged
    assert merged['est_mem_mb'] == 256, merged
    assert merged['storage_budget_mb'] == 128, merged
    # 追加实际核号
    assert merged['core_ids'] == [0, 1, 2, 3], merged


def test_apply_core_ids_does_not_mutate_input():
    from dag_offload.resource_apply import apply_core_ids
    srv_alloc = {'cores': 2, 'storage_budget_mb': 64}
    out = apply_core_ids(srv_alloc, [1, 2])
    assert 'core_ids' not in srv_alloc, '入参 srv_alloc 不得被污染'
    assert out is not srv_alloc


def test_apply_core_ids_empty_alloc():
    from dag_offload.resource_apply import apply_core_ids
    merged = apply_core_ids({}, [0])
    assert merged == {'core_ids': [0]}, merged


def test_apply_core_ids_none_alloc():
    from dag_offload.resource_apply import apply_core_ids
    merged = apply_core_ids(None, [0, 1])
    assert merged == {'core_ids': [0, 1]}, merged


def test_apply_core_ids_none_cores_sets_none():
    from dag_offload.resource_apply import apply_core_ids
    merged = apply_core_ids({'cores': 2}, None)
    assert merged['cores'] == 2
    assert merged['core_ids'] is None


# ── 结构回归：DeviceClient.process_task（复制 srv_alloc + storage_budget_mb 贯通）──

def test_process_task_uses_copy_merge_not_overwrite():
    src = _func_source(_DEVICE_CLIENT, 'process_task', 'DeviceClient')
    assert src, 'process_task not found'
    # 修复点：调用 apply_core_ids 做「复制 + 追加」，而非整体重建
    assert 'apply_core_ids' in src, 'process_task 应调用 apply_core_ids 复制合并'
    # 旧 bug 模式（整体重建、丢弃存储/估算字段）必须已移除
    assert "{'core_ids': leased_cores, 'mem_budget_mb': req_budget}" not in src, \
        'process_task 仍含旧的整体重建 _alloc 写法'


def test_process_task_threads_storage_budget():
    src = _func_source(_DEVICE_CLIENT, 'process_task', 'DeviceClient')
    # 提取 storage_budget_mb
    assert "storage_budget_mb" in src
    assert "get('storage_budget_mb')" in src
    # 记账（committed_storage_mb 累加 + 归还）
    assert 'committed_storage_mb' in src
    # 贯通到 ExecutionMetrics
    assert "metrics['storage_budget_mb']" in src


def test_device_client_declares_committed_storage():
    text = _DEVICE_CLIENT.read_text(encoding='utf-8')
    assert 'committed_storage_mb = 0' in text, 'DeviceClient 应新增 committed_storage_mb 类变量'


# ── 结构回归：fetch_blob（origin 透传）──────────────────────────────────────

def test_fetch_blob_signature_has_origin():
    src = _func_source(_QUIC_CLIENT, 'fetch_blob')
    assert src, 'fetch_blob not found'
    assert 'origin=None' in src, 'fetch_blob 签名应增加 origin=None'
    # 透传给 _fetch_pooled
    assert 'origin=origin' in src, 'fetch_blob 应将 origin 透传给 _fetch_pooled'


# ── 结构回归：fetch_range（dest_path 写盘）──────────────────────────────────

def test_fetch_range_signature_has_dest_path():
    src = _func_source(_QUIC_CLIENT, 'fetch_range')
    assert src, 'fetch_range not found'
    assert 'dest_path=None' in src, 'fetch_range 签名应增加 dest_path=None'
    # 保留 bytes 模式（未传 dest_path 时仍 return data）
    assert 'return data' in src
    # 写盘分支：offset 0 新建 / 非 0 seek 写
    assert "'wb'" in src
    assert "'r+b'" in src
    # 返回统计 dict
    assert "'offset': off" in src
    # 继续透传 origin
    assert 'origin' in src
