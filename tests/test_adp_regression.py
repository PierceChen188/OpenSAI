"""适配优化组件回归测试（WP-06 · 现有 5 项不回归）。

覆盖 §8.3「2.5」标注「现有」的 5 项接口 + 放置策略兼容，确认 WP-06 未改动其行为，
且字段/单位与契约对齐：

    UserServer._alloc_compute            —— 四维档位决策（cores/mem_budget_mb/est_mem_mb/storage_budget_mb）
    UserServer._artifact_rate_mbps       —— 带宽分配（bw_mbps / committed_bw）
    AllocationService.update_profile     —— EWMA 画像更新（可导入，功能测试）
    UserServer._update_algo_history      —— 薄委托（server.py，AST 结构校验）
    UserServer._reclaim_stale_reservations —— 超时回收（server.py，AST 结构校验）

环境说明：WSL 未装 redis，`server.py` 无法直接 import（redis_boot 依赖 redis 模块），
故 UserServer 的 4 个 classmethod 采用 `ast` 结构校验 + 关键字段/单位断言；
可导入的纯模块（AllocationService / MonitorAggregator）做真实功能回归。
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
# server.py 侧模块用 `from core.xxx` 相对导入（生产从 dag_offload/ 运行），
# 需把 dag_offload 也放进 sys.path 使 `core` 可解析（与 server.py 自身 sys.path 一致）。
_dag_offload = _offloading / 'dag_offload'
if str(_dag_offload) not in sys.path:
    sys.path.insert(0, str(_dag_offload))

_SERVER = _offloading / 'dag_offload' / 'server.py'


# ── AST 结构校验工具 ────────────────────────────────────────────────────────

def _user_server_method_source(method_name: str) -> str:
    """从 server.py AST 中取出 UserServer.<method_name> 的源码段。"""
    tree = ast.parse(_SERVER.read_text(encoding='utf-8'))
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == 'UserServer':
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name == method_name:
                    return ast.get_source_segment(_SERVER.read_text(encoding='utf-8'), item) or ''
    return ''


# ── 现有 1：AllocationService.update_profile（EWMA，功能测试）────────────────

def test_update_profile_ewma_first_sample():
    from dag_offload.core.allocation_service import AllocationService
    h = {}
    AllocationService.update_profile(h, 'Algo', {'peak_mem_mb': 100.0, 'task_mem_mb': 40.0,
                                                  'proc_secs': 5.0, 'task_storage_mb': 10.0})
    assert h['Algo']['n'] == 1
    assert h['Algo']['peak_mem_mb'] == 100.0
    assert h['Algo']['task_mem_mb'] == 40.0


def test_update_profile_ewma_converges():
    from dag_offload.core.allocation_service import AllocationService
    h = {}
    AllocationService.update_profile(h, 'Algo', {'task_mem_mb': 100.0})
    AllocationService.update_profile(h, 'Algo', {'task_mem_mb': 200.0})
    # a=0.5: 第二次 = 0.5*200 + 0.5*100 = 150
    assert abs(h['Algo']['task_mem_mb'] - 150.0) < 1e-9
    assert h['Algo']['n'] == 2


def test_update_profile_ignores_bad_input():
    from dag_offload.core.allocation_service import AllocationService
    h = {'Algo': {'task_mem_mb': 100.0, 'n': 1}}
    # 空 algo / 非 dict metrics 不得破坏已有画像
    AllocationService.update_profile(h, '', {'task_mem_mb': 999.0})
    AllocationService.update_profile(h, 'Algo', None)
    AllocationService.update_profile(h, 'Algo', 'not-a-dict')
    assert h['Algo']['task_mem_mb'] == 100.0
    assert h['Algo']['n'] == 1


def test_update_profile_keeps_storage_history():
    from dag_offload.core.allocation_service import AllocationService
    h = {}
    AllocationService.update_profile(h, 'Algo', {'task_storage_mb': 512.0})
    assert h['Algo']['task_storage_mb'] == 512.0


# ── 现有（账本/单位对齐）：MonitorAggregator.build_view ──────────────────────

def test_monitor_view_unit_fields_aligned():
    from dag_offload.core.monitor_aggregator import MonitorAggregator
    devices = {'d1': {'cores_total': 8, 'cores_committed': 3, 'worker_slots': 2,
                      'storage_total_mb': 1000, 'storage_free_mb': 600}}
    view = MonitorAggregator.build_view(
        devices, reservations={'d1': 2}, committed_bw={'d1': 12.5},
        committed_storage_mb={'d1': 200.0}, inflight={('t', 1): ('d1', 0)}, algo_history={})
    d = view['devices']['d1']
    assert d['cores_total'] == 8
    assert d['cores_available'] == 5               # cores 单位：核
    assert d['bw_committed_mbps'] == 12.5          # 带宽：MB/s
    assert d['storage_committed_mb'] == 200.0      # 存储：MiB
    assert d['placement_reserved'] == 2
    assert view['inflight_tasks'] == 1


def test_monitor_cluster_health_aligned():
    from dag_offload.core.monitor_aggregator import MonitorAggregator
    devices = {'d1': {'worker_slots': 4, 'work_status': 1, 'delay': 10, 'used_cpu': '50%'},
               'd2': {'worker_slots': 4, 'work_status': 0, 'delay': 900, 'used_cpu': '10%'}}
    health = MonitorAggregator.get_cluster_health(devices=devices)
    assert health['online_devices'] == 2
    assert health['total_slots'] == 8
    assert health['used_slots'] == 1
    assert any(a['device'] == 'd2' and a['type'] == 'high_delay' for a in health['anomalies'])


# ── 现有 2：UserServer._alloc_compute（AST：字段/单位对齐）───────────────────

def test_alloc_compute_returns_contract_fields():
    src = _user_server_method_source('_alloc_compute')
    assert src, '_alloc_compute not found'
    # §2.5.2 出参四字段
    for field in ('cores', 'mem_budget_mb', 'est_mem_mb', 'storage_budget_mb'):
        assert field in src, f"_alloc_compute 缺少出参字段 {field}"


def test_alloc_compute_reads_unit_aligned_params():
    src = _user_server_method_source('_alloc_compute')
    # 声明字段名与契约单位对齐：cores / mem_mb / storage_mb（§2.5.3）
    for key in ("param.get('cores')", "param.get('mem_mb')", "param.get('storage_mb')"):
        assert key in src, f"_alloc_compute 缺少声明读取 {key}"


def test_alloc_compute_maintains_storage_ledger():
    src = _user_server_method_source('_alloc_compute')
    assert 'committed_storage_mb' in src
    assert 'storage_reservations' in src


# ── 现有 3：UserServer._artifact_rate_mbps（AST：bw_mbps / committed_bw）────

def test_artifact_rate_reads_bw_mbps():
    src = _user_server_method_source('_artifact_rate_mbps')
    assert src, '_artifact_rate_mbps not found'
    assert "get('bw_mbps')" in src
    assert 'committed_bw' in src
    assert 'bw_reservations' in src


# ── 现有 4：UserServer._update_algo_history（薄委托）────────────────────────

def test_update_algo_history_delegates_to_allocation_service():
    src = _user_server_method_source('_update_algo_history')
    assert 'AllocationService.update_profile' in src


# ── 现有 5：UserServer._reclaim_stale_reservations（超时回收）───────────────

def test_reclaim_stale_reservations_recovers_all_ledgers():
    src = _user_server_method_source('_reclaim_stale_reservations')
    assert 'OPENSAI_RESERVATION_TTL' in src
    assert 'committed_bw' in src
    assert 'committed_storage_mb' in src
    assert 'storage_reservations' in src
    assert 'bw_reservations' in src
    assert 'reservations' in src


# ── 放置策略兼容：plan / smart / random 分支保持 ─────────────────────────────

def test_offloading_strategy_keeps_placement_modes():
    tree = ast.parse(_SERVER.read_text(encoding='utf-8'))
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == 'UserServer':
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name == 'offloading_strategy':
                    src = ast.get_source_segment(_SERVER.read_text(encoding='utf-8'), item) or ''
                    assert "'plan'" in src and "'random'" in src
                    # smart=默认 else 分支（纯贪心自适应），direct 钉选保留
                    assert 'direct_strategy' in src
                    assert 'random_strategy' in src
                    assert 'plan_dag' in src
                    assert 'locality_strategy' in src
                    return
    assert False, 'offloading_strategy not found'
