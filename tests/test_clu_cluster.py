"""WP-05 (CLU) 集群管理适配 — 单元/回归测试。

覆盖：
  - 新开发：MonitorAggregator.cluster_status / get_monitor_view、ClusterManager 的
    validate_placement_targets / resolve_allocation_index / handle_devices_info / handle_policies_info。
  - 现有 6 项回归：PlacementService.healthy、OffloadingImpl 的
    random_strategy / direct_strategy / locality_strategy / plan_dag / score_candidates。

不依赖运行中的 server / Redis（全部为纯函数 + 构造的字典夹具）。
"""

import os
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
_offloading = _project_root / 'Offloading-System'
if _offloading.is_dir() and str(_offloading) not in sys.path:
    sys.path.insert(0, str(_offloading))
_dag_offload = _offloading / 'dag_offload'
if _dag_offload.is_dir() and str(_dag_offload) not in sys.path:
    sys.path.insert(0, str(_dag_offload))


# ── 夹具 ────────────────────────────────────────────────────────────────────

def make_device(device_no, work_status=0, used_cpu='20%', used_memory=100.0,
                total_memory=1000.0, worker_slots=4, delay=10, **extra):
    """构造一个字段齐全的设备心跳 dict（对齐 DeviceInfo 全字段，供健康预筛用）。"""
    d = {
        'device_no': device_no,
        'server_no': 'srv-1',
        'used_cpu': used_cpu,
        'core_num': 8,
        'idle_memory': total_memory - used_memory,
        'used_memory': used_memory,
        'total_memory': total_memory,
        'work_status': work_status,
        'lng': 0.0,
        'lat': 0.0,
        'order_value': 1,
        'ip_address': f'192.168.1.{device_no[-1]}',
        'port': 9000,
        'throughput': 0.0,
        'bandwidth': 0.0,
        'network_usage': 0.0,
        'delay': delay,
        'data_port': 9001,
        'worker_slots': worker_slots,
        'cores_total': 8,
        'cores_committed': 0,
        'mem_committed_mb': 0,
        'storage_total_mb': 10240,
        'storage_free_mb': 8000,
    }
    d.update(extra)
    return d


# ── 新开发：ClusterManager（§2.4.4 放置目标与策略）────────────────────────

def test_resolve_allocation_index():
    from core.cluster_manager import ClusterManager, DeviceNotFoundError
    devices = {'dev-2': {}, 'dev-1': {}, 'dev-3': {}}
    assert ClusterManager.resolve_allocation_index(1, devices) == 'dev-1'
    assert ClusterManager.resolve_allocation_index(3, devices) == 'dev-3'

    for bad in (0, 4, 'x', True, 1.5):
        try:
            ClusterManager.resolve_allocation_index(bad, devices)
            assert False, f'Expected DeviceNotFoundError for {bad!r}'
        except DeviceNotFoundError:
            pass


def test_validate_placement_targets_valid():
    from core.cluster_manager import ClusterManager
    dag = {'dagData': {'nodes': [
        {'id': 1, 'form': {'directAllocationId': 2}},
        {'id': 2, 'form': {'directAllocationId': -1}},   # 未固定
        {'id': 3, 'form': {}},                            # 未指定
    ]}}
    devices = {'d1': {}, 'd2': {}, 'd3': {}}
    assert ClusterManager.validate_placement_targets(dag, devices) == []
    # 无快照时正整数直接通过（服务端始终最终确认）
    assert ClusterManager.validate_placement_targets(dag, None) == []


def test_validate_placement_targets_invalid():
    from core.cluster_manager import ClusterManager
    dag = {'dagData': {'nodes': [
        {'id': 1, 'form': {'directAllocationId': 0}},      # 非正整数
        {'id': 2, 'form': {'directAllocationId': 'abc'}},  # 非数值字符串
        {'id': 3, 'form': {'directAllocationId': True}},   # 布尔
        {'id': 4, 'form': {'directAllocationId': 9}},      # 越界（在线 2 台）
    ]}}
    devices = {'d1': {}, 'd2': {}}
    issues = ClusterManager.validate_placement_targets(dag, devices)
    codes = {(i['node_id'], i['code']) for i in issues}
    assert (1, 'INVALID_DIRECT_ALLOCATION') in codes
    assert (2, 'INVALID_DIRECT_ALLOCATION') in codes
    assert (3, 'INVALID_DIRECT_ALLOCATION') in codes
    assert (4, 'ALLOCATION_INDEX_OUT_OF_RANGE') in codes


def test_validate_placement_targets_string_tolerated():
    from core.cluster_manager import ClusterManager
    dag = {'dagData': {'nodes': [{'id': 1, 'form': {'directAllocationId': '2'}}]}}
    devices = {'d1': {}, 'd2': {}}
    assert ClusterManager.validate_placement_targets(dag, devices) == []


def test_handle_devices_info_sparse():
    from core.cluster_manager import ClusterManager
    devices = {'dev-1': make_device('dev-1', work_status=2)}
    resp = ClusterManager.handle_devices_info(devices=devices, request_id='r1')
    assert resp['request_id'] == 'r1'
    devs = resp['devicesInfo']['devices']
    assert len(devs) == 1
    d = devs[0]
    # legacy camelCase 外壳字段保留（不改 APP 冻结的外部 Schema）
    for k in ('deviceNo', 'usedCpu', 'coreNum', 'idleMemory', 'usedMemory', 'totalMemory',
              'workStatus', 'lng', 'lat', 'orderValue', 'ipAddress', 'throughput',
              'bandwidth', 'networkUsage', 'delay', 'port'):
        assert k in d, f'missing legacy field {k}'
    # 不冒充完整 MonitorView：不含大脑账本字段
    assert 'placement_reserved' not in d and 'bw_committed_mbps' not in d


def test_handle_devices_info_partial_fields():
    from core.cluster_manager import ClusterManager
    devices = {'dev-x': {'device_no': 'dev-x'}}   # 单台设备字段严重缺失
    resp = ClusterManager.handle_devices_info(devices=devices)
    d = resp['devicesInfo']['devices'][0]
    assert d['usedCpu'] == '0%' and d['coreNum'] == 0 and d['workStatus'] == 0


def test_handle_policies_info():
    from core.cluster_manager import ClusterManager
    resp = ClusterManager.handle_policies_info(request_id='r2')
    assert resp['request_id'] == 'r2'
    assert resp['policiesInfo']['policies'] == ['plan', 'smart', 'random']


# ── 新开发：MonitorAggregator.cluster_status / get_monitor_view（§2.4.1/§2.4.3）──

def test_cluster_status_empty():
    from core.monitor_aggregator import MonitorAggregator
    cs = MonitorAggregator.cluster_status()
    assert cs['online_count'] == 0
    assert cs['total_slots'] == 0
    assert cs['used_slots'] == 0
    assert cs['health']['status'] == 'empty'
    assert cs['devices'] == []
    assert cs['stale'] is False


def test_cluster_status_aggregates():
    from core.monitor_aggregator import MonitorAggregator
    devices = {
        'dev-2': make_device('dev-2', work_status=1, worker_slots=4),
        'dev-1': make_device('dev-1', work_status=0, worker_slots=2),
    }
    cs = MonitorAggregator.cluster_status(devices=devices, inflight={'a': 1})
    assert cs['online_count'] == 2
    assert cs['total_slots'] == 6
    assert cs['used_slots'] == 1
    assert cs['health']['inflight_tasks'] == 1
    # allocation_index 按 device_id 升序：dev-1=1, dev-2=2
    assert [d['device_id'] for d in cs['devices']] == ['dev-1', 'dev-2']
    assert [d['allocation_index'] for d in cs['devices']] == [1, 2]


def test_cluster_status_anomalies_and_stale():
    from core.monitor_aggregator import MonitorAggregator
    devices = {'dev-1': make_device('dev-1', used_cpu='95%', delay=600)}
    cs = MonitorAggregator.cluster_status(devices=devices, ts=0, max_age_s=30)
    assert cs['health']['status'] == 'critical'
    assert cs['stale'] is True
    types = {a['type'] for a in cs['alerts']}
    assert 'high_cpu' in types and 'high_delay' in types


def test_get_monitor_view_fields():
    from core.monitor_aggregator import MonitorAggregator
    devices = {'d1': make_device('d1', work_status=0)}
    view = MonitorAggregator.get_monitor_view(
        devices=devices, reservations={'d1': 2}, committed_bw={'d1': 1.5},
        committed_storage_mb={'d1': 100}, inflight={}, algo_history={})
    assert 'devices' in view and 'ts' in view
    d = view['devices']['d1']
    assert d['placement_reserved'] == 2
    assert d['bw_committed_mbps'] == 1.5
    assert d['storage_committed_mb'] == 100
    assert d['cores_available'] == 8


# ── 现有 6 项回归 ───────────────────────────────────────────────────────────

def test_placement_service_healthy_filters():
    from core.placement_service import PlacementService
    devices = {
        'd1': make_device('d1', work_status=0, used_cpu='10%', used_memory=50, total_memory=1000),
        'd2': make_device('d2', work_status=15),          # 过载：work_status >= 阈值
        'd3': make_device('d3', used_cpu='95%'),           # 过载：CPU >= 阈值
    }
    healthy = PlacementService.healthy(devices)
    assert set(healthy.keys()) == {'d1'}


def test_placement_service_healthy_all_overloaded_fallback():
    from core.placement_service import PlacementService
    devices = {'d1': make_device('d1', work_status=15), 'd2': make_device('d2', used_cpu='95%')}
    healthy = PlacementService.healthy(devices)
    assert set(healthy.keys()) == {'d1', 'd2'}   # 全过载 -> 退回全量，绝不阻塞放置


def test_placement_service_healthy_env_off():
    from core.placement_service import PlacementService
    os.environ['OPENSAI_HEALTH_FILTER'] = '0'
    try:
        devices = {'d1': make_device('d1', work_status=15)}
        assert set(PlacementService.healthy(devices).keys()) == {'d1'}
    finally:
        os.environ.pop('OPENSAI_HEALTH_FILTER', None)


def test_placement_service_healthy_empty():
    from core.placement_service import PlacementService
    assert PlacementService.healthy({}) == {}
    assert PlacementService.healthy(None) is None


def test_offloading_random_strategy():
    from offloading_brain.offloading_brain import OffloadingImpl
    from task.bean.sub_task_info import SubTaskInfo
    devices = {'d1': make_device('d1'), 'd2': make_device('d2')}
    sub = SubTaskInfo()
    picked = OffloadingImpl.random_strategy(devices, sub)
    assert picked in devices.values()
    assert OffloadingImpl.random_strategy({}, sub) is None


def test_offloading_direct_strategy():
    from offloading_brain.offloading_brain import OffloadingImpl
    from task.bean.sub_task_info import SubTaskInfo
    devices = {'d9': make_device('d9'), 'd5': make_device('d5'), 'd1': make_device('d1')}
    sub = SubTaskInfo()
    sub.set_sub_task_direct_allocate_id(2)
    # 1-based 按 device_no 升序 ['d1','d5','d9'] -> idx=2 -> 'd5'
    assert OffloadingImpl.direct_strategy(devices, sub) is devices['d5']
    # 越界 -> 回退随机（仍在设备集合内）
    sub.set_sub_task_direct_allocate_id(99)
    assert OffloadingImpl.direct_strategy(devices, sub) in devices.values()


def test_offloading_locality_and_score():
    from offloading_brain.offloading_brain import OffloadingImpl
    from task.bean.sub_task_info import SubTaskInfo
    devices = {'d1': make_device('d1', worker_slots=2), 'd2': make_device('d2', worker_slots=2)}
    sub = SubTaskInfo()
    sub.set_sub_task_id('s1')
    scored = OffloadingImpl.score_candidates(devices, sub, pred_info=[], reservations={})
    assert len(scored) == 2
    assert scored[0]['cost'] <= scored[1]['cost']   # 升序稳定排序
    assert OffloadingImpl.locality_strategy(devices, sub, pred_info=[], reservations={}) in devices.values()
    assert OffloadingImpl.locality_strategy({}, sub) is None


def test_offloading_plan_dag():
    from offloading_brain.offloading_brain import OffloadingImpl
    devices = {'d1': make_device('d1', worker_slots=2), 'd2': make_device('d2', worker_slots=2)}
    decoded = {'dagData': {'nodes': [
        {'id': 1, 'form': {'beforeNodes': [], 'directAllocationId': -1,
                           'taskInputInfo': {'size_mb': 0}}},
        {'id': 2, 'form': {'beforeNodes': [1], 'directAllocationId': -1,
                           'taskInputInfo': {'size_mb': 0}}},
    ]}}
    plan = OffloadingImpl.plan_dag(decoded, devices, reservations={})
    assert set(plan.keys()) == {1, 2}
    assert all(v in devices for v in plan.values())
    assert OffloadingImpl.plan_dag(decoded, {}) == {}   # 空集群 -> 空规划


def test_offloading_plan_dag_respects_direct():
    from offloading_brain.offloading_brain import OffloadingImpl
    devices = {'d1': make_device('d1', worker_slots=2), 'd2': make_device('d2', worker_slots=2)}
    decoded = {'dagData': {'nodes': [
        {'id': 1, 'form': {'beforeNodes': [], 'directAllocationId': 2,
                           'taskInputInfo': {'size_mb': 0}}},
    ]}}
    plan = OffloadingImpl.plan_dag(decoded, devices, reservations={})
    assert plan[1] == 'd2'   # directAllocationId=2 -> sorted(['d1','d2'])[1]
