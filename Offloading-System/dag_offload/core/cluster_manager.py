"""集群管理·放置目标校验与策略查询（§2.4.4）。

本模块承接设计文档 §2.4「集群管理组件」中「放置目标与策略接口」部分，提供以下**纯函数**：
  - ``validate_placement_targets``  校验 directAllocationId 类型/正整数，有快照时校验 allocation_index。
  - ``resolve_allocation_index``    按 device_id 升序的稳定 1-based 序号解析目标设备，越界抛 DeviceNotFoundError。
  - ``handle_devices_info``         返回兼容的稀疏在线设备能力（legacy camelCase 外壳），不冒充完整 MonitorView。
  - ``handle_policies_info``        返回服务端实际支持的 plan/smart/random 策略列表。

设计原则（与 MonitorAggregator / PlacementService 一致）：
  - **纯函数**：不读 Redis、不持状态、不发网络；设备快照/账本由调用方持锁读取后传入，便于单测与复用。
  - **稳定排序**：allocation_index 的语义统一为「按 device_id（即 device_no）升序后的 1-based 序号」，
    与 ``offloading_brain.direct_strategy`` 及 MonitorAggregator.cluster_status 保持一致。
  - **健壮性**：设备字段缺失 / 空集群 / 坏数据一律不抛异常，而是安全降级（见各函数 docstring）。

注：本模块的校验/解析逻辑最终可由 ``opensai_contracts``（WP-01）复用；此处先落地在 CLU 服务端，
保持对 WP-01 契约（字段名/语义）的严格对齐。
"""
from __future__ import annotations

__all__ = [
    'ClusterManager',
    'DeviceNotFoundError',
    'SUPPORTED_POLICIES',
    'DEFAULT_POLICY',
]

# 服务端实际支持的放置策略（§2.4.4 handle_policies_info）。direct 是 per-node 钉选（directAllocationId），
# 不是全局策略，故不在列表中。
SUPPORTED_POLICIES = ["plan", "smart", "random"]
DEFAULT_POLICY = "plan"


class DeviceNotFoundError(LookupError):
    """resolve_allocation_index 越界 / 非法索引时抛出（§2.4.4）。"""


class ClusterManager:
    @staticmethod
    def sorted_device_ids(devices) -> list[str]:
        """返回按 device_id 升序的稳定设备号列表（allocation_index 的唯一排序依据）。

        设备号即 device_no（server 侧唯一标识）。空 / None / 含非 dict 元素时安全返回空列表。
        """
        if not devices:
            return []
        if isinstance(devices, dict):
            return sorted(str(k) for k in devices.keys())
        # 兼容 list[DeviceInfo]（未来契约形态）：优先取 device_id，否则取 device_no
        ids = []
        for d in devices:
            if isinstance(d, dict):
                did = d.get('device_id') or d.get('device_no')
            else:
                did = getattr(d, 'device_id', None) or getattr(d, 'device_no', None)
            if did is not None:
                ids.append(str(did))
        return sorted(ids)

    # ── 放置目标校验（§2.4.4）─────────────────────────────────────────────

    @staticmethod
    def resolve_allocation_index(index, devices=None) -> str:
        """按 device_id 升序的稳定快照内 1-based 序号解析目标设备，返回 device_id(str)。

        越界（<1 或 >len(devices)）或非整数索引 → 抛 DeviceNotFoundError。
        """
        ids = ClusterManager.sorted_device_ids(devices)
        if isinstance(index, bool) or not isinstance(index, int):
            raise DeviceNotFoundError(f"allocation_index 必须是整数，收到 {index!r}")
        if not (1 <= index <= len(ids)):
            raise DeviceNotFoundError(
                f"allocation_index {index} 越界（有效范围 1..{len(ids)}）")
        return ids[index - 1]

    @staticmethod
    def validate_placement_targets(dag, devices=None) -> list[dict]:
        """校验 DAG 中每个节点的 directAllocationId，返回 list[ValidationIssue]（空列表 = 全部合法）。

        规则（§2.4.4）：
          1. directAllocationId 缺省 / None / -1 → 视为「未固定设备」，跳过（合法）。
          2. 其余值必须是正整数（int，或可解析为正整数的字符串）；0、负数、布尔、非数值 → 记
             INVALID_DIRECT_ALLOCATION。
          3. 有设备快照时，正整数索引须落在 [1, len(devices)]；越界 → ALLOCATION_INDEX_OUT_OF_RANGE。

        入参 ``dag`` 兼容两种形态：``{'dagData': {'nodes': [...]}}``（服务端 wire 格式）或
        ``{'nodes': [...]}``（SDK 契约格式）。每节点取 ``form.directAllocationId``（服务端）或
        顶层 ``directAllocationId``（SDK），后者优先。

        ValidationIssue 为 dict：``{code, node_id, value, message}``。
        """
        issues: list[dict] = []
        if not isinstance(dag, dict):
            return issues
        nodes = dag.get('nodes')
        if not isinstance(nodes, list):
            d = dag.get('dagData')
            if isinstance(d, dict):
                nodes = d.get('nodes')
        if not isinstance(nodes, list):
            return issues

        ids = ClusterManager.sorted_device_ids(devices) if devices is not None else None

        for node in nodes:
            if not isinstance(node, dict):
                continue
            nid = node.get('id')
            form = node.get('form') if isinstance(node.get('form'), dict) else {}
            # 服务端 wire 格式：form.directAllocationId；SDK 契约格式：顶层 directAllocationId。
            dai = form.get('directAllocationId', node.get('directAllocationId', -1))

            if dai is None or dai == -1:
                continue

            # 字符串容错（历史实现允许字符串 "2"）
            if isinstance(dai, str):
                try:
                    dai = int(dai)
                except (TypeError, ValueError):
                    issues.append({
                        'code': 'INVALID_DIRECT_ALLOCATION',
                        'node_id': nid,
                        'value': dai,
                        'message': f'directAllocationId 必须是正整数，收到非数值字符串 {dai!r}',
                    })
                    continue
            if isinstance(dai, bool) or not isinstance(dai, int):
                issues.append({
                    'code': 'INVALID_DIRECT_ALLOCATION',
                    'node_id': nid,
                    'value': dai,
                    'message': f'directAllocationId 必须是正整数，收到 {type(dai).__name__}',
                })
                continue
            if dai <= 0:
                issues.append({
                    'code': 'INVALID_DIRECT_ALLOCATION',
                    'node_id': nid,
                    'value': dai,
                    'message': f'directAllocationId 必须是正整数（>=1），收到 {dai}',
                })
                continue
            # 有快照时校验索引范围
            if ids is not None and dai > len(ids):
                issues.append({
                    'code': 'ALLOCATION_INDEX_OUT_OF_RANGE',
                    'node_id': nid,
                    'value': dai,
                    'message': f'allocation_index {dai} 越界（在线设备数 {len(ids)}）',
                })
        return issues

    # ── 设备 / 策略查询（§2.4.4）─────────────────────────────────────────

    @staticmethod
    def handle_devices_info(devices=None, request_id=None) -> dict:
        """返回兼容的稀疏在线设备能力（legacy camelCase 外壳），不冒充完整 MonitorView。

        仅回传设备心跳侧字段（不含 reservations/committed_bw/committed_storage 等大脑账本）。
        单台设备字段缺失时用安全默认值，绝不因一台坏数据丢弃整表（部分失败鲁棒）。
        """
        devices = devices or {}
        devices_data = []
        for device_id in ClusterManager.sorted_device_ids(devices):
            d = devices[device_id] if isinstance(devices, dict) else device_id
            if not isinstance(d, dict):
                continue
            devices_data.append({
                "deviceNo": d.get('device_name') or d.get('device_no') or device_id,
                "usedCpu": d.get('used_cpu', '0%'),
                "coreNum": d.get('core_num', 0),
                "idleMemory": d.get('idle_memory', 0),
                "usedMemory": d.get('used_memory', 0),
                "totalMemory": d.get('total_memory', 0),
                "workStatus": d.get('work_status', 0),
                "lng": d.get('lng', 0),
                "lat": d.get('lat', 0),
                "orderValue": d.get('order_value', -1),
                "ipAddress": d.get('ip_address', ''),
                "throughput": d.get('throughput', 0),
                "bandwidth": d.get('bandwidth', 0),
                "networkUsage": d.get('network_usage', 0),
                "delay": d.get('delay', 0),
                "port": d.get('port', -1),
            })
        resp = {"devicesInfo": {"devices": devices_data}}
        if request_id is not None:
            resp["request_id"] = request_id
        return resp

    @staticmethod
    def handle_policies_info(request_id=None) -> dict:
        """返回服务端实际支持的 plan/smart/random 策略列表（不再是 ``default`` 占位）。"""
        resp = {"policiesInfo": {"policies": list(SUPPORTED_POLICIES)}}
        if request_id is not None:
            resp["request_id"] = request_id
        return resp
