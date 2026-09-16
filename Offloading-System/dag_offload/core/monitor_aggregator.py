"""T1.3 聚合监测视图：把【设备心跳(Redis)】与【大脑账本(server 内存)】合并成一份权威快照。

从 `UserServer.aggregate_monitor_view` 抽出（P2）为**纯函数**：不读 Redis、不持状态，账本由调用方
在持锁下读取并传入。这样后续把账本归属给 Placement/Allocation 组件时，只需改调用处传入它们的快照。
行为与原方法完全一致。
"""
import time

from core.cluster_manager import ClusterManager


class MonitorAggregator:
    @staticmethod
    def build_view(devices, reservations, committed_bw, committed_storage_mb, inflight, algo_history):
        """devices=设备心跳字典(device_no->dict)；其余为大脑账本。返回 OPENSAI_MONITOR_VIEW 结构。"""
        view_devs = {}
        for dno, d in (devices or {}).items():
            ct = int(d.get('cores_total') or 0)
            cc = int(d.get('cores_committed') or 0)
            view_devs[dno] = {
                'used_cpu': d.get('used_cpu'),
                'idle_memory': d.get('idle_memory'), 'total_memory': d.get('total_memory'),
                'work_status': d.get('work_status', 0), 'worker_slots': d.get('worker_slots', 1),
                'cores_total': ct, 'cores_committed': cc, 'cores_available': max(0, ct - cc),
                'mem_committed_mb': d.get('mem_committed_mb', 0), 'delay': d.get('delay', 0),
                'data_port': d.get('data_port'), 'ip_address': d.get('ip_address'),
                'observed_links': d.get('observed_links') or {},
                'storage_total_mb': int(d.get('storage_total_mb') or 0),               # 设备磁盘(心跳)
                'storage_free_mb': int(d.get('storage_free_mb') or 0),
                # —— 大脑账本(只在 server 内存、Redis 心跳里没有) ——
                'placement_reserved': int((reservations or {}).get(dno, 0)),               # T2.1 放置预留
                'bw_committed_mbps': round(float((committed_bw or {}).get(dno, 0.0)), 2),   # T3.2-A 已承诺出口带宽
                'storage_committed_mb': round(float((committed_storage_mb or {}).get(dno, 0.0)), 0),  # 已承诺存储档位
            }
        return {
            'devices': view_devs,
            'inflight_tasks': len(inflight or {}),              # C1 在飞(已派发未完成)子任务数
            'algo_history': {a: {k: (round(v, 1) if isinstance(v, float) else v)
                                 for k, v in h.items()} for a, h in (algo_history or {}).items()},  # B1 画像
            'ts': time.time(),
        }

    # ── CLU-1.1: 设备查询接口 ──────────────────────────────────────────────

    @staticmethod
    def get_monitor_view(devices=None, reservations=None, committed_bw=None,
                         committed_storage_mb=None, inflight=None, algo_history=None):
        """返回 OPENSAI_MONITOR_VIEW 完整数据。

        调用方（通常是 UserServer）应在持锁下传入大脑账本快照。
        返回 dict{devices, inflight_tasks, algo_history, ts}。
        验收标准：返回 OPENSAI_MONITOR_VIEW 完整数据。
        """
        return MonitorAggregator.build_view(
            devices=devices,
            reservations=reservations,
            committed_bw=committed_bw,
            committed_storage_mb=committed_storage_mb,
            inflight=inflight,
            algo_history=algo_history,
        )

    # ── CLU-1.2: 集群状态接口 ──────────────────────────────────────────────

    @staticmethod
    def get_cluster_health(devices=None, reservations=None, committed_bw=None,
                           committed_storage_mb=None, inflight=None, algo_history=None):
        """返回集群健康状态摘要 dict。

        包含：online_devices, total_slots, used_slots, utilization_pct, anomalies。
        验收标准：返回准确集群状态摘要。
        """
        devs = devices or {}
        online = len(devs)
        total_slots = 0
        used_slots = 0
        anomalies = []

        for dno, d in devs.items():
            if not isinstance(d, dict):
                continue
            ws = int(d.get('worker_slots', 1))
            total_slots += ws
            used_slots += int(d.get('work_status', 0))
            delay = d.get('delay', 0)
            if isinstance(delay, (int, float)) and delay > 500:
                anomalies.append({'device': dno, 'type': 'high_delay', 'value_ms': delay})
            cpu = d.get('used_cpu', '0%')
            try:
                cpu_val = float(str(cpu).rstrip('%'))
            except (ValueError, AttributeError):
                cpu_val = 0
            if cpu_val > 90:
                anomalies.append({'device': dno, 'type': 'high_cpu', 'value_pct': cpu_val})

        # Count in-flight tasks
        inflight_count = len(inflight or {})

        return {
            'online_devices': online,
            'total_slots': total_slots,
            'used_slots': used_slots,
            'utilization_pct': round(used_slots / total_slots * 100, 1) if total_slots else 0,
            'inflight_tasks': inflight_count,
            'anomalies': anomalies,
            'ts': time.time(),
        }

    # ── CLU-1.2b: 集群状态接口（§2.4.1 ClusterStatus）─────────────────────

    @staticmethod
    def _parse_cpu(used_cpu) -> float:
        """把 '45%' 之类的 used_cpu 解析成 0-100 浮点数；非法/缺失返回 0.0。"""
        if isinstance(used_cpu, (int, float)):
            return float(used_cpu)
        try:
            return float(str(used_cpu).strip().rstrip('%'))
        except (ValueError, AttributeError, TypeError):
            return 0.0

    @staticmethod
    def cluster_status(devices=None, reservations=None, committed_bw=None,
                       committed_storage_mb=None, inflight=None, algo_history=None,
                       ts=None, max_age_s=30.0):
        """§2.4.1 ClusterStatus：聚合设备、资源账本、链路与可选画像，返回集群权威快照。

        返回字段：
          online_count / total_slots / used_slots
          health：{status, utilization_pct, online_devices, inflight_tasks, anomalies}
          devices：规范化 DeviceInfo 列表（device_id/allocation_index/ip/status/cpu_percent/
                   slots_used/slots_total/memory_mib/storage_mib/snapshot_ts/view_stale）
          links：观测链路摘要（src_device/endpoint/mbps/last_ms）
          alerts：异常告警（同 health.anomalies）
          stale：bool（视图年龄 > max_age_s 时为 True）
          ts：本快照时间戳

        设备缺失/空集群 → online_count=0、status='empty'、devices=[]，不抛异常。
        """
        devs = devices or {}
        now = ts if ts is not None else time.time()
        stale = (time.time() - now) > max_age_s if max_age_s is not None else False

        online_count = 0
        total_slots = 0
        used_slots = 0
        anomalies = []
        norm_devices = []
        links = []

        for idx, dno in enumerate(ClusterManager.sorted_device_ids(devs), start=1):
            d = devs[dno]
            if not isinstance(d, dict):
                continue
            online_count += 1
            ws = int(d.get('worker_slots', 1) or 1)
            used = int(d.get('work_status', 0) or 0)
            total_slots += ws
            used_slots += used
            cpu = MonitorAggregator._parse_cpu(d.get('used_cpu'))
            delay = d.get('delay', 0)
            if isinstance(delay, (int, float)) and delay > 500:
                anomalies.append({'device': dno, 'type': 'high_delay', 'value_ms': delay})
            if cpu > 90:
                anomalies.append({'device': dno, 'type': 'high_cpu', 'value_pct': cpu})
            for ep, lk in (d.get('observed_links') or {}).items():
                links.append({
                    'src_device': dno,
                    'endpoint': ep,
                    'mbps': lk.get('mbps') if isinstance(lk, dict) else None,
                    'last_ms': lk.get('last_ms') if isinstance(lk, dict) else None,
                })
            norm_devices.append({
                'device_id': dno,
                'allocation_index': idx,
                'ip': d.get('ip_address'),
                'status': 'online',
                'cpu_percent': cpu,
                'slots_used': used,
                'slots_total': ws,
                'memory_mib': d.get('total_memory'),
                'storage_mib': int(d.get('storage_total_mb') or 0),
                'snapshot_ts': now,
                'view_stale': stale,
            })

        utilization = round(used_slots / total_slots * 100, 1) if total_slots else 0
        if not online_count:
            status = 'empty'
        elif len(anomalies) >= online_count:
            status = 'critical'
        elif anomalies:
            status = 'degraded'
        else:
            status = 'healthy'

        return {
            'online_count': online_count,
            'total_slots': total_slots,
            'used_slots': used_slots,
            'health': {
                'status': status,
                'utilization_pct': utilization,
                'online_devices': online_count,
                'inflight_tasks': len(inflight or {}),
                'anomalies': anomalies,
            },
            'devices': norm_devices,
            'links': links,
            'alerts': anomalies,
            'stale': stale,
            'ts': now,
        }
