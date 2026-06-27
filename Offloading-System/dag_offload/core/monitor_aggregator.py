"""T1.3 聚合监测视图：把【设备心跳(Redis)】与【大脑账本(server 内存)】合并成一份权威快照。

从 `UserServer.aggregate_monitor_view` 抽出（P2）为**纯函数**：不读 Redis、不持状态，账本由调用方
在持锁下读取并传入。这样后续把账本归属给 Placement/Allocation 组件时，只需改调用处传入它们的快照。
行为与原方法完全一致。
"""
import time


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
