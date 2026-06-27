"""适配优化·任务画像（B1）：用设备上报的 metrics 以 EWMA 更新算法历史画像，供四维资源自动定档。

从 `UserServer` 抽出（P5）。注意：四维资源档位的**计算**与**预算账本**（committed_bw / committed_storage_mb
及其预留）与派发 / 完成 / 回收临界区强耦合（同一把 data_lock），暂仍由协调器（UserServer）维护，
以避免高风险的跨锁重构；本模块先放可干净分离的**画像更新**逻辑。
行为与原 `_update_algo_history` 完全一致（就地更新传入的 algo_history dict）。
"""


class AllocationService:
    @staticmethod
    def update_profile(algo_history: dict, algo: str, metrics: dict) -> None:
        """B1：用设备上报的 metrics(EWMA) 更新算法历史画像。就地更新传入的 algo_history。"""
        if not algo or not isinstance(metrics, dict):
            return
        h = algo_history.setdefault(algo, {'peak_mem_mb': 0.0, 'task_mem_mb': 0.0,
                                           'proc_secs': 0.0, 'task_storage_mb': 0.0, 'n': 0})
        a = 0.5
        for k in ('peak_mem_mb', 'task_mem_mb', 'proc_secs', 'task_storage_mb'):   # 增记产出存储(供自动定档)
            v = metrics.get(k)
            if v:
                h[k] = float(v) if h['n'] == 0 else a * float(v) + (1 - a) * h[k]
        h['n'] += 1
