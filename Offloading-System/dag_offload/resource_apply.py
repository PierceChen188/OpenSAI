"""设备端资源档位应用（WP-07 · RSC）。

把 server（COM）下发的四维档位 ``_alloc`` 与设备实际租借到的 CPU 核号合并，
供 ``task_pool.run_task`` 在 worker 内施加（绑核 / 内存上限 / 存储记账）。

设计约束（§2.6.2 / §8.3「2.6」）：
    - ``DeviceClient.process_task`` 保留签名；
    - 复制完整 ``srv_alloc``（cores/mem_budget_mb/est_mem_mb/storage_budget_mb）
      后追加 ``core_ids``，**不覆盖存储/估算字段**（历史 bug：重建 ``_alloc`` 时
      用 ``{'core_ids':..., 'mem_budget_mb':...}`` 整体覆盖，丢失了
      ``storage_budget_mb`` / ``est_mem_mb`` / ``cores``）。

本模块为纯函数、无重型依赖，便于单测。
"""


def apply_core_ids(srv_alloc, leased_cores):
    """复制完整 ``srv_alloc`` 后追加实际绑核核号 ``core_ids``。

    参数：
        srv_alloc:    服务端下发的档位 dict（cores/mem_budget_mb/est_mem_mb/storage_budget_mb），
                      可为 None 或空 dict。
        leased_cores: CoreAllocator.lease 租借到的核号列表。

    返回：
        新 dict：保留 srv_alloc 全部字段，新增/覆盖 ``core_ids`` 键。
        不修改入参 srv_alloc。
    """
    merged = dict(srv_alloc or {})
    merged['core_ids'] = list(leased_cores) if leased_cores is not None else None
    return merged
