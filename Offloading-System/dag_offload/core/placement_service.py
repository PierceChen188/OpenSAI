"""集群管理·健康预筛（6a）：放置前从候选设备中剔除过载设备（CPU/内存/队列超阈值）。

从 `UserServer` 抽出（P4）。放置算法本体在 `offloading_brain.OffloadingImpl`；放置预留账本
（reservations/placement_plan）仍由协调器（UserServer）在 `data_lock` 下维护——与派发/完成/回收
同锁，保证账本一致，避免高风险的跨锁重构。本模块只放**纯决策**部分（健康预筛）。
行为与原 `_healthy_devices` 完全一致。
"""
import logging
import os

from device.implements.idle_device_impl import IdleDeviceImpl


class PlacementService:
    @staticmethod
    def healthy(device_info_list):
        """6a 资源健康预筛：过滤掉过载设备（复用 IdleDeviceImpl 阈值）。
        受 OPENSAI_HEALTH_FILTER 控制（默认开）。安全兜底：过滤后为空（全部过载）→退回全量，绝不阻塞放置。"""
        if not device_info_list:
            return device_info_list
        if os.environ.get('OPENSAI_HEALTH_FILTER', '1') in ('0', '', 'false', 'False'):
            return device_info_list
        healthy = {}
        for dno, d in device_info_list.items():
            try:
                if IdleDeviceImpl.idle_devices(d):
                    healthy[dno] = d
            except Exception:
                healthy[dno] = d        # 心跳字段不全 -> 不误杀，保留候选
        if not healthy:
            logging.warning('[health] 所有设备均过载，资源预筛退回全量(不阻塞放置)')
            return device_info_list
        if len(healthy) < len(device_info_list):
            logging.info(f'[health] 资源预筛: {len(healthy)}/{len(device_info_list)} 台健康设备进入放置')
        return healthy
