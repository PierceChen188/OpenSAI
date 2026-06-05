from __future__ import annotations

from typing import Dict


class DeviceMonitorService:

    def write_history_monitor(self, device_info: dict):
        """
        将设备历史监控信息写入缓存
        :param device_info: 设备信息
        :return: None
        """
        pass

    def read_history_monitor(self, device_no: str) -> Dict[int, dict] | None:
        """
        读取某段时间内一连串的设备历史监控信息
        :param device_no: 设备编号
        :return: [时间，设备信息]的字典
        """
        pass
