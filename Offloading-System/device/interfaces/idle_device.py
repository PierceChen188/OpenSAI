from typing import Dict


class IdleDevice:
    def idle_devices(self, device_info: dict) -> bool:
        """
        计算当前适合分配任务的单个空闲设备信息
        :param device_info: 设备信息
        :return: 是/否空闲且适合工作的设备
        """
        pass

    def idle_device_list(self, device_info_list: Dict[str, dict]):
        """
        计算当前适合分配任务的空闲设备信息列表
        :param device_info_list: 设备信息列表
        :return: 空闲且适合工作的设备列表
        """
        pass
