from typing import Dict

from device.constant import configuration
from device.bean.devce_info import DeviceInfo
from device.interfaces.idle_device import IdleDevice


class IdleDeviceImpl(IdleDevice):
    @classmethod
    def idle_devices(cls, device_info: dict) -> bool:
        device_info_1 = DeviceInfo()
        device_info_1.set_device_from_dict(device_info)
        return device_info_1.get_work_status() < configuration.WORK_TASK_THRESHOLD and \
            float(device_info_1.get_used_cpu().strip("%")) < configuration.CPU_IDLE_THRESHOLD and \
            device_info_1.get_used_memory() / device_info_1.get_total_memory() < configuration.MEMORY_IDLE_THRESHOLD

    @classmethod
    def idle_device_list(cls, device_info_list: Dict[str, dict]):
        super().idle_device_list(device_info_list)
