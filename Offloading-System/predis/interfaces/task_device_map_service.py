from typing import List

from device.bean.devce_info import DeviceInfo
from task.bean.sub_task_info import SubTaskInfo


class TaskDeviceMapService:
    def query_task_info_work_device(self, task_id: str) -> List[DeviceInfo]:
        """
        查询工作任务所在的所有设备信息
        :param task_id: 任务编号
        :return: 工作任务所在的所有设备的列表
        """
        pass

    def query_work_task_info_work_device(self, sub_task_info_list: List[SubTaskInfo]) -> List[DeviceInfo]:
        """
        查询工作设备信息列表中的工作任务所在的设备列表
        :param sub_task_info_list: 工作任务信息列表
        :return: 工作设备信息列表中的工作任务所在的设备列表
        """
        pass

    def query_work_device_task_info(self, device_id: str) -> List[SubTaskInfo]:
        """
        查询工作设备中的工作任务列表
        :param device_id: 设备编号
        :return: deviceId下所有对应的子任务
        """
        pass

    def query_work_device_work_task_info(self, device_info_list: List[DeviceInfo]) -> List[SubTaskInfo]:
        """
        查询工作设备信息列表中的工作任务列表
        :param device_info_list: 设备信息列表
        :return: deviceInfoList下所有的任务
        """
        pass
