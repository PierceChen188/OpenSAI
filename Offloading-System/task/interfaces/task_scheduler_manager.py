from typing import List

from device.bean.devce_info import DeviceInfo
from task.bean.sub_task_info import SubTaskInfo
from task.bean.task_info import TaskInfo


class TaskSchedulerManager:
    def send_subtask_to_device(self, sub_task_info: SubTaskInfo, device_info: DeviceInfo) -> bool:
        """

        :param sub_task_info: 子任务
        :param device_info: 映射设备信息
        :return: 发送成功（true）或不成功（false）信号
        """
        pass

    def send_task_to_device(self, task_info: TaskInfo) -> List[bool]:
        """

        :param task_info: 任务信息
        :return: 发送成功（true）或不成功（false）信号列表
        """
        pass

    def send_multi_task_to_device(self, multi_task_info) -> List[bool]:
        """

        :param multi_task_info: 任务集合
        :return: 发送成功（true）或不成功（false）信号列表
        """
        pass
