from typing import List

from task.bean.task_info import TaskInfo


class TaskCenter:
    def select_device_num(self, device_num: int, device_use_per: float) -> int:
        """
        以某个百分比乘以从Cache中得到的设备数量并向上取整后的设备个数
        :param device_num: 从Cache中得到的设备数量
        :param device_use_per: 设备利用百分比
        :return: 设备个数
        """
        pass

    def select_task_device_num(self, device_num, task_info) -> int:
        """
        能够被子数据文件个数整除的设备数量
        :param device_num: 设备数量
        :param task_info: 当前传入的带有数据的任务
        :return: 设备数量
        """
        pass

    def assemble_subtask_with_subdata(self, task_info: TaskInfo, data_index: int, device_num: int) -> list:
        """

        :param task_info: 当前执行的任务信息
        :param data_index: 第几个数据大类
        :param device_num: 实际执行本任务的设备数量
        :return: 拆分好的子数据列表
        """
        pass

    def assemble_subtask_with_subdata2(self, task_info: TaskInfo, data_index: int, device_num: int) -> list:
        """

        :param task_info: 当前执行的任务信息
        :param data_index: 第几个数据大类
        :param device_num: 实际执行本任务的设备数量
        :return: 拆分好的子数据列表
        """
        pass

    def assemble_subtask_with_data(self, task_info: TaskInfo, device_num: int) -> list:
        """

        :param task_info: 当前执行的任务信息
        :param device_num: 实际执行本任务的设备数量
        :return: 封装好的子任务列表
        """
        pass

    def assemble_subtask_nodata(self, task_info: TaskInfo, device_num: int) -> list:
        """

        :param task_info: 当前执行的任务信息
        :param device_num: 实际执行本任务的设备数量
        :return: 封装好的子任务列表
        """
        pass

    def task_partition(self, task_info: TaskInfo) -> list:
        """
        任务分解
        :param task_info: 当前执行的任务信息
        :return: 分解后的子任务队列
        """
        pass

    def task_map(self, task_info: TaskInfo) -> List[bool]:
        """
        任务的MAP过程
        :param task_info: 当前执行的单任务信息
        :return: 发送事件（成功或失败）队列
        """
        pass
