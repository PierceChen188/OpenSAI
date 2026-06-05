from __future__ import annotations

from typing import Dict, List

from config.sub_task_status import SubTaskStatus
from config.work_status import WorkStatus


class TaskStateService:
    def write_task_to_cache(self, task_state_info: dict):
        """
        将收集到的任务状态信息写入缓存
        :param task_state_info: 任务状态信息
        :return: None
        """
        pass

    def read_task_info(self) -> Dict[str, dict] | None:
        """
        将redis中的任务信息取出
        :return: 任务状态信息列表
        """
        pass

    def read_task_state_info(self, task_id: str, sub_task_id: str) -> Dict[str, dict]:
        """
        读取指定taskId和subTaskId的任务状态
        :param task_id: 任务编号
        :param sub_task_id: 子任务编号
        :return: 以dict形式返回指定任务信息
        """
        pass

    def read_device_task_state(self, device_id: str) -> None:
        """
        通过设备ID查找设备上面的任务运行状态信息
        :param device_id: 设备编号
        :return: 以list形式返回所有任务运行状态信息
        """
        pass

    def read_task_info_to_web(self) -> Dict[str, dict] | None:
        """
        将redis中的任务信息取出到前端展示
        :return: 以list形式返回所有任务运行状态信息
        """
        pass

    def get_sub_task_num(self, task_id: str) -> int:
        """
        获取cache中task_id下已经完成的子任务个数
        :param task_id: 任务编号
        :return: None
        """
        pass

    def get_task_from_cache(self, task_id: str) -> Dict[str, dict]:
        """
        获取redis中子任务个数
        :param task_id: 任务编号
        :return: 任务状态列表
        """
        pass

    def get_task_status(self, task_id: str) -> WorkStatus:
        """
        获取redis中特定任务状态
        :param task_id: 任务编号
        :return: 任务状态
        """
        pass

    def get_sub_task_status(self, task_id: str, sub_task_id: str) -> SubTaskStatus:
        """
        获取redis中特定子任务状态
        :param task_id: 任务编号
        :param sub_task_id: 子任务编号
        :return: 当前查询子任务的任务状态和子任务状态
        """
        pass

    def get_task_status_is_finish(self, task_id: str) -> bool:
        """
        获取redis中特定任务状态
        :param task_id: 任务编号
        :return: 任务状态
        """
        pass

    def get_sub_task_status_is_finish(self, task_id: str, sub_task_id: str) -> bool:
        """
        获取redis中特定子任务状态
        :param task_id: 任务编号
        :param sub_task_id: 子任务编号
        :return: 当前查询子任务的任务状态和子任务状态
        """
        pass

    def get_finish_task(self) -> List[str]:
        """
        获取redis中任务完结的所有子任务ID
        :return: 任务完结的所有子任务ID列表
        """
        pass

    def change_sub_task_status(self, task_id: str, sub_task_id: str, sub_task_status: SubTaskStatus):
        """
        修改redis中子任务状态
        :param task_id: 任务编号
        :param sub_task_id: 子任务编号
        :param sub_task_status: 子任务状态
        :return: None
        """
        pass

    def change_task_status(self, task_id: str, work_status: WorkStatus):
        """
        修改redis中任务状态
        :param task_id: 任务编号
        :param work_status: 工作状态
        :return: None
        """
        pass

    def delete_task_online_info(self, task_id: str):
        """
        删除任务状态
        :param task_id: 需要删除任务状态编号
        :return: None
        """
        pass

    def query_task_state_info(self, task_id: str, sub_task_id: str) -> Dict[str, dict]:
        """
        查询任务状态信息
        :param task_id: 任务编号
        :param sub_task_id: 子任务编号
        :return: 以dict形式返回任务状态信息
        """
        pass
