from __future__ import annotations

from typing import List, Dict


class WorkTaskService:

    def write_work_task_to_cache(self, sub_task_info: dict):
        """
        将正在工作的子任务信息写入缓存
        :param sub_task_info: 子任务信息
        :return: None
        """
        pass

    def read_work_task_info(self) -> Dict[str, dict] | None:
        """
        将正在工作的子任务信息从缓存读出
        :return: 所有任务下的所有子任务
        """
        pass

    def add_work_task_info(self, sub_task_info: dict) -> bool:
        """
        添加一条工作的子任务信息
        :param sub_task_info: 子任务信息
        :return: 是否添加成功True or False
        """
        pass

    def delete_work_task_info(self, sub_task_info: dict) -> bool:
        """
        删除一条工作的子任务信息
        :param sub_task_info: 子任务信息
        :return: 是否删除成功True or False
        """
        pass

    def delete_work_task_info_by_id(self, task_id: str, sub_task_id: str) -> bool:
        """
        删除一条工作的子任务信息
        :param task_id: 任务编号
        :param sub_task_id: 子任务编号
        :return: 是否删除成功True or False
        """
        pass

    def modify_work_task_info(self, sub_task_info: dict) -> bool:
        """
        修改一条工作的子任务信息
        :param sub_task_info: 子任务信息
        :return: 是否修改成功True or False
        """
        pass

    def query_work_task_info(self, sub_task_info: dict) -> dict:
        """
        查询一条工作的子任务信息
        :param sub_task_info: 子任务信息
        :return: taskId下subTaskID对应的子任务
        """
        pass

    def query_work_task_info_by_id(self, task_id: str, sub_task_id: str) -> dict:
        """
        查询一条工作的子任务信息
        :param task_id: 任务编号
        :param sub_task_id: 子任务编号
        :return: taskId下subTaskID对应的子任务
        """
        pass

    def add_work_task_list(self, sub_task_info_list: List[dict]) -> bool:
        """
        添加一个工作的任务信息
        :param sub_task_info_list: 任务信息
        :return: 是否添加成功True or False
        """
        pass

    def delete_work_task_list(self, sub_task_info_list: List[dict]) -> bool:
        """
        删除一条工作的任务信息
        :param sub_task_info_list: 任务信息
        :return: 是否删除成功True or False
        """
        pass

    def delete_work_task_list_by_id(self, task_id: str) -> bool:
        """
        删除一条工作的子任务信息
        :param task_id: 任务编号
        :return: 是否删除成功True or False
        """
        pass

    def modify_work_task_list(self, sub_task_info_list: List[dict]) -> bool:
        """
        修改一条工作的任务信息
        :param sub_task_info_list: 任务信息
        :return: 是否修改成功True or False
        """
        pass

    def query_work_task_list(self, sub_task_info_list: List[dict]) -> List[dict] | None:
        """
        查询一条工作的任务信息
        :param sub_task_info_list: 任务信息
        :return: taskId下所有subTaskID对应的子任务
        """
        pass

    def query_work_task_list_by_id(self, task_id: str) -> List[dict] | None:
        """
        查询task_id下的工作子任务信息
        :param task_id: 任务编号
        :return: taskId下所有subTaskID对应的子任务，以list形式返回
        """
        pass

    def query_work_task_num(self, task_id: str) -> int:
        """
        获取taskID任务下子任务个数
        :param task_id: 任务编号
        :return: taskID任务下子任务个数
        """
        pass

    def query_work_task_key_set_num(self) -> int:
        """
        判断Cache中当前有几个工作子任务信息
        :return: Cache中当前有几个工作子任务信息
        """
        pass

    def obtain_could_parameter(self, task_id: str) -> Dict[str, str]:
        """
        获取当前执行任务云端的相关计算参数
        :param task_id: 任务编号
        :return: 获取当前执行任务云端的相关计算参数列表
        """
        pass
