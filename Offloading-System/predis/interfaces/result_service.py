from __future__ import annotations

from typing import Dict, List


class ResultService:
    def get_num_revc_sub_task(self, task_id: str) -> int:
        """
        查询收到的总结果数量
        :param task_id: 任务编号
        :return: 收到总结果数量
        """
        pass
    def read_result_info_by_result_id(self, task_id: str):
        """
        读取任务编号下所有的子任务结果
        :param task_id: 任务编号
        :return: 以list形式返回所有子任务结果
        """
        pass

    def read_result_info(self) -> List[dict]:
        """
        读取Cache资源池中所结果信息
        :return: 以list形式返回所有子任务结果
        """
        pass

    def write_result_to_cache(self, result_info: dict):
        """
        将子任务结果信息信息写入缓存
        :param result_info: 子任务结果信息
        :return: None
        """
        pass

    def add_result_info(self, result_info: dict) -> bool:
        """
        添加一条子任务结果信息
        :param result_info: 子任务结果信息
        :return: 是否添加结果信息成功
        """
        pass

    def delete_result_info(self, result_info: dict) -> bool:
        """
        删除一条子任务结果信息
        :param result_info: 子任务结果信息
        :return:  是否删除结果信息成功
        """

    def delete_result_info_by_id(self, task_id: str, sub_task_id: str) -> bool:
        """
        删除一条子任务结果信息
        :param task_id: 任务编号
        :param sub_task_id: 子任务编号
        :return: 是否删除结果信息成功
        """
        pass

    def modify_result_info(self, result_info: dict) -> dict | bool:
        """
        修改一条子任务结果信息
        :param result_info: 子任务结果信息
        :return: 是否修改结果信息成功
        """
        pass

    def query_result_info(self, result_info: dict) -> dict:
        """
        查询一个子任务结果信息
        :param result_info: 子任务结果信息
        :return: 查询到的结果信息
        """
        pass

    def query_result_info_by_id(self, task_id: str, sub_task_id: str) -> dict:
        """
        查询一个子任务结果信息
        :param task_id: 任务编号
        :param sub_task_id: 子任务编号
        :return: taskId下subTaskID对应的子任务
        """
        pass

    def add_result_list(self, result_info_list: Dict[str, dict]) -> bool:
        """
        添加一个结果信息列表
        :param result_info_list: 结果信息列表
        :return: None
        """
        pass

    def delete_result_list(self, result_info_list: List[dict]) -> bool:
        """
        删除一个结果信息列表
        :param result_info_list: 结果信息列表
        :return: None
        """
        pass

    def delete_result_list_by_id(self, task_id: str) -> bool:
        """
        删除对应taskId子任务结果信息
        :param task_id: 任务编号
        :return: None
        """
        pass

    def modify_result_list(self, result_info_list: List[dict]) -> bool:
        """
        修改一条结果信息列表
        :param result_info_list: 结果信息列表
        :return: None
        """
        pass

    def query_result_list(self, result_info_list: List[dict]) -> List[dict] | None:
        """
        查询一条工作的任务信息
        :param result_info_list: 结果信息列表
        :return: None
        """
        pass

    def query_result_list_by_id(self, task_id: str) -> List[dict] | None:
        """
        查询一个taskId下所有子任务结果
        :param task_id: 任务编号
        :return: task_id对应所有子任务信息
        """
        pass

    def query_result_num(self, task_id: str) -> int:
        """
        获取task_id任务下子任务结果个数
        :param task_id: 任务编号
        :return: task_id任务下子任务结果个数
        """
        pass

    def query_result_key_set_num(self) -> int:
        """
        判断Cache中当前有几个结果信息
        :return: Cache中当前有几个结果信息
        """
        pass

    def write_result_to_web(self, result_info_web: dict):
        """
        将收集到的计算结果信息写入缓存,间接提供给web前端展示
        :param result_info_web: 前端显示的结果信息格式
        :return: None
        """
        pass

    def read_result_to_web(self) -> Dict[str, dict] | None:
        """
        将Cache中的计算结果信息取出到Web前端展示
        :return: 任务结果信息
        """
        pass

    def write_progress_to_web(self, result_info: dict):
        """
        将收集到的子任务计算进度写入缓存,间接提供给web前端展示
        :param result_info: 前端显示的结果信息格式
        :return:
        """
        pass

    def read_progress_to_web(self) -> Dict[str, float] | None:
        """
        将Cache中的子任务计算进度取出到Web前端展示
        :return: 子任务计算进度
        """
        pass

    def query_progress(self, result_info: dict) -> float:
        """
        读取Cache最新的计算进度用于对比
        :param result_info: 前端显示的结果信息格式
        :return: 子任务计算进度
        """
        pass

    def write_progress_to_lbfo(self, result_info: dict):
        """
        将Cache中的子任务计算结果缓存存入Cache用以传给LBFO调度
        :param result_info: 子任务结果信息
        :return: None
        """
        pass

    def read_progress_to_lbfo(self, task_id: str, sub_task_id: str) -> dict:
        """
        查询结果进度信息给LBFO调度使用
        :param task_id: 任务编号
        :param sub_task_id: 子任务编号
        :return: 结果进度信息
        """
        pass
