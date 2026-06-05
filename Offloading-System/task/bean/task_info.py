from __future__ import annotations

from typing import List

from config.work_status import WorkStatus
from result.bean.result_info import ResultInfo
from task.bean.algorithm_info import AlgorithmInfo
from task.bean.data_info import DataInfo


class TaskInfo:
    def __init__(self):
        # 任务编号
        self.__task_id = ""
        # 任务名称
        self.__task_name = ""
        # 任务位置
        self.__task_location = ""
        # 任务版本
        self.__task_version = ""
        # 任务上传者
        self.__task_uploader = ""
        # 任务算法
        self.__task_algorithm = None
        # 任务数据
        self.__task_data_list = None
        # 任务结果信息
        self.__task_result_info = None
        # 任务状态
        self.__task_status = WorkStatus.UN_WORK
        # 子任务编号集
        self.__sub_task_id = []
        # 设备性能选择：
        # (1)高性能设备：device_param{key,value} =<"high_per",0/1>
        # (2)低时延：device_param{key,value} = < "Low_latency", 0 / 1 >
        # (3)大内存：device_param{key,value} = < "large_mem", 0 / 1 >
        # (4)设备数量：device_param{key,value} = <"device_num", x>
        self.__device_param = {}

    def set_task_info(self, task_id: str, task_name: str, task_location: str, task_version: str, task_uploader: str,
                      task_algorithm: AlgorithmInfo | None, task_data_list: List[DataInfo],
                      task_result_info: ResultInfo | None, task_status: WorkStatus, sub_task_id: str,
                      device_param: dict):
        self.__task_id = task_id
        self.__task_name = task_name
        self.__task_location = task_location
        self.__task_version = task_version
        self.__task_uploader = task_uploader
        self.__task_algorithm = task_algorithm
        self.__task_data_list = task_data_list
        self.__task_result_info = task_result_info
        self.__task_status = task_status
        self.__sub_task_id = sub_task_id
        self.__device_param = device_param

    def set_task_info_by_dict(self, task_info_dict: dict):
        self.__task_id = task_info_dict['task_id']
        self.__task_name = task_info_dict['task_name']
        self.__task_location = task_info_dict['task_location']
        self.__task_version = task_info_dict['task_version']
        self.__task_uploader = task_info_dict['task_uploader']
        self.__task_algorithm = task_info_dict['task_algorithm']
        self.__task_data_list = task_info_dict['task_data_list']
        self.__task_result_info = task_info_dict['task_result_info']
        self.__task_status = task_info_dict['task_status']
        self.__sub_task_id = task_info_dict['sub_task_id']
        self.__device_param = task_info_dict['device_param']

    def get_task_info_by_dict(self) -> dict:
        task_info_dict = {"task_id": self.get_task_id(), "task_name": self.get_task_name(),
                          "task_location": self.get_task_location(), "task_version": self.get_task_version(),
                          "task_uploader": self.get_task_uploader(), "task_algorithm": self.get_task_algorithm(),
                          "task_data_list": self.get_task_data_list(), "task_result_info": self.get_task_result_info(),
                          "task_status": self.get_task_status(), "sub_task_id": self.get_sub_task_id(),
                          "device_param": self.get_device_param()}
        return task_info_dict

    def set_task_id(self, task_id: str):
        self.__task_id = task_id

    def get_task_id(self) -> str:
        return self.__task_id

    def set_task_name(self, task_name: str):
        self.__task_name = task_name

    def get_task_name(self) -> str:
        return self.__task_name

    def set_task_location(self, task_location: str):
        self.__task_location = task_location

    def get_task_location(self) -> str:
        return self.__task_location

    def set_task_version(self, task_version: str):
        self.__task_version = task_version

    def get_task_version(self) -> str:
        return self.__task_version

    def set_task_uploader(self, task_uploader: str):
        self.__task_uploader = task_uploader

    def get_task_uploader(self) -> str:
        return self.__task_uploader

    def set_task_algorithm(self, task_algorithm: AlgorithmInfo | None):
        self.__task_algorithm = task_algorithm

    def get_task_algorithm(self) -> AlgorithmInfo | None:
        return self.__task_algorithm

    def set_task_data_list(self, task_data_list: List[DataInfo]):
        self.__task_data_list = task_data_list

    def get_task_data_list(self) -> List[DataInfo]:
        return self.__task_data_list

    def set_task_result_info(self, task_result_info: ResultInfo | None):
        self.__task_result_info = task_result_info

    def get_task_result_info(self) -> ResultInfo | None:
        return self.__task_result_info

    def set_task_status(self, task_status):
        self.__task_status = task_status

    def get_task_status(self) -> WorkStatus:
        return self.__task_status

    def set_sub_task_id(self, sub_task_id: list):
        self.__sub_task_id = sub_task_id

    def get_sub_task_id(self) -> list:
        return self.__sub_task_id

    def set_device_param(self, device_param: dict):
        self.__device_param = device_param

    def get_device_param(self) -> dict:
        return self.__device_param

    def __str__(self):
        return f"TaskInfo(task_id={self.__task_id}, task_name={self.__task_name}, task_location={self.__task_location}, " \
               f"task_version={self.__task_version}, task_uploader={self.__task_uploader}, task_algorithm={self.__task_algorithm}, " \
               f"task_data_list={self.__task_data_list}, task_result_info={self.__task_result_info}, task_status={self.__task_status}, " \
               f"sub_task_id={self.__sub_task_id}, device_param={self.__device_param})"
