from __future__ import annotations

from typing import List, TYPE_CHECKING

from config.sub_task_status import SubTaskStatus
from result.bean.result_info import ResultInfo
from task.bean.algorithm_info import AlgorithmInfo
from task.bean.data_info import DataInfo

if TYPE_CHECKING:
    # 仅用于类型注解；本文件已 from __future__ import annotations，注解为惰性字符串，
    # 不会在运行时求值，因而避免潜在的循环导入。
    from device.bean.devce_info import DeviceInfo


class SubTaskInfo:
    def __init__(self):
        # 任务编号
        self.__task_id = ""
        # 子任务编号
        self.__sub_task_id = ""
        # 任务名称
        self.__sub_task_name = ""
        # 任务版本
        self.__sub_task_version = ""
        # 任务位置
        self.__sub_task_location = ""
        # 任务算法
        self.__task_algorithm = None
        # 任务数据
        self.__sub_task_data_list = []
        # 任务结果信息
        self.__sub_task_result_info = None
        # 任务状态
        self.__sub_task_status = None
        # 子任务个数
        self.__sub_task_count = 0
        # 被依赖的节点（后继）
        self.__run_before_list = []
        # 依赖的节点信息（前驱）
        self.__dependence_list = []
        # 选择卸载到的device节点id
        self.__device_info = None
        # 子任务的时延 (初始化时为子任务就绪时间)
        self.__subtask_delay = -1
        # 子任务输入参数
        self.__sub_task_input_param = None
        # 子任务指定卸载对象
        self.__sub_task_direct_allocate_id = -1



    def set_sub_task_info(self, task_id: str, sub_task_id: str, sub_task_name: str, sub_task_version: str,
                          sub_task_location: str, task_algorithm: AlgorithmInfo | None, sub_task_data_list: List[DataInfo],
                          sub_task_result_info: ResultInfo | None, sub_task_status: SubTaskStatus, sub_task_count: int,
                          run_before_list = None, dependence_list = None, device_info = None,
                          subtask_delay = -1, sub_task_input_param = None, sub_task_direct_allocate_id = -1
                          ):
        if dependence_list is None:
            dependence_list = []
        if run_before_list is None:
            run_before_list = []
        self.__task_id = task_id
        self.__sub_task_id = sub_task_id
        self.__sub_task_name = sub_task_name
        self.__sub_task_version = sub_task_version
        self.__sub_task_location = sub_task_location
        self.__task_algorithm = task_algorithm
        self.__sub_task_data_list = sub_task_data_list
        self.__sub_task_result_info = sub_task_result_info
        self.__sub_task_status = sub_task_status
        self.__sub_task_count = sub_task_count

        self.__run_before_list = run_before_list
        self.__dependence_list = dependence_list
        self.__device_info = device_info
        self.__subtask_delay = subtask_delay
        self.__sub_task_input_param = sub_task_input_param

    def set_sub_task_info_by_dict(self, sub_task_info_dict: dict):
        self.__task_id = sub_task_info_dict['task_id']
        self.__sub_task_id = sub_task_info_dict['sub_task_id']
        self.__sub_task_name = sub_task_info_dict['sub_task_name']
        self.__sub_task_version = sub_task_info_dict['sub_task_version']
        self.__sub_task_location = sub_task_info_dict['sub_task_location']
        self.__task_algorithm = sub_task_info_dict['task_algorithm']
        self.__sub_task_data_list = sub_task_info_dict['sub_task_data_list']
        self.__sub_task_result_info = sub_task_info_dict['sub_task_result_info']
        self.__sub_task_status = sub_task_info_dict['sub_task_status']
        self.__sub_task_count = sub_task_info_dict['sub_task_count']

        self.__run_before_list = sub_task_info_dict['run_before_list'] if 'run_before_list' in sub_task_info_dict else []
        self.__dependence_list = sub_task_info_dict['dependence_list'] if 'dependence_list' in sub_task_info_dict else []
        self.__device_info = sub_task_info_dict['device_info'] if 'device_info' in sub_task_info_dict else None
        self.__subtask_delay = sub_task_info_dict['subtask_delay'] if 'subtask_delay' in sub_task_info_dict else -1
        self.__sub_task_input_param = sub_task_info_dict['sub_task_input_param'] if 'sub_task_input_param' in sub_task_info_dict else {}
        self.__sub_task_direct_allocate_id = sub_task_info_dict['sub_task_direct_allocate_id'] if 'sub_task_direct_allocate_id' in sub_task_info_dict else -1


    def get_sub_task_info_by_dict(self) -> dict:
        sub_task_info_dict = {"task_id": self.get_task_id(), "sub_task_id": self.get_sub_task_id(),
                              "sub_task_name": self.get_sub_task_name(),
                              "sub_task_version": self.get_sub_task_version(),
                              "sub_task_location": self.get_sub_task_location(),
                              "task_algorithm": self.get_task_algorithm(),
                              "sub_task_data_list": self.get_sub_task_data_list(),
                              "sub_task_result_info": self.get_sub_task_result_info(),
                              "sub_task_status": self.get_sub_task_status(),
                              "sub_task_count": self.get_sub_task_count(),

                              "run_before_list": self.get_run_before_list(),
                              "dependence_list": self.get_dependence_list(),
                              "device_info": self.get_device_info(),
                              'subtask_delay': self.get_sub_task_delay(),
                              'sub_task_input_param': self.get_sub_task_input_param(),
                              'sub_task_direct_allocate_id': self.get_sub_task_direct_allocate_id(),
                              }
        return sub_task_info_dict

    def set_sub_task_info_by_node(self, task_id:str, subtask: dict):
        # [Merged] 合并 containerConfig 到 taskInputInfo 中
        task_input_info = subtask["form"]["taskInputInfo"]
        container_config = subtask["form"].get('containerConfig', {})
        if container_config:    
            task_input_info['containerConfig'] = container_config

        # 修正：beforeNodes = 前驱；afterNodes = 后继
        self.set_sub_task_info(
            task_id=task_id,
            sub_task_id=subtask["id"],
            sub_task_name=subtask["name"],
            sub_task_version=subtask["form"]["taskVersion"],
            sub_task_location=subtask["form"]["taskInputInfo"]["inDataPath"],
            task_algorithm=subtask["form"]["taskAlgorithm"],
            sub_task_data_list=[],
            sub_task_result_info=subtask["result"]["taskResultInfo"],
            sub_task_status=subtask["result"]["status"],
            sub_task_count=1,
            run_before_list=subtask["form"]["afterNodes"],      # 后继
            dependence_list=subtask["form"]["beforeNodes"],     # 前驱
            device_info=None,
            subtask_delay=-1,
            sub_task_input_param=task_input_info,
            sub_task_direct_allocate_id=subtask["form"]["directAllocationId"]
        )

    def get_sub_task_result_by_node(self) -> dict:
        sub_task_result_dict = {
              "id": self.get_sub_task_id(),
              "name": self.get_sub_task_name(),
              "taskInputInfo": self.get_sub_task_input_param(),
              "taskAlgorithm": self.get_task_algorithm(),
              'result': {
                  'status': self.get_sub_task_status(),
                  'delay': self.get_sub_task_delay(),
                  'deviceId': self.get_device_info().get_device_no() if self.get_device_info() != None else "Unknown"
              },
              "ResultInfo":  self.get_sub_task_result_info()
        }

        return sub_task_result_dict

    def set_task_id(self, task_id: str):
        self.__task_id = task_id

    def get_task_id(self) -> str:
        return self.__task_id

    def set_sub_task_id(self, sub_task_id: str):
        self.__sub_task_id = sub_task_id

    def get_sub_task_id(self) -> str:
        return self.__sub_task_id

    def set_sub_task_name(self, sub_task_name: str):
        self.__sub_task_name = sub_task_name

    def get_sub_task_name(self) -> str:
        return self.__sub_task_name

    def set_sub_task_version(self, sub_task_version: str):
        self.__sub_task_version = sub_task_version

    def get_sub_task_version(self) -> str:
        return self.__sub_task_version

    def set_sub_task_location(self, sub_task_location: str):
        self.__sub_task_location = sub_task_location

    def get_sub_task_location(self) -> str:
        return self.__sub_task_location

    def set_task_algorithm(self, task_algorithm: AlgorithmInfo | None):
        self.__task_algorithm = task_algorithm

    def get_task_algorithm(self) -> AlgorithmInfo | None:
        return self.__task_algorithm

    def set_sub_task_data_list(self, sub_task_data_list: List[DataInfo]):
        self.__sub_task_data_list = sub_task_data_list

    def get_sub_task_data_list(self) -> List[DataInfo]:
        return self.__sub_task_data_list

    def set_sub_task_result_info(self, sub_task_result_info: ResultInfo | None):
        self.__sub_task_result_info = sub_task_result_info

    def get_sub_task_result_info(self) -> ResultInfo | None:
        return self.__sub_task_result_info

    def set_sub_task_status(self, sub_task_status: SubTaskStatus):
        self.__sub_task_status = sub_task_status

    def get_sub_task_status(self) -> SubTaskStatus:
        return self.__sub_task_status

    def set_sub_task_delay(self, delay: int):
        self.__subtask_delay = delay

    def get_sub_task_delay(self):
        return self.__subtask_delay

    def set_sub_task_count(self, sub_task_count: int):
        self.__sub_task_count = sub_task_count

    def get_sub_task_count(self) -> int:
        return self.__sub_task_count

    def get_run_before_list(self) -> list:
        return self.__run_before_list

    def set_run_before_list(self, run_before_list: list):
        self.__run_before_list = run_before_list

    def get_dependence_list(self) -> list:
        return self.__dependence_list

    def set_dependence_list(self, dependence_list: list):
        self.__dependence_list = dependence_list

    def get_device_info(self) -> DeviceInfo:
        return self.__device_info

    def set_device_info(self, device_info:DeviceInfo):
        self.__device_info = device_info

    def get_sub_task_input_param(self) -> dict:
        return self.__sub_task_input_param
    def set_sub_task_input_param(self, param_info:dict):
        self.__sub_task_input_param = param_info

    def get_sub_task_direct_allocate_id(self) -> int:
        return self.__sub_task_direct_allocate_id

    def set_sub_task_direct_allocate_id(self, direct_allocate_id: int):
        self.__sub_task_direct_allocate_id = direct_allocate_id

    def __str__(self):
        return f"SubTaskInfo(task_id={self.__task_id}, sub_task_id={self.__sub_task_id}, " \
               f"sub_task_name={self.__sub_task_name}, sub_task_version={self.__sub_task_version}, " \
               f"sub_task_location={self.__sub_task_location}, task_algorithm={self.__task_algorithm}, " \
               f"sub_task_data_list={self.__sub_task_data_list}, sub_task_result_info={self.__sub_task_result_info}, " \
               f"sub_task_status={self.__sub_task_status}, sub_task_count={self.__sub_task_count}" \
               f"run_before_list={self.__run_before_list}, dependence_list={self.__dependence_list}, " \
               f"device_info={self.__device_info})"
