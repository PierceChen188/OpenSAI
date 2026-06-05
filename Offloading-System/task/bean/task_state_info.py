from __future__ import annotations

from config.work_status import WorkStatus
from task.bean.sub_task_state_info import SubTaskStateInfo


class TaskStateInfo:
    def __init__(self):
        # 任务编号
        self.__task_id = ""
        # 算法编号
        self.__task_algo_id = ""
        # 任务状态
        self.__task_status = WorkStatus.UN_WORK
        # 任务开始时间
        self.__task_start_time = None
        # 任务运行时间
        self.__task_run_time = None
        # 子任务状态信息
        self.__sub_task_state_info = None
        # 子任务个数
        self.__sub_task_count = 0

    def set_task_state_info(self, task_id: str, task_algo_id: str, task_status: WorkStatus,
                            task_start_time: float | None, task_run_time: float | None,
                            sub_task_state_info: SubTaskStateInfo, sub_task_count: int):
        self.__task_id = task_id
        self.__task_algo_id = task_algo_id
        self.__task_status = task_status
        self.__task_start_time = task_start_time
        self.__task_run_time = task_run_time
        self.__sub_task_state_info = sub_task_state_info
        self.__sub_task_count = sub_task_count

    def set_task_state_info_by_dict(self, task_state_info_dict: dict):
        self.__task_id = task_state_info_dict['task_id']
        self.__task_algo_id = task_state_info_dict['task_algo_id']
        self.__task_status = task_state_info_dict['task_status']
        self.__task_start_time = task_state_info_dict['task_start_time']
        self.__task_run_time = task_state_info_dict['task_run_time']
        self.__sub_task_state_info = task_state_info_dict['sub_task_state_info']
        self.__sub_task_count = task_state_info_dict['sub_task_count']

    def get_task_state_info_by_dict(self):
        task_state_info_dict = {"task_id": self.get_task_id(), "task_algo_id": self.get_task_algo_id(),
                                "task_status": self.get_task_status(), "task_start_time": self.get_task_start_time(),
                                "task_run_time": self.get_task_run_time(),
                                "sub_task_state_info": self.get_sub_task_state_info(),
                                "sub_task_count": self.get_sub_task_count()}
        return task_state_info_dict

    def set_task_id(self, task_id: str):
        self.__task_id = task_id

    def get_task_id(self) -> str:
        return self.__task_id

    def set_task_algo_id(self, task_algo_id: str):
        self.__task_algo_id = task_algo_id

    def get_task_algo_id(self) -> str:
        return self.__task_algo_id

    def set_task_status(self, task_status: WorkStatus):
        self.__task_status = task_status

    def get_task_status(self) -> WorkStatus:
        return self.__task_status

    def set_task_start_time(self, task_start_time: float | None):
        self.__task_start_time = task_start_time

    def get_task_start_time(self) -> float | None:
        return self.__task_start_time

    def set_task_run_time(self, task_run_time: float | None):
        self.__task_run_time = task_run_time

    def get_task_run_time(self) -> float | None:
        return self.__task_run_time

    def set_sub_task_state_info(self, sub_task_state_info: SubTaskStateInfo):
        self.__sub_task_state_info = sub_task_state_info

    def get_sub_task_state_info(self) -> SubTaskStateInfo:
        return self.__sub_task_state_info

    def set_sub_task_count(self, sub_task_count: int):
        self.__sub_task_count = sub_task_count

    def get_sub_task_count(self) -> int:
        return self.__sub_task_count

    def __str__(self):
        return f"TaskStateInfo(task_id={self.__task_id}, task_algo_id={self.__task_algo_id}, task_status={self.__task_status}, " \
               f"task_start_time={self.__task_start_time}, task_run_time={self.__task_run_time}," \
               f"sub_task_state_info={self.__sub_task_state_info}, sub_task_count={self.__sub_task_count})"
