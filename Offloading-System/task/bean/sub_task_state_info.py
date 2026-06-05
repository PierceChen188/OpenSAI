from config.sub_task_status import SubTaskStatus


class SubTaskStateInfo:

    def __init__(self):
        # 子任务编号
        self.__sub_task_id = ""
        # 子任务所在设备编号
        self.__device_id = ""
        # 子任务状态
        self.__sub_task_status = None

    def set_sub_task_state_info(self, sub_task_id: str, device_id: str, sub_task_status: SubTaskStatus):
        self.__sub_task_id = sub_task_id
        self.__device_id = device_id
        self.__sub_task_status = sub_task_status

    def set_sub_task_state_info_by_dict(self, sub_task_state_info_dict: dict):
        self.__sub_task_id = sub_task_state_info_dict['sub_task_id']
        self.__device_id = sub_task_state_info_dict['device_id']
        self.__sub_task_status = sub_task_state_info_dict['sub_task_status']

    def get_sub_task_state_info_by_dict(self) -> dict:
        algo_info_dict = {"sub_task_id": self.get_sub_task_id(), "device_id": self.get_device_id(),
                          "sub_task_status": self.get_sub_task_status()}
        return algo_info_dict

    def set_sub_task_id(self, sub_task_id: str):
        self.__sub_task_id = sub_task_id

    def get_sub_task_id(self) -> str:
        return self.__sub_task_id

    def set_device_id(self, device_id: str):
        self.__device_id = device_id

    def get_device_id(self) -> str:
        return self.__device_id

    def set_sub_task_status(self, sub_task_status: SubTaskStatus):
        self.__sub_task_status = sub_task_status

    def get_sub_task_status(self) -> SubTaskStatus:
        return self.__sub_task_status

    def __str__(self):
        return f"SubTaskStateInfo(sub_task_id={self.__sub_task_id}, device_id={self.__device_id}, " \
               f"sub_task_status={self.__sub_task_status})"
