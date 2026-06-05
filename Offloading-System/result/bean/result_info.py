class ResultInfo:
    def __init__(self):
        # 设备编号
        self.__device_no = ""
        # 任务编号
        self.__task_id = ""
        # 子任务编号
        self.__sub_task_id = ""
        # 算法名称
        self.__algo_name = ""
        # 计算结果
        self.__result_dict = {}
        # 计算结果url(给出结果网页链接)
        self.__result_url = []

    def set_result_info(self, device_no: str, task_id: str, sub_task_id: str, algo_name: str, result_dict: dict,
                        result_url: list):
        self.__device_no = device_no
        self.__task_id = task_id
        self.__sub_task_id = sub_task_id
        self.__algo_name = algo_name
        self.__result_dict = result_dict
        self.__result_url = result_url

    def set_result_info_by_dict(self, result_info_dict: dict):
        self.__device_no = result_info_dict['device_no']
        self.__task_id = result_info_dict['task_id']
        self.__sub_task_id = result_info_dict['sub_task_id']
        self.__algo_name = result_info_dict['algo_name']
        self.__result_dict = result_info_dict['result_dict']
        self.__result_url = result_info_dict['result_url']

    def get_result_info_by_dict(self) -> dict:
        result_info_dict = {"device_no": self.get_device_no(), "task_id": self.get_task_id(),
                            "sub_task_id": self.get_sub_task_id(), "algo_name": self.get_algo_name(),
                            "result_dict": self.get_result_dict(), "result_url": self.get_result_url()}
        return result_info_dict

    def set_device_no(self, device_info: dict):
        self.__device_no = device_info

    def get_device_no(self) -> str:
        return self.__device_no

    def set_task_id(self, task_id: str):
        self.__task_id = task_id

    def get_task_id(self) -> str:
        return self.__task_id

    def set_sub_task_id(self, sub_task_id: str):
        self.__sub_task_id = sub_task_id

    def get_sub_task_id(self) -> str:
        return self.__sub_task_id

    def set_algo_name(self, algo_name: str):
        self.__algo_name = algo_name

    def get_algo_name(self) -> str:
        return self.__algo_name

    def set_result_dict(self, result_dict: dict):
        self.__result_dict = result_dict

    def get_result_dict(self) -> dict:
        return self.__result_dict

    def set_result_url(self, result_url: list):
        self.__result_url = result_url

    def get_result_url(self) -> list:
        return self.__result_url

    def __str__(self):
        return f"ResultInfo(device_no={self.__device_no}, task_id={self.__task_id}, sub_task_id={self.__sub_task_id} " \
               f"algo_name={self.__algo_name}, result_dict={self.__result_dict}, result_url={self.__result_url})"
