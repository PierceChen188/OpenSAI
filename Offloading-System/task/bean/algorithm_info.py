class AlgorithmInfo:
    def __init__(self):
        # 算法编号
        self.__algo_id = ""
        # 算法名称
        self.__algo_name = ""
        # 算法版本
        self.__algo_version = ""
        # 算法参数列表
        self.__param_list = {}

    def set_algorithm_info(self, algo_id: str, algo_name: str, algo_version: str, param_list: dict):
        self.__algo_id = algo_id
        self.__algo_name = algo_name
        self.__algo_version = algo_version
        self.__param_list = param_list

    def set_algorithm_info_by_dict(self, algorithm_info_dict: dict):
        self.__algo_id = algorithm_info_dict['algo_id']
        self.__algo_name = algorithm_info_dict['algo_name']
        self.__algo_version = algorithm_info_dict['algo_version']
        self.__param_list = algorithm_info_dict['param_list']

    def get_algorithm_info_by_dict(self):
        algo_info_dict = {"algo_id": self.get_algo_id(), "algo_name": self.get_algo_name(),
                          "algo_version": self.get_algo_version(), "param_list": self.get_param_list()}
        return algo_info_dict

    def set_algo_id(self, algo_id: str):
        self.__algo_id = algo_id

    def get_algo_id(self) -> str:
        return self.__algo_id

    def set_algo_name(self, algo_name: str):
        self.__algo_name = algo_name

    def get_algo_name(self) -> str:
        return self.__algo_name

    def set_algo_version(self, algo_version: str):
        self.__algo_version = algo_version

    def get_algo_version(self) -> str:
        return self.__algo_version

    def set_param_list(self, param_list: dict):
        self.__param_list = param_list

    def get_param_list(self) -> dict:
        return self.__param_list

    def __str__(self):
        return f"AlgorithmInfo(algo_id={self.__algo_id}, algo_name={self.__algo_name}, " \
               f"algo_version={self.__algo_version}, param_list={self.__param_list})"
