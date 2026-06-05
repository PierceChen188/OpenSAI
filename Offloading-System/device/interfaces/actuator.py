class Actuator:

    def load_algorithm(self, algorithm_file: str):
        """
        加载算法文件
        :param algorithm_file: 算法文件地址
        :return: 算法运行模块
        """
        pass

    def run_actuator(self, algorithm_file, param_dict, data_dict):
        """
        运行算法
        :param algorithm_file: 算法文件地址
        :param param_dict: 参数列表
        :param data_dict: 数据列表
        :return: 无
        """
        pass

    def run_cloud_actuator(self, algorithm_file):
        """
        运行云端后处理算法
        :param algorithm_file: 算法文件地址
        :return: 无
        """
        pass
