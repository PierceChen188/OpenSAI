__all__ = ['Algorithm_Pic']

import os

class Algorithm_Pic:
    def __init__(self, param_dict, data_dict):
        self.num_samples = param_dict

    def run(self):

        # 获取程序运行时的执行路径
        execution_path = os.getcwd()
        picture_path = execution_path + "./data/opensai.png"
        result = "Pic:" + picture_path
        return result
