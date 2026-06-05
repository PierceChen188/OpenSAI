import importlib
import logging
import os
import sys
from config.logging_config import LOG_FORMAT
from device.interfaces.actuator import Actuator


class ActuatorImpl(Actuator):
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

    @classmethod
    def load_algorithm(cls, algorithm_file: str):
        try:
            print(algorithm_file)
            if algorithm_file in sys.modules:
                return sys.modules[algorithm_file]
            else:
                return importlib.import_module(algorithm_file)

        except ImportError:
            logging.info(f"无法加载算法文件：{algorithm_file}")
            return None

    @classmethod
    def run_actuator(cls, algorithm_file: str, param_dict: dict, data_dict: dict):
        module = cls.load_algorithm(algorithm_file)
        if module:
            class_name = module.__all__[0]
            algo_class = getattr(module, class_name)
            algo = algo_class(param_dict, data_dict)
            return algo.run()
        else:
            return None

    @classmethod
    def run_could_actuator(cls, algorithm_file: str):
        module = cls.load_algorithm(algorithm_file)
        if module:
            class_name = module.__all__[0]
            algo_class = getattr(module, class_name)
            algo = algo_class()
            return algo.run()
        else:
            return None

    @classmethod
    def read_algorithms(cls):
        # 获取当前类所在的文件的目录路径
        current_directory = os.path.dirname(os.path.abspath(__file__))

        # 列出当前目录中的所有文件
        all_files = os.listdir(current_directory)

        # 过滤出 .py 文件（排除自身）
        python_files = [f for f in all_files if f.endswith('.py') and f.startswith('Algorithm')]

        algorithms_name = []
        # 导入每个 Python 文件作为模块
        for py_file in python_files:
            module_name = py_file[:-3]  # 去掉 .py 后缀
            algorithms_name.append(module_name)
        return algorithms_name


if __name__ == '__main__':
    result_1 = ActuatorImpl.run_actuator('algorithm_taskA', 1, 2)
    logging.info(f"任务1计算结果：{result_1}")
    result_2 = ActuatorImpl.run_actuator('algorithm_taskB', 10, None)
    logging.info(f"任务2计算结果：{result_2}")
    result_3 = ActuatorImpl.run_actuator('algorithm_taskC', 10, None)
    logging.info(f"任务2计算结果：{result_3}")

