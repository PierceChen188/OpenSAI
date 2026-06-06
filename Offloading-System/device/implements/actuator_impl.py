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
    def preload_algorithms(cls):
        """在主线程预加载全部算法模块，返回成功加载的算法名列表。

        必须在设备启动早期、由主线程调用。原因：算法首次执行发生在 process_task 的
        子线程中，若其中首次 import 重型 C 扩展（numpy / torch 等），在 Windows 上会与
        进程内其它阻塞调用争用加载器锁而死锁。提前在主线程完成这些 import 后，子线程里的
        import 只是命中 sys.modules 缓存，不再触发 C 扩展加载，从而避免死锁。
        每个模块独立 try/except：某个算法导入失败（如本机无 torch）只是该算法不可用，
        不影响其余算法与基础功能。
        """
        loaded = []
        for module_name in cls.read_algorithms():
            try:
                if cls.load_algorithm("device.implements." + module_name) is not None:
                    loaded.append(module_name)
            except Exception as e:  # noqa: BLE001 - 预热失败不应阻断设备启动
                logging.warning(f"预加载算法失败 {module_name}: {e}")
        logging.info(f"已在主线程预加载算法模块: {loaded}")
        return loaded

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

