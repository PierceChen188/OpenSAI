from device.bean.devce_info import DeviceInfo
from task.bean.sub_task_info import SubTaskInfo


class AlgorithmTransmission:
    def send_algo_no_data(self, sub_task_info: SubTaskInfo, device_info: DeviceInfo, task_id: str, sub_task_id: str,
                          device_no: str, algo_name: str, algo_version: str, param_dict: dict) -> bool:
        """
        给目标算力设备推送不带数据的算法任务
        :param sub_task_info: 子任务信息
        :param device_info: 目标算力设备信息
        :param task_id: 任务编号
        :param sub_task_id: 子任务编号
        :param device_no: 算法接收设备编号
        :param algo_name: 算法名称
        :param algo_version: 算法版本
        :param param_dict: 算法参数列表
        :return: 是否发送成功(True or False)
        """
        pass

    def send_algo_with_data(self, sub_task_info: SubTaskInfo, device_info: DeviceInfo, task_id: str, sub_task_id: str,
                            device_no: str, algo_name: str, algo_version: str, param_dict: dict,
                            data_dict: dict) -> bool:
        """
        给目标算力设备推送带数据的算法任务
        :param sub_task_info: 子任务信息
        :param device_info: 目标算力设备信息
        :param task_id: 任务编号
        :param sub_task_id: 子任务编号
        :param device_no: 算法接收设备编号
        :param algo_name: 算法名称
        :param algo_version: 算法版本
        :param param_dict: 算法参数列表
        :param data_dict: 算法数据列表
        :return: 是否发送成功(True or False)
        """
        pass
