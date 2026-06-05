import random
import logging
from task.bean.sub_task_info import SubTaskInfo
class OffloadingImpl():

    @classmethod
    def random_strategy(cls, device_info_list: dict, subtask_info:SubTaskInfo):
        device_size = len(device_info_list)

        if device_size > 0:
            keys_list = list(device_info_list.keys())
            select_id = random.randrange(0, device_size)
            return device_info_list[keys_list[select_id]]
        else:
            logging.info("无在线设备...")
            return None

    @classmethod
    def direct_strategy(cls, device_info_list: dict, subtask_info: SubTaskInfo):
        idx = subtask_info.get_sub_task_direct_allocate_id()
        device_size = len(device_info_list)
        if device_size > idx and idx > 0:
            keys_list = list(device_info_list.keys())
            return device_info_list[keys_list[idx]]
        else:
            logging.info("无指定的在线设备,为该任务随机分配一个设备...")
            return cls.random_strategy(device_info_list, subtask_info)


    def rl_based_strategy(cls, device_info_list: dict, subtask_info: SubTaskInfo):
        pass
        # device_size = len(device_info_list)
        # cpu_consume = [1 for i in range(device_size)]
        # band_width = [0 for i in range(device_size)]
        # base info : data size, remain_time/total_time, task_id/total_id, is_exit, device cpu,
        # test
        # in_data = subtask_info.get_sub_task_input_param()
        # data_size = in_data["DataSize"]
        # is_exit = in_data["isExit"]
        # task_id = subtask_info.get_sub_task_id()
        # keys_list = list(device_info_list.keys())
        # for i in range(device_size):
        #     device_dict = device_info_list[keys_list[i]]
        #     cpu_consume[i] = float(device_dict['used_cpu'].replace('%', ''))
        #     band_width[i] = device_dict['bandwidth']
        # print(is_exit, data_size, task_id, cpu_consume, band_width)





