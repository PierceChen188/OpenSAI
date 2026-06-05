import time
from typing import List

from config.sub_task_status import SubTaskStatus
from config.work_status import WorkStatus
from device.bean.devce_info import DeviceInfo
from predis.implements.result_service_impl import ResultServiceImpl
from predis.implements.task_state_service_impl import TaskStateServiceImpl
from result.bean.result_info import ResultInfo
from task.bean.sub_task_info import SubTaskInfo
from task.bean.sub_task_state_info import SubTaskStateInfo

from task.bean.task_info import TaskInfo
from task.bean.task_state_info import TaskStateInfo
from task.implements.algorithm_transmission_impl import AlgorithmTransmissionImpl
from task.interfaces.task_scheduler_manager import TaskSchedulerManager


class TaskSchedulerManagerImpl(TaskSchedulerManager):
    def send_subtask_to_device(self, sub_task_info: SubTaskInfo, device_info: DeviceInfo) -> bool:
        task_state_info = TaskStateInfo()
        task_state_info.set_task_id(sub_task_info.get_task_id())
        task_state_info.set_task_algo_id(sub_task_info.get_task_algorithm().get_algo_id())
        task_state_info.set_task_status(WorkStatus.UN_WORK)
        task_state_info.set_task_start_time(int(round(time.time() * 1000)))
        task_state_info.set_task_run_time(0)

        sub_task_state_info = SubTaskStateInfo()
        sub_task_state_info.set_sub_task_id(sub_task_info.get_sub_task_id())
        sub_task_state_info.set_device_id(device_info.get_device_no())
        sub_task_state_info.set_sub_task_status(SubTaskStatus.UN_WORK)
        task_state_info.set_sub_task_state_info(sub_task_state_info)
        task_state_info.set_sub_task_count(sub_task_info.get_sub_task_count())
        TaskStateServiceImpl.write_task_to_cache(task_state_info.get_task_state_info_by_dict())

        task_id = sub_task_info.get_task_id()
        sub_task_id = sub_task_info.get_sub_task_id()
        device_id = device_info.get_device_no()
        algo_name = sub_task_info.get_task_algorithm().get_algo_name()
        algo_version = sub_task_info.get_task_algorithm().get_algo_version()

        param_list = {}
        if sub_task_info.get_task_algorithm().get_param_list() is not None:
            param = sub_task_info.get_task_algorithm().get_param_list()
            key_set = param.keys()
            for key in key_set:
                param_list[key] = param.get(key)
                result_info_dict = ResultServiceImpl.read_progress_to_lbfo(task_id, sub_task_id)
                result_info = ResultInfo()
                result_info.set_result_info_by_dict(result_info_dict)
                if sub_task_info.get_sub_task_location() is not None:
                    param_list["TaskLocation"] = sub_task_info.get_sub_task_location()
                if result_info is not None:
                    param_list["progress"] = result_info.get_result_dict().get("progress")
                    if "result" in result_info.get_result_dict():
                        param_list["result"] = result_info.get_result_dict().get("result")
                    else:
                        param_list["result"] = result_info.get_result_dict()
        if sub_task_info.get_sub_task_data_list() is not None:
            sub_task_data_list = sub_task_info.get_sub_task_data_list()
            data_name_dict = data_location_dict = data_type_dict = {}
            num = 0
            for data_info in sub_task_data_list:
                data_name_dict[str(num)] = data_info.get_data_name()
                data_location_dict[str(num)] = data_info.get_data_location()
                data_type_dict[str(num)] = data_info.get_data_type()
                num += 1

            data_dict = {"data_name": data_name_dict, "data_location": data_location_dict,
                         "file_suffix": data_type_dict}
            result = AlgorithmTransmissionImpl.send_algo_with_data(sub_task_info, device_info, task_id, sub_task_id,
                                                                   device_id, algo_name, algo_version, param_list,
                                                                   data_dict)
        else:
            result = AlgorithmTransmissionImpl.send_algo_no_data(sub_task_info, device_info, task_id, sub_task_id,
                                                                 device_id, algo_name, algo_version, param_list)

        return result

    def send_task_to_device(self, task_info: TaskInfo) -> List[bool]:
        return super().send_task_to_device(task_info)

    def send_multi_task_to_device(self, multi_task_info) -> List[bool]:
        return super().send_multi_task_to_device(multi_task_info)
