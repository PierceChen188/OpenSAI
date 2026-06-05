from __future__ import annotations

import logging
from typing import List, TypeVar, Dict

from config.logging_config import LOG_FORMAT
from device.bean.devce_info import DeviceInfo
from predis.implements.task_state_service_impl import TaskStateServiceImpl
from predis.implements.work_device_service_impl import WorkDeviceServiceImpl
from predis.implements.work_task_service_impl import WorkTaskServiceImpl
from predis.interfaces.task_device_map_service import TaskDeviceMapService
from task.bean.sub_task_info import SubTaskInfo


class TaskDeviceMapServiceImpl(TaskDeviceMapService):
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
    T = TypeVar('T')

    @classmethod
    def query_task_info_work_device(cls, task_id: str) -> Dict[dict, dict] | None:
        sub_task_info_list = WorkTaskServiceImpl.query_work_task_list_by_id(task_id)
        if sub_task_info_list is not None:
            device_info_dict = {}
            for sub_task_info in sub_task_info_list:
                task_state_info = TaskStateServiceImpl.read_task_state_info(sub_task_info['task_id'],
                                                                            sub_task_info['sub_task_id'])
                work_device_id = task_state_info['device_id']
                device_info = WorkDeviceServiceImpl.query_work_device_info(work_device_id)
                device_info_dict[work_device_id] = device_info
            return device_info_dict
        return None

    @classmethod
    def query_work_task_info_work_device(cls, sub_task_info_list: List[SubTaskInfo]) -> Dict[dict, dict]:
        device_info_dict = {}
        if sub_task_info_list is not None:
            for sub_task_info in sub_task_info_list:
                device_info_dict_1 = cls.query_task_info_work_device(sub_task_info.get_task_id())
                device_info_dict.update(device_info_dict_1)
        return device_info_dict

    @classmethod
    def query_work_device_task_info(cls, device_id: str) -> List[SubTaskInfo] | None:
        device_info_dict = WorkDeviceServiceImpl.read_work_device_info()
        key_set = device_info_dict.keys()
        task_state_info_dict = TaskStateServiceImpl.read_task_info()
        if task_state_info_dict is None:
            return None
        sub_task_info_list = []
        key_set_1 = task_state_info_dict.keys()
        for key in key_set:
            device_info = device_info_dict.get(key)
            if device_info['device_no'] == device_id:
                if key_set_1 is not []:
                    for key_1 in key_set_1:
                        task_state_info = task_state_info_dict.get(key_1)
                        work_device_id = task_state_info[device_id]
                        task_id = task_state_info['task_id']
                        sub_task_id = task_state_info['sub_task_id']
                        if work_device_id == device_id:
                            sub_task_info_list = sub_task_info_list.append(
                                WorkTaskServiceImpl.query_work_task_info_by_id(task_id, sub_task_id))
                    return sub_task_info_list
        return None

    @classmethod
    def query_work_device_work_task_info(cls, device_info_list: Dict[DeviceInfo]) -> List[SubTaskInfo]:
        key_set = device_info_list.keys()
        sub_task_info_list = []
        for key in key_set:
            sub_task_info_list_1 = cls.query_work_device_task_info(device_info_list.get(key).get_device_no())
            sub_task_info_list.extend(sub_task_info_list_1)
        return sub_task_info_list
