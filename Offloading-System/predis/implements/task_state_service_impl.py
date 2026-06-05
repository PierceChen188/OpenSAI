from __future__ import annotations

import json
import logging
import time
from typing import Dict, List

from config.common_redis_key_constant import ComRedisKeyConstant
from config.logging_config import LOG_FORMAT
from config.redis_boot import REDIS_TOOLS
from config.sub_task_status import SubTaskStatus
from config.work_status import WorkStatus
from predis.interfaces.task_state_service import TaskStateService


class TaskStateServiceImpl(TaskStateService):
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

    @classmethod
    def write_task_to_cache(cls, task_state_info: dict):
        task_id_key = ComRedisKeyConstant.TASK_ID_ONLINE_SET_WEB
        task_id_web_key = ComRedisKeyConstant.TASK_ID_ONLINE_PREFIX + task_state_info['task_id']
        REDIS_TOOLS.sset_and_time(task_id_key, 86400, task_state_info['task_id'])
        REDIS_TOOLS.set_key_value_with_ttl(task_id_web_key, json.dumps(task_state_info), 86400)
        # 任务编号集合
        task_info_key = ComRedisKeyConstant.TASK_ONLINE_SET
        task_info_web_key = ComRedisKeyConstant.TASK_ONLINE_SET_WEB
        REDIS_TOOLS.sset_and_time(task_info_key, 86400,
                                  task_state_info['task_id'] + task_state_info['sub_task_state_info']['sub_task_id'])
        REDIS_TOOLS.sset_and_time(task_info_web_key, 86400,
                                  task_state_info['task_id'] + task_state_info['sub_task_state_info']['sub_task_id'])
        # 单个任务信息集合对应的Key
        task_key = ComRedisKeyConstant.TASK_ONLINE_PREFIX + task_state_info['task_id'] + \
                   task_state_info['sub_task_state_info']['sub_task_id']
        task_web_key = ComRedisKeyConstant.TASK_ONLINE_PREFIX_WEB + task_state_info['task_id'] + \
                       task_state_info['sub_task_state_info']['sub_task_id']
        REDIS_TOOLS.set_key_value_with_ttl(task_key, json.dumps(task_state_info), 86400)
        REDIS_TOOLS.set_key_value_with_ttl(task_web_key, json.dumps(task_state_info), 86400)

    @classmethod
    def read_task_info(cls) -> Dict[str, dict] | None:
        task_state_info_list = {}
        key_set = REDIS_TOOLS.sget(ComRedisKeyConstant.TASK_ONLINE_SET)
        if key_set is not None:
            for s in key_set:
                task_state_id = s.decode()
                key = ComRedisKeyConstant.TASK_ONLINE_PREFIX + task_state_id
                task_state_info_json = REDIS_TOOLS.get(key)
                task_state_info = json.loads(task_state_info_json)
                if task_state_info is not None:
                    task_state_info_list[task_state_id] = task_state_info
                logging.info(f"所有设备信息: {task_state_info_list}")
            return task_state_info_list
        return None

    @classmethod
    def read_task_state_info(cls, task_id: str, sub_task_id: str) -> Dict[str, dict]:
        key = ComRedisKeyConstant.TASK_ONLINE_PREFIX + task_id + sub_task_id
        task_state_info_json = REDIS_TOOLS.get(key)
        task_state_info = json.loads(task_state_info_json)
        return task_state_info

    @classmethod
    def read_device_task_state(cls, device_id: str) -> None:
        return None

    @classmethod
    def read_task_info_to_web(cls) -> Dict[str, dict] | None:
        task_state_info_list = {}
        key_set = REDIS_TOOLS.sGet(ComRedisKeyConstant.TASK_ONLINE_SET_WEB)
        if key_set is not None:
            for s in key_set:
                task_state_id = s.decode()
                key = ComRedisKeyConstant.TASK_ONLINE_PREFIX_WEB + task_state_id
                task_state_info_json = REDIS_TOOLS.get(key)
                task_state_info = json.loads(task_state_info_json)
                if task_state_info is not None:
                    task_state_info_list[task_state_id] = task_state_info
            logging.debug(f"所有任务信息:{task_state_info_list}")
            return task_state_info_list
        return None

    @classmethod
    def get_sub_task_num(cls, task_id: str) -> int:
        sub_task_num = 0
        key_set = REDIS_TOOLS.sGet(ComRedisKeyConstant.TASK_ONLINE_SET)
        if key_set is not None:
            for s in key_set:
                task_state_id = s.decode()
                key = ComRedisKeyConstant.TASK_ONLINE_PREFIX + task_state_id
                task_state_info_json = REDIS_TOOLS.get(key)
                task_state_info = json.loads(task_state_info_json)
                if task_state_info['task_id'] == task_id:
                    sub_task_num += 1
        else:
            logging.info("Cache中没有任何任务信息")
        return sub_task_num

    @classmethod
    def get_task_from_cache(cls, task_id: str) -> Dict[str, dict]:
        sub_task_list = {}
        key_set = REDIS_TOOLS.sGet(ComRedisKeyConstant.TASK_ONLINE_SET)
        if key_set is not None:
            for s in key_set:
                task_state_id = s.decode()
                key = ComRedisKeyConstant.TASK_ONLINE_PREFIX + task_state_id
                task_state_info_json = REDIS_TOOLS.get(key)
                task_state_info = json.loads(task_state_info_json)
                if task_state_info['task_id'] == task_id:
                    sub_task_list[task_state_info['task_id'] + task_state_info['sub_task_id']] = task_state_info
        else:
            logging.info("Cache中没有任何任务信息")
        return sub_task_list

    @classmethod
    def get_task_status(cls, task_id: str) -> WorkStatus:
        sub_task_list = cls.get_task_from_cache(task_id)
        sub_task_num = sub_task_list[task_id + '0']['sub_task_count']
        task_finish_num = 0
        for i in range(sub_task_num):
            task_state_info = sub_task_list[task_id + str(i)]
            task_status = task_state_info['task_status']
            if task_status == WorkStatus.FINISH:
                task_finish_num += 1
                if task_finish_num == sub_task_num:
                    return WorkStatus.FINISH
        return WorkStatus.WORK

    @classmethod
    def get_sub_task_status(cls, task_id: str, sub_task_id: str) -> SubTaskStatus:
        task_state_info_list = cls.get_task_from_cache(task_id)
        task_state_info = task_state_info_list[task_id + sub_task_id]
        task_status = task_state_info['task_status']
        sub_task_status = task_state_info['sub_task_state_info']['sub_task_status']
        return sub_task_status

    @classmethod
    def get_task_status_is_finish(cls, task_id: str) -> bool:
        task_state_info_list = cls.read_task_info()
        if task_state_info_list is None:
            return True
        key_set = task_state_info_list.keys()
        num = 0
        for s in key_set:
            if task_state_info_list[s]['task_status'] == num:
                return False
        return True

    @classmethod
    def get_sub_task_status_is_finish(cls, task_id: str, sub_task_id: str) -> bool:
        task_state_info_list = cls.read_task_info()
        if task_state_info_list is None:
            return True
        key_set = task_state_info_list.keys()
        num = 0
        for s in key_set:
            if task_state_info_list[s]['sub_task_state_info']['sub_task_status'] == SubTaskStatus.FINISH:
                num += 1
                if len(task_state_info_list) == num:
                    return False
        return True

    @classmethod
    def get_finish_task(cls) -> List[str]:
        task_id_list = []
        task_state_info_list = cls.read_task_info()
        key_set = task_state_info_list.keys()
        for s in key_set:
            task_id = task_state_info_list[s]['task_id']
            if cls.get_task_status(task_id) == WorkStatus.FINISH:
                task_id_list.append(task_id)
        return task_id_list

    @classmethod
    def change_sub_task_status(cls, task_id: str, sub_task_id: str, sub_task_status: SubTaskStatus):
        task_key = ComRedisKeyConstant.TASK_ONLINE_PREFIX + task_id + sub_task_id
        task_web_key = ComRedisKeyConstant.TASK_ONLINE_PREFIX_WEB + task_id + sub_task_id
        if REDIS_TOOLS.has_key(task_key):
            task_state_info_json = REDIS_TOOLS.get(task_key)
            task_state_info = json.loads(task_state_info_json)
            task_state_web_json = REDIS_TOOLS.get(task_web_key)
            task_state_web = json.loads(task_state_web_json)
            sub_task_state = task_state_info['sub_task_state_info']
            sub_task_state_web = task_state_web['sub_task_state_info']
            sub_task_state['sub_task_status'] = sub_task_status
            sub_task_state_web['sub_task_status'] = sub_task_status
            task_state_info['sub_task_state_info'] = sub_task_state
            task_state_web['sub_task_state_info'] = sub_task_state_web
            REDIS_TOOLS.set_key_value(task_key, task_state_info)
            REDIS_TOOLS.set_key_value(task_web_key, task_state_web)
        else:
            logging.info(f"没有找到返回结果{task_id + sub_task_id}对应的任务信息, 修改子任务状态失败")

    @classmethod
    def change_task_status(cls, task_id: str, work_status: WorkStatus):
        key_set = REDIS_TOOLS.sGet(ComRedisKeyConstant.TASK_ONLINE_SET)
        if key_set is not None:
            for s in key_set:
                task_state_id = s.decode()
                key = ComRedisKeyConstant.TASK_ONLINE_PREFIX + task_state_id
                key_web = ComRedisKeyConstant.TASK_ONLINE_PREFIX_WEB + task_state_id
                task_state_info_json = REDIS_TOOLS.get(key)
                task_state_info = json.loads(task_state_info_json)
                task_state_web_json = REDIS_TOOLS.get(key_web)
                task_state_web = json.loads(task_state_web_json)
                if task_state_info['task_id'] == task_id:
                    task_state_info['task_status'] = work_status
                    task_state_web['task_status'] = work_status
                    task_state_info['task_run_time'] = int(round(time.time() * 1000)) \
                                                       - task_state_info['task_start_time']
                    task_state_web['task_run_time'] = int(round(time.time() * 1000)) - task_state_web['task_start_time']
                    REDIS_TOOLS.set_key_value(key, json.dumps(task_state_info))
                    REDIS_TOOLS.set_key_value(key_web, json.dumps(task_state_web))
        else:
            logging.info("Cache中没有任何任务信息")

    @classmethod
    def delete_task_online_info(cls, task_id: str):
        task_state_info_list = cls.get_task_from_cache(task_id)
        key_set = task_state_info_list.keys()
        for s in key_set:
            task_state_id = s.decode()
            key = ComRedisKeyConstant.TASK_ONLINE_PREFIX + task_state_id
            key_web = ComRedisKeyConstant.TASK_ONLINE_SET
            REDIS_TOOLS.delete(key)
            REDIS_TOOLS.set_remove(key_web, task_state_id)

    @classmethod
    def query_task_state_info(cls, task_id: str, sub_task_id: str) -> Dict[str, dict]:
        task_state_id = task_id + sub_task_id
        key = ComRedisKeyConstant.TASK_ONLINE_PREFIX + task_state_id
        task_state_info_json = REDIS_TOOLS.get(key)
        task_state_info = json.loads(task_state_info_json)
        return task_state_info
