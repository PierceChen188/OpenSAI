from __future__ import annotations

import json
import logging
from typing import Dict, List

from config.common_redis_key_constant import ComRedisKeyConstant
from config.logging_config import LOG_FORMAT
from config.redis_boot import REDIS_TOOLS
from predis.interfaces.work_task_service import WorkTaskService


class WorkTaskServiceImpl(WorkTaskService):
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

    @classmethod
    def write_work_task_to_cache(cls, sub_task_info: dict):
        task_set_key = ComRedisKeyConstant.WORK_TASK_ONLINE_SET
        REDIS_TOOLS.sset_and_time(task_set_key, 86400, sub_task_info['task_id'] + sub_task_info['sub_task_id'])
        task_key = ComRedisKeyConstant.WORK_TASK_ONLINE_PREFIX + sub_task_info['task_id'] + sub_task_info['sub_task_id']
        REDIS_TOOLS.set_key_value_with_ttl(task_key, json.dumps(sub_task_info), 86400)

    @classmethod
    def read_work_task_info(cls) -> Dict[str, dict] | None:
        work_task_info_list = {}
        key_set = REDIS_TOOLS.sget(ComRedisKeyConstant.WORK_TASK_ONLINE_SET)
        if key_set is not None:
            for s in key_set:
                work_task_id = s.decode()
                key = ComRedisKeyConstant.WORK_TASK_ONLINE_PREFIX + work_task_id
                work_task_info_json = REDIS_TOOLS.get(key)
                work_task_info = json.loads(work_task_info_json)
                if work_task_info is not None:
                    work_task_info_list[work_task_id] = work_task_info
                logging.info(f"所有工作任务信息: {work_task_info_list}")
            return work_task_info_list
        return None

    @classmethod
    def add_work_task_info(cls, sub_task_info: dict) -> bool:
        task_key = ComRedisKeyConstant.WORK_TASK_ONLINE_PREFIX + sub_task_info['task_id'] + sub_task_info['sub_task_id']
        work_task_info_json = REDIS_TOOLS.get(task_key)
        work_task_info = json.loads(work_task_info_json)
        if work_task_info is not None and work_task_info['task_id'] + work_task_info['sub_task_id'] \
                == sub_task_info['task_id'] + sub_task_info['sub_task_id']:
            logging.debug("当前添加工作子任务已存在在Cache中，可以正常工作")
            return False
        cls.write_work_task_to_cache(sub_task_info)
        return True

    @classmethod
    def delete_work_task_info(cls, sub_task_info: dict) -> bool:
        task_key = ComRedisKeyConstant.WORK_TASK_ONLINE_PREFIX + sub_task_info['task_id'] + sub_task_info['sub_task_id']
        task_info_key = ComRedisKeyConstant.WORK_TASK_ONLINE_SET
        work_task_info_json = REDIS_TOOLS.get(task_key)
        work_task_info = json.loads(work_task_info_json)
        if work_task_info is not None and work_task_info['task_id'] + work_task_info['sub_task_id'] \
                == sub_task_info['task_id'] + sub_task_info['sub_task_id']:
            REDIS_TOOLS.delete(task_key)
            REDIS_TOOLS.set_remove(task_info_key, sub_task_info['task_id'] + sub_task_info['sub_task_id'])
            return True
        else:
            logging.info("没有找到要删除的工作任务" + sub_task_info['task_id'] + sub_task_info['sub_task_id'])
        return False

    @classmethod
    def delete_work_task_info_by_id(cls, task_id: str, sub_task_id: str) -> bool:
        task_key = ComRedisKeyConstant.WORK_TASK_ONLINE_PREFIX + task_id + sub_task_id
        task_info_key = ComRedisKeyConstant.WORK_TASK_ONLINE_SET
        work_task_info_json = REDIS_TOOLS.get(task_key)
        work_task_info = json.loads(work_task_info_json)
        if work_task_info is not None and work_task_info['task_id'] + work_task_info['sub_task_id'] \
                == task_id + sub_task_id:
            REDIS_TOOLS.delete(task_key)
            REDIS_TOOLS.set_remove(task_info_key, task_id + sub_task_id)
            return True
        else:
            logging.info("没有找到要删除的工作任务" + task_id + sub_task_id)
        return False

    @classmethod
    def modify_work_task_info(cls, sub_task_info: dict) -> bool:
        task_key = ComRedisKeyConstant.WORK_TASK_ONLINE_PREFIX + sub_task_info['task_id'] + sub_task_info['sub_task_id']
        work_task_info_json = REDIS_TOOLS.get(task_key)
        work_task_info = json.loads(work_task_info_json)
        if work_task_info is not None and work_task_info['task_id'] + work_task_info['sub_task_id'] \
                == sub_task_info['task_id'] + sub_task_info['sub_task_id']:
            cls.write_work_task_to_cache(sub_task_info)
            return True
        else:
            logging.info("没有找到要修改的工作任务" + sub_task_info['task_id'] + sub_task_info['sub_task_id'])
        return False

    @classmethod
    def query_work_task_info(cls, sub_task_info: dict) -> dict:
        task_key = ComRedisKeyConstant.WORK_TASK_ONLINE_PREFIX + sub_task_info['task_id'] + sub_task_info['sub_task_id']
        work_task_info_json = REDIS_TOOLS.get(task_key)
        work_task_info = json.loads(work_task_info_json)
        return work_task_info

    @classmethod
    def query_work_task_info_by_id(cls, task_id: str, sub_task_id: str) -> dict:
        task_key = ComRedisKeyConstant.WORK_TASK_ONLINE_PREFIX + task_id + sub_task_id
        work_task_info_json = REDIS_TOOLS.get(task_key)
        work_task_info = json.loads(work_task_info_json)
        return work_task_info

    @classmethod
    def add_work_task_list(cls, sub_task_info_list: List[dict]) -> bool:
        num = 0
        if len(sub_task_info_list) != 0:
            for sub_task_info in sub_task_info_list:
                event = cls.add_work_task_info(sub_task_info)
                num += 1 if event else 0
            if len(sub_task_info_list) == num:
                logging.debug("该工作任务队列添加到Cache成功")
                return True
        logging.info("该工作任务队列存入Cache失败，存在发送失败子任务")
        return False

    @classmethod
    def delete_work_task_list(cls, sub_task_info_list: List[dict]) -> bool:
        num = 0
        if len(sub_task_info_list) != 0:
            for sub_task_info in sub_task_info_list:
                event = cls.delete_work_task_info(sub_task_info)
                num += 1 if event else 0
            if len(sub_task_info_list) == num:
                logging.debug("该工作任务队列从Cache中删除成功")
                return True
        logging.info("该工作任务队列存从Cache中删除失败")
        return False

    @classmethod
    def delete_work_task_list_by_id(cls, task_id: str) -> bool:
        sub_task_info = cls.query_work_task_info_by_id(task_id, '0')
        if sub_task_info is not None:
            num = sub_task_info['sub_task_count']
            for i in range(num):
                cls.delete_work_task_info_by_id(task_id, str(i))
            return True
        return False

    @classmethod
    def modify_work_task_list(cls, sub_task_info_list: List[dict]) -> bool:
        num = 0
        if len(sub_task_info_list) != 0:
            for sub_task_info in sub_task_info_list:
                event = cls.modify_work_task_info(sub_task_info)
                num += 1 if event else 0
            if len(sub_task_info_list) == num:
                logging.debug("该工作任务队列从Cache中修改成功")
                return True
        logging.info("该工作任务队列存从Cache中修改失败")
        return False

    @classmethod
    def query_work_task_list(cls, sub_task_info_list: List[dict]) -> List[dict] | None:
        sub_task_list = []
        if len(sub_task_info_list) != 0:
            for sub_task_info in sub_task_info_list:
                sub_task_info_2 = cls.query_work_task_info(sub_task_info)
                if sub_task_info_2 is None:
                    sub_task_list.append(cls.query_work_task_info(sub_task_info))
            logging.debug("该工作任务队列从Cache中找到")
            return sub_task_list
        logging.info("该工作任务队列从Cache中没有找到")
        return None

    @classmethod
    def query_work_task_list_by_id(cls, task_id: str) -> List[dict] | None:
        sub_task_list = []
        sub_task_info = cls.query_work_task_info_by_id(task_id, '0')
        if sub_task_info is not None:
            num = sub_task_info['sub_task_count']
            for i in range(num):
                sub_task_list.append(cls.query_work_task_info_by_id(task_id, str(i)))
            return sub_task_list
        return None

    @classmethod
    def query_work_task_num(cls, task_id: str) -> int:
        sub_task_info = cls.query_work_task_info_by_id(task_id, '0')
        if sub_task_info is not None:
            return sub_task_info['sub_task_count']
        return 0

    @classmethod
    def query_work_task_key_set_num(cls) -> int:
        key_set = REDIS_TOOLS.sget(ComRedisKeyConstant.WORK_TASK_ONLINE_SET)
        if key_set is not None:
            return len(key_set)
        return 0

    @classmethod
    def obtain_could_parameter(cls, task_id: str) -> Dict[str, str]:
        sub_task_info = cls.query_work_task_info_by_id(task_id, '0')
        param_dict = sub_task_info['task_algorithm']['param_dict']
        key_set = param_dict.keys()
        for param_name in key_set:
            param = param_name.split("_")
            if param[0] == 'CLOUD':
                param_dict[param[1]] = param_dict[param_name]
        return param_dict
