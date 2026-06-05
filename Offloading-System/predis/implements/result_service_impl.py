from __future__ import annotations

import json
import logging
from typing import List, Dict

from config.common_redis_key_constant import ComRedisKeyConstant
from config.logging_config import LOG_FORMAT
from config.redis_boot import REDIS_TOOLS
from predis.interfaces.result_service import ResultService


class ResultServiceImpl(ResultService):
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

    @classmethod
    def get_num_revc_sub_task(cls, task_id: str) -> int:
        fuzz_key = ComRedisKeyConstant.RESULT_ONLINE_PREFIX + task_id + "*"
        subtask_keys = REDIS_TOOLS.keys(fuzz_key)
        return len(subtask_keys)

    @classmethod
    def read_result_info_by_result_id(cls, task_id: str):
        fuzz_key = ComRedisKeyConstant.RESULT_ONLINE_PREFIX + task_id + "*"
        result_keys = REDIS_TOOLS.keys(fuzz_key)
        result_list = REDIS_TOOLS.mget(result_keys)
        return result_list

    @classmethod
    def read_result_info(cls) -> List[dict]:
        result_info_list = []
        key_set = REDIS_TOOLS.sget(ComRedisKeyConstant.RESULT_ONLINE_SET)
        if key_set is not None:
            for s in key_set:
                task_subtask_id = s.decode()
                key = ComRedisKeyConstant.RESULT_ONLINE_PREFIX + task_subtask_id
                result_info_json = REDIS_TOOLS.get(key)
                result_info = json.loads(result_info_json)
                if result_info is not None:
                    result_info_list.append(result_info)
                logging.info(f"所有结果信息: {result_info_list}")
        return result_info_list

    @classmethod
    def write_result_to_cache(cls, result_info: dict):
        result_set_key = ComRedisKeyConstant.RESULT_ONLINE_SET
        REDIS_TOOLS.sset_and_time(result_set_key, 86400, result_info['task_id'] + result_info['sub_task_id'])
        result_key = ComRedisKeyConstant.RESULT_ONLINE_PREFIX + result_info['task_id'] + result_info['sub_task_id']
        REDIS_TOOLS.set_key_value_with_ttl(result_key, json.dumps(result_info), 86400)

    @classmethod
    def add_result_info(cls, result_info: dict) -> bool:
        result_key = ComRedisKeyConstant.RESULT_ONLINE_PREFIX + result_info['task_id'] + result_info['sub_task_id']
        result_info_json = REDIS_TOOLS.get(result_key)
        result_info_temp = result_info_json.load(result_info_json)
        if result_info_temp is not None and result_info_temp['task_id'] + result_info_temp['sub_task_id'] == \
                result_info['task_id'] + result_info['sub_task_id']:
            logging.debug("当前添加结果已存在Cache中，可以正常工作")
            return False
        cls.write_result_to_cache(result_info)
        return True

    @classmethod
    def delete_result_info(cls, result_info: dict) -> bool:
        result_key = ComRedisKeyConstant.RESULT_ONLINE_PREFIX + result_info['task_id'] + result_info['sub_task_id']
        result_set_key = ComRedisKeyConstant.RESULT_ONLINE_SET
        result_info_json = REDIS_TOOLS.get(result_key)
        result_info_temp = result_info_json.load(result_info_json)
        if result_info_temp is not None and result_info_temp['task_id'] + result_info_temp['sub_task_id'] == \
                result_info['task_id'] + result_info['sub_task_id']:
            REDIS_TOOLS.delete(result_key)
            REDIS_TOOLS.set_remove(result_set_key, result_info['task_id'] + result_info['sub_task_id'])
            return True
        else:
            if result_info['sub_task_id'] != '':
                logging.info("没有找到要删除的结果" + result_info['task_id'] + result_info['sub_task_id'])
        return False

    @classmethod
    def delete_result_info_by_id(cls, task_id: str, sub_task_id: str) -> bool:
        result_key = ComRedisKeyConstant.RESULT_ONLINE_PREFIX + task_id + sub_task_id
        result_set_key = ComRedisKeyConstant.RESULT_ONLINE_SET
        result_info_json = REDIS_TOOLS.get(result_key)
        result_info_temp = result_info_json.load(result_info_json)
        if result_info_temp is not None and result_info_temp['task_id'] + result_info_temp['sub_task_id'] == \
                task_id + sub_task_id:
            REDIS_TOOLS.delete(result_key)
            REDIS_TOOLS.set_remove(result_set_key, task_id + sub_task_id)
            return True
        else:
            logging.info("没有找到要删除的结果的key值")
        return False

    @classmethod
    def modify_result_info(cls, result_info: dict) -> dict | bool:
        result_key = ComRedisKeyConstant.RESULT_ONLINE_PREFIX + result_info['task_id'] + result_info['sub_task_id']
        result_info_json = REDIS_TOOLS.get(result_key)
        result_info_temp = result_info_json.load(result_info_json)
        if result_info_temp is not None and result_info_temp['task_id'] + result_info_temp['sub_task_id'] == \
                result_info['task_id'] + result_info['sub_task_id']:
            cls.write_result_to_cache(result_info)
            return result_info
        else:
            logging.info(f"没有找到要修改的结果{result_info['task_id']}{result_info['sub_task_id']}")
        return False

    @classmethod
    def query_result_info(cls, result_info: dict) -> dict:
        result_key = ComRedisKeyConstant.RESULT_ONLINE_PREFIX + result_info['task_id'] + result_info['sub_task_id']
        result_info_json = REDIS_TOOLS.get(result_key)
        result_info_temp = result_info_json.load(result_info_json)
        return result_info_temp

    @classmethod
    def query_result_info_by_id(cls, task_id: str, sub_task_id: str) -> dict:
        result_key = ComRedisKeyConstant.RESULT_ONLINE_PREFIX + task_id + sub_task_id
        result_info_json = REDIS_TOOLS.get(result_key)
        result_info_temp = result_info_json.load(result_info_json)
        return result_info_temp

    @classmethod
    def add_result_list(cls, result_info_list: Dict[str, dict]) -> bool:
        result_list = []
        num = 0
        if len(result_list) != 0:
            for resultInfo in result_list:
                event = cls.add_result_info(resultInfo)
                num += 1 if event else 0
            if num == len(result_list):
                logging.debug("该结果队列添加到Cache成功")
                return True
        logging.info("该结果队列存入Cache失败")
        return False

    @classmethod
    def delete_result_list(cls, result_info_list: List[dict]) -> bool:
        if result_info_list:
            keys = [ComRedisKeyConstant.RESULT_ONLINE_PREFIX + ri['task_id'] + ri['[sub_task_id]'] for ri in
                    result_info_list]
            prefix_task_sub_task_list = []
            task_sub_task_list = []
            for ri in result_info_list:
                prefix_task_sub_task_list.append(
                    ComRedisKeyConstant.RESULT_ONLINE_PREFIX + ri['task_id'] + ri['sub_task_id'])
                task_sub_task_list.append(ri['task_id'] + ri['sub_task_id'])
            REDIS_TOOLS.delete(prefix_task_sub_task_list)
            REDIS_TOOLS.set_remove(ComRedisKeyConstant.RESULT_ONLINE_SET, task_sub_task_list)
        return True

    @classmethod
    def delete_result_list_by_id(cls, task_id: str) -> bool:
        result_info_list = cls.read_result_info()
        if result_info_list is not None:
            for result_info in result_info_list:
                if result_info['task_id'] == task_id:
                    cls.delete_result_info(result_info)
            return True
        return False

    @classmethod
    def modify_result_list(cls, result_info_list: List[dict]) -> bool:
        num = 0
        if len(result_info_list) != 0:
            for result_info in result_info_list:
                event = cls.modify_result_info(result_info)
                num += 1 if event else 0
            if num == len(result_info_list):
                logging.debug("该任务队列从Cache中修改成功")
                return True
        logging.info("该任务队列存从Cache中修改失败")
        return False

    @classmethod
    def query_result_list(cls, result_info_list: List[dict]) -> List[dict] | None:
        result_list = []
        if len(result_info_list) != 0:
            for result_info in result_info_list:
                result_info_2 = cls.query_result_info(result_info)
                result_list.append(result_info_2)
            logging.debug("该任务队列从Cache中修改成功")
            return result_list
        logging.info("该任务队列从Cache中修改失败")
        return None

    @classmethod
    def query_result_list_by_id(cls, task_id: str) -> List[dict] | None:
        result_info_list = cls.read_result_info()
        result_info_list_temp = []
        if result_info_list is not None:
            for result_info in result_info_list:
                if result_info['task_id'] == task_id:
                    result_info_list_temp.append(result_info)
            return result_info_list_temp
        return None

    @classmethod
    def query_result_num(cls, task_id: str) -> int:
        return len(cls.read_result_info_by_result_id(task_id))

    @classmethod
    def query_result_key_set_num(cls) -> int:
        key_set = REDIS_TOOLS.sget(ComRedisKeyConstant.RESULT_ONLINE_SET)
        if key_set is not None:
            return len(key_set)
        return 0

    @classmethod
    def write_result_to_web(cls, result_info_web: dict):
        result_set_key = ComRedisKeyConstant.RESULT_ONLINE_SET
        REDIS_TOOLS.sset(result_set_key, result_info_web['task_id'])
        result_web_key = ComRedisKeyConstant.RESULT_WEB_PREFIX + result_info_web['task_id']
        result_info_json = json.dumps(result_info_web)
        REDIS_TOOLS.set_key_value(result_web_key, result_info_json)

    @classmethod
    def read_result_to_web(cls) -> Dict[str, dict] | None:
        result_info_dict = {}
        key_set = REDIS_TOOLS.sGet(ComRedisKeyConstant.RESULT_ONLINE_SET)
        if key_set is not None:
            for s in key_set:
                task_id = s.decode()
                result_info_json = REDIS_TOOLS.get(ComRedisKeyConstant.RESULT_ONLINE_PREFIX + task_id)
                result_info_web = json.loads(result_info_json)
                if result_info_web is not None:
                    result_info_dict[task_id] = result_info_web
            logging.debug(f"给Web前端推送的所有任务结果信息:{result_info_dict}")
            return result_info_dict
        return None

    @classmethod
    def write_progress_to_web(cls, result_info: dict):
        process_set_key = ComRedisKeyConstant.RESULT_PROGRESS_SET
        REDIS_TOOLS.sset(process_set_key, result_info['task_id'] + result_info['sub_task_id'])
        process_web_key = ComRedisKeyConstant.RESULT_WEB_PROGRESS + result_info['task_id'] + result_info['sub_task_id']
        REDIS_TOOLS.set_key_value(process_web_key, result_info['result_dict']['progress'])

    @classmethod
    def read_progress_to_web(cls) -> Dict[str, float] | None:
        key_set = REDIS_TOOLS.sget(ComRedisKeyConstant.RESULT_PROGRESS_SET)
        process_dict = {}
        if key_set is not None:
            for s in key_set:
                task_subtask_id = s.decode()
                process_web = REDIS_TOOLS.get(ComRedisKeyConstant.RESULT_WEB_PROGRESS + task_subtask_id)
                if process_web is not None:
                    process_dict[task_subtask_id] = process_web
            logging.debug("给Web前端推送的所有子任务进度:{}", process_dict)
            return process_dict
        return None

    @classmethod
    def query_progress(cls, result_info: dict) -> float:
        progress = -1
        result_key = ComRedisKeyConstant.RESULT_LBFO_PROGRESS + result_info['task_id'] + result_info['sub_task_id']
        result_info_json = REDIS_TOOLS.get(result_key)
        result_info_1 = json.loads(result_info_json)
        if result_info_1 is not None:
            return result_info_1['result_dict']['progress']
        return progress

    @classmethod
    def write_progress_to_lbfo(cls, result_info: dict):
        process_lbfo_key = ComRedisKeyConstant.RESULT_LBFO_PROGRESS + result_info['task_id'] \
                           + result_info['sub_task_id']
        result_info_json = json.dumps(result_info)
        REDIS_TOOLS.set_key_value(process_lbfo_key, result_info_json)

    @classmethod
    def read_progress_to_lbfo(cls, task_id: str, sub_task_id: str) -> dict:
        process_lbfo_key = ComRedisKeyConstant.RESULT_LBFO_PROGRESS + task_id + sub_task_id
        result_info_json = REDIS_TOOLS.get(process_lbfo_key)
        result_info_lbfo = json.loads(result_info_json)
        return result_info_lbfo
