from __future__ import annotations

import json
import logging
from typing import Dict

from config.common_redis_key_constant import ComRedisKeyConstant
from config.logging_config import LOG_FORMAT
from config.redis_boot import REDIS_TOOLS
from predis.interfaces.work_device_service import WorkDeviceService


class WorkDeviceServiceImpl(WorkDeviceService):
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

    @classmethod
    def write_work_device_to_cache(cls, device_info: dict):
        device_set_key = ComRedisKeyConstant.WORK_DEVICE_ONLINE_SET
        REDIS_TOOLS.sset_and_time(device_set_key, 86400, device_info['device_no'])
        device_channel_set_key = ComRedisKeyConstant.WORK_DEVICE_CHANNEL_ID_SET
        REDIS_TOOLS.sSetAndTime(device_channel_set_key, 86400, device_info['ip_address'] + ':' + device_info['port'])
        device_key = ComRedisKeyConstant.WORK_DEVICE_ONLINE_PREFIX + device_info['device_no']
        REDIS_TOOLS.set_key_value_with_ttl(device_key, json.dumps(device_info), 86400)
        device_channel_key = ComRedisKeyConstant.WORK_DEVICE_CHANNEL_ID_PREFIX + device_info['ip_address'] + ':' + \
                             device_info['port']
        REDIS_TOOLS.set_key_value_with_ttl(device_channel_key, device_info, 86400)

    @classmethod
    def read_work_device_info(cls) -> Dict[str, dict] | None:
        work_device_info_list = {}
        key_set = REDIS_TOOLS.sget(ComRedisKeyConstant.WORK_DEVICE_ONLINE_SET)
        if key_set is not None:
            for s in key_set:
                work_device_id = s.decode()
                key = ComRedisKeyConstant.WORK_TASK_ONLINE_PREFIX + work_device_id
                work_device_info_json = REDIS_TOOLS.get(key)
                work_device_info = json.loads(work_device_info_json)
                if work_device_info is not None:
                    work_device_info_list[work_device_id] = work_device_info
            logging.info(f"所有工作设备信息: {work_device_info_list}")
            return work_device_info_list
        return None

    @classmethod
    def add_work_device_info(cls, device_info: dict) -> bool:
        key = ComRedisKeyConstant.WORK_DEVICE_ONLINE_PREFIX + device_info['device_no']
        work_device_info_json = REDIS_TOOLS.get(key)
        work_device_info = json.loads(work_device_info_json)
        if work_device_info is not None and work_device_info['device_no'] == device_info['device_no']:
            logging.debug("当前添加设备已经在Cache中，可以正常工作")
            return False
        cls.write_work_device_to_cache(device_info)
        return True

    @classmethod
    def delete_work_device_info(cls, device_info: dict) -> bool:
        key = ComRedisKeyConstant.WORK_DEVICE_ONLINE_PREFIX + device_info['device_no']
        device_set_key = ComRedisKeyConstant.WORK_DEVICE_ONLINE_SET
        device_channel_key = ComRedisKeyConstant.WORK_DEVICE_CHANNEL_ID_PREFIX + device_info['ip_address'] + ':' + \
                             device_info['port']
        device_channel_set_key = ComRedisKeyConstant.WORK_DEVICE_CHANNEL_ID_SET
        device_info_json = REDIS_TOOLS.get(key)
        device_info_temp = json.loads(device_info_json)
        if device_info_temp is not None and device_info_temp['device_no'] == device_info['device_no']:
            REDIS_TOOLS.delete(key)
            REDIS_TOOLS.delete(device_channel_key)
            REDIS_TOOLS.set_remove(device_set_key, device_info['device_no'])
            REDIS_TOOLS.set_remove(device_channel_set_key, device_info['ip_address'] + ':' + device_info['port'])
            return True
        else:
            logging.info(f"没有找到要删除的设备{device_info['device_no']}")
        return False

    @classmethod
    def delete_work_device_info_by_id(cls, device_id: str) -> bool:
        key = ComRedisKeyConstant.WORK_DEVICE_ONLINE_PREFIX + device_id
        device_set_key = ComRedisKeyConstant.WORK_DEVICE_ONLINE_SET
        device_channel_set_key = ComRedisKeyConstant.WORK_DEVICE_CHANNEL_ID_SET
        device_info_json = REDIS_TOOLS.get(key)
        device_info_temp = json.loads(device_info_json)
        if device_info_temp is not None and device_info_temp['device_no'] == device_id:
            device_channel_key = ComRedisKeyConstant.WORK_DEVICE_CHANNEL_ID_PREFIX \
                                 + device_info_temp['ip_address'] + ':' + device_info_temp['port']
            REDIS_TOOLS.delete(key)
            REDIS_TOOLS.delete(device_channel_key)
            REDIS_TOOLS.set_remove(device_set_key, device_id)
            REDIS_TOOLS.set_remove(device_channel_set_key,
                                   device_info_temp['ip_address'] + ':' + device_info_temp['port'])
            return True
        else:
            logging.info(f"没有找到要删除的设备{device_id}")
        return False

    @classmethod
    def modify_work_device_info(cls, device_info: dict) -> bool:
        key = ComRedisKeyConstant.WORK_DEVICE_ONLINE_PREFIX + device_info['device_no']
        device_info_json = REDIS_TOOLS.get(key)
        device_info_temp = json.loads(device_info_json)
        if device_info_temp is not None and device_info_temp['device_no'] == device_info['device_no']:
            cls.write_work_device_to_cache(device_info)
            return True
        else:
            logging.info(f"没有找到要修改的的设备{device_info['device_no']}")
        return False

    @classmethod
    def query_work_device_info(cls, device_info: dict) -> dict:
        key = ComRedisKeyConstant.WORK_DEVICE_ONLINE_PREFIX + device_info['device_no']
        device_info_json = REDIS_TOOLS.get(key)
        device_info_temp = json.loads(device_info_json)
        return device_info_temp

    @classmethod
    def query_work_device_info_by_id(cls, device_id: str) -> dict:
        key = ComRedisKeyConstant.WORK_DEVICE_ONLINE_PREFIX + device_id
        device_info_json = REDIS_TOOLS.get(key)
        device_info_temp = json.loads(device_info_json)
        return device_info_temp

    @classmethod
    def query_work_device_by_address(cls, address: tuple[str, int]) -> dict:
        device_channel_key = ComRedisKeyConstant.WORK_DEVICE_CHANNEL_ID_PREFIX + address[0] + ':' + str(address[1])
        device_info_json = REDIS_TOOLS.get(device_channel_key)
        device_info_temp = json.loads(device_info_json)
        return device_info_temp

    @classmethod
    def delete_work_device_by_address(cls, address: tuple[str, int]) -> bool:
        device_channel_set_key = ComRedisKeyConstant.WORK_DEVICE_CHANNEL_ID_SET
        device_channel_key = ComRedisKeyConstant.WORK_DEVICE_CHANNEL_ID_PREFIX + address[0] + ':' + address[1]
        device_info_json = REDIS_TOOLS.get(device_channel_key)
        device_info_temp = json.loads(device_info_json)
        if device_info_temp is not None and device_info_temp['ip_address'] + ':' + device_info_temp['port'] \
                == address[0] + ':' + str(address[1]):
            REDIS_TOOLS.delete(device_channel_key)
            REDIS_TOOLS.set_remove(device_channel_set_key,
                                   device_info_temp['ip_address'] + ':' + device_info_temp['port'])
            return True
        else:
            logging.info(f"没有找到要删除设备的ChannelId值{address}")
        return False

    @classmethod
    def add_work_device_list(cls, device_info_list: Dict[str, dict]) -> bool:
        num = 0
        key_set = device_info_list.keys()
        for s in key_set:
            event = cls.add_work_device_info(device_info_list[s])
            num += 1 if event else 0
        if len(device_info_list) == num:
            logging.debug("该设备队列添加到Cache成功")
            return True
        return False

    @classmethod
    def delete_work_device_list(cls, device_info_list: Dict[str, dict]) -> bool:
        num = 0
        key_set = device_info_list.keys()
        for s in key_set:
            event = cls.delete_work_device_info(device_info_list[s])
            num += 1 if event else 0
        if len(device_info_list) == num:
            logging.debug("该设备队列从Cache删除成功")
            return True
        return False

    @classmethod
    def modify_work_device_list(cls, device_info_list: Dict[str, dict]) -> bool:
        num = 0
        key_set = device_info_list.keys()
        for s in key_set:
            event = cls.modify_work_device_info(device_info_list[s])
            num += 1 if event else 0
        if len(device_info_list) == num:
            logging.debug("该设备队列从Cache删除成功")
            return True
        return False

    @classmethod
    def query_work_device_list(cls, device_info_list: Dict[str, dict]) -> bool:
        num = 0
        key_set = device_info_list.keys()
        for s in key_set:
            event = cls.query_work_device_info(device_info_list[s])
            num += 1 if event else 0
        if len(device_info_list) == num:
            logging.debug("该设备队列已经添加到Cache中")
            return True
        return False

    @classmethod
    def query_work_device_num(cls) -> int:
        device_info_list = cls.read_work_device_info()
        return len(device_info_list)

    @classmethod
    def query_work_device_key_set_num(cls) -> int:
        key_set = REDIS_TOOLS.sget(ComRedisKeyConstant.WORK_DEVICE_ONLINE_SET)
        if key_set is not None:
            return len(key_set)
        return 0
