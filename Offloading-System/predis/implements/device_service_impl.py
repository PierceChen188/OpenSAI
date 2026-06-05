from __future__ import annotations

import json
import logging
import random
from typing import Dict, Set, TypeVar

from config.common_redis_key_constant import ComRedisKeyConstant
from config.logging_config import LOG_FORMAT
from config.redis_boot import REDIS_TOOLS
from device.bean.devce_info import DeviceInfo
from device.implements.idle_device_impl import IdleDeviceImpl
from predis.interfaces.device_service import DeviceService


class DeviceServiceImpl(DeviceService):
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
    T = TypeVar('T')

    device_map = {}    # key:device_no, value:device_name
    device_num = 0

    @classmethod
    def write_device_to_cache(cls, device_info: dict):
        if device_info['device_no'] not in cls.device_map:
            cls.device_num += 1
            cls.device_map[device_info['device_no']] = "设备"+str(cls.device_num)
        device_set_key = ComRedisKeyConstant.DEVICE_ONLINE_SET
        REDIS_TOOLS.sset_and_time(device_set_key, 86400, device_info['device_no'])
        device_key = ComRedisKeyConstant.DEVICE_ONLINE_PREFIX + device_info['device_no']
        REDIS_TOOLS.set_key_value_with_ttl(device_key, json.dumps(device_info), 86400)

    @classmethod
    def query_device_info_by_id(cls, device_id: str) -> dict:
        key = ComRedisKeyConstant.DEVICE_ONLINE_PREFIX + device_id
        device_info_json = REDIS_TOOLS.get(key)
        logging.debug(f"找到设备：{device_info_json}")
        device_info = json.loads(device_info_json)
        return device_info

    @classmethod
    def write_device_to_cache_performance(cls, device_info: dict):
        logging.info(f"请求性能信息参数信息为：{device_info}")
        device_info_1 = DeviceInfo()
        device_info_1.set_device_from_dict(device_info)
        if IdleDeviceImpl.idle_devices(device_info):
            # 计算设备性能
            core_num = device_info_1.get_core_num()
            work_status = device_info_1.get_work_status()
            used_cpu = device_info_1.get_used_cpu()
            idle_memory = device_info_1.get_idle_memory()
            total_memory = device_info_1.get_total_memory()
            idle_core_num = core_num - work_status
            idle_cpu = 1.0 - float(used_cpu.strip("%"))
            high_index = -(idle_core_num * idle_cpu * idle_memory / total_memory)
            # 设备性能编号集合10秒自动销毁
            device_set_key = ComRedisKeyConstant.DEVICE_ONLINE_PERFORMANCE_SET
            REDIS_TOOLS.zset_with_ttl(device_set_key, device_info_1.get_device_no(), high_index, 10)
        else:
            logging.info(f"设备{device_info_1.get_device_no()}不满足空闲设备条件...")

    def write_device_to_cache_memory(self, device_info: dict):
        logging.info(f"请求空闲内存信息参数信息为：{device_info}")
        device_info_1 = DeviceInfo()
        device_info_1.set_device_from_dict(device_info)
        if IdleDeviceImpl.idle_devices(device_info):
            delay = device_info_1.get_idle_memory()
            # 设备空闲内存编号集合10秒自动销毁
            device_set_key = ComRedisKeyConstant.DEVICE_ONLINE_MEMORY_SET
            REDIS_TOOLS.zset_with_ttl(device_set_key, device_info_1.get_device_no(), delay, 10)
        else:
            logging.info(f"设备{device_info_1.get_device_no()}不满足空闲设备条件...")

    def write_device_to_cache_delay(self, device_info: dict):
        logging.info(f"请求距离信息参数信息为：{device_info}")
        device_info_1 = DeviceInfo()
        device_info_1.set_device_from_dict(device_info)
        if IdleDeviceImpl.idle_devices(device_info):
            delay = device_info_1.get_delay()
            # 设备延迟编号集合10秒自动销毁
            device_set_key = ComRedisKeyConstant.DEVICE_ONLINE_DELAY_SET
            REDIS_TOOLS.zset_with_ttl(device_set_key, device_info_1.get_device_no(), delay, 10)
        else:
            logging.info(f"设备{device_info_1.get_device_no()}不满足空闲设备条件...")

    @classmethod
    def read_device_info(cls) -> Dict[str, dict] | None:
        device_info_list = {}
        key_set = REDIS_TOOLS.sget(ComRedisKeyConstant.DEVICE_ONLINE_SET)
        if key_set is not None:
            for s in key_set:
                device_no = s.decode()
                key = ComRedisKeyConstant.DEVICE_ONLINE_PREFIX + device_no
                device_info_json = REDIS_TOOLS.get(key)
                device_info = json.loads(device_info_json)
                device_name = cls.device_map[device_no] if device_no in cls.device_map else "Unknown"
                if device_info is not None:
                    device_info["device_name"] = device_name  # 添加 device_name 字段
                    device_info_list[device_no] = device_info
                # logging.info(f"所有设备信息: {device_info_list}")
        return device_info_list

    @classmethod
    def read_device_info_random(cls, count: int) -> Dict[str, dict] | None:
        set_size = 0
        num = 0
        device_info_list = {}
        device_id_set = REDIS_TOOLS.sget(ComRedisKeyConstant.DEVICE_ONLINE_SET)
        device_id_list = list(device_id_set)
        device_size = len(device_id_set)
        if device_size != 0:
            while set_size < count and num < device_size:
                device_id = random.choice(device_id_list).decode()
                device_id_list.remove(device_id.encode())
                key = ComRedisKeyConstant.DEVICE_ONLINE_PREFIX + device_id
                device_info_json = REDIS_TOOLS.get(key)
                device_info = json.loads(device_info_json)
                device_name = cls.device_map[device_id] if device_id in cls.device_map else "Unknown"
                if device_info is not None:
                    device_info["device_name"] = device_name  # 添加 device_name 字段
                print("device_info", device_info_json)
                if IdleDeviceImpl.idle_devices(device_info):
                    device_info_list[device_id] = device_info
                set_size = len(device_info_list)
                num = num + 1
                if set_size < count:
                    logging.info(f"请求设备数量：{count}, 实际满足条件设备数量：{set_size}, 符合条件的设备数量不足...")
                    if set_size == 0 and count == 1:
                        logging.info("无满足空闲条件设备，随机返回一台设备...")
                        device_id_set = REDIS_TOOLS.sget_count(ComRedisKeyConstant.DEVICE_ONLINE_SET, 1)
                        device_id_1 = next(iter(device_id_set)).__str__()
                        key_1 = ComRedisKeyConstant.DEVICE_ONLINE_PREFIX + device_id_1
                        device_info_1 = REDIS_TOOLS.get(key_1)
                        device_info_list[key_1] = device_info_1
                        logging.info(f"设备信息:{device_info_list}")
                    else:
                        logging.info(f"设备编号:{set(device_info_list.keys())}")
                        logging.info(f"所有设备信息:{device_info_list}")
                logging.info(f"read_device_info_random: {device_info_list}")
                return device_info_list
        else:
            logging.info("无在线设备...")
            return None

    @classmethod
    def read_device_info_delay(cls, min: float, max: float) -> Dict[str, dict] | None:
        key_set = REDIS_TOOLS.zget(ComRedisKeyConstant.DEVICE_ONLINE_DELAY_SET, min, max)
        REDIS_TOOLS.remove_range(ComRedisKeyConstant.DEVICE_ONLINE_DELAY_SET, min, max)
        logging.info(f"取出低延迟设备编号:{key_set}")
        return cls.return_device_info_map(key_set)

    @classmethod
    def read_device_info_memory(cls, min: float, max: float) -> Dict[str, dict] | None:
        key_set = REDIS_TOOLS.zget(ComRedisKeyConstant.DEVICE_ONLINE_MEMORY_SET, min, max)
        REDIS_TOOLS.remove_range(ComRedisKeyConstant.DEVICE_ONLINE_MEMORY_SET, min, max)
        logging.info(f"取出空闲内存设备编号:{key_set}")
        return cls.return_device_info_map(key_set)

    @classmethod
    def read_device_info_performance(cls, start: float, end: float) -> Dict[str, dict] | None:
        key_set = REDIS_TOOLS.zget(ComRedisKeyConstant.DEVICE_ONLINE_PERFORMANCE_SET, start, end)
        REDIS_TOOLS.remove_range(ComRedisKeyConstant.DEVICE_ONLINE_PERFORMANCE_SET, start, end)
        logging.info(f"取出高性能设备编号:{key_set}")
        return cls.return_device_info_map(key_set)

    @classmethod
    def return_device_info_map(cls, key_set: Set[T]) -> Dict[str, dict] | None:
        device_info_list = {}
        if key_set is not None:
            for s in key_set:
                device_no = s.decode()
                key = ComRedisKeyConstant.DEVICE_ONLINE_PREFIX + device_no
                device_info_json = REDIS_TOOLS.get(key)
                device_info = json.loads(device_info_json)
                if device_info is not None:
                    device_info_list[device_no] = device_info
            logging.info(f"所有设备信息: {device_info_list}")
            return device_info_list
        else:
            return None

    @classmethod
    def clear_cache(cls):

        # 清空设备在线集合
        REDIS_TOOLS.delete(ComRedisKeyConstant.DEVICE_ONLINE_SET)

        # 清空设备在线缓存
        device_keys = REDIS_TOOLS.keys(ComRedisKeyConstant.DEVICE_ONLINE_PREFIX + "*")
        if device_keys:
            REDIS_TOOLS.delete(*device_keys)
        # 清空性能、高性能、低延迟和空闲内存相关集合
        REDIS_TOOLS.delete(ComRedisKeyConstant.DEVICE_ONLINE_PERFORMANCE_SET)
        REDIS_TOOLS.delete(ComRedisKeyConstant.DEVICE_ONLINE_DELAY_SET)
        REDIS_TOOLS.delete(ComRedisKeyConstant.DEVICE_ONLINE_MEMORY_SET)

        logging.info("Redis 缓存清空完成！")