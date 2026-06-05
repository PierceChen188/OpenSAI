from __future__ import annotations

import json
import logging
import time
from typing import Dict

from config.common_redis_key_constant import ComRedisKeyConstant
from config.logging_config import LOG_FORMAT
from config.redis_boot import REDIS_TOOLS
from predis.interfaces.device_monitor_service import DeviceMonitorService


class DeviceMonitorServiceImpl(DeviceMonitorService):
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

    def write_history_monitor(self, device_info: dict):
        device_set_key = ComRedisKeyConstant.DEVICE_HISTORY_MONITOR_SET
        REDIS_TOOLS.sset_and_time(device_set_key, 86400, device_info['device_no'])
        device_key = ComRedisKeyConstant.DEVICE_HISTORY_MONITOR_PREFIX + device_info['device_no']
        REDIS_TOOLS.set_key_value_with_ttl(device_key+'&'+str(round(time.time() * 1000)), json.dumps(device_info), 300)

    def read_history_monitor(self, device_no: str) -> Dict[int, dict] | None:
        fuzz_key = ComRedisKeyConstant.DEVICE_HISTORY_MONITOR_PREFIX + device_no + "*"
        device_keys = REDIS_TOOLS.keys(fuzz_key)
        device_history_list = REDIS_TOOLS.mget(device_keys)
        return device_history_list

