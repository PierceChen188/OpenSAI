import logging

from config.logging_config import LOG_FORMAT
from config.redis_boot import REDIS_TOOLS
from predis.interfaces.function_runtime_service import FunctionRunTimeService


class FunctionRunTimeServiceImpl(FunctionRunTimeService):
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

    @classmethod
    def write_function_runtime_to_cache(cls, task_id: str, function_name: str, run_time: int):
        result_info_key = function_name + "_SET"
        REDIS_TOOLS.sset_and_time(result_info_key, 86400, task_id)
        result_key = function_name + "_PREFIX" + task_id
        REDIS_TOOLS.set(result_key, run_time, 86400)

    @classmethod
    def read_function_runtime(cls, task_id: str, function_name: str) -> int:
        return REDIS_TOOLS.get(function_name + "_PREFIX" + task_id)
