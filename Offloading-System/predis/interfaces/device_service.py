from __future__ import annotations

from typing import Set, Dict, TypeVar


class DeviceService:
    T = TypeVar('T')

    def write_device_to_cache(self, device_info: dict):
        """
        将设备性能信息写入缓存
        :param device_info: 设备信息
        :return: None
        """
        pass

    def write_device_to_cache_performance(self, device_info: dict):
        """
        设备性能信息存入Cache接口实现，设备信息依据性能自动排序
        :param device_info: 设备信息
        :return: None
        """
        pass

    def write_device_to_cache_memory(self, device_info: dict):
        """
        设备空闲内存信息存入Cache接口实现，设备信息依据空闲内存自动排序
        :param device_info: 设备信息
        :return: None
        """
        pass

    def write_device_to_cache_delay(self, device_info: dict):
        """
        将设备延迟信息写入缓存
        :param device_info: 设备信息
        :return: None
        """
        pass

    def query_device_info_by_id(self, device_id: str) -> dict:
        """
        通过device_id查询一个设备信息
        :param device_id: 设备编号
        :return: 查询到的设备信息
        """
        pass

    def read_device_info(self) -> Dict[str, dict] | None:
        """
        将redis中的设备信息取出
        :return: None
        """
        pass

    def read_device_info_random(self, count: int) -> Dict[str, dict] | None:
        """
        随机将redis中的设备信息取出，可选择取出台数
        :param count: 值
        :return: 设备信息列表
        """
        pass

    def read_device_info_delay(self, min: float, max: float) -> Dict[str, dict] | None:
        """
        将redis中的设备延迟信息列表取出
        :param min: 值
        :param max: 值
        :return: 设备延迟顺序列表
        """
        pass

    def read_device_info_memory(self, min: float, max: float) -> Dict[str, dict] | None:
        """
        将redis中的设备空闲内存信息列表取出
        :param min: 值
        :param max: 值
        :return: 设备空闲内存顺序列表
        """
        pass

    def read_device_info_performance(self, start: float, end: float) -> Dict[str, dict] | None:
        """
        将redis中的设备性能信息列表取出
        :param start: 值
        :param end: 值
        :return: 设备性能顺序列表
        """
        pass

    def return_device_info_map(self, key_set: Set[T]) -> Dict[str, dict] | None:
        """
        返回设备信息
        :param key_set: 设备编号集合
        :return: 设备信息列表
        """
        pass

    def clear_cache(cls):
        """
        清空 Redis 中与设备相关的所有缓存
        :return: None
        """
        pass