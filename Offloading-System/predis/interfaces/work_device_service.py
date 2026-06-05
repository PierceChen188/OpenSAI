from __future__ import annotations

from typing import Dict


class WorkDeviceService:
    def write_work_device_to_cache(self, device_info: dict):
        """
        将正在工作的设备信息写入缓存
        :param device_info: 工作设备信息
        :return: None
        """
        pass

    def read_work_device_info(self) -> Dict[str, dict] | None:
        """
        将正在工作的设备信息从缓存读出
        :return: 所有当前正在执行任务的设备
        """
        pass

    def add_work_device_info(self, device_info: dict) -> bool:
        """
        添加一条工作的设备信息
        :param device_info: 工作设备信息
        :return: 是否添加设备信息成功
        """
        pass

    def delete_work_device_info(self, device_info: dict) -> bool:
        """
        删除一条工作的设备信息
        :param device_info: 工作设备信息
        :return: 是否删除设备信息成功
        """
        pass

    def delete_work_device_info_by_id(self, device_id: str) -> bool:
        """
        删除一条工作的设备信息
        :param device_id: 设备编号
        :return: 是否删除设备信息成功
        """
        pass

    def modify_work_device_info(self, device_info: dict) -> bool:
        """
        修改一条工作的设备信息
        :param device_info: 工作设备信息
        :return: 是否修改设备信息成功
        """
        pass

    def query_work_device_info(self, device_info: dict) -> dict:
        """
        查询一个正在工作的设备信息
        :param device_info: 工作设备信息
        :return: 查询到的设备信息
        """
        pass

    def query_work_device_info_by_id(self, device_id: str) -> dict:
        """
        查询一个正在工作的设备信息
        :param device_id: 设备编号
        :return: 查询到的设备信息
        """
        pass

    def query_work_device_by_address(self, address: tuple[str, int]) -> dict:
        """
        通过address(IP, PORT)查询一个正在工作的设备信息
        :param address: 设备通信地址(IP, PORT)
        :return: 查询到的设备信息
        """
        pass

    def delete_work_device_by_address(self, address: tuple[str, int]) -> bool:
        """
        通过address(IP, PORT)删除一个正在工作的设备信息
        :param address: 设备通信地址(IP, PORT)
        :return:
        """
        pass

    def add_work_device_list(self, device_info_list: Dict[str, dict]) -> bool:
        """
        添加工作设备信息列表到Cache
        :param device_info_list: 工作设备信息列表
        :return: 是否添加成功True or False
        """
        pass

    def delete_work_device_list(self, device_info_list: Dict[str, dict]) -> bool:
        """
        从Cache删除工作设备信息列表
        :param device_info_list: 工作设备信息列表
        :return: 是否删除成功True or False
        """
        pass

    def modify_work_device_list(self, device_info_list: Dict[str, dict]) -> bool:
        """
        修改工作设备信息列表
        :param device_info_list: 工作设备信息列表
        :return: 是否修改成功True or False
        """
        pass

    def query_work_device_list(self, device_info_list: Dict[str, dict]) -> bool:
        """
        查询工作设备信息列表中的工作任务列表
        :param device_info_list: 工作设备信息列表
        :return: 设备信息列表中的设备是否在记录工作设备的Cache中
        """
        pass

    def query_work_device_num(self) -> int:
        """
        获取正在工作的设备个数
        :return: 正在进行工作的设备数量
        """
        pass

    def query_work_device_key_set_num(self) -> int:
        """
        判断Cache中当前有几个工作设备信息
        :return: Cache中当前有几个工作设备信息
        """
        pass


