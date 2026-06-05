from typing import Dict, List

from task.bean.data_info import DataInfo


class DataCenter:
    def get_sub_data_info(self, data_name) -> list:
        """

        :param data_name: 数据文件名称
        :return: 数据库中该数据文件下的所有子数据信息
        """
        pass

    def get_all_data_info(self, data_info_list) -> Dict[str, List[DataInfo]]:
        """

        :param data_info_list: 前端传来的任务数据文件信息
        :return: 所有不同类型数据的子数据信息集合
        """
        pass

    def get_all_data_info_num(self, data_info_list: list) -> int:
        """

        :param data_info_list: 任务数据文件(子数据文件)信息
        :return: 任务数据文件(子数据文件)的个数
        """
