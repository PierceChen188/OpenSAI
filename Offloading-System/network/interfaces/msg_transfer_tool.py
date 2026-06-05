from typing import List, Tuple


class MsgTransferTool:
    def send_p2p_msg(self, msg: dict, msg_type: str, from_device_no: str, to_device_no: str,
                     to_addr: Tuple[str, int]) -> bool:
        """
        发送p2p消息到指定设备(不分片传输)
        :param msg: 消息正文
        :param msg_type: 消息类型
        :param from_device_no: 消息发送设备编号
        :param to_device_no: 消息接收设备编号
        :param to_addr: 接收地址, (ip, port)元组形式
        :return: 发送是否成功(True or False)
        """
        pass

    def send_bc_msg(self, msg: dict, msg_type: str, from_device_no: str, to_device_no_list: List[str],
                    to_addr_list: List[Tuple[str, int]]) -> bool:
        """
        广播消息到指定设备群组(不分片传输)
        :param msg: 消息正文
        :param msg_type: 消息类型
        :param from_device_no: 消息发送设备编号
        :param to_device_no_list: 消息接收设备编号列表
        :param to_addr_list: 接收地址列表, 列表元素为(ip, port)元组形式
        :return: 发送是否成功(True or False)
        """
        pass

    def send_p2p_big_msg(self, msg: bytes, msg_type: str, from_device_no: str, to_device_no: str,
                         to_addr: Tuple[str, int]) -> bool:
        """
        发送p2p消息到指定设备(分片传输)
        :param msg: 消息正文
        :param msg_type: 消息类型
        :param from_device_no: 消息发送设备编号
        :param to_device_no: 消息接收设备编号
        :param to_addr: 接收地址, (ip, port)元组形式
        :return: 发送是否成功(True or False)
        """
        pass

    def send_bc_big_msg(self, msg: bytes, msg_type: str, from_device_no: str, to_device_no_list: List[str],
                        to_addr_list: List[Tuple[str, int]]) -> bool:
        """
        广播消息到指定设备群组(分片传输)
        :param msg: 消息正文
        :param msg_type: 消息类型
        :param from_device_no: 消息发送设备编号
        :param to_device_no_list: 消息接收设备编号列表
        :param to_addr_list: 接收地址列表, 列表元素为(ip, port)元组形式
        :return: 发送是否成功(True or False)
        """
        pass

    def receive_p2p_msg(self, msg: dict) -> bool:
        """
        对收到的p2p消息(不分片传输)进行处理
        :return: 发送处理成功(True or False)
        """
        pass

    def receive_p2p_big_msg(self, msg: dict) -> bool:
        """
        对收到的p2p消息(分片传输)进行处理
        :param msg: 消息正文
        :return: 处理是否成功(True or False)
        """
