from __future__ import annotations

from typing import List


class BroadCastMessageInfo:
    def __init__(self):
        self.__udp_bc_msg = "UDP_BROADCAST_MESSAGE_INFO"
        self.__from_device_no = ""
        self.__from_device_ip = ""
        self.__from_device_port = -1
        self.__to_device_no_list = []
        self.__to_device_ip_list = []
        self.__to_device_port_list = []
        self.__msg = ""
        self.__msg_type = ""
        self.__is_slice = False
        self.__slice_no = -1
        self.__slice_len = 0

    def set_message_info(self, from_device_no: str, from_device_ip: str, from_device_port: int,
                         to_device_no_list: List[str], to_device_ip_list: List[str], to_device_port_list: List[int],
                         msg: dict | bytes, msg_type: str, slice_no: int, is_slice=False, slice_len=0):
        self.__udp_bc_msg = "UDP_BROADCAST_MESSAGE_INFO"
        self.__from_device_no = from_device_no
        self.__from_device_ip = from_device_ip
        self.__from_device_port = from_device_port
        self.__to_device_no_list = to_device_no_list
        self.__to_device_ip_list = to_device_ip_list
        self.__to_device_port_list = to_device_port_list
        self.__msg = msg
        self.__msg_type = msg_type
        self.__is_slice = is_slice
        self.__slice_no = slice_no
        self.__slice_len = slice_len

    def set_message_info_from_dict(self, message_info_dict: dict):
        self.__udp_bc_msg = "UDP_BROADCAST_MESSAGE_INFO"
        self.__from_device_no = message_info_dict['from_device_no']
        self.__from_device_ip = message_info_dict['from_device_ip']
        self.__from_device_port = message_info_dict['from_device_port']
        self.__to_device_no_list = message_info_dict['to_device_no_list']
        self.__to_device_ip_list = message_info_dict['to_device_ip_list']
        self.__to_device_port_list = message_info_dict['to_device_port_list']
        self.__msg = message_info_dict['msg']
        self.__msg_type = message_info_dict['msg_type']
        self.__is_slice = message_info_dict['is_slice']
        self.__slice_no = message_info_dict['slice_no']
        self.__slice_len = message_info_dict['slice_len']

    def get_message_by_dict(self) -> dict:
        message_info_dict = {'udp_bc_msg': "UDP_BROADCAST_MESSAGE_INFO",
                             'from_device_no': self.get_from_device_no(),
                             'from_device_ip': self.get_from_device_ip(),
                             'from_device_port': self.get_from_device_port(),
                             'to_device_no_list': self.get_to_device_no_list(),
                             'to_device_ip_list': self.get_to_device_ip_list(),
                             'to_device_port_list': self.get_to_device_port_list(),
                             'msg': self.get_msg(), 'msg_type': self.get_msg_type(), 'is_slice': self.get_is_slice(),
                             'slice_no': self.get_slice_no(), 'slice_len': self.get_slice_len()}
        return message_info_dict

    def set_from_device_no(self, from_device_no: str):
        self.__from_device_no = from_device_no

    def get_from_device_no(self) -> str:
        return self.__from_device_no

    def set_from_device_ip(self, from_device_ip: str):
        self.__from_device_ip = from_device_ip

    def get_from_device_ip(self) -> str:
        return self.__from_device_ip

    def set_from_device_port(self, from_device_port: int):
        self.__from_device_port = from_device_port

    def get_from_device_port(self) -> str:
        return self.__from_device_no

    def set_to_device_no_list(self, to_device_no_list: List[str]):
        self.__to_device_no_list = to_device_no_list

    def get_to_device_no_list(self) -> List[str]:
        return self.__to_device_no_list

    def set_to_device_ip_list(self, to_device_ip_list: List[str]):
        self.__to_device_ip_list = to_device_ip_list

    def get_to_device_ip_list(self) -> List[str]:
        return self.__to_device_ip_list

    def set_to_device_port_list(self, to_device_port_list: List[int]):
        self.__to_device_port_list = to_device_port_list

    def get_to_device_port_list(self) -> List[int]:
        return self.__to_device_port_list

    def set_msg(self, msg: dict | bytes):
        self.__msg = msg

    def get_msg(self):
        return self.__msg

    def set_msg_type(self, msg_type: str):
        self.__msg_type = msg_type

    def get_msg_type(self) -> str:
        return self.__msg_type

    def set_is_slice(self, is_slice: bool):
        self.__is_slice = is_slice

    def get_is_slice(self) -> bool:
        return self.__is_slice

    def set_slice_no(self, slice_no: str):
        self.__slice_no = slice_no

    def get_slice_no(self) -> int:
        return self.__slice_no

    def set_slice_len(self, slice_len: int):
        self.__slice_len = slice_len

    def get_slice_len(self) -> int:
        return self.__slice_len

    def __str__(self):
        return f"BroadCastMessageInfo(udp_bc_msg={self.__udp_bc_msg}, from_device_no={self.__from_device_no}, " \
               f"from_device_ip={self.__from_device_ip}, from_device_port={self.__from_device_port}, " \
               f"to_device_no_list={self.__to_device_no_list}, to_device_ip_list={self.__to_device_ip_list}, " \
               f"to_device_port_list={self.__to_device_port_list}, msg={self.__msg}, msg_type={self.__msg_type}, " \
               f"slice_no={self.__slice_no}, is_slice={self.__is_slice}, slice_no={self.__slice_no}," \
               f"slice_len={self.__slice_len})"
